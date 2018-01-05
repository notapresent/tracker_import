import logging
from peewee import Model, PrimaryKeyField, IntegerField, SmallIntegerField, CharField, Proxy
#, DateTimeField, ForeignKeyField, BigIntegerField
import gevent
from playhouse import db_url
import perfstats


logger = logging.getLogger(__name__)
_db = Proxy()


def init(database_url):
    """Initialize database connection with parameters from url"""
    real_db = db_url.connect(database_url)
    _db.initialize(real_db)
    return _db


def close():
    _db.close()


def get_db():
    return _db


class BaseModel(Model):
    """Base class for DB models"""
    class Meta:
        database = _db


class Torrent(BaseModel):
    """Represents torrent"""
    tid = IntegerField(primary_key=True)
    fid = SmallIntegerField()
    asize = IntegerField()   # approximate size, Kb
    title = CharField(max_length=512)


def all_models():
    """Returns list of all model classes"""
    return [Torrent]


def create_tables():
    """Create database tables for models"""
    _db.create_tables(all_models(), safe=True)
    logger.info('Tables created: %r' % all_models())


def drop_tables():
    """Drop database tables for models"""
    _db.drop_tables(all_models(), cascade=False, safe=True)
    logger.info('Tables dropped: %r' % all_models())


class InsertBuffer:
    """Accumulate db records and insert them in batches"""
    def __init__(self, db, model, batch_size=1000):
        self._batch_size = batch_size
        self._db = db
        self._model = model
        self._buf = []

    def add(self, obj):
        """Add one record to buffer"""
        self._buf.append(obj)

        if len(self._buf) >= self._batch_size:
            self._insert_batch()

    def _insert_batch(self):
        """Remove one batch from buffer and insert to database"""
        chunk = self._chunk()
        with perfstats.Timer() as t:
            self._do_insert(chunk)
        logger.info('Inserted %d rows in %.3f sec.' % (len(chunk), t.elapsed))

    def _do_insert(self, chunk):
        try:
            if self._db.is_closed():
                self._db.connect()
            with self._db.transaction():
                self._model.insert_many(chunk).upsert(upsert=True).execute()
        finally:
            self._db.close()

    def _chunk(self):
        chunk, self._buf = split_chunk(self._buf, self._batch_size)
        return chunk

    def flush(self):
        """Insert all records from buffer to DB"""
        while self._buf:
            self._insert_batch()

    def __len__(self):
        return len(self._buf)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()


class GeventInsertBuffer(InsertBuffer):
    def __init__(self, db, model, batch_size):
        super().__init__(db, model, batch_size)
        self._greenlets = set()

    def _insert_batch(self):
        """Remove one batch from buffer and insert to database"""
        chunk = self._chunk()
        t = perfstats.Timer()
        t.start()
        greenlet = gevent.Greenlet(self._do_insert, chunk)
        greenlet._timer = t
        greenlet._chunk_length = len(chunk)
        self._greenlets.add(greenlet)
        greenlet.link(self._greenlet_done)
        greenlet.start()

    def _greenlet_done(self, g):
        t = g._timer
        t.stop()
        self._greenlets.remove(g)
        logger.info('Inserted %d rows in %.3f sec. %d greenlets to do, Buf: %d' % (g._chunk_length, t.elapsed, len(self._greenlets), len(self._buf)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        gevent.joinall(self._greenlets, raise_error=False)


def split_chunk(lst, size):
    """Cut a chunk from a list. Returns (chunk, remaining_list)"""
    return lst[:size], lst[size:]


class DBContext:
    def __init__(self, db):
        self._db = db

    def __enter__(self):
        if self._db.is_closed():
            self._db.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.close()
