import logging
from peewee import Model, PrimaryKeyField, IntegerField, SmallIntegerField, CharField, Proxy
#, DateTimeField, ForeignKeyField, BigIntegerField
import gevent
import gevent.hub
from playhouse import db_url
import perfstats


logger = logging.getLogger(__name__)
# _db = PostgresqlExtDatabase(None, register_hstore=False)     # Uninitialized db connection
_db = Proxy()


# def gevent_waiter(fd, hub=gevent.hub.get_hub()):
#     hub.wait(hub.loop.io(fd, 1))


class BaseModel(Model):
    """Base class for DB models"""
    class Meta:
        database = _db

class Torrent(BaseModel):
    """Represents torrent"""
    tid = PrimaryKeyField()
    fid = SmallIntegerField()
    asize = IntegerField()   # approximate size, Kb
    title = CharField(max_length=512)


def all_models():
    """Returns list of all model classes"""
    return [Torrent]


def connect(database_url):
    """Initialize database connection with parameters from url"""
    if database_url.startswith('mysql'):
        real_db = db_url.connect(database_url)  # , waiter=gevent_waiter
    else:
        real_db = db_url.connect(database_url)

    _db.initialize(real_db)


def get_db():
    return _db


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
        chunk, self._buf = split_chunk(self._buf, self._batch_size)

        with perfstats.Timer() as t:
            self._do_insert(chunk)
            # g = gevent.spawn(self._do_insert, chunk)
            # gevent.wait([g], 5)

        logger.info('Inserted %d rows in %.3f sec. %d rows remaining' % (len(chunk), t.elapsed, len(self._buf)))

    def _do_insert(self, chunk):
        with self._db.transaction():
            # query = self._model.insert_many(chunk).upsert(upsert=True).execute()
            # query = self._model.insert_many(chunk).on_conflict('ignore').execute()
            query = self._model.insert_many(chunk).upsert(upsert=True)
            print(query)
            query.execute()

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


def split_chunk(lst, size):
    """Cut a chunk from a list. Returns (chunk, remaining_list)"""
    return lst[:size], lst[size:]
