import logging
import os
import glob
import ujson as json
from gevent import Greenlet, subprocess
from perfstats import Timer


logger = logging.getLogger(__name__)


class JsonlStorage:

    SUFFIX = 'jsonl'

    def __init__(self, dirpath, chunk_size, file_done_handler=None):
        self._seq = 0
        self._nitems = 0
        self._chunk_size = chunk_size
        self._dirpath = os.path.realpath(dirpath)
        self._fp = None
        self.file_done_handler = file_done_handler
        self.ensure_dir()

    def put_all(self, items):
        for item in items:
            self.put(item)

    def put(self, record):
        json.dump(record, self._fp, ensure_ascii=False)
        self._fp.write("\n")
        self._nitems += 1
        if self._nitems == self._chunk_size:
            self.next_chunk()
            self._nitems = 0

    def ensure_dir(self):
        if not os.path.exists(self._dirpath):
            os.makedirs(self._dirpath, exist_ok=True)
        assert os.access(self._dirpath, os.W_OK)

    def purge(self):
        files = glob.glob("%s/*.%s" % (self._dirpath, self.SUFFIX))
        for fn in files:
            os.unlink(fn)
        logger.info("Purge completed, %s files removed" % len(files))

    def next_chunk(self):
        self.closefile()
        self._seq += 1
        self.openfile()

    def openfile(self):
        filename = self.filename()
        logger.debug("Opening %s" % filename)
        self._fp = open(filename, 'w')

    def closefile(self):
        self._fp.flush()
        self._fp.close()
        if self.file_done_handler is not None:
            self.file_done_handler(self.filename())

    def __enter__(self):
        self.openfile()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.closefile()

    def filename(self):
        return "%s/%d.%s" % (self._dirpath, self._seq, self.SUFFIX)

    def __repr__(self):
        return "<JsonlStorage dirpath=%s chunk_size=%d>" % (self._dirpath, self._chunk_size)


class WebdavWrapper:
    def __init__(self, store, pool, url, curl_options=''):
        self._store = store
        self._store.file_done_handler = self.file_done
        self._pool = pool
        self._url = url
        self._curl_options = curl_options

    def put_all(self, records):
        self._store.put_all(records)

    def put(self, record):
        self._store.put(record)

    def file_done(self, filename):
        self.start_upload(filename)

    def start_upload(self, filename):
        gr = Greenlet(self.do_upload, filename)
        gr.link_value(self.upload_done)
        self._pool.start(gr)

    def do_upload(self, filename):
        cmd = self.upload_cmd(filename)
        with Timer() as t:
            logger.debug('Uploading %s to %s' % (filename, cmd[-1]))
            try:
                subprocess.check_call(cmd, stdout=open(os.devnull, 'wb'))
            except subprocess.CalledProcessError as e:
                logger.exception(e)
                raise
        return t, filename

    def upload_done(self, greenlet):
        timer, filename = greenlet.get()
        os.unlink(filename)
        logger.debug("Uploaded %s in %.3f" % (filename, timer.elapsed))

    def upload_cmd(self, filename):
        file_url = '%s/%s' % (self._url, os.path.basename(filename))
        return ['curl', '-s', self._curl_options, ("-T%s" % filename), file_url]

    def __enter__(self):
        self._store.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._store.__exit__(exc_type, exc_value, exc_tb)

    def __repr__(self):
        return "<WebdavWrapper url=%s wrapping %r>" % (self._url, self._store)
