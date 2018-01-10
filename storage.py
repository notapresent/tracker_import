import logging
import perfstats
import os
import glob
import ujson as json


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
        if self._fp is None:
            self.openfile()
        for item in items:
            json.dump(item, self._fp, ensure_ascii=False)
            self._fp.write("\n")
            self._nitems += 1
            if self._nitems == self._chunk_size:
                self.next_chunk()

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
        self._nitems = 0
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

    def __exit__(self, type, value, tb):
        self.closefile()

    def filename(self):
        return "%s/%d.%s" % (self._dirpath, self._seq, self.SUFFIX)


class WebdavWrapper:
    def __init__(self, store, pool, url, curl_options=''):
        self._store = store
        self._store.file_done_handler = self.file_done
        self._pool = pool
        self._url = url
        self._curl_options = curl_options

    def put_all(self, records):
        self._store.put_all(records)

    def file_done(self, filename):
        self.upload_file(filename)
        # os.unlink(filename)   # TODO

    def upload_file(self, filename):
        file_url = '%s/%s' % (self._url, os.path.basename(filename))
        cmd = 'curl %s -T %s %s' % (self._curl_options, filename, file_url)
        logger.info("Executing %s" % cmd)
