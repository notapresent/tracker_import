import logging
import perfstats
import os
import glob
import ujson as json


logger = logging.getLogger(__name__)


class JsonlStorage:

    SUFFIX = 'jsonl'

    def __init__(self, dirpath, chunk_size):
        self._seq = 0
        self._nitems = 0
        self._chunk_size = chunk_size
        self._dirpath = dirpath
        self._fp = None
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
        for fn in glob.glob("%s/*.%s" % (self._dirpath, self.SUFFIX)):
            os.unlink(fn)

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

    def __enter__(self):
        self.openfile()
        return self

    def __exit__(self, type, value, tb):
        self.closefile()

    def filename(self):
        return "%s/%d%s" % (self._dirpath, self._seq, self.SUFFIX)

