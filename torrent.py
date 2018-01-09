from collections import namedtuple
from enum import Enum


Torrent = namedtuple('Torrent', 'tid status fid asize title body')
# mtime, aid # TODO

class TorrentStatus(Enum):
    OK = 1
    PREMOD = 2
    DOWNLOAD_ERROR = -1
    PARSE_ERROR = -2

