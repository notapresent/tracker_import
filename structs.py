from enum import Enum
from itertools import repeat

class Forum:
    def __init__(self, id, title, num_pages=None):
        self.id = id
        self.title = title
        self.num_pages = num_pages

    def __repr__(self):
        return "<Forum id=%d pages=%s title='%s'>" % (self.id, self.num_pages, self.title)


class TorrentStatus(Enum):
    NEW = 0
    OK = 1
    PREMOD = 2
    DOWNLOAD_ERROR = -1
    PARSE_ERROR = -2


class Torrent:
    def __init__(self, id, title, status=TorrentStatus.NEW, **kwargs):
        self.id = id
        self.title = title
        self.status = status
        self.asize = kwargs.pop('asize', None)
        self.forum_id = kwargs.pop('forum_id', None)
        self.body = kwargs.pop('body', None)

    def __repr__(self):
        values = (self.id, self.forum_id, self.status.name, self.asize, self.title)
        return "<Torrent id=%d forum_id=%s status=%s asize=%d title='%s'>" % values

    def to_dict(self):
        return {
            'id': self.id,
            'status': self.status.name,
            'forum_id': self.forum_id,
            'asize': self.asize,
            'title': self.title,
            'body': self.body
        }