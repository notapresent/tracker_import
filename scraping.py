import logging
import parsing
import itertools
from gevent.pool import Pool


logger = logging.getLogger(__name__)


class Scraper:
    def __init__(self, httpclient, url_builder, store, encoding=None):
        self._httpclient = httpclient
        self._urlbuilder = url_builder
        self._encoding = encoding
        self._store = store

    def run(self):
        logger.info('Starting scrape with %s, %s, %s' % (self._httpclient, self._urlbuilder, self._store))
        fids_npages = list(self.forum_npages(self.forum_ids()))
        forum_pages = ForumPages(fids_npages)
        logger.info('%d pages in %d forums. Encoding is %s' % (forum_pages.size(), len(fids_npages), self._encoding))

        with self._store:
            self.scrape_all(forum_pages)
        logger.info('Scrape finished')

    def scrape_all(self, pagegen):
        urls = itertools.starmap(self._urlbuilder.page_url, pagegen)
        for resp in self._httpclient.multifetch(urls):
            resp.encoding = self._encoding
            torrent_dicts = list(parsing.extract_torrents(resp.text, resp.url))
            logger.debug("%s - (%.3f s) - %d" % (resp.url, resp.elapsed.total_seconds(), len(torrent_dicts)))
            self._store.put_all(torrent_dicts)

    def forum_ids(self):
        url = self._urlbuilder.map_url()
        resp = next(self._httpclient.multifetch([url]))
        if self._encoding is None:
            self._encoding = resp.apparent_encoding
            logger.debug('Autodetected html encoding as %s' % self._encoding)
        resp.encoding = self._encoding
        return parsing.extract_forum_ids(resp.text, url)

    def forum_npages(self, fids):
        url2fid = {self._urlbuilder.page_url(fid): fid for fid in fids}
        for resp in self._httpclient.multifetch(url2fid.keys()):
            npages = parsing.extract_num_pages(resp.text, resp.url)
            yield (url2fid[resp.url], npages)


def all_pages(fid, num_pages):
    return zip(itertools.repeat(fid), range(1, num_pages + 1))


class ForumPages:
    def __init__(self, tuples):
        self.tuples = tuples

    def size(self):
        return sum([t[1] for t in self.tuples])

    def __iter__(self):
        return itertools.chain.from_iterable(itertools.starmap(all_pages, self.tuples))
