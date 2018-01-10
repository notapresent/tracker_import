import logging
import parsing
import itertools
from gevent.pool import Pool


logger = logging.getLogger(__name__)


FORUM_ENCODING = 'windows-1251'


class Scraper:
    def __init__(self, httpclient, url_builder, store):
        self._httpclient = httpclient
        self._urlbuilder = url_builder
        self._store = store

    def run(self):
        logger.info('Starting scrape')
        fids = self.forum_ids()
        fid_npages = list(self.forum_npages(fids))
        logger.info('%d pages in %d forums' % (sum(map(lambda t: t[1], fid_npages)), len(fid_npages)))
        forum_pages = ForumPages(fid_npages)
        self.scrape_all(forum_pages)
        logger.info('Scrape finished')

    def scrape_all(self, pagegen):
        urls = itertools.starmap(self._urlbuilder.page_url, pagegen)
        for req in self._httpclient.multiget(urls):
            resp = req.response
            if resp is None:
                logger.warn("%s - %s" % (req.url, req.exception))
                continue

            elif not resp.ok:
                logger.warn("%s - %s (%.3f s)" % (req.url, resp.reason, resp.elapsed.total_seconds()))
                continue

            resp.encoding = FORUM_ENCODING
            html = resp.text
            torrent_dicts = list(parsing.extract_torrents(html, req.url))
            logger.debug("%s - (%.3f s) - %d" % (req.url, resp.elapsed.total_seconds(), len(torrent_dicts)))
            self._store.put_all(torrent_dicts)

    def forum_ids(self):
        url = self._urlbuilder.map_url()
        maprequest = next(self._httpclient.multiget([url]))
        html = maprequest.response.text
        return parsing.extract_forum_ids(html, url)

    def forum_npages(self, fids):
        url2fid = {self._urlbuilder.page_url(fid): fid for fid in fids}
        for req in self._httpclient.multiget(url2fid.keys()):
            html = req.response.text
            npages = parsing.extract_num_pages(html, req.url)
            yield (url2fid[req.url], npages)


def all_pages(fid, num_pages):
    return zip(itertools.repeat(fid), range(1, num_pages + 1))


class ForumPages:
    def __init__(self, tuples):
        self.tuples = tuples

    def __len__(self):
        return sum([t[1] for t in self.tuples])

    def __iter__(self):
        return itertools.chain.from_iterable(itertools.starmap(all_pages, self.tuples))
