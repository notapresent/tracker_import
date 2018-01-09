import logging
import parsing
import itertools
import requests
import grequests
import storage
from gevent.pool import Pool



logger = logging.getLogger(__name__)


FORUM_ENCODING = 'windows-1251'
CONCURRENCY = 16


class Scraper:
    def __init__(self, httpclient, url_builder, settings):
        self._cfg = settings
        self._httpclient = httpclient
        self._urlbuilder = url_builder

    def run(self):
        logger.info('Starting scrape')
        fids = self.forum_ids()
        fid_npages = list(self.forum_npages(fids))
        logger.info('%d pages in %d forums' % (sum(map(lambda t: t[1], fid_npages)), len(fid_npages)))
        forum_pages = ForumPages(fid_npages)
        self.scrape_all(forum_pages)
        logger.info('Scrape finished')

    def scrape_all(self, pagegen):
        store = storage.JsonlStorage('./data', 10000)
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

            store.put_all(torrent_dicts)
            # for tdict in torrent_dicts:
            #     tdict['fid'] = 0                # TODO

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


class GRequestsHttpClient:
    def __init__(self, concurrency=CONCURRENCY):
        self._concurrency = concurrency
        self._session = make_session(concurrency)
        self._pool = Pool(concurrency)

    def multiget(self, urls):

        def send(r):
            return r.send()

        requests = map(lambda u: grequests.request('GET', u, session=self._session), urls)
        for request in self._pool.imap_unordered(send, requests):
            yield request

        self._pool.join()


def make_session(concurrency):
    session = grequests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=5,
        pool_maxsize=concurrency
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
