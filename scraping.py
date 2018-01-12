import logging
from itertools import starmap, chain, repeat

import parsing
from httpclient import HttpError
from perfstats import StopWatch


logger = logging.getLogger(__name__)


class ForumScraper:
    def __init__(self, httpclient, url_builder, store):
        self._httpclient = httpclient
        self._urlbuilder = url_builder
        self._store = store
        self._forums = None
        logger.info('Scraper created with %s, %s, %s' % (self._httpclient, self._urlbuilder, self._store))

    @property
    def forum_pageiter(self):
        if self._forums is None:
            forum_seq = self.forum_seq(self.raw_forum_seq())
            self._forums = ForumPageIterator(forum_seq)
            logger.info("Initialized %r" % self._forums)
        return self._forums

    def forum_seq(self, raw_forums):
        reqs = map(self.forum_page_request, raw_forums)
        for resp in self._httpclient.multisend(reqs):
            forum, _ = resp._extra
            forum.num_pages = parsing.extract_num_pages(resp.text, resp.url)
            yield forum

    def forum_page_request(self, forum, page=1):
        url = self._urlbuilder.page_url(forum.id, page)
        return self._httpclient.make_request(url, extra=(forum, page))

    def raw_forum_seq(self):
        req = self._httpclient.make_request(self._urlbuilder.map_url())
        resp = next(self._httpclient.multisend([req]))
        return parsing.extract_forums(resp.text, resp.url)

    def raw_torrents(self):
        page_requests = starmap(self.forum_page_request, self.forum_pageiter)
        for resp in self._httpclient.multisend(page_requests):
            forum, page = resp._extra
            sw = StopWatch("P#%d_%d" % (forum.id, page))
            sw.register_segment("F", resp.elapsed.total_seconds())
            with sw.register("P"):
                torrents = parsing.extract_torrents(resp.text, resp.url)

            num_torrents = 0
            for torrent in torrents:
                torrent.forum_id = forum.id
                yield torrent
                num_torrents += 1

            logger.info("PERF %s %d" % (sw, num_torrents))

    def topic_request(self, torrent):
        url = self._urlbuilder.topic_url(torrent.id)
        return self._httpclient.make_request(url, extra=torrent)

    def torrents(self):
        reqs = map(self.topic_request, self.raw_torrents())
        for resp in self._httpclient.multisend(reqs):
            torrent = resp._extra
            sw = StopWatch("T#%d" % torrent.id)
            sw.register_segment("F", resp.elapsed.total_seconds())
            with sw.register("P"):
                bs_dict = parsing.extract_body(resp.text, resp.url)
            torrent.body = bs_dict['body']
            torrent.status = bs_dict['status']
            yield torrent
            logger.info("PERF %s %d" % (sw, len(torrent.body) if torrent.body else 0))

    def run(self):
        logger.info("Scrape started")
        total = 0
        with self._store:
            for torrent in self.torrents():
                self._store.put(torrent.to_dict())
                total += 1
        logger.info("Scrape completed, %d torrents saved" % total)


class ForumPageIterator:
    def __init__(self, forum_seq):
        self.forums = list(forum_seq)

    def __len__(self):
        return sum([f.num_pages for f in self.forums])

    def page_tuples(self, forum):
        return zip(repeat(forum), range(1, forum.num_pages + 1))

    def __iter__(self):
        return chain.from_iterable(map(self.page_tuples, self.forums))

    def __repr__(self):
        return "<ForumPageIterator pages=%d forums=%d>" % (self.__len__(), len(self.forums))
