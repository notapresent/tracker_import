import logging
import parsing
import itertools
import storage


logger = logging.getLogger(__name__)


class Scraper:
    def __init__(self, httpclient, dbconn, settings):
        self._dbconn = dbconn
        self._cfg = settings
        self._httpclient = httpclient

    def run(self):
        logger.info('Starting scrape')
        fids = self.forum_ids()
        fid_npages = list(self.forum_npages(fids))
        logger.info('%d pages in %d forums' % (sum(map(lambda t: t[1], fid_npages)), len(fid_npages)))
        pagegen = page_gen(fid_npages)
        self.scrape_all(pagegen)

        logger.info('Scrape finished')

    def scrape_all(self, pagegen):
        urls = itertools.starmap(self.page_link, pagegen)
        buf = storage.InsertBuffer(self._dbconn, storage.Torrent, 1000)
        with buf:
            for req in self._httpclient.multiget(urls):
                resp = req.response
                if resp is None:
                    logger.warn("%s - %s" % (req.url, req.exception))
                elif not resp.ok:
                    logger.warn("%s - %s (%.3f s)" % (req.url, resp.reason, resp.elapsed.total_seconds()))
                else:
                    # logger.debug("%s - %s (%.3f s)" % (req.url, resp.reason, resp.elapsed.total_seconds()))
                    resp.encoding = 'windows-1251'
                    html = resp.text
                    torrent_dicts = parsing.extract_torrents(html, req.url)

                    for tdict in torrent_dicts:
                        tdict['fid'] = 0                # TODO
                        buf.add(tdict)

    def forum_ids(self):
        url = self._cfg.FORUM_URL
        maprequest = next(self._httpclient.multiget([url]))
        html = maprequest.response.text
        return parsing.extract_forum_ids(html, url)

    def forum_npages(self, fids):
        url2fid = {self.page_link(fid): fid for fid in fids}
        for req in self._httpclient.multiget(url2fid.keys()):
            html = req.response.text
            npages = parsing.extract_num_pages(html, req.url)
            yield (url2fid[req.url], npages)

    def page_link(self, fid, page=1):
        offset = self._cfg.TORRENTS_PER_PAGE * (page - 1)
        if offset:
            return "%sviewforum.php?f=%d&start=%d" % (self._cfg.FORUM_URL, fid, offset)
        else:
            return "%sviewforum.php?f=%d" % (self._cfg.FORUM_URL, fid)


def page_gen(tuples):     # [(fid, npages), ...] -> [(fid, page1), (fid, page2), ...]

    def explode_tuple(t):
        return zip(itertools.repeat(t[0]), range(1, t[1] + 1))

    return itertools.chain.from_iterable(map(explode_tuple, tuples))
