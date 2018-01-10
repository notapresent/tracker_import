import logging
import urlbuilder
import storage
import scraping
import httpclient
from gevent.pool import Pool


logger = logging.getLogger(__name__)


class App:
    def __init__(self, settings):
        self.settings = settings
        init_logging(self.settings.LOG_LEVEL)
        self._storage = None
        self._pool = Pool(self.settings.CONCURRENCY)

    def run(self):
        httpc = httpclient.GrequestsHttpClient(self._pool)
        url_builder = urlbuilder.URLBuilder(self.settings.FORUM_URL)
        scraper = scraping.Scraper(httpc, url_builder, self.storage)
        scraper.run()
        self._pool.join()

    def purge(self):
        self.storage.purge()

    @property
    def storage(self):
        if self._storage is None:
            self._storage = storage.JsonlStorage('./data', 10000)
        return self._storage


def init_logging(level=logging.INFO):
    """Set up logging parameters"""
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s %(message)s', level=level)

    for pkgname in ('urllib3', 'chardet'):
        logging.getLogger(pkgname).setLevel(logging.WARN)
