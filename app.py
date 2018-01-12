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
        self._localstore = None
        self._pool = Pool(self.settings.CONCURRENCY)
        self._scraper = None

    def start(self):
        httpc = httpclient.GrequestsHttpClient(self._pool, encoding=self.settings.HTML_ENCODING)
        url_builder = urlbuilder.URLBuilder(self.settings.FORUM_URL)
        self._scraper = scraping.ForumScraper(httpc, url_builder, self.localstore)
        self._scraper.run()
        self._pool.join()

    def purge(self):
        self.localstore.purge()

    @property
    def localstore(self):
        if self._localstore is None:
            self._localstore = storage.JsonlStorage(
                self.settings.STORAGE_PATH,
                self.settings.STORAGE_BATCH_SIZE)
        return self._localstore


def init_logging(level=logging.INFO):
    """Set up logging parameters"""
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s %(message)s', level=level)
    logging.getLogger('urllib3').setLevel(logging.WARN)
