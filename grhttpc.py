import requests
import grequests
from gevent.pool import Pool


class GRequestsHttpClient:
    def __init__(self, concurrency):
        self._concurrency = concurrency
        self._session = grequests.Session()
        adapter = requests.adapters.HTTPAdapter(
            max_retries=5,
            pool_maxsize=concurrency
        )
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        self._pool = Pool(concurrency)

    def multiget(self, urls):

        def send(r):
            return r.send()

        requests = map(lambda u: grequests.request('GET', u, session=self._session), urls)
        for request in self._pool.imap_unordered(send, requests):
            yield request

        self._pool.join()
