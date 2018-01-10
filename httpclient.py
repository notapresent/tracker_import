import requests
import grequests


class GrequestsHttpClient:
    def __init__(self, pool, max_conns=None):
        self._pool = pool
        http_max_pool_size = max_conns if max_conns is not None else pool.size
        self._session = make_session(http_max_pool_size)

    def multiget(self, urls):

        def send(r):
            return r.send()

        reqs = map(lambda u: grequests.request('GET', u, session=self._session), urls)
        for request in self._pool.imap_unordered(send, reqs):
            yield request

        # self._pool.join()


def make_session(http_max_pool_size):
    session = grequests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=5,
        pool_maxsize=http_max_pool_size
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
