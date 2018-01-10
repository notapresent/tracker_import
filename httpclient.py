import requests
import grequests


class GrequestsHttpClient:
    DEFAULT_RETRIES = 5

    def __init__(self, pool, max_conns=None, num_retries=DEFAULT_RETRIES):
        self._pool = pool
        self._http_max_pool_size = pool.size if max_conns is None else max_conns
        self._num_retries = num_retries
        self._session = make_session(self._http_max_pool_size, self._num_retries)

    def multifetch(self, urls):
        reqs = map(lambda u: grequests.request('GET', u, session=self._session), urls)
        for request in self._pool.imap_unordered(lambda r: r.send(), reqs):
            response = request.response

            if response is None:
                raise HttpError("Failed to fetch %s" % request.url) from request.exception

            elif not response.ok:
                raise HttpError("Failed to fetch %s : %s" % (request.url, response.reason))

            yield response

    def __repr__(self):
        return "<GrequestsHttpClient max_conns=%d num_retries=%d>" % (self._http_max_pool_size, self._num_retries)


class HttpError(Exception):
    pass


def make_session(http_max_pool_size, num_retries):
    session = grequests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=num_retries,
        pool_maxsize=http_max_pool_size
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
