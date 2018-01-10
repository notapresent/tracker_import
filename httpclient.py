import requests
import grequests


class GrequestsHttpClient:
    def __init__(self, pool, max_conns=None):
        self._pool = pool
        http_max_pool_size = max_conns if max_conns is not None else pool.size
        self._session = make_session(http_max_pool_size)

    def multifetch(self, urls):
        reqs = map(lambda u: grequests.request('GET', u, session=self._session), urls)
        for request in self._pool.imap_unordered(lambda r: r.send(), reqs):
            response = request.response

            if response is None:
                raise HttpError("Failed to fetch %s" % request.url) from request.exception
                continue

            elif not response.ok:
                raise HttpError("Failed to fetch %s : %s" % (request.url, response.reason))
                continue

            yield response


class HttpError(Exception):
    pass


def make_session(http_max_pool_size):
    session = grequests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=5,
        pool_maxsize=http_max_pool_size
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
