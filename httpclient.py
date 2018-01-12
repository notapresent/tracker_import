from requests import adapters
import grequests


class GrequestsHttpClient:
    DEFAULT_RETRIES = 5

    def __init__(self, pool, encoding=None, max_conns=None, num_retries=DEFAULT_RETRIES):
        self._pool = pool
        self._encoding = encoding
        self._http_max_pool_size = pool.size if max_conns is None else max_conns
        self._num_retries = num_retries
        self._session = make_session(self._http_max_pool_size, self._num_retries)

    def multisend(self, requests):
        for request in self._pool.imap_unordered(send, requests, maxsize=self._http_max_pool_size):
            response = request.response

            if response is None:
                raise HttpError("Failed to fetch %s" % request.url) from request.exception

            elif not response.ok:
                raise HttpError("Failed to fetch %s : %s" % (request.url, response.reason))

            if self._encoding is not None:
                response.encoding = self._encoding
            else:
                response.encoding = response.apparent_encoding

            extra = getattr(request, '_extra', None)
            if extra is not None:
                response._extra = extra

            yield response

    def make_request(self, url, method='GET', **kwargs):
        extra = kwargs.pop('extra', None)
        request = grequests.request(method, url, session=self._session, **kwargs)
        if extra is not None:
            request._extra = extra
        return request

    def __repr__(self):
        return "<GrequestsHttpClient max_conns=%d num_retries=%d>" % (self._http_max_pool_size, self._num_retries)


class HttpError(Exception):
    pass


def make_session(http_max_pool_size, num_retries):
    session = grequests.Session()
    adapter = adapters.HTTPAdapter(
        max_retries=num_retries,
        pool_maxsize=http_max_pool_size,
        pool_block=True
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def send(r):
    return r.send()
