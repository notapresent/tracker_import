class URLBuilder:
    TORRENTS_PER_PAGE = 50

    def __init__(self, base_url, per_page=TORRENTS_PER_PAGE):
        self.base_url = base_url
        self.per_page = per_page

    def map_url(self):
        return self.base_url

    def page_url(self, forum_id, page_no=1):
        offset = self.per_page * (page_no - 1)
        if offset:
            return "%sviewforum.php?f=%d&start=%d" % (self.base_url, forum_id, offset)
        else:
            return "%sviewforum.php?f=%d" % (self.base_url, forum_id)

    def topic_url(self, topic_id):
        return "%sviewtopic.php?t=%d" % (self.base_url, topic_id)
