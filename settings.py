import os

# Required settings
DATABASE_URL = os.environ['DATABASE_URL']

FORUM_URL = os.environ['FORUM_URL']

# Optional settins
DEBUG = os.environ.get('DEBUG', False)
SECRET_KEY = os.environ.get('SECRET_KEY', 'Development key')
CONCURRENCY = os.environ.get('CONCURRENCY', 16)       # TODO tune this. Something between 8 and 32 should be fine
TORRENTS_PER_PAGE = os.environ.get('TORRENTS_PER_PAGE', 50)
