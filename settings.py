import os

# Required settings
DATABASE_URL = os.environ['DATABASE_URL']

FORUM_URL =  os.environ['FORUM_URL']
# FORUM_USER = os.environ['FORUM_USER']
# FORUM_PASSWORD = os.environ['FORUM_PASSWORD']

# Optional settins
DEBUG = os.environ.get('DEBUG', False)
SECRET_KEY = os.environ.get('SECRET_KEY', 'Development key')
CONCURRENCY = os.environ.get('CONCURRENCY', 16)       # TODO Tune this
TORRENTS_PER_PAGE = os.environ.get('TORRENTS_PER_PAGE', 50)
