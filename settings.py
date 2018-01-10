import os
import logging

# Required settings
FORUM_URL = os.environ['FORUM_URL']
CONCURRENCY = 16
STORAGE_URL = os.environ['STORAGE_URL']

# Optional settings with sensible defaults
LOG_LEVEL = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO'), logging.INFO)
HTML_ENCODING = os.environ.get('HTML_ENCODING')     # None means autodetect (slower)
STORAGE_CURLOPTS = os.environ.get('STORAGE_CURLOPTS', '')
STORAGE_BATCH_SIZE = int(os.environ.get('STORAGE_BATCH_SIZE', 5000))
