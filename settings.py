import os
import logging

# Required settings
FORUM_URL = os.environ['FORUM_URL']
STORAGE_PATH = os.environ['STORAGE_PATH']

# Optional settings with sensible defaults
CONCURRENCY = int(os.environ.get('CONCURRENCY', 16))
LOG_LEVEL = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO'), logging.INFO)
HTML_ENCODING = os.environ.get('HTML_ENCODING')     # None means autodetect (slower)
STORAGE_CURLOPTS = os.environ.get('STORAGE_CURLOPTS', '')
STORAGE_BATCH_SIZE = int(os.environ.get('STORAGE_BATCH_SIZE', 5000))
