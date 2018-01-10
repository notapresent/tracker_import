import os
import logging

# Required settings
FORUM_URL = os.environ['FORUM_URL']

CONCURRENCY = 16

# Optional settings with sensible defaults
LOG_LEVEL = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO'), logging.INFO)
