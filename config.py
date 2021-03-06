import sys
import logging
import logging.config

RESTART_ALL = False

BASE_URL = "http://esango.un.org/civilsociety/"
DB_FILE_ORG = 'organizations.json'
DB_FILE_ORG_LIST = 'org_ids.json'
LIST_FILE = 'search_results.html'

LOG_FILE = "icso.log"

wait_time = range(2, 5)
wait_mu = 2.5
wait_sd = 0.5
long_wait_gap = 100
long_wait_mu = 15
long_wait_sd = 3

user_agents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.1.17 (KHTML, like Gecko) Version/7.1 Safari/537.85.10',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36'
]


# ------------
# Set up log
# ------------
# http://victorlin.me/posts/2012/08/26/good-logging-practice-in-python
LOG_SETTINGS = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(levelname)-8s %(name)-8s %(funcName)-20s %(asctime)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'INFO',
            'formatter': 'standard',
            "class": "logging.FileHandler",
            'filename': LOG_FILE,
            'mode': 'a',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        '': {
            'handlers': ['file', 'console'],
            'level': 'DEBUG',  # Max level overall
            'propagate': True
        }
    }
}

# Function to redirect exceptions to the log
# Via http://stackoverflow.com/a/16993115/120898
# Assign to sys.excepthook like so:
#   sys.excepthook = handle_exception
def handle_exception(exc_type, exc_value, exc_traceback):
    # Ignore KeyboardInterrupt so a console Python program can exit with Ctrl + C
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logging.error("Uncaught exception",
                  exc_info=(exc_type, exc_value, exc_traceback))

# Load log configuration
logging.config.dictConfig(LOG_SETTINGS)

# Requests is too verbose. Turn the level down to WARNING.
logging.getLogger("requests").setLevel(logging.WARNING)

# Send exceptions to the log too
sys.excepthook = handle_exception

# Start the log
logger = logging.getLogger(__name__)
logger.info("Configuration file loaded. Ready to start.")
