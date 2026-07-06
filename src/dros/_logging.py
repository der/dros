import logging
import sys

logger = logging.getLogger("dros")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stderr))
