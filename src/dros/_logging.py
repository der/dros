import logging
import os
import sys


class DrosLogger(logging.Logger):
    def __init__(self, name: str):
        level=getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper(), logging.INFO)
        super().__init__(name, level)
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.addHandler(handler)

logger = DrosLogger("dros")
