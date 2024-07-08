import argparse
import logging
from typing import List

from dotenv import load_dotenv

logging.basicConfig()

logger = logging.getLogger(__name__)


def init_logging(args: argparse.Namespace, module_names: List[str]):
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for module_name in module_names:
            logging.getLogger(module_name).setLevel(logging.DEBUG)
    if args.debug_boto:
        logging.getLogger('botocore').setLevel(logging.DEBUG)
    logger.debug("Command-line arguments: %s", args)


def load_env_vars() -> None:
    if load_dotenv():
        logger.debug("Loaded environment variables from .env")
    else:
        logger.warning("No environment variables in .env")
