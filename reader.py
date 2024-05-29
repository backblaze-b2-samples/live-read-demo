import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from downloader import LiveReadDownloader

logging.basicConfig()

logger = logging.getLogger(os.path.basename(__file__))

# Default to minimum part size
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024

# Limit the number of chunks we read into memory
DEFAULT_QUEUE_SIZE = 4


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Copy stdin to a Backblaze B2 Live Read file')
    parser.add_argument('key', type=str, help='object key (filename)')
    parser.add_argument('--poll-interval', type=int, required=False, default=1, help='poll interval')
    parser.add_argument('--chunk-size', type=int, required=False, default=DEFAULT_CHUNK_SIZE, help='chunk size')
    parser.add_argument('--debug', action='store_true', help='debug logging')
    parser.add_argument('--debug-boto', action='store_true', help='debug logging for boto3')
    parser.add_argument('--queue-size', type=int, required=False, default=DEFAULT_QUEUE_SIZE, help='queue size')
    args = parser.parse_args()
    return args


# noinspection DuplicatedCode
def main():
    args = parse_command_line_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('downloader').setLevel(logging.DEBUG)

    if args.debug_boto:
        logging.getLogger('botocore').setLevel(logging.DEBUG)

    logger.debug("Command-line arguments: %s", args)

    if load_dotenv():
        logger.debug("Loaded environment variables from .env")
    else:
        logger.warning("No environment variables in .env")

    downloader = LiveReadDownloader(os.environ['BUCKET_NAME'], args.key, args.poll_interval, args.chunk_size, args.queue_size)
    downloader.start()

    logger.debug('###')

    while data := downloader.get_data():
        sys.stdout.buffer.write(data)

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
