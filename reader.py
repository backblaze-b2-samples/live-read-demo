import argparse
import logging
import os
import sys

from common import init_logging, load_env_vars
from downloader import LiveReadDownloader

logging.basicConfig()

logger = logging.getLogger(__name__)

# Default to minimum part size
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024

# Limit the number of chunks we read into memory
DEFAULT_QUEUE_SIZE = 4


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Copy a Backblaze B2 Live Read file to a file or stdout')
    parser.add_argument('key', type=str, help='object key (filename) in B2')
    parser.add_argument('filename', type=str, nargs='?', help='local output file; omit for stdout')
    parser.add_argument('--poll-interval', type=int, required=False, default=1, help='poll interval')
    parser.add_argument('--chunk-size', type=int, required=False, default=DEFAULT_CHUNK_SIZE, help='chunk size')
    parser.add_argument('--no-wait', action='store_true', help='read file even if it is completed')
    parser.add_argument('--debug', action='store_true', help='debug logging')
    parser.add_argument('--debug-boto', action='store_true', help='debug logging for boto3')
    parser.add_argument('--queue-size', type=int, required=False, default=DEFAULT_QUEUE_SIZE, help='queue size')
    args = parser.parse_args()
    return args


def write_data_from_queue(filename: str, downloader: LiveReadDownloader):
    with sys.stdout.buffer if not filename else open(filename, 'wb') as f:
        while data := downloader.get_data():
            f.write(data)


def main() -> None:
    args = parse_command_line_args()

    init_logging(args, [__name__, 'downloader'])

    load_env_vars()

    downloader = LiveReadDownloader(os.environ['BUCKET_NAME'], args.key, args.poll_interval, args.chunk_size,
                                    args.queue_size, args.no_wait)
    downloader.start()

    write_data_from_queue(args.filename, downloader)

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
