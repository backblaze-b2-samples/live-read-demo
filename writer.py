import argparse
import logging
import os
import signal
import sys
import time
from types import FrameType
from typing import Any, Callable

from dotenv import load_dotenv

from uploader import LiveReadUploader, LiveReadCreate, LiveReadUpload, LiveReadComplete

logging.basicConfig()

logger = logging.getLogger(os.path.basename(__file__))

# See https://www.backblaze.com/docs/cloud-storage-large-files
MIN_CHUNK_SIZE = 5 * 1024 * 1024
MAX_CHUNK_SIZE = 5 * 1024 * 1024 * 1024
DEFAULT_CHUNK_SIZE = MIN_CHUNK_SIZE
DEFAULT_INTERVAL = 0.5

shutdown_requested = False


def signal_handler(sig, _frame):
    global shutdown_requested

    """
    By default, Python would raise a KeyboardInterrupt on SIGINT and terminate the program on SIGTERM. We want to
    finish processing any buffered input from stdin, at which point read_stdin_stream will return, and we will
    complete the multipart upload. So, we don't need to do anything here other than log the fact that we caught
    the signal.
    """
    logger.info('Caught signal %s. Processing remaining data.', signal.Signals(sig).name)
    # Stop reading data in the main loop
    shutdown_requested = True


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Copy a file or stdin to a Backblaze B2 Live Read file')
    parser.add_argument('key', type=str, help='object key (filename) in B2')
    parser.add_argument('filename', type=str, nargs='?', help='local input file; omit for stdin')
    parser.add_argument('--chunk_size', type=int, required=False, default=DEFAULT_CHUNK_SIZE, help='chunk size')
    parser.add_argument('--debug', action='store_true', help='debug logging')
    parser.add_argument('--debug-boto', action='store_true', help='debug logging for boto3')
    parser.add_argument('--interval', type=float, required=False, default=DEFAULT_INTERVAL, help='polling interval')
    return parser.parse_args()


def read_data_to_queue(filename: str, uploader: LiveReadUploader, chunk_size, interval=0):
    uploader.put(LiveReadCreate())

    buffer: bytes = b''
    with open(filename, 'rb', buffering=chunk_size) if filename else sys.stdin.buffer as f:
        # Loop until app is stopped
        while not shutdown_requested:
            buffer += f.read(chunk_size - len(buffer))
            if not filename:
                # Reading stdin - stop at EOF/interruption
                if buffer == b'':
                    # EOF or read() was interrupted and there is no data left
                    break
            else:
                # Following a file - don't stop at EOF
                if len(buffer) < chunk_size:
                    # Sleep a little and keep reading until we have a chunk
                    time.sleep(interval)
                    continue

            uploader.put(LiveReadUpload(buffer))
            buffer = b''

        # Upload any remaining data
        if len(buffer) > 0:
            uploader.put(LiveReadUpload(buffer))

    uploader.put(LiveReadComplete())


def main() -> None:
    args = parse_command_line_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('uploader').setLevel(logging.DEBUG)

    if args.debug_boto:
        logging.getLogger('botocore').setLevel(logging.DEBUG)

    logger.debug("Command-line arguments: %s", args)

    if load_dotenv():
        logger.debug("Loaded environment variables from .env")
    else:
        logger.warning("No environment variables in .env")

    # # Install handler to override the KeyboardInterrupt on SIGINT or SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    uploader = LiveReadUploader(os.environ['BUCKET_NAME'], args.key)
    uploader.start()

    read_data_to_queue(args.filename, uploader, args.chunk_size, args.interval)

    # Wait for the uploader to finish
    uploader.wait_until_complete()

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
