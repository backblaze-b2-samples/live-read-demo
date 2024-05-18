import argparse
import logging
import os
import signal
import sys

from dotenv import load_dotenv

from uploader import LiveReadUploader, LiveReadCreate, LiveReadUpload, LiveReadComplete

logging.basicConfig()

logger = logging.getLogger(os.path.basename(__file__))

# See https://www.backblaze.com/docs/cloud-storage-large-files
MIN_CHUNK_SIZE = 5 * 1024 * 1024
MAX_CHUNK_SIZE = 5 * 1024 * 1024 * 1024
DEFAULT_CHUNK_SIZE = MIN_CHUNK_SIZE


def signal_handler(sig, _frame):
    """
    By default, Python would raise a KeyboardInterrupt on SIGINT and terminate the program on SIGTERM. We want to
    finish processing any buffered input from stdin, at which point read_stdin_stream will return, and we will
    complete the multipart upload. So, we don't need to do anything here other than log the fact that we caught
    the signal.
    """
    logger.info('Caught signal %s. Processing remaining data.', signal.Signals(sig).name)


def parse_command_line_args():
    parser = argparse.ArgumentParser(description='Copy stdin to a Backblaze B2 Live Read file')
    parser.add_argument('bucket', type=str, help='a bucket name')
    parser.add_argument('key', type=str, help='object key')
    parser.add_argument('--chunk_size', type=int, required=False, default=DEFAULT_CHUNK_SIZE, help='chunk size')
    parser.add_argument('--debug', action='store_true', help='debug logging')
    parser.add_argument('--debug-boto', action='store_true', help='debug logging for boto3')
    return parser.parse_args()


def read_stdin_to_queue(uploader: LiveReadUploader, chunk_size):
    uploader.put(LiveReadCreate())

    # Read stdin until there is no more data, sending parts to the uploader
    with sys.stdin as f:
        while True:
            buffer = f.buffer.read(chunk_size)
            if buffer == b'':
                # EOF or read() was interrupted and there is no data left
                break
            uploader.put(LiveReadUpload(buffer))

    uploader.put(LiveReadComplete())


# noinspection DuplicatedCode
def main():
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

    uploader = LiveReadUploader(args.bucket, args.key)
    uploader.start()

    read_stdin_to_queue(uploader, args.chunk_size)

    # Wait for the uploader to finish
    uploader.wait_until_complete()

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
