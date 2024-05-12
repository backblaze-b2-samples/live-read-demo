import argparse
import logging
import os
import signal
import sys

import boto3
from dotenv import load_dotenv

logging.basicConfig()

logger = logging.getLogger(os.path.basename(__file__))

# Stdin processing with signal handling from https://stackoverflow.com/a/67195451/33905

# See https://www.backblaze.com/docs/cloud-storage-large-files
MIN_CHUNK_SIZE = 5 * 1024 * 1024
MAX_CHUNK_SIZE = 5 * 1024 * 1024 * 1024
DEFAULT_CHUNK_SIZE = MIN_CHUNK_SIZE

args = None
b2_client = None
parts = []
part_number = 1
upload_id = None


def signal_handler(sig, _frame):
    """
    By default, Python would raise a KeyboardInterrupt on SIGINT and terminate the program on SIGTERM. We want to
    finish processing any buffered input from stdin, at which point read_stdin_stream will return, and we will
    complete the multipart upload.
    """
    logger.info('Caught signal %s. Processing remaining data.', signal.Signals(sig).name)


# noinspection PyUnresolvedReferences
def add_custom_header(params, **_kwargs):
    global args

    params["headers"]['x-bz-active-read-enabled'] = 'true'
    params["headers"]['x-bz-active-read-part-size'] = str(args.chunk_size)


# noinspection PyUnresolvedReferences
def start_upload():
    global upload_id, args

    response = b2_client.create_multipart_upload(Bucket=args.bucket, Key=args.key)
    upload_id = response['UploadId']
    logger.debug("Created multipart upload. UploadId is %s", upload_id)


# noinspection PyUnresolvedReferences
def upload_part(buffer):
    global b2_client, parts, part_number, args, upload_id

    logger.debug("Uploading part number %s with size %s", part_number, len(buffer))
    response = b2_client.upload_part(
        Bucket=args.bucket,
        Key=args.key,
        Body=buffer,
        PartNumber=part_number,
        UploadId=upload_id
    )
    logger.debug("Uploaded part number %s; ETag is %s", part_number, response['ETag'])
    if args.dots:
        print('.', flush=True, end='')
    parts.append({
        "ETag": response['ETag'],
        'PartNumber': part_number
    })
    part_number += 1


# noinspection PyUnresolvedReferences
def read_stdin_stream(handler, chunk_size=DEFAULT_CHUNK_SIZE):
    with sys.stdin as f:
        while True:
            buffer = f.buffer.read(chunk_size)
            if buffer == b'':
                # EOF or read() was interrupted and there is no data left
                break
            handler(buffer)


# noinspection PyUnresolvedReferences
def complete_upload():
    global b2_client, args, parts, upload_id

    if len(parts) > 0:
        logger.debug("Completing multipart upload with %s parts", len(parts))
        b2_client.complete_multipart_upload(
            Bucket=args.bucket,
            Key=args.key,
            MultipartUpload={
                'Parts': parts
            },
            UploadId=upload_id
        )
    elif upload_id:
        logger.warning("Aborting multipart upload since there are no parts!")
        b2_client.abort_multipart_upload(
            Bucket=args.bucket,
            Key=args.key,
            UploadId=upload_id
        )
    else:
        logger.warning("No upload to complete")
    if args.dots:
        print('\n', flush=True, end='')


# noinspection DuplicatedCode
def main():
    global b2_client, args, upload_id

    # Install handler to override the KeyboardInterrupt on SIGINT or SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Copy stdin to a Backblaze B2 Live Read file')
    parser.add_argument('bucket', type=str, help='a bucket name')
    parser.add_argument('key', type=str, help='object key')
    parser.add_argument('--chunk_size', type=int, required=False, default=DEFAULT_CHUNK_SIZE, help='chunk size')
    parser.add_argument('--debug', action='store_true', help='debug logging')
    parser.add_argument('--dots', action='store_true', help='print a dot as each part is uploaded')
    args = parser.parse_args()

    logger.setLevel(logging.DEBUG if args.debug else logging.WARN)

    logger.debug("Command-line arguments: %s", args)

    loaded = load_dotenv()
    if loaded:
        logger.debug("Loaded environment variables from .env")
    else:
        logger.warning("No environment variables in .env")

    # Create a boto3 client base on configuration in .env file
    # AWS_ACCESS_KEY_ID=<Your Backblaze Application Key ID>
    # AWS_SECRET_ACCESS_KEY=<Your Backblaze Application Key>
    # AWS_ENDPOINT_URL=<Your B2 bucket endpoint, with https protocol, e.g. https://s3.us-west-004.backblazeb2.com>
    b2_client = boto3.client('s3')
    logger.debug("Created boto3 client")

    event_system = b2_client.meta.events
    event_system.register('before-call.s3.CreateMultipartUpload', add_custom_header)

    start_upload()

    read_stdin_stream(upload_part, chunk_size=args.chunk_size)

    complete_upload()

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
