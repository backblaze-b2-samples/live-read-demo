import argparse
import logging
import os
import signal
import sys

import boto3
from dotenv import load_dotenv

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


def add_custom_header(params, **_kwargs):
    """
    Add the Live Read custom headers to the outgoing request.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
    """
    params['headers']['x-bz-active-read-enabled'] = 'true'


def start_upload(b2_client, bucket, key):
    response = b2_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = response['UploadId']
    logger.debug("Created multipart upload. UploadId is %s", upload_id)
    return upload_id


# noinspection PyUnresolvedReferences
def upload_part(b2_client, bucket, key, upload_id, buffer, part_number):
    logger.debug("Uploading part number %s with size %s", part_number, len(buffer))
    response = b2_client.upload_part(
        Bucket=bucket,
        Key=key,
        Body=buffer,
        PartNumber=part_number,
        UploadId=upload_id
    )
    logger.debug("Uploaded part number %s; ETag is %s", part_number, response['ETag'])
    return {
        "ETag": response['ETag'],
        'PartNumber': part_number
    }


# noinspection PyUnresolvedReferences
def upload_from_stdin(b2_client, bucket, key, upload_id, chunk_size):
    parts = []
    part_number = 1
    with sys.stdin as f:
        while True:
            buffer = f.buffer.read(chunk_size)
            if buffer == b'':
                # EOF or read() was interrupted and there is no data left
                break
            parts.append(upload_part(b2_client, bucket, key, upload_id, buffer, part_number))
            part_number += 1
    return parts


# noinspection PyUnresolvedReferences
def complete_upload(b2_client, bucket, key, upload_id, parts):
    if len(parts) > 0:
        logger.debug("Completing multipart upload with %s parts", len(parts))
        b2_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            MultipartUpload={
                'Parts': parts
            },
            UploadId=upload_id
        )
    elif upload_id:
        logger.warning("Aborting multipart upload since there are no parts!")
        b2_client.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )
    else:
        # This should never happen!
        logger.error("No upload to complete")


# noinspection DuplicatedCode
def main():
    args = parse_command_line_args()

    logger.setLevel(logging.DEBUG if args.debug else logging.WARN)

    if args.debug_boto:
        logging.getLogger('botocore').setLevel(logging.DEBUG)

    logger.debug("Command-line arguments: %s", args)

    if load_dotenv():
        logger.debug("Loaded environment variables from .env")
    else:
        logger.warning("No environment variables in .env")

    # Install handler to override the KeyboardInterrupt on SIGINT or SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create a boto3 client based on configuration in .env file
    # AWS_ACCESS_KEY_ID=<Your Backblaze Application Key ID>
    # AWS_SECRET_ACCESS_KEY=<Your Backblaze Application Key>
    # AWS_ENDPOINT_URL=<Your B2 bucket endpoint, with https protocol, e.g. https://s3.us-west-004.backblazeb2.com>
    b2_client = boto3.client('s3')
    logger.debug("Created boto3 client")

    b2_client.meta.events.register('before-call.s3.CreateMultipartUpload', add_custom_header)

    upload_id = start_upload(b2_client, args.bucket, args.key)

    parts = upload_from_stdin(b2_client, args.bucket, args.key, upload_id, args.chunk_size)

    complete_upload(b2_client, args.bucket, args.key, upload_id, parts)

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
