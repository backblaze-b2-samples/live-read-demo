import argparse
import http
import json
import logging
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logging.basicConfig()

logger = logging.getLogger(os.path.basename(__file__))


def add_custom_header(params, **_kwargs):
    """
    Add the Live Read custom headers to the outgoing request.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
    """
    params["headers"]['x-bz-active-read-enabled'] = 'true'


# noinspection DuplicatedCode
def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Copy stdin to a Backblaze B2 Live Read file')
    parser.add_argument('bucket', type=str, help='a bucket name')
    parser.add_argument('key', type=str, help='object key')
    parser.add_argument('--poll_interval', type=int, required=False, default=1, help='poll interval')
    parser.add_argument('--debug', action='store_true', help='debug logging')
    parser.add_argument('--dots', action='store_true', help='print a dot as each chunk is downloaded')
    args = parser.parse_args()

    logger.setLevel(logging.DEBUG if args.debug else logging.WARN)

    logger.debug("Command-line arguments: %s", args)

    loaded = load_dotenv()
    if loaded:
        logger.debug("Loaded environment variables from .env")
    else:
        logger.warning("No environment variables in .env")

    # Create a boto3 client based on configuration in .env file
    # AWS_ACCESS_KEY_ID=<Your Backblaze Application Key ID>
    # AWS_SECRET_ACCESS_KEY=<Your Backblaze Application Key>
    # AWS_ENDPOINT_URL=<Your B2 bucket endpoint, with https protocol, e.g. https://s3.us-west-004.backblazeb2.com>
    b2_client = boto3.client('s3')
    logger.debug("Created boto3 client")

    b2_client.meta.events.register('before-call.s3.GetObject', add_custom_header)

    part_number = 1
    while True:
        try:
            logger.debug('Getting part number %s', part_number)
            response = b2_client.get_object(
                Bucket=args.bucket,
                Key=args.key,
                PartNumber=part_number
            )
            logger.debug('Got part number %s with size %s', part_number, response['ContentLength'])
            sys.stdout.buffer.write(response['Body'].read())
            sys.stdout.buffer.flush()
            part_number += 1
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                # The requested part does not exist
                # Is the upload still in progress?
                response = b2_client.list_multipart_uploads(
                    Bucket=args.bucket,
                    KeyMarker=args.key,
                    MaxUploads=1
                )
                if 'Uploads' not in response:
                    # Upload has finished - we're done
                    logger.debug("Upload is complete.")
                    break
                else:
                    logger.warning('Cannot get part number %s. Will retry in %s second(s)', part_number,
                                   args.poll_interval)

            elif e.response['ResponseMetadata']['HTTPStatusCode'] == http.HTTPStatus.NOT_FOUND:
                # Keep trying until the parts become available
                logger.warning('%s/%s does not (yet?) exist. Will retry in %s second(s)', args.bucket, args.key,
                               args.poll_interval)
            else:
                logger.error('get_object returned HTTP status %s\n%s\nExiting',
                             e.response['ResponseMetadata']['HTTPStatusCode'],
                             json.dumps(e.response['Error']))
                exit(1)

            time.sleep(args.poll_interval)

    logger.debug("Exiting Normally.")


if __name__ == "__main__":
    main()
