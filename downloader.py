import http
import json
import logging
import time

from queue import Queue
from threading import Thread

import boto3
from botocore.exceptions import ClientError, ResponseStreamingError

logger = logging.getLogger('downloader')


class LiveReadDownloader(Thread):
    """
    LiveReadDownloader encapsulates all the logic associated with a Live Read download.
    """

    def __init__(self, bucket: str, key: str, poll_interval: int, chunk_size: int, queue_size: int, no_wait: bool):
        # Make this a daemon thread, so it is stopped when the foreground thread ends
        super().__init__(daemon=True)
        """
        Initialize a new LiveReadUploader
        """
        self._buffer_queue: Queue[bytes | None] = Queue(queue_size)
        self._offset = 0
        self._polling_offset = 0
        self._logged_not_found = False
        self._upload_id: str | None = None
        self._bucket = bucket
        self._key = key
        self._poll_interval = poll_interval
        self._chunk_size = chunk_size
        self._no_wait = no_wait

        # Create a boto3 client based on configuration in .env file
        # AWS_ACCESS_KEY_ID=<Your Backblaze Application Key ID>
        # AWS_SECRET_ACCESS_KEY=<Your Backblaze Application Key>
        # AWS_ENDPOINT_URL=<Your B2 bucket endpoint, with https protocol, e.g. https://s3.us-west-004.backblazeb2.com>
        self.b2_client = boto3.client('s3')
        logger.debug("Created boto3 client")

        self.b2_client.meta.events.register('before-call.s3.GetObject', add_custom_header)

    def run(self):
        """
        Get the most recent upload_id for the file, then loop, putting buffers on the queue, until there is no more data
        """
        self._get_current_upload_id()
        logger.debug('Reading UploadId %s', self._upload_id)

        logger.info("Starting multipart download")

        while data := self._get_next_chunk():
            self._buffer_queue.put(data)

        logger.info("Finished multipart download")
        self._buffer_queue.put(None)

    def get_data(self, block=True) -> bytes:
        """
        Get the next chunk of data from the queue. Returns None if there are no more.
        """
        return self._buffer_queue.get(block=block)

    def _get_next_chunk(self) -> bytes | None:
        """
        Get the next chunk of data from the file. Returns None if there are no more.
        """
        while True:
            byte_range = f'bytes={self._offset}-{self._offset + self._chunk_size - 1}'
            try:
                if self._polling_offset != self._offset:
                    logger.debug('Getting range %s', byte_range)
                if self._upload_id:
                    response = self.b2_client.get_object(
                        Bucket=self._bucket,
                        Key=self._key,
                        VersionId=self._upload_id,
                        Range=byte_range
                    )
                else:
                    response = self.b2_client.get_object(
                        Bucket=self._bucket,
                        Key=self._key,
                        Range=byte_range
                    )
                bytes_read = response['ContentLength']
                logger.debug('Got range %s with size %s', byte_range, bytes_read)
                self._offset += bytes_read
                return response['Body'].read()
            except ResponseStreamingError as e:
                logger.debug('Caught ResponseStreamingError: %s\nWill retry.', e)
            except ClientError as e:
                if e.response['ResponseMetadata']['HTTPStatusCode'] == http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                    # The requested range does not exist
                    if self._is_upload_in_progress():
                        # Only log once per part
                        if self._polling_offset != self._offset:
                            logger.warning('Polling every %s second(s) for range %s.', self._poll_interval,
                                           byte_range)
                            self._polling_offset = self._offset
                    else:
                        # Upload has finished - we're done
                        logger.debug(f"Download is complete. Downloaded {self._offset} bytes")
                        return None
                elif e.response['ResponseMetadata']['HTTPStatusCode'] == http.HTTPStatus.NOT_FOUND:
                    # Keep trying until the parts become available
                    if not self._logged_not_found:
                        logger.warning('%s/%s does not (yet?) exist. Will check every %s second(s)', self._bucket,
                                       self._key, self._poll_interval)
                        self._logged_not_found = True
                else:
                    raise RuntimeError(
                        f"get_object returned HTTP status {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
                        f"{json.dumps(e.response['Error'])}\nExiting"
                    )

                time.sleep(self._poll_interval)

    def _get_current_upload_id(self):
        # Get the UploadId of the current upload. This is the file version we will get data from
        logged = False
        while True:
            response = self.b2_client.list_multipart_uploads(
                Bucket=self._bucket,
                KeyMarker=self._key,
                MaxUploads=1
            )
            if 'Uploads' in response or self._no_wait:
                break

            if not logged:
                logger.info('No active upload for %s/%s. Will retry every %s second(s).',
                            self._bucket, self._key, self._poll_interval)
                logged = True
            time.sleep(self._poll_interval)

        # The last entry in the list is the most recent upload
        # The UploadId is the same as the file's VersionId
        self._upload_id = response['Uploads'][len(response['Uploads']) - 1]['UploadId'] \
            if 'Uploads' in response else None

    def _is_upload_in_progress(self):
        if self._no_wait:
            return False

        response = self.b2_client.list_multipart_uploads(
            Bucket=self._bucket,
            KeyMarker=self._key,
            UploadIdMarker=self._upload_id,
            MaxUploads=1
        )
        found = False
        if 'Uploads' in response:
            for upload in response['Uploads']:
                if upload['UploadId'] == self._upload_id:
                    found = True
                    break
        return found


def add_custom_header(params, **_kwargs):
    """
    Add the Live Read custom headers to the outgoing request.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
    """
    params['headers']['x-backblaze-live-read-enabled'] = 'true'
