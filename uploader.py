from __future__ import annotations
import abc
import logging

from queue import Queue
from threading import Thread

import boto3

logger = logging.getLogger('uploader')


class LiveReadTask:
    @abc.abstractmethod
    def execute(self, uploader: LiveReadUploader) -> bool:
        pass


class LiveReadCreate(LiveReadTask):
    def __str__(self):
        return 'Create task'

    def execute(self, uploader: LiveReadUploader) -> bool:
        return uploader.create_multipart_upload()


class LiveReadUpload(LiveReadTask):
    def __init__(self, buffer: bytes):
        self.buffer = buffer

    def __str__(self):
        return f'Upload task with buffer length {len(self.buffer)}'

    def execute(self, uploader: LiveReadUploader) -> bool:
        return uploader.upload_part(self.buffer)


class LiveReadComplete(LiveReadTask):
    def __str__(self):
        return 'Complete task'

    def execute(self, uploader: LiveReadUploader) -> bool:
        return uploader.complete_multipart_upload()


class LiveReadUploader(Thread):
    """
    LiveReadUploader encapsulates all the logic associated with a Live Read multipart upload.
    """

    def __init__(self, bucket: str, key: str):
        """
        Initialize a new LiveReadUploader
        """
        super().__init__()
        self._task_queue: Queue[LiveReadTask] = Queue()
        self.parts = []
        self.part_number = 1
        self.upload_id: str | None = None
        self.bucket = bucket
        self.key = key

        # Create a boto3 client based on configuration in .env file
        # AWS_ACCESS_KEY_ID=<Your Backblaze Application Key ID>
        # AWS_SECRET_ACCESS_KEY=<Your Backblaze Application Key>
        # AWS_ENDPOINT_URL=<Your B2 bucket endpoint, with https protocol, e.g. https://s3.us-west-004.backblazeb2.com>
        self.b2_client = boto3.client('s3')
        logger.debug("Created boto3 client")

        self.b2_client.meta.events.register('before-call.s3.CreateMultipartUpload', add_custom_header)

    def run(self):
        """
        Loop, reading the task queue, until we complete the upload
        """
        logger.info("Starting multipart upload")

        while True:
            done = self.get_task().execute(self)
            if done:
                break

        logger.info("Finished multipart upload")

    def put(self, task: LiveReadTask):
        """
        Add a task to the queue
        """
        self._task_queue.put(task)

    def get_task(self, block=True) -> LiveReadTask:
        """
        Retrieve the first task from the queue
        """
        return self._task_queue.get(block=block)

    def wait_until_complete(self):
        self.join()

    def create_multipart_upload(self):
        response = self.b2_client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self.upload_id = response['UploadId']
        logger.debug("Created multipart upload. UploadId is %s", self.upload_id)
        return False

    def upload_part(self, buffer: bytes):
        logger.debug("Uploading part number %s with size %s", self.part_number, len(buffer))
        response = self.b2_client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            Body=buffer,
            PartNumber=self.part_number,
            UploadId=self.upload_id
        )
        logger.debug("Uploaded part number %s; ETag is %s", self.part_number, response['ETag'])
        self.parts.append({
            "ETag": response['ETag'],
            'PartNumber': self.part_number
        })
        self.part_number += 1
        return False

    def complete_multipart_upload(self):
        if len(self.parts) > 0:
            logger.debug("Completing multipart upload with %s parts", len(self.parts))
            self.b2_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                MultipartUpload={
                    'Parts': self.parts
                },
                UploadId=self.upload_id
            )
        elif self.upload_id:
            logger.warning("Aborting multipart upload since there are no parts!")
            self.b2_client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id
            )
        else:
            # This should never happen!
            raise RuntimeError("No upload to complete")
        return True


def add_custom_header(params, **_kwargs):
    """
    Add the Live Read custom headers to the outgoing request.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
    """
    params['headers']['x-backblaze-live-read-enabled'] = 'true'
