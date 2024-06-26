# Backblaze B2 Live Read Demo

Backblaze B2's Live Read feature allows clients to read multipart uploads before they are complete, providing the
flexibility of uploading a stream of data as multiple files with the manageability of keeping the stream in a single
file. This is particularly useful in working with live video streams using formats such as Fragmented MP4.

Backblaze B2 Live Read is currently in private preview. Read the [announcement blog post](https://www.backblaze.com/blog/announcing-b2-live-read/); [click here](https://www.surveymonkey.com/r/PY7CR35) to join the preview.

This webinar explains how Live Read works and shows it in action, using OBS Studio to generate a live video stream.

[![Live Read Webinar on YouTube](https://github.com/backblaze-b2-samples/live-read-demo/assets/723517/6daa897f-0f6d-4ffe-8668-4cc2379620d9)](https://www.youtube.com/watch?v=4GQUWo2wHUQ)

[This short video](https://www.youtube.com/watch?v=JTI2kkysRWE) shows a simpler version of the demo, using FFmpeg to capture video from a webcam.

## How Does Live Read Work?

A producer client starts a Live Read upload by sending a [`CreateMultipartUpload`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html) request with one or two custom HTTP parameters: `x-bz-active-read-enabled` and, optionally, `x-bz-active-read-part-size`.`x-bz-active-read-enabled` must be set to `true` for a Live Read upload to be initiated, while `x-bz-active-read-part-size` may be set to the part size that will be used.  If `x-bz-active-read-enabled` is set to `true` and `x-bz-active-read-part-size` is not present then the size of the first part will be used. All parts except the last one must have the same size.

The producer client then uploads a series of parts, via [`UploadPart`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html), as normal. As noted above, all parts except the last one must have the same size. Once the producer client has uploaded all of its data, it calls [`CompleteMultipartUpload`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), again, as it usually would.

Under the standard S3 API semantics, consumer clients must wait for the upload to be completed before they may download any data from the file. With Live Read, in contrast, consumer clients may attempt to download data, using [`GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) with the custom HTTP header `x-bz-active-read-enabled` set to `true`, from the file at any time after the upload is created. Consumer clients MUST include either `Range` or `PartNumber` in the `GetObject` call to specify the required portion of the file. If the client requests a range or part that does not exist, then Backblaze B2 responds with a `416 Range Not Satisfiable` error. On receiving this error, a consumer client might repeatedly retry the request, waiting for a short interval after each unsuccessful request.

After the upload is completed, clients can retrieve the file using standard S3 API calls.

## What's in This Repository?

This repository contains a pair of simple Python apps that use [`boto3`, the AWS SDK for Python](https://aws.amazon.com/sdk-for-python/) 
to write and read Live Read uploads:

* `writer.py` creates a Live Read upload then reads its standard input in chunks corresponding to the desired part size, 
  which defaults to the minimum part size, 5 MB. Each chunk is uploaded as a part. When the app receives end-of-file 
  from `stdin`, it completes the upload. A signal handler ensures that pending data is uploaded if the app receives
  `SIGINT` (Ctrl+C) or `SIGTERM` (the default signal sent by the `kill` command).
* `reader.py` reads a Live Read upload. The app attempts to download the file part-by-part. If the file does not yet exist,
  the app retries until it does. If a part is not available, the app uses [`ListMultipartUploads`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
  to check if the upload is still in progress. If it is, then the app retries getting the part; otherwise, the app terminates, since the upload has been completed.

The apps use `boto3`'s [Event System](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html) to 
inject the custom headers into the relevant SDK calls. For example, in the writer:

```python
self.b2_client = boto3.client('s3')
logger.debug("Created boto3 client")

self.b2_client.meta.events.register('before-call.s3.CreateMultipartUpload', add_custom_header)

...

def add_custom_header(params, **_kwargs):
  """
  Add the Live Read custom headers to the outgoing request.
  See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html
  """
  params['headers']['x-bz-live-read-enabled'] = 'true'
```

## What Are Fragmented MP4 Streams?

MP4 video _files_ typically begin or end with metadata describing the video data - its duration, resolution, codec, etc. This metadata is
known as the `MOOV` atom. The default placement is at the end of the file, as the metadata is not available until the
video data has been rendered. Video files intended for [progressive download](https://www.backblaze.com/blog/roll-camera-streaming-media-from-backblaze-b2/)
use an optimization known as "fast start", where the rendering app leaves space for the metadata at the beginning of the
file, writes the video data, then overwrites the placeholder with the actual metadata. This optimization allows media
viewers to start playing the video while it is being downloaded.

MP4 video _streams_, in contrast, typically comprise a metadata header containing information such as track and sample 
descriptions, followed by a series of 'fragments' of video data, each containing its own 
metadata, known as media segments. This format is termed [Fragmented MPEG-4](https://datatracker.ietf.org/doc/html/rfc8216#section-3.3), 
abbreviated as fMP4. Video stream creators choose an appropriate fragment size in the region of two to six seconds. 
Shorter fragments allow lower latency and faster encoding, while longer fragments allow better encoding efficiency.

Historically, fMP4 fragments were written to storage as individual files, with a [Playlist](https://datatracker.ietf.org/doc/html/rfc8216#section-4)
file listing the media segment files comprising a stream. During a live stream, the Playlist file would be updated as 
each media segment file was written.

As an example, a one-hour live stream of 1920x1080 video at 30 frames/second, with a fragment length of two seconds would
comprise the Playlist file and 1,800 media segment files of around 900 kB each. A video player must read the Playlist,
then make an HTTPS request for each media segment file.

## How do I Demonstrate Live Read with fMP4?

This demo shows how Live Read allows an fMP4 stream to be written to a single multipart file. A video player can read
already-uploaded parts while the file is still being written. The main constraint is that the S3 API imposes a minimum 
part size of 5 MB. For our 1080p 30 fps example, this means that there is a minimum latency of about six seconds between
the video data being written and it being available for download.

The demo instructions below include:

* Using FFmpeg to capture video from a webcam or RTMP stream.
* Piping raw video to `ffplay` for monitoring.
* Piping fMP4 video to `writer.py` to be written to a Live Read file.
* Using `reader.py` to play back video from a Live Read file.
* Piping fMP4 video to FFmpeg for conversion to HTTP Live Streaming (HLS) format. 

This demo was created on a MacBook Pro with an Apple M1 Pro CPU and macOS Sonoma 14.4.1 running Python 3.11.5 and FFmpeg version 7.0. It
should also run on Linux if you change the input device and URL appropriately.

### Create a Backblaze B2 Account, Bucket and Application Key

Follow these instructions, as necessary:

* [Create a Backblaze B2 Account](https://www.backblaze.com/sign-up/cloud-storage).
* [Create a Backblaze B2 Bucket](https://www.backblaze.com/docs/cloud-storage-create-and-manage-buckets).
* [Create an Application Key](https://www.backblaze.com/docs/cloud-storage-create-and-manage-app-keys#create-an-app-key) with access to the bucket you wish to use.

Be sure to copy the application key as soon as you create it, as you will not be able to retrieve it later!

### Download the Source Code

```shell
git clone git@github.com:backblaze-b2-samples/live-read-demo.git
cd live-read-demo
```

### Create a Python Virtual Environment

Virtual environments allow you to encapsulate a project's dependencies; we recommend that you create a virtual environment thus:

```shell
python -m venv .venv
```

You must then activate the virtual environment before installing dependencies:

```shell
source .venv/bin/activate
```

You will need to reactivate the virtual environment, with the same command, if you close your Terminal window and return to the demo later. Both the producer and consumer apps use the same dependencies and can share the same virtual environment. If you are running the two apps in separate Terminal windows, remember to activate the virtual environment in the second window before you run the app!

### Install Python Dependencies

```shell
pip install -r requirements.txt
```

### Install FFmpeg

If you do not already have FFmpeg installed on your system, you can download it from one of the links at https://ffmpeg.org/download.html
or use a package manager to install it. For example, using [Homebrew](https://brew.sh/) on a Mac:

```shell
brew install ffmpeg
```

### Create a Named Pipe

Since we want FFmpeg to write two streams, we create a [named pipe](https://en.wikipedia.org/wiki/Named_pipe) ('fifo') for it to send the raw video stream to `ffplay`:

```shell
mkfifo raw_video_fifo
```

### Configure the Demo Apps

The demo apps read their configuration from a `.env` file. Copy the included `.env.template` to `.env`:

```shell
cp .env.template .env
```

Now edit `.env`, pasting in your application key, its ID, bucket name, and endpoint:

```dotenv
# Copy this file to .env then edit the following values
AWS_ACCESS_KEY_ID='<Your Backblaze application key ID>'
AWS_SECRET_ACCESS_KEY='<Your Backblaze application key>'
AWS_ENDPOINT_URL='<Your bucket endpoint, prefixed with https://, for example, https://s3.us-west-004.backblazeb2.com>'
BUCKET_NAME='<Your Backblaze B2 bucket name>'
```

### Capture Video and Pipe it to the Writer

Now we can start FFplay in the background, reading the fifo, and FFmpeg in the foreground, writing raw video to the
fifo and fMP4 to its standard output. We pipe the standard output into `writer.py`, giving it a B2 key.
The `--debug` flag provides some useful insight into its operation.

```shell
ffplay -vf "drawtext=text='%{pts\:hms}':fontsize=72:box=1:x=(w-tw)/2:y=h-(2*lh)" \
       -f rawvideo -video_size 1920x1080 -framerate 30 -pixel_format uyvy422 raw_video_fifo &
ffmpeg -f avfoundation -video_size 1920x1080 -r 30 -pix_fmt uyvy422 -probesize 10000000 -i "0:0" \
       -f rawvideo raw_video_fifo -y \
       -f mp4 -vcodec libx264 -g 60 -movflags empty_moov+frag_keyframe - | \
python writer.py myfile.mp4 --debug
```

Picking apart the FFmpeg command line:

```shell
ffmpeg -f avfoundation -video_size 1920x1080 -r 30 -pix_fmt uyvy422 -probesize 10000000 -i "0:0" \
```
* FFmpeg is reading video and audio data from [AVFoundation](https://developer.apple.com/av-foundation/) input devices `0` and `0` respectively. On my MacBook Pro, these
  are the built-in FaceTime HD camera and MacBook Pro microphone. Video data is captured with 1920x1080 resolution (1080p)
  at 30 fps, using the `uyvy422` pixel format. FFmpeg will analyze the first 10 MB of data to get stream information
  (omitting this option results in a warning: `not enough frames to estimate rate; consider increasing probesize`).

  > On a Mac, you can list the available video and audio devices with `ffmpeg -f avfoundation -list_devices true -i ""`. 

```shell
       -f rawvideo raw_video_fifo -y \
```
* The raw video stream is sent, without any processing, to the fifo. Using the raw video stream minimizes latency in
  monitoring the webcam video.

```shell
       -f mp4 -vcodec libx264 -g 60 -movflags empty_moov+frag_keyframe - | \
```
* A second stream is encoded as MP4 using the H.264 codec and sent to `stdout`. FFmpeg writes an empty `moov` 
  atom at the start of the stream, then sends fragments of up to 60 frames (two seconds), with a key frame at the start
  of each fragment.

```shell
ffplay -vf "drawtext=text='%{pts\:hms}':fontsize=72:box=1:x=(w-tw)/2:y=h-(2*lh)" \
       -f rawvideo -video_size 1920x1080 -framerate 30 -pixel_format uyvy422 raw_video_fifo &
```

The `ffplay` command line shows a timestamp on the display and specifies the video format, resolution, frame rate and 
pixel format, since none of this information is in the raw video stream.

After a few seconds, the `ffplay` window appears, showing the live camera feed.

`writer.py` creates a Live Read upload then, every few seconds, uploads a part to Backblaze B2:

```text
DEBUG:writer.py:Created multipart upload. UploadId is 4_zf1f51fb913357c4f74ed0c1b_f2008fdd4c7c9303e_d20240515_m185832_c004_v0402005_t0032_u01715799512833
...
DEBUG:writer.py:Uploading part number 1 with size 52428800      
DEBUG:writer.py:Uploaded part number 1; ETag is "7c223b579b7da8dd1b433d6eb2d0f141"    
```

_In practice, the debug output from FFmpeg and `writer.py` is interleaved. I've removed FFmpeg's debug output for 
clarity._

Once the first part has been uploaded, you can use the included `watch_upload.sh` script in a second Terminal window to monitor the total size of the uploaded parts:

```shell
./watch_upload.sh my-bucket myfile.mp4
```

### Receive an RTMP Stream and Pipe it to the Writer

As an alternative to using FFmpeg to capture video directly from the webcam, you can use [OBS Studio](https://obsproject.com/) to generate a [Real-Time Messaging Protocol (RTMP)](https://en.wikipedia.org/wiki/Real-Time_Messaging_Protocol) stream.

Start FFmpeg, listening as an RTMP server, receiving Flash Video-formatted (FLV) data, and piping its output to the writer app:

```shell
ffmpeg -listen 1 -f flv -i rtmp://localhost/app/streamkey \
    -f mp4 -g 60 -movflags empty_moov+frag_keyframe - | \
python writer.py myfile.mp4 --debug
```

Start OBS Studio, navigate to the **Settings** page, and click **Stream** on the left. Set **Service** to 'Custom', **Server** to `rtmp://localhost/app/` and **Stream Key** to `streamkey`. (You can change `app` and `streamkey` in the FFmpeg command and OBS Studio configuration, but both values must be present).

![OBS Studio Settings](https://github.com/backblaze-b2-samples/live-read-demo/assets/723517/fd65496d-0213-415d-8b60-4aca7a5b2715)

Start streaming in OBS Studio. As above, `writer.py` creates a Live Read upload then, every few seconds, uploads a part to Backblaze B2.

### Start the Reader, Piping Its Output to the Display

Open another Terminal window in the same directory and activate the virtual environment:

```shell
source .venv/bin/activate
```

Start `reader.py`, piping its output to `ffplay`. Note the `-` argument at the end - this tells 
`ffplay` to read `stdin`:

```shell
python reader.py myfile.mp4 --debug \
    | ffplay -vf "drawtext=text='%{pts\:hms}':fontsize=72:box=1:x=(w-tw)/2:y=h-(2*lh)" -
```

`reader.py` will start reading the available parts as soon as they are available:

```text
DEBUG:reader.py:Getting part number 1
WARNING:reader.py:Cannot get part number 1. Will retry in 1 second(s)
...
DEBUG:reader.py:Getting part number 1
DEBUG:reader.py:Got part number 1 with size 5242880
```

After a few seconds, a second `ffplay` window appears, showing the video data that was read from the Live Read file.

You can leave the demo running as long as you like. The writer will continue uploading parts, and the reader will 
continue downloading them.

### Pipe fMP4 video to FFmpeg for conversion to HTTP Live Streaming (HLS) format

You can use `reader.py` with FFmpeg to create HLS-formatted video data:

```shell
python reader.py stream.mp4 --debug |
ffmpeg -i - \
    -flags +cgop -hls_time 4 -hls_playlist_type event out/stream.m3u8
```

In this example, FFmpeg writes the HLS manifest to a file named `stream.m3u8` in the `out` directory, writing video data in 4 second segments (`-hls_time 4`) to the same directory.

You can use `rclone mount` to write the HLS data to a Backblaze B2 Bucket (you can use the same bucket as the Live Read file, or a different bucket altogether):

```shell
rclone mount b2://my-bucket ./out --vfs-cache-mode writes --vfs-write-back 1s
```

Upload `index.html` to the same location as the HLS data. Open `https://<your-bucket-name>.<your-bucket-endpoint>/index.html` (for example, `https://my-bucket.s3.us-west-004.backblazeb2.com`) in a browser. You should see the live stream:

![Live Read_Stream](https://github.com/backblaze-b2-samples/live-read-demo/assets/723517/e32ba24c-b7af-49eb-90c1-bdd85fcae447)

### Terminating the demo

When you terminate FFmpeg and `writer.py` via Ctrl+C, the writer uploads any remaining data in its `stdin` buffer and
completes the multipart upload:

```text
INFO:writer.py:Caught signal SIGINT. Processing remaining data.
DEBUG:writer.py:Uploading part number 3 with size 5150190 
DEBUG:writer.py:Uploaded part number 3; ETag is "9453bc700d885233d1b9f43efcf14f4d"
DEBUG:writer.py:Completing multipart upload with 3 parts
DEBUG:writer.py:Exiting Normally.
```

The reader detects that the upload is complete, and exits:

```text
DEBUG:reader.py:Got part number 3 with size 5150190 
DEBUG:reader.py:Getting part number 4 
DEBUG:reader.py:Upload is complete. 
DEBUG:reader.py:Exiting Normally.
```

The uploaded video data is stored as a single file, and can be accessed in the usual way:

```console
% aws s3 ls --human-readable s3://my-bucket/myfile.mp4
2024-05-15 12:15:50   14.9 MiB myfile.mp4

% ffprobe -hide_banner -i https://s3.us-west-004.backblazeb2.com/my-bucket/myfile.mp4
Input #0, mov,mp4,m4a,3gp,3g2,mj2, from 'https://s3.us-west-004.backblazeb2.com/my-bucket/myfile.mp4':
  Metadata:
    major_brand     : isom
    minor_version   : 512
    compatible_brands: isomiso6iso2avc1mp41
    encoder         : Lavf61.1.100
  Duration: 00:00:35.34, start: 0.000000, bitrate: 3539 kb/s
  Stream #0:0[0x1](und): Video: h264 (High 4:2:2) (avc1 / 0x31637661), yuv422p(progressive), 1920x1080, 3692 kb/s, 30 fps, 30 tbr, 15360 tbn (default)
      Metadata:
        handler_name    : VideoHandler
        vendor_id       : [0][0][0][0]
        encoder         : Lavc61.3.100 libx264
  Stream #0:1[0x2](und): Audio: aac (LC) (mp4a / 0x6134706D), 44100 Hz, mono, fltp, 62 kb/s (default)
      Metadata:
        handler_name    : SoundHandler
        vendor_id       : [0][0][0][0]
```

### Converting the HLS Data from a Live Event to Video On Demand

If you ran the FFmpeg command to create HLS data then, after you close the applications, you can edit the HLS manifest file to change the stream from a live event to video on demand (VOD):

* Download `stream.m3u8`.
* Open it in an editor.
* Change the line
  ```text
  #EXT-X-PLAYLIST-TYPE:EVENT
  ```
  to
  ```text
  #EXT-X-PLAYLIST-TYPE:VOD
  ```
* Append the following line to the file:
  ```text
  #EXT-X-ENDLIST
  ```

Now, when you open the stream in a browser, you will be able to view the recording from its start.

## How Do I Apply Live Read to Other Use Cases?

Nothing in `reader.py` or `writer.py` is specific to fMP4 or the streaming video use case. Feel free to experiment with
Live Read and your own use cases. Fork this repository and use it as the basis for your own implementation. Let us know
at evangelism@backblaze.com if you come up with something interesting!
