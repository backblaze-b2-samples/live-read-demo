# Backblaze B2 Live Read Demo

Backblaze B2's Live Read feature allows clients to read multipart uploads before they are complete, providing the
flexibility of uploading a stream of data as multiple files with the manageability of keeping the stream in a single
file. This is particularly useful in working with live video streams using formats such as Fragmented MP4.

[TBD - how to get access to Live Read.]

[TBD - add demo video]

## How Does Live Read Work?

A producer client starts a Live Read upload by sending a [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
request with two custom HTTP parameters: `x-bz-active-read-enabled` and, optionally, `x-bz-active-read-part-size`. `x-bz-active-read-enabled`
must be set to `true` for a Live Read upload to be initiated, while `x-bz-active-read-part-size` may be set to the
part size that will be used.  If `x-bz-active-read-enabled` is set to `true` and `x-bz-active-read-part-size` is not
present then the size of the first part will be used. All parts except the last one must have the same size.

The producer client then uploads a series of parts, via [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html), as normal. As noted above, all parts except the last one must have the same size. Once the producer client has uploaded all of its data, it calls [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), again, as it usually would.

Under the standard S3 API semantics, consumer clients must wait for the upload to be completed before they may download any data from the file. With Live Read, in contrast, consumer clients may attempt to download data from the file at any time after the upload is created. TBD... 

## What's in This Repository?

This repository contains a pair of simple Python apps that use [`boto3`, the AWS SDK for Python](https://aws.amazon.com/sdk-for-python/) 
to write and read Live Read uploads:

* `writer.py` creates a Live Read upload then reads its standard input in chunks corresponding to the desired part size, 
  which defaults to the minimum part size, 5 MB. Each chunk is uploaded as a part. When the app receives end-of-file 
  from `stdin`, it completes the upload. A signal handler ensures that pending data is uploaded if the app receives
  `SIGINT` (Ctrl+C) or `SIGTERM` (the default signal sent by the `kill` command).
* `reader.py` reads a Live Read upload. The attempts to download the file part-by-part. If the file does not yet exist,
  the app retries until it does. If a part is not available, the app uses [ListMultipartUploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
  to check if the upload is still in progress. If it is, then the app retries getting the part, otherwise, the upload is
  complete, and the app terminates.

The apps use `boto3`'s [Event System](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/events.html) to 
inject the custom headers into the relevant SDK calls. 

## What Are Fragmented MP4 Streams?

MP4 video _files_ typically begin or end with metadata describing the video data - its duration, resolution, codec, etc. -
known as the `MOOV` atom. The default placement is at the end of the file, as the metadata is not available until the
video data has been rendered. Video files intended for [progressive download](https://www.backblaze.com/blog/roll-camera-streaming-media-from-backblaze-b2/)
use an optimization known as "fast start", where the rendering app leaves space for the metadata at the beginning of the
file, writes the video data, then overwrites the placeholder with the actual metadata. This optimization allows media
viewers to start playing the video as it is downloaded.

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

We can use `ffmpeg` to capture video from a webcam, pipe raw video to `ffplay` for monitoring, and also pipe fMP4 video 
to `writer.py` to be written to a Live Read file.

This demo was created on a MacBook Pro with an Apple M1 Pro CPU and macOS Sonoma 14.4.1 running `ffmpeg` version 7.0. It
should also run on Linux if you change the input device and URL appropriately.

### Create a Named Pipe

Since we want `ffmpeg` to write two streams, we create a named pipe ('fifo') for it to send the raw video stream to `ffplay`:

```shell
mkfifo raw_video_fifo
```

### Capture Video and Pipe it to the Writer

Now we can start `ffplay` in the background, reading the fifo, and `ffmpeg` in the foreground, writing raw video to the
fifo and fMP4 to its standard output. We pipe the standard output into `writer.py`, giving it a B2 bucket name and key.
The `--debug` flag provides some useful insight into its operation.

```shell
ffplay -vf "drawtext=text='%{pts\:hms}':fontsize=72:box=1:x=(w-tw)/2:y=h-(2*lh)" \
       -f rawvideo -video_size 1920x1080 -framerate 30 -pixel_format uyvy422 raw_video_fifo &
ffmpeg -f avfoundation -video_size 1920x1080 -r 30 -pix_fmt uyvy422 -probesize 10000000 -i "0:0" \
       -f rawvideo raw_video_fifo -y \
       -f mp4 -vcodec libx264 -g 60 -movflags empty_moov+frag_keyframe - | \
python writer.py my-bucket myfile.mp4 --debug
```

Picking apart the `ffmpeg` command line:

```shell
ffmpeg -f avfoundation -video_size 1920x1080 -r 30 -pix_fmt uyvy422 -probesize 10000000 -i "0:0" \
```
* `ffmpeg` is reading video and audio data from AVFoundation input devices 0 and 0 respectively. On my MacBook Pro, these
  are the built-in FaceTime HD camera and MacBook Pro microphone. Video data is captured with 1920x1080 resolution (1080p)
  at 30 fps, using the `uyvy422` pixel format. `ffmpeg` will analyze the first 10 MB of data to get stream information
  (omitting this option results in a warning: `not enough frames to estimate rate; consider increasing probesize`)

```shell
       -f rawvideo raw_video_fifo -y \
```
* The raw video stream is sent, without any processing, to the fifo. Using the raw video stream minimizes latency in
  monitoring the webcam video.

```shell
       -f mp4 -vcodec libx264 -g 60 -movflags empty_moov+frag_keyframe - | \
```
* A second stream is encoded as MP4 using the H.264 codec and sent to `stdout`. `ffmpeg` writes an empty `moov` 
  atom at the start of the stream, then sends fragments of up to 60 frames (two seconds), with a key frame at the start
  of each fragment.

```shell
ffplay -vf "drawtext=text='%{pts\:hms}':fontsize=72:box=1:x=(w-tw)/2:y=h-(2*lh)" \
       -f rawvideo -video_size 1920x1080 -framerate 30 -pixel_format uyvy422 raw_video_fifo &
```

The `ffplay` command line shows a timestamp on the display and specifies the video format, resolution, frame rate and 
pixel format, since none of this information is in the stream.

After a few seconds, the `ffplay` window appears, showing the live camera feed.

`writer.py` creates a Live Read upload then, every few seconds, uploads a part to Backblaze B2:

```text
DEBUG:writer.py:Created multipart upload. UploadId is 4_zf1f51fb913357c4f74ed0c1b_f2008fdd4c7c9303e_d20240515_m185832_c004_v0402005_t0032_u01715799512833
...
DEBUG:writer.py:Uploading part number 1 with size 52428800      
DEBUG:writer.py:Uploaded part number 1; ETag is "7c223b579b7da8dd1b433d6eb2d0f141"    
```

_In practice, the debug output from `ffmpeg` and `writer.py` is interleaved. I've removed `ffmpeg`'s debug output for 
clarity._

Once the first part has been uploaded, you can monitor the total size of the uploaded parts:

```shell
#! /bin/bash

UPLOAD_ID="null"
until [ $UPLOAD_ID != "null" ]
do
    UPLOAD_ID=$(aws s3api list-multipart-uploads --bucket my-bucket --key-marker myfile.mp4 --max-uploads 1 \
        | jq -r '.Uploads[0].UploadId')
    sleep 1
    echo -n "."
done
watch -n 1 "aws s3api list-parts --bucket my-bucket --key myfile.mp4 --upload-id ${UPLOAD_ID} | jq '[.Parts[].Size] | add'"
```

### Start the Reader, Piping Its Output to the Display

Start `reader.py` in a second Terminal window, piping its output to `ffplay`. Note the `-` argument - this tells 
`ffplay` to read `stdin`:

```shell
python reader.py my-bucket myfile.mp4 --debug \
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

After a few seconds, a second `ffplay` window appears, showing the video read from the Live Read file.

You can leave the demo running as long as you like. The writer will continue uploading parts, and the reader will 
continue downloading them.

### Terminating the demo

When you terminate `ffmpeg` and `writer.py` via Ctrl+C, the writer uploads any remaining data in its `stdin` buffer and
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

## How Do I Apply Live Read to Other Use Cases?

Nothing in `reader.py` or `writer.py` is specific to fMP4 or the streaming video use case. Feel free to experiment with
Live Read and your own use cases. Fork this repository and use it as the basis for your own implementation. Let us know
at evangelism@backblaze.com if you come up with something interesting!
