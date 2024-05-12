
```shell
mkfifo fifo
```

```shell
ffplay -f rawvideo -framerate 30 -pixel_format uyvy422 -video_size 1920x1080 fifo &
ffmpeg -f avfoundation -video_size 1920x1080 -r 30 -pix_fmt uyvy422 -probesize 10000000 -i "0" \
-f rawvideo fifo -y \
-g 60 -vcodec libx264 -f mp4 -movflags frag_keyframe+empty_moov - | \
python writer.py metadaddy-public liveread.mp4 --debug
```

```shell
python reader.py metadaddy-public liveread.mp4 --debug | ffplay -
```
