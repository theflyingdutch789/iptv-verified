#!/usr/bin/env bash
# relay.sh - local ffmpeg relay for one live stream.
#
# Pulls the origin once, keeps a deep local buffer and re-serves the stream on
# this machine as HLS, so any player on the Mac or the home network (VLC,
# Apple TV, a smart TV) reads a smooth local feed instead of a bursty origin.
# The relay cannot create bandwidth: if the origin delivers slower than real
# time the buffer still drains, only later.
#
#   ./relay.sh "<stream url>"            # copy mode: no re-encode, zero quality loss
#   ./relay.sh "<stream url>" --upscale  # re-encode to 1080p with a sharpening pass
#
# Then open   http://localhost:8787/live.m3u8   in the player.
# The upscale mode uses the Apple hardware encoder; it sharpens, it does not add
# detail. For real upscaling use IINA/mpv with the FSRCNNX shader instead.

set -euo pipefail
URL="${1:?usage: relay.sh <stream url> [--upscale]}"
MODE="${2:-}"
PORT="${PORT:-8787}"
DIR="$(mktemp -d /tmp/relay.XXXXXX)"
PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null; rm -rf "$DIR"' EXIT

if [[ "$MODE" == "--upscale" ]]; then
  VIDEO=(-vf "scale=1920:-2:flags=lanczos,unsharp=5:5:0.6:5:5:0.0"
         -c:v h264_videotoolbox -b:v 8M -maxrate 10M -bufsize 16M -profile:v high -g 50
         -c:a aac -b:a 160k)
else
  VIDEO=(-c copy)
fi

ffmpeg -hide_banner -loglevel warning -nostdin \
  -user_agent "Mozilla/5.0" \
  -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 \
  -rw_timeout 15000000 \
  -i "$URL" \
  "${VIDEO[@]}" \
  -f hls -hls_time 4 -hls_list_size 40 \
  -hls_flags delete_segments+append_list+omit_endlist \
  -hls_segment_filename "$DIR/seg_%05d.ts" \
  "$DIR/live.m3u8" &
PIDS+=($!)

# wait for the first segments, then serve the folder
for _ in $(seq 1 60); do [[ -s "$DIR/live.m3u8" ]] && break; sleep 0.5; done
[[ -s "$DIR/live.m3u8" ]] || { echo "origin produced nothing in 30 s" >&2; exit 1; }
python3 -m http.server "$PORT" --directory "$DIR" --bind 0.0.0.0 >/dev/null 2>&1 &
PIDS+=($!)
echo "relay ready:  http://localhost:$PORT/live.m3u8   (buffer ~160 s, ctrl-c to stop)"
wait "${PIDS[0]}"
