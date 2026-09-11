#!/usr/bin/env bash
# Times the protocol path with the disk taken out of it.
#
# The netem script writes its payload and its output to the real filesystem,
# which on a compressing filesystem costs more than the transfer does. This
# serves the payload from a tmpfs and throws the output away, so what is left
# is the sender, the network and the receiver.
#
#   scripts/loopback-throughput.sh [megabytes] [receivers]
#
# Re-executes itself in a private namespace, so it needs no root.

set -euo pipefail

if [[ ${IN_NETNS:-} != 1 ]]; then
    exec env IN_NETNS=1 unshare --map-root-user --net --mount "$0" "$@"
fi

megabytes=${1:-256}
receivers=${2:-1}

port=5900
group=239.1.1.61
scuttle=${SCUTTLE:-target/release/scuttle}

if [[ ! -x $scuttle ]]; then
    echo "build $scuttle first" >&2
    exit 1
fi

ip link set lo up

work=$(mktemp -d)
mount -t tmpfs -o size=$((megabytes + 64))m tmpfs "$work"

echo "generating ${megabytes}MiB in a tmpfs"
payload=$work/payload.bin
head -c "$((megabytes * 1024 * 1024))" /dev/zero >"$payload"
echo "starting ${receivers} receiver(s) on ${group}"

pids=()
for index in $(seq 0 $((receivers - 1))); do
    "$scuttle" receive -l 127.0.0.1 -g "$group" -p "$port" -f /dev/null \
        >"$work/recv$index.log" 2>&1 &
    pids+=($!)
done
sleep 1

for index in $(seq 0 $((receivers - 1))); do
    if ! kill -0 "${pids[$index]}" 2>/dev/null; then
        echo "receiver $index died immediately:" >&2
        cat "$work/recv$index.log" >&2
        exit 1
    fi
done
echo "sending, one line per 200ms tick"
started=$(date +%s%N)
RUST_LOG=scuttle=info "$scuttle" send \
    -l 127.0.0.1 -g "$group" -p "$port" -f "$payload" -m "$receivers" 2>&1 |
    tee "$work/send.log" |
    stdbuf -oL grep --line-buffered -o 'rate="[^"]*"\|limited_by=[a-z ]*' |
    stdbuf -oL paste - - |
    stdbuf -oL sed 's/^/  /' || true
elapsed_ms=$((($(date +%s%N) - started) / 1000000))

for pid in "${pids[@]}"; do
    wait "$pid" || true
done

gbps=$(awk -v mb="$megabytes" -v ms="$elapsed_ms" 'BEGIN { printf "%.2f", mb * 8 / (ms / 1000) / 1024 }')
echo "${megabytes}MiB to ${receivers} receivers: ${elapsed_ms}ms = ${gbps} Gbps"

sed -n 's/.*limited_by=\(.*\)/\1/p' "$work/send.log" |
    sed 's/,.*//' | sort | uniq -c | sort -rn |
    awk '{ ticks = $1; $1 = ""; printf "  %3d ticks:%s\n", ticks, $0 }'


