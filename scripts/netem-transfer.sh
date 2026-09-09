#!/usr/bin/env bash
# Runs a real transfer through a real lossy network.
#
# The loss rule in tests/lossy.rs drops decoded messages inside the receiver,
# which exercises the protocol but never the socket. This puts netem in front
# of the datagrams instead, so kernel queues and timing are involved too.
#
#   scripts/netem-transfer.sh [loss%] [receivers] [megabytes]
#
# Re-executes itself in a private network namespace, so it needs no root and
# leaves the host's loopback alone.

set -euo pipefail

if [[ ${IN_NETNS:-} != 1 ]]; then
    exec env IN_NETNS=1 unshare --map-root-user --net "$0" "$@"
fi

loss=${1:-10}
receivers=${2:-3}
megabytes=${3:-8}

dev=lo
port=5900
data_port=$((port + 1))
group=239.1.1.60
scuttle=${SCUTTLE:-target/release/scuttle}

if [[ ! -x $scuttle ]]; then
    echo "no binary at $scuttle; run: cargo build --release -p scuttlecast-cli" >&2
    echo "or point SCUTTLE at one" >&2
    exit 64
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

ip link set "$dev" up
tc qdisc add dev "$dev" root handle 1: prio
tc qdisc add dev "$dev" parent 1:3 handle 30: netem loss "${loss}%"
tc filter add dev "$dev" protocol ip parent 1: prio 3 u32 \
    match ip dport "$data_port" 0xffff flowid 1:3

if ! tc qdisc show dev "$dev" | grep -q netem; then
    echo "netem did not attach; refusing to report a clean network as a pass" >&2
    exit 1
fi
echo "netem: ${loss}% loss on $dev port $data_port"

payload=$work/payload.bin
head -c "$((megabytes * 1024 * 1024))" /dev/urandom >"$payload"
want=$(sha256sum "$payload" | cut -d' ' -f1)

pids=()
for index in $(seq 0 $((receivers - 1))); do
    RUST_LOG=scuttle=info "$scuttle" receive \
        -l 127.0.0.1 -g "$group" -p "$port" -f "$work/out$index.bin" \
        >"$work/recv$index.log" 2>&1 &
    pids+=("$!")
done
sleep 1

started=$(date +%s%N)
RUST_LOG=scuttle=info "$scuttle" send \
    -l 127.0.0.1 -g "$group" -p "$port" -f "$payload" -m "$receivers" \
    >"$work/send.log" 2>&1 || true
elapsed_ms=$((($(date +%s%N) - started) / 1000000))

status=0
for pid in "${pids[@]}"; do
    wait "$pid" || status=1
done

echo
printf '%-4s %-8s %-8s %-6s %-6s %s\n' idx result loss naks late bytes
for index in $(seq 0 $((receivers - 1))); do
    log=$work/recv$index.log
    got=$(sha256sum "$work/out$index.bin" 2>/dev/null | cut -d' ' -f1)
    field() { sed -n "s/.*$1=\\([^ \"]*\\)\"\\?.*/\\1/p" "$log" | tail -1; }
    observed=$(sed -n 's/.*loss="\([^"]*\)".*/\1/p' "$log" | tail -1)
    result=CORRUPT
    [[ $got == "$want" ]] && result=ok || status=1

    printf '%-4s %-8s %-8s %-6s %-6s %s\n' \
        "$index" "$result" "${observed:-?}" "$(field naks)" "$(field late)" "$(field bytes)"

    if [[ -z $observed || $observed == 0.00% ]]; then
        echo "  receiver $index saw no loss, so this proved nothing" >&2
        status=1
    fi
done

echo
echo "${megabytes}MiB to $receivers receivers in ${elapsed_ms}ms"
sed -n 's/.*\(limited_by=.*\)/  last attribution: \1/p' "$work/send.log" | tail -1

if [[ $status != 0 ]]; then
    echo "FAILED; logs in $work" >&2
    trap - EXIT
    exit 1
fi
echo PASSED
