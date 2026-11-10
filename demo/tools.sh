#!/bin/sh
# The demo's steps. compose.yaml runs each in a container: push and check in crane's image, stats in
# the S3 store's, which can read the bucket's directory.
set -eu

case "$1" in
push)
  # each image as published, compressed layers and all
  for t in $IMAGES; do
    echo "copying $SOURCE/$t"
    crane copy --platform linux/amd64 --insecure "$SOURCE/$t" "registry:5000/$t"
  done
  ;;
check)
  # downloads every layer back through the registry and rift, and checks it against its digest
  for t in $IMAGES; do
    crane validate --insecure --remote "registry:5000/$t"
  done
  ;;
stats)
  m=$(wget -qO- http://rift:9100/-/metrics)
  metric() { echo "$m" | awk -v k="$1" '$1 == k { print $2 }'; }
  disk=$(find /data/registry -type f -exec stat -c %s {} + | awk '{ s += $1 } END { print s + 0 }')
  awk -v l="$(metric rift_bucket_logical_bytes)" -v s="$(metric rift_bucket_stored_bytes)" -v d="$disk" \
    -v n="$(metric rift_compacted_blobs)" 'BEGIN {
    printf "The registry sees  %8.1f MB\n", l / 1e6
    printf "The bucket holds   %8.1f MB  by rift'"'"'s count\n", s / 1e6
    printf "                   %8.1f MB  in files on disk\n", d / 1e6
    if (l > 0) printf "Saved              %8.1f %%\n", 100 * (1 - s / l)
    printf "Blobs compacted    %8d\n", n
  }'
  ;;
esac
