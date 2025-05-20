#!/bin/bash

set -e

host="$1"
port="${2:-29092}"  # Default to 29092 if not specified
shift 2
cmd="$@"

until nc -z -v "$host" "$port"; do
  >&2 echo "Waiting for $host:$port to be ready..."
  sleep 2
done

>&2 echo "$host:$port is up - executing command"
exec $cmd 