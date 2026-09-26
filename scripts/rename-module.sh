#!/usr/bin/env bash
# Rename the Go module path across the repository.
#
# Usage: scripts/rename-module.sh github.com/you/your-service
#
# After renaming, update the go_package option in api/greeter/v1/greeter.proto
# and regenerate the protobuf code (make proto).
set -euo pipefail

old="example.com/gosvc"
new="${1:?usage: rename-module.sh <new-module-path>}"

if [[ "$new" == "$old" ]]; then
  echo "module is already $old"
  exit 0
fi

grep -rl --exclude-dir=.git --exclude-dir=bin "$old" . | while read -r f; do
  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' "s|$old|$new|g" "$f"
  else
    sed -i "s|$old|$new|g" "$f"
  fi
done

go mod edit -module "$new"
echo "renamed module to $new"
echo "remember to update api/greeter/v1/greeter.proto and run: make proto"
