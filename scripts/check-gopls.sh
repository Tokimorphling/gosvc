#!/usr/bin/env bash
# Scan handwritten Go sources in both modules with the pinned editor analyzer.
# gopls check prints diagnostics but can still exit 0, so inspect its output too.
set -euo pipefail

repo_root=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
gopls_version=${GOPLS_VERSION:-v0.23.0}
report=$(mktemp "${TMPDIR:-/tmp}/gosvc-gopls.XXXXXX")
trap 'rm -f "$report"' EXIT

root_files=()
kitex_files=()
while IFS= read -r -d '' relative; do
  source_file="$repo_root/$relative"
  # A tracked file may have been deleted without being staged yet.
  if [ ! -f "$source_file" ]; then continue; fi
  # Go's generated-code marker belongs before the package clause. Checking
  # only this prefix avoids treating embedded source templates as generated.
  if awk '
    /^[[:space:]]*package[[:space:]]/ { exit }
    /^\/\/ Code generated .*DO NOT EDIT\.$/ { generated = 1 }
    END { exit !generated }
  ' "$source_file"; then
    continue
  fi
  case "$relative" in
    examples/kitex/*) kitex_files+=("$source_file") ;;
    *) root_files+=("$source_file") ;;
  esac
done < <(git -C "$repo_root" ls-files --cached --others --exclude-standard -z -- '*.go')

check_module() {
  local directory=$1
  shift
  if [ "$#" -eq 0 ]; then return; fi
  if ! (cd "$directory" && go run "golang.org/x/tools/gopls@$gopls_version" check -severity=hint "$@") >"$report" 2>&1; then
    cat "$report"
    return 1
  fi
  cat "$report"
  if grep -Eq ':[0-9]+:[0-9]+(-[0-9]+(:[0-9]+)?)?: ' "$report"; then
    return 1
  fi
  printf 'gopls: checked %s (%d source files)\n' "$directory" "$#"
}

check_module "$repo_root" "${root_files[@]}"
check_module "$repo_root/examples/kitex" "${kitex_files[@]}"
