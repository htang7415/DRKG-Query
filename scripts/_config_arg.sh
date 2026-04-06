#!/usr/bin/env bash

resolve_config_path() {
  local root="$1"
  shift

  local config_path="$root/config.yaml"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --config)
        if [[ $# -lt 2 || -z "${2:-}" ]]; then
          echo "missing value for --config; use '--config config.yaml' or '--config=config.yaml'" >&2
          return 1
        fi
        config_path="$2"
        shift 2
        ;;
      --config=*)
        config_path="${1#--config=}"
        if [[ -z "$config_path" ]]; then
          echo "missing value for --config; use '--config config.yaml' or '--config=config.yaml'" >&2
          return 1
        fi
        shift
        ;;
      *)
        shift
        ;;
    esac
  done

  if [[ "$config_path" != /* ]]; then
    if [[ -f "$PWD/$config_path" ]]; then
      config_path="$PWD/$config_path"
    elif [[ -f "$root/$config_path" ]]; then
      config_path="$root/$config_path"
    fi
  fi

  printf '%s\n' "$config_path"
}
