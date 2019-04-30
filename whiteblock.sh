#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail


main () {
    ~/.local/bin/pipenv sync
    # whiteblock build stop || true
    ~/.local/bin/pipenv run env LOGURU_FORMAT="{message}" ./whiteblock/test_script.py
}


main "$@"
