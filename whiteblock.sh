#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail


main () {
    ~/.local/bin/pipenv sync
    whiteblock build stop || true
    ~/.local/bin/pipenv run ./test_script.py
}


main "$@"
