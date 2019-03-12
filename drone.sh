#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail


ssh-master () {
    ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no ${SSH_USERNAME}@${SSH_HOST} "$@"
}


main () {
    mkdir --parents ~/.ssh
    echo -n "$SSH_KEY_B64" | base64 --decode >~/.ssh/id_rsa
    chmod --recursive go-rwx ~/.ssh

    ssh-master rm --recursive --force whiteblock
    ssh-master git clone https://github.com/rchain/whiteblock.git
    ssh-master "cd whiteblock && ./whiteblock.sh"
}


main "$@"
