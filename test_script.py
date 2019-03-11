#!/usr/bin/env python

import os
import sys
import logging


handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def main():
    image = 'rchain/rnode:latest'
    static_nodes = 1
    validator_nodes = 3
    total_num_nodes = validator_nodes + static_nodes

    build_command = [
        'whiteblock',
        'build',
        '--blockchain=rchain',
        '--image={}'.format(image),
        '--nodes={}'.format(total_num_nodes),
        '--validators={}'.format(validator_nodes),
        '--cpus=0',
        '--memory=0',
        '--yes',
    ]

    logging.info(build_command)
    os.system(' '.join(build_command))

    # TODO Deploy smart contract
    # os.system('-----')
    return 0


if __name__ == '__main__':
    sys.exit(main())
