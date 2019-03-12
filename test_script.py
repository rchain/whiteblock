#!/usr/bin/env python

import os
import sys

from loguru import logger


def main():
    image = 'rchainops/rnode:whiteblock'
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

    logger.info(build_command)
    # os.system(' '.join(build_command))

    # TODO Deploy smart contract
    # os.system('-----')
    return 0


if __name__ == '__main__':
    sys.exit(main())
