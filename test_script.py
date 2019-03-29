#!/usr/bin/env python

import os
import sys
import time
import queue
import asyncio
import threading
import subprocess
import collections

from loguru import logger


LogEntry = collections.namedtuple('LogEntry', ['node', 'line'])


NODE_STARTED_LOG = 'coop.rchain.node.NodeRuntime - Listening for traffic on rnode'
APPROVED_BLOCK_RECEIVED_LOG = 'Making a transition to ApprovedBlockRecievedHandler state.'


async def gen_node_log_lines(whiteblock_node_id):
    args = ['ssh', str(whiteblock_node_id), '--', 'tail', '--lines=+1', '--follow', '/output.log']
    proc = await asyncio.create_subprocess_exec('whiteblock', *args, stdout=asyncio.subprocess.PIPE)
    while True:
        data = await proc.stdout.readline()
        if len(data) == 0:
            break
        line = data.decode('ascii').rstrip()
        yield LogEntry(whiteblock_node_id, line)


async def serialize_node_log_lines(log_lines_queue, whiteblock_node_id):
    async for log_entry in gen_node_log_lines(whiteblock_node_id):
        await log_lines_queue.put(log_entry)


async def background_logs_queueing(log_lines_queue):
    await asyncio.gather(
        serialize_node_log_lines(log_lines_queue, 0),
        serialize_node_log_lines(log_lines_queue, 1),
        serialize_node_log_lines(log_lines_queue, 2),
        serialize_node_log_lines(log_lines_queue, 3),
        serialize_node_log_lines(log_lines_queue, 4),
    )


def whiteblock_build():
    image = 'rchainops/rnode:whiteblock'
    validator_nodes = 5
    total_nodes = validator_nodes + 1
    build_command = [
        'whiteblock',
        'build',
        '--blockchain=rchain',
        '--image={}'.format(image),
        '--nodes={}'.format(total_nodes),
        '--validators={}'.format(validator_nodes),
        '--cpus=0',
        '--memory=0',
        '--yes',
        '-o "command=/rchain/node/target/rnode-0.8.3.git07d2167a/usr/share/rnode/bin/rnode"',
    ]
    logger.info('COMMAND {}'.format(build_command))
    # assert os.system(' '.join(build_command)) == 0


async def all_nodes_ready(logs_gen):
    unstarted_nodes = set([0, 1, 2, 3, 4])
    async for log_entry in logs_gen:
        if APPROVED_BLOCK_RECEIVED_LOG in log_entry.line:
            try:
                unstarted_nodes.remove(log_entry.node)
            except KeyError:
                pass
        yield (log_entry, unstarted_nodes)


async def gen_serialized_logs(logs_queue):
    while True:
        log_entry = await logs_queue.get()
        yield log_entry


async def shell_out(command, args):
    proc = await asyncio.create_subprocess_exec('whiteblock', *args, stdout=asyncio.subprocess.PIPE)
    while True:
        data = await proc.stdout.readline()
        if len(data) == 0:
            break
        line = data.decode('ascii').rstrip()
        yield line


async def deploy(whiteblock_node_id):
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/rchain/node/target/rnode-0.8.3.git07d2167a/usr/share/rnode/bin/rnode',
        'deploy',
        '--from=0x1',
        '--phlo-limit=1000000',
        '--phlo-price=1',
        '--nonce=0',
        '/rchain/rholang/examples/dupe.rho',
    ]
    await shell_out('whiteblock', args)


async def propose(whiteblock_node_id):
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/rchain/node/target/rnode-0.8.3.git07d2167a/usr/share/rnode/bin/rnode',
        'propose',
    ]
    await shell_out('whiteblock', args)
    # XXX print output



async def async_main():
    whiteblock_build()

    logs_queue = asyncio.Queue(maxsize=1024)
    asyncio.get_event_loop().create_task(background_logs_queueing(logs_queue))

    logs_gen = gen_serialized_logs(logs_queue)
    logs_unstarted_nodes_gen = all_nodes_ready(logs_gen)

    async for (log_entry, unstarted_nodes) in logs_unstarted_nodes_gen:
        print(log_entry.node, log_entry.line, flush=True)
        if len(unstarted_nodes) == 0:
            # start proposing...

    return 0


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    sys.exit(loop.run_until_complete(async_main()))
