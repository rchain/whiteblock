#!/usr/bin/env python

import os
import sys
import time
import asyncio
import aiostream
import threading
import subprocess
import collections
from typing import (
    Set,
    List,
    Tuple,
    TypeVar,
    AsyncGenerator,
)

from loguru import logger


ElementType = TypeVar('ElementType')
LogEntry = collections.namedtuple('LogEntry', ['node', 'line'])


APPROVED_BLOCK_RECEIVED_LOG = 'Making a transition to ApprovedBlockRecievedHandler state.'


async def gen_node_log_lines(whiteblock_node_id: int) -> AsyncGenerator[LogEntry, None]:
    """Yield node logs lines by line
    """
    args = ['ssh', str(whiteblock_node_id), '--', 'tail', '--lines=+1', '--follow', '/output.log']
    proc = await asyncio.create_subprocess_exec('whiteblock', *args, stdout=asyncio.subprocess.PIPE)
    assert proc.stdout is not None
    while True:
        data = await proc.stdout.readline()
        if len(data) == 0:
            break
        line = data.decode('ascii').rstrip()
        yield LogEntry(whiteblock_node_id, line)


async def race_generators(generators: List[AsyncGenerator[ElementType, None]]) -> AsyncGenerator[ElementType, None]:
    merged = aiostream.stream.merge(*generators)
    async with merged.stream() as streamer:
        async for element in streamer:
            yield element


async def enqueue_generator_elements(gen: AsyncGenerator[ElementType, None], queue: 'asyncio.Queue[ElementType]') -> None:
    async for elem in gen:
        await queue.put(elem)


async def background_logs_queueing(logs_queue: 'asyncio.Queue[LogEntry]') -> None:
    node_logs_generators = [
        gen_node_log_lines(0),
        gen_node_log_lines(1),
        gen_node_log_lines(2),
        gen_node_log_lines(3),
        gen_node_log_lines(4),
    ]
    logs_generator = race_generators(node_logs_generators)
    enqueue_generator_elements(logs_generator, logs_queue)


def whiteblock_build() -> None:
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
    assert os.system(' '.join(build_command)) == 0


async def all_nodes_ready(logs_gen: AsyncGenerator[LogEntry, None]) -> AsyncGenerator[Tuple[LogEntry, Set[int]], None]:
    unstarted_nodes = set([0, 1, 2, 3, 4])
    async for log_entry in logs_gen:
        if APPROVED_BLOCK_RECEIVED_LOG in log_entry.line:
            try:
                unstarted_nodes.remove(log_entry.node)
            except KeyError:
                pass
        yield (log_entry, unstarted_nodes)


async def gen_serialized_logs(logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[LogEntry, None]:
    while True:
        log_entry = await logs_queue.get()
        yield log_entry


async def shell_out(command: str, args: List[str]) -> AsyncGenerator[str, None]:
    print(command, args, flush=True)
    proc = await asyncio.create_subprocess_exec('whiteblock', *args, stdout=asyncio.subprocess.PIPE)
    assert proc.stdout is not None
    while True:
        data = await proc.stdout.readline()
        if len(data) == 0:
            break
        line = data.decode('ascii').rstrip()
        print('out: ', line, flush=True)
        yield line


async def deploy(whiteblock_node_id: int) -> AsyncGenerator[str, None]:
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
    async for line in shell_out('whiteblock', args):
        yield line


async def propose(whiteblock_node_id: int) -> AsyncGenerator[str, None]:
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/rchain/node/target/rnode-0.8.3.git07d2167a/usr/share/rnode/bin/rnode',
        'propose',
    ]
    async for line in shell_out('whiteblock', args):
        yield line


async def propose_loop(whiteblock_node_id: int) -> AsyncGenerator[LogEntry, None]:
    for _ in range(200):
        async for line in deploy(whiteblock_node_id):
            log_entry = LogEntry(whiteblock_node_id, line)
            yield log_entry
        async for line in propose(whiteblock_node_id):
            log_entry = LogEntry(whiteblock_node_id, line)
            yield log_entry


async def background_proposing(logs_queue: 'asyncio.Queue[LogEntry]') -> None:
    propose_logs_generators = [
        propose_loop(0),
        propose_loop(1),
        propose_loop(2),
        propose_loop(3),
        propose_loop(4),
    ]
    logs_generator = race_generators(propose_logs_generators)
    enqueue_generator_elements(logs_generator, logs_queue)


async def async_main() -> int:
    whiteblock_build()

    logs_queue: asyncio.Queue[LogEntry] = asyncio.Queue(maxsize=1024)
    asyncio.get_event_loop().create_task(background_logs_queueing(logs_queue))

    logs_gen = gen_serialized_logs(logs_queue)
    logs_unstarted_nodes_gen = all_nodes_ready(logs_gen)

    proposing_task = None
    async for (log_entry, unstarted_nodes) in logs_unstarted_nodes_gen:
        print(log_entry.node, log_entry.line, unstarted_nodes, flush=True)
        if len(unstarted_nodes) == 0 and proposing_task is None:
            proposing_task = asyncio.get_event_loop().create_task(background_proposing(logs_queue))

    return 0


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    sys.exit(loop.run_until_complete(async_main()))
