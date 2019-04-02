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


TEST_DRIVER_NODE = -1


NODE_NAME_FROM_ID = {
    -1: 'testDriver',
    0: 'bootstrap',
    1: 'validatorA',
    2: 'validatorB',
    3: 'validatorC',
    4: 'validatorD',
}


def get_nodes_ids() -> List[int]:
    return [node_id for node_id in NODE_NAME_FROM_ID.keys() if node_id >= 0]


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


async def detect_all_nodes_up(logs_gen: AsyncGenerator[LogEntry, None], all_nodes_ready_event: asyncio.Event) -> AsyncGenerator[LogEntry, None]:
    unstarted_nodes = set(get_nodes_ids())
    async for log_entry in logs_gen:
        if APPROVED_BLOCK_RECEIVED_LOG in log_entry.line:
            try:
                unstarted_nodes.remove(log_entry.node)
                if len(unstarted_nodes) == 0:
                    all_nodes_ready_event.set()
            except KeyError:
                pass
        yield log_entry


async def logs_enqueuing_task(logs_queue: 'asyncio.Queue[LogEntry]', all_nodes_ready_event: asyncio.Event) -> None:
    node_logs_generators = [gen_node_log_lines(node_id) for node_id in get_nodes_ids()]
    logs_generator = race_generators(node_logs_generators)
    logs_generator = detect_all_nodes_up(logs_generator, all_nodes_ready_event)
    await enqueue_generator_elements(logs_generator, logs_queue)


async def gen_serialized_logs(logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[LogEntry, None]:
    while True:
        log_entry = await logs_queue.get()
        yield log_entry


async def logs_printing_task(logs_queue: 'asyncio.Queue[LogEntry]') -> None:
    async for log_entry in gen_serialized_logs(logs_queue):
        logger.info('{} {}'.format(NODE_NAME_FROM_ID[log_entry.node], log_entry.line))


async def whiteblock_build(logs_queue: 'asyncio.Queue[LogEntry]') -> None:
    image = 'rchainops/rnode:whiteblock'
    validator_nodes = 5
    total_nodes = validator_nodes + 1
    build_args = [
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
    async for line in shell_out('whiteblock', build_args, logs_queue):
        log_entry = LogEntry(node=TEST_DRIVER_NODE, line=line)
        await logs_queue.put(log_entry)


async def shell_out(command: str, args: List[str], logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[str, None]:
    log_entry = LogEntry(node=TEST_DRIVER_NODE, line='COMMAND {} {}'.format(command, args))
    await logs_queue.put(log_entry)
    proc = await asyncio.create_subprocess_exec(command, *args, stdout=asyncio.subprocess.PIPE)
    assert proc.stdout is not None
    while True:
        data = await proc.stdout.readline()
        if len(data) == 0:
            break
        line = data.decode('ascii').rstrip()
        yield line


async def deploy(whiteblock_node_id: int, logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[str, None]:
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
    async for line in shell_out('whiteblock', args, logs_queue):
        yield line


async def propose(whiteblock_node_id: int, logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[str, None]:
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/rchain/node/target/rnode-0.8.3.git07d2167a/usr/share/rnode/bin/rnode',
        'propose',
    ]
    async for line in shell_out('whiteblock', args, logs_queue):
        yield line


async def deploy_propose_round(whiteblock_node_id: int, logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[LogEntry, None]:
    for _ in range(200):
        async for line in deploy(whiteblock_node_id, logs_queue):
            log_entry = LogEntry(whiteblock_node_id, line)
            yield log_entry
        async for line in propose(whiteblock_node_id, logs_queue):
            log_entry = LogEntry(whiteblock_node_id, line)
            yield log_entry


async def deploy_propose(logs_queue: 'asyncio.Queue[LogEntry]') -> None:
    propose_logs_generators = [deploy_propose_round(node_id, logs_queue) for node_id in get_nodes_ids()]
    logs_generator = race_generators(propose_logs_generators)
    await enqueue_generator_elements(logs_generator, logs_queue)


async def async_main(event_loop: asyncio.AbstractEventLoop) -> int:
    logs_queue: asyncio.Queue[LogEntry] = asyncio.Queue(maxsize=1024)
    background_logs_printing_task = event_loop.create_task(logs_printing_task(logs_queue))

    await whiteblock_build(logs_queue)

    all_nodes_ready_event = asyncio.Event()
    background_logs_enqueuing_task = event_loop.create_task(logs_enqueuing_task(logs_queue, all_nodes_ready_event))
    await all_nodes_ready_event.wait()

    await deploy_propose(logs_queue)

    background_logs_printing_task.cancel()
    await background_logs_printing_task

    background_logs_enqueuing_task.cancel()
    await background_logs_enqueuing_task

    return 0


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    sys.exit(event_loop.run_until_complete(async_main(event_loop)))
