#!/usr/bin/env python

import sys
import json
import asyncio
import collections
from typing import (
    List,
    Any,
    Set,
    Dict,
    Tuple,
    TypeVar,
    Awaitable,
    AsyncGenerator,
)

import aiostream
import async_generator

from loguru import logger


class NonZeroExitCodeError(Exception):
    def __init__(self, command: List[str], exit_code: int, stdout: str, stderr: str) -> None:
        super().__init__()
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr

    def __repr__(self) -> str:
        args = [
            self.command,
            self.exit_code,
            self.stdout,
            self.stderr,
        ]
        return '{}({})'.format(self.__class__.__name__, ', '.join(repr(a) for a in args))


ElementType = TypeVar('ElementType')
LogEntry = collections.namedtuple('LogEntry', ['node', 'line'])


APPROVED_BLOCK_RECEIVED_LOG = 'Making a transition to Running state.'


NODE_NAME_FROM_ID = {
    -1: 'testDriver',
    0: 'bootstrap',
    1: 'validatorA',
    2: 'validatorB',
    3: 'validatorC',
    4: 'validatorD',
}

NODE_ID_FROM_NAME = dict((v, k) for (k, v) in NODE_NAME_FROM_ID.items())


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


async def detect_all_nodes_up(logs_gen: AsyncGenerator[LogEntry, None], nodes: List[int], all_nodes_ready_event: asyncio.Event) -> AsyncGenerator[LogEntry, None]:
    unstarted_nodes = set(nodes)
    async for log_entry in logs_gen:
        if APPROVED_BLOCK_RECEIVED_LOG in log_entry.line:
            try:
                unstarted_nodes.remove(log_entry.node)
                if len(unstarted_nodes) == 0:
                    all_nodes_ready_event.set()
            except KeyError:
                pass
        yield log_entry


async def logs_enqueuing_task(logs_queue: 'asyncio.Queue[LogEntry]', nodes: List[int], all_nodes_ready_event: asyncio.Event) -> None:
    node_logs_generators = [gen_node_log_lines(node_id) for node_id in nodes]
    logs_generator = race_generators(node_logs_generators)
    logs_generator = detect_all_nodes_up(logs_generator, nodes, all_nodes_ready_event)
    await enqueue_generator_elements(logs_generator, logs_queue)


async def gen_serialized_logs(logs_queue: 'asyncio.Queue[LogEntry]') -> AsyncGenerator[LogEntry, None]:
    while True:
        log_entry = await logs_queue.get()
        yield log_entry


async def logs_printing_task(logs_queue: 'asyncio.Queue[LogEntry]') -> None:
    async for log_entry in gen_serialized_logs(logs_queue):
        logger.info('{} {}'.format(NODE_NAME_FROM_ID[log_entry.node], log_entry.line))


async def whiteblock_build() -> None:
    # The container needs to have apt-get update && apt-get install -y openssh-server procps
    image = 'rchainops/rnode:whiteblock'
    validator_nodes = 4
    build_args = [
        'build',
        '--blockchain=rchain',
        '--image={}'.format(image),
        '--force-docker-pull',
        '--nodes={}'.format(1),
        '--validators={}'.format(validator_nodes),
        '--cpus=0',
        '--memory=0',
        '--yes',
        '-t"0;rchain.conf.mustache;/home/master/whiteblock/config/bootstrap.conf.mustache"',
        '-o',
        'command=/opt/docker/bin/rnode',
        '-tprivatekeys.json=/home/master/whiteblock/config/privatekeys.json',
        '-tpublickeys.json=/home/master/whiteblock/config/publickeys.json',
    ]
    await shell_out('whiteblock', build_args)


async def whiteblock_build_append() -> None:
    image = 'rchainops/rnode:whiteblock'
    validator_nodes = 4
    build_args = [
        'build',
        'append',
        '--blockchain=rchain',
        '--image={}'.format(image),
        '--nodes={}'.format(validator_nodes),
        '--cpus=0',
        '--memory=0',
        '-trchain.conf.mustache=/home/master/whiteblock/config/validator.conf.mustache',
        '-y',
        '-tprivatekeys.json=/home/master/whiteblock/config/privatekeys.json',
        '-tpublickeys.json=/home/master/whiteblock/config/publickeys.json',
    ]
    await shell_out('whiteblock', build_args)


async def shell_out(command: str, args: List[str]) -> Tuple[str, str]:
    logger.info('COMMAND {} {}'.format(command, args))
    proc = await asyncio.create_subprocess_exec(command, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    (stdout_bytes, stderr_bytes) = await proc.communicate()
    stdout_str = stdout_bytes.decode('ascii')
    stderr_str = stderr_bytes.decode('ascii')
    if proc.returncode != 0:
        raise NonZeroExitCodeError([command] + args, proc.returncode, stdout_str, stderr_str)
    return (stdout_str, stderr_str)


async def deploy(whiteblock_node_id: int, private_key: str) -> Tuple[str, str]:
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/opt/docker/bin/rnode',
        'deploy',
        '--phlo-limit=1000000',
        '--phlo-price=1',
        '--private-key={}'.format(private_key),
        '/opt/docker/examples/dupe.rho',
    ]
    return await shell_out('whiteblock', args)


async def propose(whiteblock_node_id: int) -> Tuple[str, str]:
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/opt/docker/bin/rnode',
        'propose',
    ]
    return await shell_out('whiteblock', args)


async def mvdag(whiteblock_node_id: int) -> Tuple[str, str]:
    args = [
        'ssh',
        str(whiteblock_node_id),
        '--',
        '/opt/docker/bin/rnode',
        'mvdag',
    ]
    return await shell_out('whiteblock', args)


def parse_mvdag(mvdag: str) -> Dict[str, Set[str]]:
    dag: Dict[str, Set[str]] = collections.defaultdict(set)
    for line in mvdag.splitlines():
        parent_hash, child_hash = line.split(' ')
        dag[parent_hash].add(child_hash)
    return dag


async def get_mvdag(whiteblock_node_id: int) -> Dict[str, Set[str]]:
    (stdout, _) = await mvdag(whiteblock_node_id)
    return parse_mvdag(stdout)



async def deploy_propose_from_node(whiteblock_node_id: int, private_key: str) -> None:
    for _ in range(10):
        (stdout_data, stderr_data) = await deploy(whiteblock_node_id, private_key)
        for line in stdout_data.splitlines():
            logger.info('STDOUT {}'.format(line))
        for line in stderr_data.splitlines():
            logger.info('STDERR {}'.format(line))
        (stdout_data, stderr_data) = await propose(whiteblock_node_id)
        for line in stdout_data.splitlines():
            logger.info('STDOUT {}'.format(line))
        for line in stderr_data.splitlines():
            logger.info('STDERR {}'.format(line))


async def deploy_propose(nodes: List[Tuple[int, str]]) -> None:
    coroutines = [deploy_propose_from_node(node_id, private_key) for (node_id, private_key) in nodes]
    await asyncio.gather(*coroutines)


@async_generator.asynccontextmanager
async def background_task(event_loop: asyncio.AbstractEventLoop, task_function: Awaitable[Any]) -> AsyncGenerator[None, None]:
    task = event_loop.create_task(task_function)
    try:
        yield
    except:
        task.cancel()
        raise
    else:
        task.cancel()


async def test_body(event_loop: asyncio.AbstractEventLoop) -> None:
    with open('config/privatekeys.json') as file:
        private_keys = json.load(file)

    await whiteblock_build()

    logs_queue: asyncio.Queue[LogEntry] = asyncio.Queue(maxsize=1024)
    validator_nodes = [
        NODE_ID_FROM_NAME['validatorA'],
        NODE_ID_FROM_NAME['validatorB'],
        NODE_ID_FROM_NAME['validatorC'],
        NODE_ID_FROM_NAME['validatorD'],
    ]

    validator_nodes_with_private_keys = list(zip(validator_nodes, private_keys))

    logger.info('Starting test body')
    async with background_task(event_loop, logs_printing_task(logs_queue)):
        bootstrap_node_ready_event = asyncio.Event()
        validator_nodes_ready_event = asyncio.Event()
        async with background_task(event_loop, logs_enqueuing_task(logs_queue, [NODE_ID_FROM_NAME['bootstrap']], bootstrap_node_ready_event)):
            logger.info('Waiting for the bootstrap node readiness')
            await bootstrap_node_ready_event.wait()
            logger.info('Adding validator nodes')
            await whiteblock_build_append()
            logger.info('Done adding validators')
            async with background_task(event_loop, logs_enqueuing_task(logs_queue, validator_nodes, validator_nodes_ready_event)):
                logger.info('Waiting for validators to be ready')
                await validator_nodes_ready_event.wait()
                logger.info('Starting propose loop')
                await deploy_propose(validator_nodes_with_private_keys)

                await asyncio.sleep(30)

                bootstrap_dag = await get_mvdag(NODE_ID_FROM_NAME['bootstrap'])
                validatorA_dag = await get_mvdag(NODE_ID_FROM_NAME['validatorA'])
                validatorB_dag = await get_mvdag(NODE_ID_FROM_NAME['validatorB'])
                validatorC_dag = await get_mvdag(NODE_ID_FROM_NAME['validatorC'])
                validatorD_dag = await get_mvdag(NODE_ID_FROM_NAME['validatorD'])

                assert bootstrap_dag == validatorA_dag
                assert bootstrap_dag == validatorB_dag
                assert bootstrap_dag == validatorC_dag
                assert bootstrap_dag == validatorD_dag


async def async_main(event_loop: asyncio.AbstractEventLoop) -> int:
    try:
        await test_body(event_loop)
    except Exception as e:
        if isinstance(e, NonZeroExitCodeError):
            logger.info(e.stdout)
            logger.info(e.stderr)
        logger.exception("Failure")
        return 1

    return 0


def test_parse_mvdag() -> None:
    input = """d5db034e82e10ee1037454a70737ac9e1a6f4900d28590776b5ccc5eef087312 a75e6ec04d42b3fa0a02160d0bd2d19cbe563016283f362eb114f19c0a2bbad7
a75e6ec04d42b3fa0a02160d0bd2d19cbe563016283f362eb114f19c0a2bbad7 9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8
3851ce1c5f7a26b444c45edde5cff7fae20aa5b90aa6ce882f058c7834d748d6 9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8
f591cea354b70a9c6b753d13d8912d7fd0219fd45b80f449a08431cb6b265ea2 9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8
9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8 b29aaeb2ae774bfa573c4e5e37bc84bbaa1616263fd83c820b0dd9a795a57907
879b1499c4bb5b8359559ab2a308ce76dd01ae1a3693f0edbdbf4a7126767d93 b29aaeb2ae774bfa573c4e5e37bc84bbaa1616263fd83c820b0dd9a795a57907
b52e9a808053703353a16ea85a4cda5820a2af115bad87b6cebfef03111f5541 b29aaeb2ae774bfa573c4e5e37bc84bbaa1616263fd83c820b0dd9a795a57907
b0880ca496258ebd0c8c36446ac7596681600e3ab90a9db44b464dd4767f5adf 9547694c620c3e78b39da3db3a2090aa863a0c1174686a4de105350f7d4e77f4"""

    output = parse_mvdag(input)

    assert output == {
        "d5db034e82e10ee1037454a70737ac9e1a6f4900d28590776b5ccc5eef087312": set(['a75e6ec04d42b3fa0a02160d0bd2d19cbe563016283f362eb114f19c0a2bbad7']),
        "a75e6ec04d42b3fa0a02160d0bd2d19cbe563016283f362eb114f19c0a2bbad7": set(['9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8']),
        "3851ce1c5f7a26b444c45edde5cff7fae20aa5b90aa6ce882f058c7834d748d6": set(['9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8']),
        "f591cea354b70a9c6b753d13d8912d7fd0219fd45b80f449a08431cb6b265ea2": set(['9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8']),
        "9fa2d387275ff5019c26809e6d6b2ef6a250090892e3b9269fa303d19db15ee8": set(['b29aaeb2ae774bfa573c4e5e37bc84bbaa1616263fd83c820b0dd9a795a57907']),
        "879b1499c4bb5b8359559ab2a308ce76dd01ae1a3693f0edbdbf4a7126767d93": set(['b29aaeb2ae774bfa573c4e5e37bc84bbaa1616263fd83c820b0dd9a795a57907']),
        "b52e9a808053703353a16ea85a4cda5820a2af115bad87b6cebfef03111f5541": set(['b29aaeb2ae774bfa573c4e5e37bc84bbaa1616263fd83c820b0dd9a795a57907']),
        "b0880ca496258ebd0c8c36446ac7596681600e3ab90a9db44b464dd4767f5adf": set(['9547694c620c3e78b39da3db3a2090aa863a0c1174686a4de105350f7d4e77f4']),
    }


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    sys.exit(event_loop.run_until_complete(async_main(event_loop)))
