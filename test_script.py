#!/usr/bin/env python

import os
import sys
import time
import queue
import threading
import subprocess

from loguru import logger


class LoggingThread(threading.Thread):
    def __init__(self, node_label, whiteblock_node_id, log_lines_queue, terminate_thread_event):
        super().__init__(name='{}.logging'.format(node_label))
        self.whiteblock_node_id = whiteblock_node_id
        self.node_label = node_label
        self.log_lines_queue = log_lines_queue
        self.terminate_thread_event = terminate_thread_event

    def run(self):
        command = ['whiteblock', 'get', 'log', '--tail=-1', str(self.whiteblock_node_id)]
        logger.info('COMMAND {}'.format(command))
        with subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                try:
                    while True:
                        if self.terminate_thread_event.is_set():
                            break
                        line = next(p.stdout).rstrip()
                        self.log_lines_queue.put('{} {}'.format(self.node_label, line))
                except StopIteration:
                    pass


class ProposingThread(threading.Thread):
    def __init__(self, node_label, whiteblock_node_id, log_lines_queue, finished_proposing_event, test_failed_event):
        super().__init__(name='{}.proposing'.format(node_label))
        self.whiteblock_node_id = whiteblock_node_id
        self.log_lines_queue = log_lines_queue
        self.finished_proposing_event = finished_proposing_event
        self.test_failed_event = test_failed_event

    def shell_out(self, command):
        logger.info('COMMAND {}'.format(command))
        deploy_result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        for line in deploy_result.stdout.splitlines():
            self.log_lines_queue.put(line)
        for line in deploy_result.stderr.splitlines():
            self.log_lines_queue.put(line)

    def deploy(self):
        deploy_command = [
            'whiteblock',
            'ssh',
            str(self.whiteblock_node_id),
            '--',
            '/opt/docker/bin/rnode',
            'deploy',
            '--from=0x1',
            '--phlo-limit=1000000',
            '--phlo-price=1',
            '--nonce=0',
            '/opt/docker/examples/dupe.rho',
        ]
        self.shell_out(deploy_command)

    def propose(self):
        propose_command = [
            'whiteblock',
            'ssh',
            str(self.whiteblock_node_id),
            '--',
            '/opt/docker/bin/rnode',
            'propose',
        ]
        self.shell_out(propose_command)

    def run(self):
        try:
            for _ in range(100):
                self.deploy()
                self.propose()
                time.sleep(30)
        except:
            self.test_failed_event.set()
            logger.exception("")
        finally:
            self.finished_proposing_event.set()


class Node:
    def __init__(self, node_label, whiteblock_node_id, log_lines_queue, terminate_logging_event, test_failed_event):
        self.whiteblock_node_id = whiteblock_node_id
        self.terminate_logging_event = terminate_logging_event
        self.logging_thread = LoggingThread(node_label, whiteblock_node_id, log_lines_queue, terminate_logging_event)
        self.finished_proposing_event = threading.Event()
        self.proposing_thread = ProposingThread(node_label, whiteblock_node_id, log_lines_queue, self.finished_proposing_event, test_failed_event)

    def start(self):
        self.logging_thread.start()
        self.proposing_thread.start()

    def has_finished_proposing(self):
        return self.finished_proposing_event.is_set()

    def join_proposing_thread(self):
        self.proposing_thread.join()

    def join_logging_thread(self):
        self.logging_thread.join()


def start_nodes(nodes):
    result = []
    for n in nodes:
        n.start()
        result.append(n)
    return result


def without_finished_nodes(running_nodes):
    indexes_to_delete = []

    for i, node in enumerate(running_nodes):
        if node.has_finished_proposing():
            indexes_to_delete.append(i)
            node.join_proposing_thread()

    if indexes_to_delete:
        return [node for index, node in enumerate(running_nodes) if index not in indexes_to_delete]

    return running_nodes


def main():
    image = 'rchainops/rnode:whiteblock'
    validator_nodes = 5
    build_command = [
        'whiteblock',
        'build',
        '--blockchain=rchain',
        '--image={}'.format(image),
        '--nodes={}'.format(validator_nodes + 1),
        '--validators={}'.format(validator_nodes),
        '--cpus=0',
        '--memory=0',
        '--yes',
        '-o "command=/rchain/node/target/rnode-0.8.3.git07d2167a/usr/share/rnode/bin/rnode"',
    ]
    logger.info('COMMAND {}'.format(build_command))
    # assert os.system(' '.join(build_command)) == 0

    log_lines_queue = queue.Queue(maxsize=1024)
    terminate_logging_event = threading.Event()
    test_failed_event = threading.Event()

    nodes = [
        Node('validatorA', 1, log_lines_queue, terminate_logging_event, test_failed_event),
        Node('validatorB', 2, log_lines_queue, terminate_logging_event, test_failed_event),
        Node('validatorC', 3, log_lines_queue, terminate_logging_event, test_failed_event),
        Node('validatorD', 4, log_lines_queue, terminate_logging_event, test_failed_event),
        Node('validatorE', 5, log_lines_queue, terminate_logging_event, test_failed_event),
    ]

    running_nodes = start_nodes(nodes)
    while True:
        if len(running_nodes) == 0:
            break
        try:
            log_line = log_lines_queue.get(block=True, timeout=1)
            logger.info(log_line)
        except queue.Empty:
            running_nodes = without_finished_nodes(running_nodes)
    terminate_logging_event.set()

    for n in nodes:
        n.join_logging_thread()
        pass

    return test_failed_event.is_set()


if __name__ == '__main__':
    sys.exit(main())
