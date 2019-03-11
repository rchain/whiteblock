# Test Script for RChain based on the test plan in this link:
# https://github.com/rchain/rchain/blob/dev/docs/whiteblock-test-plan.md
# This can be modifid later for a generic test automation

#!/usr/bin/env python
import sys
import os
import json
import time
import yaml
import logging
import subprocess
from optparse import OptionParser


handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


class TestPlanWorker:
    def __init__(self, test_name, test_plan_file, test_period):
        self.test_name = test_name
        self.input_file_name = test_plan_file
        self.test_plan_yaml = {}
        self.test_period = test_period

    def initialize(self):
        print "Test Plan:", self.test_name
        print "Initialize test plan..."
        with open(self.input_file_name, 'r') as stream:
            try:
                self.test_plan_yaml = yaml.load(stream)
                # print self.test_plan_yaml
                # print yaml.dump(test_plan, default_flow_style=False)
                # print json.dumps(self.test_plan_yaml, indent=4, sort_keys=True)

            except yaml.YAMLError as exc:
                print exc
                print "[ERROR] Yaml file parsing failed"
        print "Finished initialization. \n"


    # Setup the blockchain network based on the case info
    def setup_test_case(self, case_params):
        print "Case Params:", case_params
        validator = case_params['validator']
        static_nodes = case_params['static_nodes']
        total_num_nodes = validator + static_nodes
        contract = case_params['contract']
        bandwidth = case_params['bandwidth']
        network_latency = case_params['network_latency']
        packetloss = case_params['packetloss']

        build_command = [
            'whiteblock',
            'build',
            '--blockchain=rchain',
            '--nodes={}'.format(total_num_nodes),
            '--validators={}'.format(validator),
            '--cpus=0',
            '--memory=0',
            '--yes',
        ]

        logging.info(build_command)
        os.system(' '.join(build_command))

        netconfig_delay_command = ['whiteblock', 'netconfig', 'set', '--delay', str(network_latency)]
        logging.info(netconfig_delay_command)
        os.system(' '.join(netconfig_delay_command))

        netconfig_loss_command = ['whiteblock', 'netconfig', 'set', '--loss', str(packetloss)]
        logging.info(netconfig_loss_command)
        os.system(' '.join(netconfig_loss_command))

        netconfig_bandwidth_command = ['whiteblock', 'netconfig', 'set', '--bandwidth', bandwidth]
        logging.info(netconfig_bandwidth_command)
        os.system(' '.join(netconfig_bandwidth_command))

        # Deploy smart contract
        # os.system('-----')

        print "Test Period: " + str(self.test_period) + " seconds. " + " Testing ..."


    def reset_test_case(self, case_params):
        netconfig_clear_command = ['whiteblock', 'netconfig', 'clear']
        logging.info(netconfig_clear_command)
        os.system(' '.join(netconfig_clear_command))



    # run the file series by series
    def run(self):
        tests = self.test_plan_yaml
        # print tests
        for test in tests['test_series']:
            series_id = test.keys()[0]
            series_name = test[series_id]['name']
            print "Current Series: ", series_id, series_name, "\n"

            for case in test[series_id]['cases']:
                case_id = case.keys()[0]
                print "\nCurrent Case:", case_id
                case_params = case[case_id]
                self.setup_test_case(case_params)
                # Run each test for self.test_period seconds
                time.sleep(float(self.test_period))
                self.reset_test_case(case_params)


        print self.test_name, "Complete!!!"


if __name__ == '__main__':

    optparser = OptionParser()
    optparser.add_option('-f', '--inputFile', dest='input', help='filename containing yaml')
    optparser.add_option('-n', '--testName', dest='test_name', help='teat name', default="RChain Perfromance Test")
    optparser.add_option('-p', '--testPeriod', dest='test_period', help='teat period', default=300)

    (options, args) = optparser.parse_args()


    inFile = None
    errorFlag = 0
    errorMsg = ''
    if options.input is None:
        errorFlag = 1
        errorMsg = "Please use -f to specify test plan file"
    elif options.input is not None:
        # check if file exist and is the correct file type
        if (not os.path.isfile(options.input) or not options.input.endswith('.yaml')):
            errorFlag = 1
            errorMsg = "Unable to find test plan; Test plan should be in a yaml file \n"


    if errorFlag:
        print '[ERROR]' + errorMsg
        sys.exit('System will exit')
    else:
        inFile = options.input

    tp_workder = TestPlanWorker(options.test_name, inFile, options.test_period)
    tp_workder.initialize()
    tp_workder.run()
