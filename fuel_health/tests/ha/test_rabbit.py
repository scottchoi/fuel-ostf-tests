from operator import eq
from nose.plugins.attrib import attr
from nose.tools import timed

from fuel_health.common.ssh import Client as SSHClient
from fuel_health.exceptions import SSHExecCommandFailed
from fuel_health.test import BaseTestCase



class RabbitSmokeTest(BaseTestCase):
    """
    TestClass contains tests check RabbitMQ.
    Special requirements:
            1. A controllers' IPs should be specified in
                controller_nodes parameter of the config file.
            2. The controllers' domain names should be specified in
                controller_nodes_name parameter of the config file.
            3. SSH user credentials should be specified in
                controller_node_ssh_user/password parameters
                of the config file.
            4. List of services are expected to be run should be specified in
                enabled_services parameter of the config file.
            5. SSH user should have root permissions on controllers
    """

    @classmethod
    def setUpClass(cls):
        cls._clients = {}
        cls._queue = ''
        cls._controllers = cls.config.compute.controller_nodes
        cls._usr = cls.config.compute.controller_node_ssh_user
        cls._pwd = cls.config.compute.controller_node_ssh_password
        cls._key = cls.config.compute.controller_node_ssh_key_path
        cls._ssh_timeout = cls.config.compute.ssh_timeout

    def _format_output(self, output):
            """
            Internal function allows remove all the not valuable chars
            from the output
            """
            output = output.split('running_nodes,')[-1].split('...done.')[0]
            for char in ' {[]}\n\r':
                output = output.replace(char, '')
            return output.split(',')

    @attr(type=['fuel', 'ha', 'non-destructive'])
    @timed(60.0)
    def test_rabbitmqctl_status(self):
        """Test verifies RabbitMQ has proper cluster structure
         is available from all the controllers"""

        cmd = 'sudo rabbitmqctl cluster_status'
        nodes = []
        for node in self._controllers:
            output = ''
            error = ''
            try:
                output = self._format_output(SSHClient(host=node,
                                   username=self._usr,
                                   password=self._pwd,
                                   pkey=self._key,
                                   timeout=self._ssh_timeout).exec_command(
                    cmd))
            except SSHExecCommandFailed as exc:
                self.fail(("Cannot get cluster status for %s node. "
                           "The following error occurs: " % node) +
                          exc._error_string)

            nodes.append({'controller': node, 'clusters': output})
        #for HA configuration number of controllers and
        #number of clusters should be the same
        _num_of_controllers = len(self._controllers)
        _aux_node_clusters = sorted(nodes[0]['clusters'])
        for node in nodes[1:]:
            self.assertEqual(_num_of_controllers, len(node["clusters"]),
                             'The number of clusters for node %s is not equal '
                             'to number of controllers' % node['controller'])

            self.assertTrue(map(eq, sorted(node['clusters']),
                                _aux_node_clusters), "Cluster lists on nodes %s"
                                                     " and %s are different" %
                                                     (nodes[0]["controller"],
                                                      node["controller"]))

    @attr(type=['fuel', 'ha', 'non-destructive'])
    @timed(60.0)
    def test_rabbit_queues(self):
        """Test verifies RabbitMQ has proper queue list
         are available from all the controllers"""
        cmd = 'sudo rabbitmqctl list_queues'
        temp_set = set()
        get_name = lambda x: x.split('\t')[0]
        for node in self._controllers:
            try:
                output = SSHClient(host=node,
                                   username=self._usr,
                                   password=self._pwd,
                                   pkey=self._key,
                                   timeout=self._ssh_timeout).exec_command(
                    cmd).splitlines()
            except SSHExecCommandFailed as exc:
                self.fail(("Cannot get queue list for %s node. "
                           "The following error occurs: " % node) +
                          exc._error_string)
            output = [get_name(x) for x in output[1:-1]]
            output = set(output)
            if len(temp_set) == 0:
                #this means it is the first node,
                #this case we check there are queues only
                temp_set = output
                self.assertTrue(len(output), "Queue list for %s controller is empty" % node)
                continue
            #check all the queues are present on all the nodes
            self.assertEqual(len(output.symmetric_difference(temp_set)), 0,
                             "Queue lists are different for %s and %s "
                             "controllers" % (self._controllers[0], node))