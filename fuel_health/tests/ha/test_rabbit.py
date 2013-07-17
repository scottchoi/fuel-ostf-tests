from operator import eq
from nose.plugins.attrib import attr
from nose.tools import timed

from fuel_health.common.ssh import Client as SSHClient
from fuel_health.exceptions import SSHExecCommandFailed
from fuel_health.test import BaseTestCase



class RabbitSmokeTest(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls._clients = {}
        cls._queue = ''
        cls._controllers = cls.config.compute.controller_nodes
        cls._usr = cls.config.compute.controller_node_ssh_user
        cls._pwd = cls.config.compute.controller_node_ssh_password
        cls._key = cls.config.compute.controller_node_ssh_key_path
        cls._ssh_timeout = cls.config.compute.ssh_timeout

    @classmethod
    def tearDownClass(cls):
        pass

    @attr(type=['fuel', 'ha', 'non-destructive'])
    @timed(45.9)
    def test_rabbitmqctl_status(self):
        """Test verifies RabbitMQ has proper cluster structure
         is available from all the controllers"""
        def _format_output(output):
            """
            Internal function allows remove all the not valuable chars
            from the output
            """
            output = output.split('running_nodes,')[-1].split('...done.')[0]
            for char in ' {[]}\n\r':
                output = output.replace(char, '')
            return output.split(',')

        cmd = 'sudo rabbitmqctl cluster_status'
        nodes = []
        for node in self._controllers:
            output = ''
            error = ''
            try:
                output = _format_output(SSHClient(host=node,
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

            self.assertTrue(map(eq, sorted(node['clusters']), _aux_node_clusters))