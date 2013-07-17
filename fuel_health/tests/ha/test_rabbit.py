from operator import eq

from fuel_health.common.amqp_client import AmqpClient
from fuel_health.common.amqp_client import AmqpEx
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
        #cls._key = cls.config.compute.controller_node_ssh_key_path
        cls._ssh_timeout = cls.config.compute.ssh_timeout

    @classmethod
    def tearDownClass(cls):
        pass

    def test_rabbitmqctl_status(self):
        def _format_output(output):
            """
            Internal function allows remove all the not valuable chars
            from the output
            """
            output = output.split('running_nodes,')[-1].split('...done.')[0]
            for char in '{[]}\n':
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
                                   #pkey=self._key,
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





        # temp_set = set()
        # for i in self.clients.keys():
        #     output = self.clients[i]['ssh'].exec_command(command)
        #     output = output.split('running_nodes,')[-1].split('...done.')[0]
        #     for char in '{[]} ':
        #         output = output.replace(char, '')
        #     output = output.replace('\n', '')
        #     output = output.split(',')
        #     output = set(output)
        #     if len(temp_set) == 0:
        #         temp_set = output
        #
        #     self.assertEqual(len(output.symmetric_difference(temp_set)), 0,
        #                      "Cluster's nodes lists are different")
        #     self.assertEqual(len(output), len(self.clients.keys()),
        #                      "Count of nodes in cluster less than"
        #                      " count of controllers.")

    def _rabbit_ha_messages(self):
        self.queue = 'Test_queue'
        ips = self.clients.keys()
        message = 'Test Message ' + str(randint(100000, 999999))
        try:
            self.clients[ips[0]]['rabbit'].create_queue(self.queue)
        except AmqpEx.AMQPConnectionError:
            self.fail("Can not create queue")

        for i in ips:
            try:
                self.clients[ips[0]]['rabbit'].send_message(self.queue, message)
            except AmqpEx.AMQPConnectionError:
                self.fail("Cannot send message to %s" % str(i))

        for i in ips:
            try:
                mes = self.clients[i]['rabbit'].receive_message(self.queue)
            except AmqpEx.AMQPConnectionError:
                self.fail("Cannot receive message from %s" % str(i))

            self.assertEqual(mes, message,
                             "Received message is different from the sent")

    def _rabbit_queues(self):
        command = 'rabbitmqctl list_queues'
        temp_set = set()
        get_name = lambda x: x.split('\t')[0]
        for i in self.clients.keys():
            output = self.clients[i]['ssh'].exec_command(command).splitlines()
            output = output[1:-1]
            output = [get_name(x) for x in output]
            output = set(output)
            if len(temp_set) == 0:
                temp_set = output
            self.assertEqual(len(output.symmetric_difference(temp_set)), 0,
                             "Queues lists are different")
