import yaml

from fuel_health.common.ssh import Client as SSHClient


class AmqpClient(object):
    def __init__(self, host, user, password, key, timeout):

        self.host = host
        self.username = user
        self.password = password
        self.key = key
        self.timeout = timeout
        self.rabbit_username, self.rabbit_password = \
            self._get_rabbit_credentials(
                "/var/lib/puppet/yaml/facts/controller-1.domain.tld.yaml")
        ssh = SSHClient(host=host,
                        username=self.username,
                        password=self.password,
                        key_filename=self.key,
                        timeout=self.timeout)

    def list_queues(self, queue_name):
        return self.ssh.exec_command(self.get("queues"))

    def create_queue(self, queue_name):
        return self.ssh.exec_command(
            self._put("queues/%2f/newAAAAAAAAAA -d {}"))

    def send_message(self, queue_name, message):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host,
                                      credentials=self.credentials))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, passive=True)
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=message)
        connection.close()

    def receive_message(self, queue_name):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host,
                                      credentials=self.credentials))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, passive=True)
        method_frame, _, message = channel.basic_get(queue=queue_name)
        if method_frame is None:
            connection.close()
            return None
        else:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            connection.close()
            return message

    def empty_queue(self, queue_name):
        message = 'Something'
        while message:
            message = self.receive_message(queue_name)

    def close(self, queue_name):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host,
                                      credentials=self.credentials))
        channel = connection.channel()
        try:
            channel.queue_delete(queue=queue_name)
        except pika.exceptions.AMQPChannelError:
            pass

    def _get_rabbit_credentials(self, path):
        conf = open(path)
        dataMap = yaml.safe_load(conf)
        conf.close()
        usr = 'nova'
        pwd = dataMap['values']['rabbit']['password']
        return (usr, pwd)

    def _get(self, addition):
        return 'curl -i -u %s:%s http://localhost:55672/api/%s' % \
               self.rabbit_username, \
               self.rabbit_password, \
               addition
    def _put(self, addition):
        return 'curl -i -u %s:%s -H "content-type:application/json" ' + \
               '-XPUT http://localhost:55672/api/%s' \
               % self.rabbit_username, self.rabbit_password, addition