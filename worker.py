# coding: utf-8

import logging
import argparse
import asyncio
import json
import queue
from Mapper import Mapper
from Reducer import Reducer

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker') 

class Worker():

    def __init__(self, worker_id, host, port):
        self.host = host
        self.port = port
        self.worker_id = worker_id
        mapper = Mapper()
        reducer = Reducer()
        self.queue_in = queue.Queue()
        self.queue_out = queue.Queue()
        logger.debug('Worker connecting to %s:%d', self.host, self.port)
        self.register() # register on first time

    def send(self, message):
        self.queue_out.put(json.dumps(message))

    def receive(self, message):
        self.queue_in.put(json.loads(message))

    def proccess_msg(self):
        pass

    def register(self):
        message = { 'task' : 'register', 'id' : self.worker_id }
        self.send(message)

    async def tcp_echo_client(self, host, port, loop):
        reader, writer = await asyncio.open_connection(host, port, loop=loop) # open connection

        while True:
            message_json = self.queue_out.get() # get message from out queue

            print('Send: %r' % message_json)
            writer.write(message_json.encode()) # send message

            data = await reader.read(100)
            print('Received: %r' % data.decode())
            self.receive(data.decode()) # receive message

            self.proccess_msg() # process message

        print('Close the socket')
        writer.close()

def main(args):
    worker = Worker(1, args.hostname, args.port)
    
    message = 'Hello World'
    worker.send(message)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.tcp_echo_client(worker.host, worker.port, loop))

    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)

