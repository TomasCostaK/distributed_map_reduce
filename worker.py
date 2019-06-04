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
        self.mapper = Mapper()
        self.reducer = Reducer()
        self.queue_in = queue.Queue()
        self.queue_out = queue.Queue()
        logger.debug('Worker connecting to %s:%d', self.host, self.port)
        self.register() # register on first time

    def parse_msg(self, msg):
        msg_len = len(msg)
        if msg_len < 10:
            return '00000' + str(msg_len) + msg
        elif msg_len < 100:
            return '0000' + str(msg_len) + msg
        elif msg_len < 1000:
            return '000' + str(msg_len) + msg
        elif msg_len < 10000:
            return '00' + str(msg_len) + msg
        elif msg_len < 100000:
            return '0' + str(msg_len) + msg
        else:
            return str(msg_len) + msg

    def send(self, message):
        self.queue_out.put(json.dumps(message))

    def receive(self, message):
        self.queue_in.put(json.loads(message))

    def proccess_msg(self):
        msg = self.queue_in.get()
        if msg['task'] == 'map_request':
            # logger.debug('THIS IS A MAP REQ')
            result = self.mapper.map(msg['value'])
            self.send({ 'task' : 'map_reply', 'value' : result })
        elif msg['task'] == 'reduce_request':
            # logger.debug('THIS IS A REDUCE REQ')
            result = self.reducer.reduce(msg['value'])
            self.send({ 'task' : 'reduce_reply', 'value' : result })
        # logger.debug(result)

    def register(self):
        message = { 'task' : 'register', 'id' : self.worker_id }
        self.send(message)

    async def tcp_echo_client(self, host, port, loop):
        reader, writer = await asyncio.open_connection(host, port, loop=loop) # open connection

        while True:
            message_json = self.queue_out.get() # get message from out queue
            parsed_msg = self.parse_msg(message_json)

            logger.info('Sending: %r' % parsed_msg)
            writer.write(parsed_msg.encode()) # send message

            data = await reader.read(6)
            logger.info('Received (size of json str): %r ' % data.decode() )

            data = await reader.read(int(data.decode()))
            logger.info('Received: %r ' % data.decode() )

            self.receive(data.decode()) # receive message

            self.proccess_msg() # process message

        logger.info('Close the socket')
        writer.close()

def main(args):
    worker = Worker(1, args.hostname, args.port)
    
    # message = 'Hello World'
    # worker.send(message)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.tcp_echo_client(worker.host, worker.port, loop))

    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--id', dest='id', type=int, help='worker id', default=0)
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)
