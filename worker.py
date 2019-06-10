# coding: utf-8

import logging
import argparse
import asyncio
import json
import queue
from Mapper import Mapper
from Reducer import Reducer

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')

MAX_N_BYTES = 16
CHUNK = 1024

class Worker():

    def __init__(self, worker_id, host, port):
        self.host = host
        self.port = port
        self.worker_id = worker_id
        self.mapper = Mapper()
        self.reducer = Reducer()
        self.logger = logging.getLogger('worker ' + str(self.worker_id))
        self.logger.debug('Worker connecting to %s:%d', self.host, self.port)

    def parse_msg(self, msg):
        msg_len = len(msg)
        return '0'*(MAX_N_BYTES-len(str(int(msg_len)))) + str(msg_len) + msg

    def proccess_msg(self, msg):
        # msg = self.queue_in.get()
        if msg['task'] == 'map_request':
            # logger.debug('THIS IS A MAP REQ')
            result = self.mapper.map(msg['value'])
            reply = { 'task' : 'map_reply', 'value' : result }
            return reply
        elif msg['task'] == 'reduce_request':
            # logger.debug('THIS IS A REDUCE REQ')
            result = self.reducer.reduce(msg['value'])
            reply = { 'task' : 'reduce_reply', 'value' : result }
            return reply
        elif msg['task'] == 'done':
            pass
        else:
            self.logger.debug('THIS IS NOT FOR ME: %s', msg['task'])

    def register(self):
        message = { 'task' : 'register', 'id' : self.worker_id }
        return message

    async def tcp_echo_client(self, host, port, loop):
        self.logger.debug('Openning connection')
        reader, writer = await asyncio.open_connection(host, port, loop=loop) # open connection

        # register
        to_send = self.register() # register on first time
        msg_json = json.dumps(to_send)
        parsed_msg = self.parse_msg(msg_json)
        self.logger.info('Sending to: %s' % host)
        writer.write(parsed_msg.encode()) # send message
        await writer.drain()

        while True:

            # receive data
            try: 
                data = await reader.read(MAX_N_BYTES)
            except ConnectionResetError:
                await asyncio.sleep(3) # give the backup coordinator time to start 
                break

            if not data:
                await asyncio.sleep(3) # give the backup coordinator time to start 
                break

            # self.logger.info('Received (size of json str): %r ' % data.decode() )

            cur_size = 0  
            total_size = int(data.decode())
            final_str = ''

            while (total_size - cur_size) >= CHUNK :
                data = await reader.read(CHUNK)
                final_str = final_str + data.decode()
                cur_size += len(data)

            data = await reader.read(total_size - cur_size )
            final_str = final_str + data.decode()

            # self.logger.info('Received: %r ' % final_str['task'] )
            self.logger.info('Received from: %s ' % host )

            to_send = self.proccess_msg(json.loads(final_str)) # process message

            if to_send is not None:
                msg_json = json.dumps(to_send)
                parsed_msg = self.parse_msg(msg_json)
                self.logger.info('Sending to: %s' % host)
                writer.write(parsed_msg.encode()) # send message
                await writer.drain()

        self.logger.info('Close the socket')
        writer.close()

def main(args):
    worker = Worker(args.id, args.hostname, args.port)
    
    # message = 'Hello World'
    # worker.send(message)

    loop = asyncio.get_event_loop()

    while True:
        try:
            loop.run_until_complete(worker.tcp_echo_client(worker.host, worker.port, loop))
        except KeyboardInterrupt:
            break

    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--id', dest='id', type=int, help='worker id', default=0)
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)
