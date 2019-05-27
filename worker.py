# coding: utf-8

import logging
import argparse
import asyncio
from Mapper import Mapper
from Reducer import Reducer

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker') 

class Worker():

    def __init__(self, host, port):
        self.host = host
        self.port = port
        mapper = Mapper()
        reducer = Reducer()
        logger.debug('Worker connecting to %s:%d', self.host, self.port)

    async def tcp_echo_client(self, message, host, port, loop):
        reader, writer = await asyncio.open_connection(host, port, loop=loop)

        print('Send: %r' % message)
        writer.write(message.encode())

        data = await reader.read(100)
        print('Received: %r' % data.decode())
        return data.decode()

        print('Close the socket')
        writer.close()	

def main(args):
    worker = Worker(args.hostname, args.port)
    
    message = 'Hello World'

    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.tcp_echo_client(message, worker.host, worker.port, loop))
    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--port', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str, help='coordinator hostname', default='localhost')
    args = parser.parse_args()
    
    main(args)

