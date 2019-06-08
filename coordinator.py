# coding: utf-8

import csv
import logging
import argparse
import asyncio
import queue
import time
import json


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')

start = time.time()
connectionsMap = {}
# queue_out = queue.Queue()

class Coordinator():
    def __init__(self, datastore, data_array):
        self.datastore = datastore
        self.connectionsMap = {}
        self.data_array = data_array  # array that stores results from mapping and reducing

    def proccess_msg(self, message_json):
        message = json.loads(message_json)
        if(message['task'] == 'register'):
            return
        self.data_array.put(message['value'])
        # return self.scheduler()

    def scheduler(self):
        #Tambem nao queremos fazer muito mais porque senao perdemos muito trabalho
        queueSize = self.data_array.qsize()
        if(len(self.datastore) > 0): # blobs available
            new_message = self.datastore.pop() # get blob from out queue
            result = {'task': 'map_request', 'value': new_message}
            return result
        elif(queueSize >= 2):
            if(queueSize % 2 == 0): #enviamos 2 sempre que pares
                new_message = []
                new_message.append(self.data_array.get())
                new_message.append(self.data_array.get())
                result = {'task': 'reduce_request', 'value': new_message}
                return result
            else: #enviamos 3 sempre que temos impares
                new_message = []
                new_message.append(self.data_array.get())
                new_message.append(self.data_array.get())
                new_message.append(self.data_array.get())
                result = {'task': 'reduce_request', 'value': new_message}
                return result
        else: # done
            end = time.time()
            logger.info('TIME TAKEN: %f (s)', end-start)
            result = {'task': 'done', 'value': 'done'}
            return result

    def parse_msg(self, msg):
        msg_len = len(msg)
        return '0'*(7-len(str(int(msg_len)))) + str(msg_len) + msg

    async def handle_echo(self, reader, writer):

        # # start by sending blobs in datastore
        # for blob in self.datastore:
        #     msg = {'task': 'map_request', 'value': blob}
        #     msg_json = json.dumps(msg)
        #     parsed_msg = self.parse_msg(msg_json)
        #     # logger.info('Sending to: %s ', addr )
        #     writer.write(parsed_msg.encode())

        # await writer.drain()

        while True:
            data = await reader.read(7)
            addr = writer.get_extra_info('peername')

            cur_size = 0
            total_size = int(data.decode())
            final_str = ''

            while (total_size - cur_size) >= 1024 :
                data = await reader.read(1024)
                final_str = final_str + data.decode()
                cur_size += len(data)

            data = await reader.read(total_size - cur_size)
            final_str = final_str + data.decode()

            # logger.info('Received: %r ' % final_str )
            logger.info('Received from: %s ', addr )

            message = final_str
            self.proccess_msg(message)

            to_send = self.scheduler()
            # logger.debug(to_send)
            if to_send is not None:
                connectionsMap[addr] = to_send

                msg_json = json.dumps(to_send)
                parsed_msg = self.parse_msg(msg_json)

                # logger.debug("Sending: %r " % parsed_msg)
                logger.info('Sending to: %s ', addr )

                # logger.info('CONNS MAP: %r' % connectionsMap)
                
                writer.write(parsed_msg.encode())
                await writer.drain()

        logger.debug("Close the client socket")
        writer.close()

def main(args):
    datastore = []
    data_array = queue.Queue()  # array that stores results from mapping and reducing

    # load txt file and divide it into blobs
    with args.file as f:
        while True:
            blob = f.read(args.blob_size)
            if not blob:
                break
            # This loop is used to not break word in half
            while not str.isspace(blob[-1]):
                ch = f.read(1)
                if not ch:
                    break
                blob += ch
            logger.debug('\nBlob: %s', blob)
            datastore.append(blob)
            start = time.time()

    logger.debug('Number of blobs: %s', len(datastore))

    coordinator = Coordinator(datastore, data_array)

    # queue_out.put(json.dumps({'task': 'map_request', 'value': blob}))

    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(coordinator.handle_echo, 'localhost', args.port, loop=loop)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

    hist = data_array.get()
    # store final histogram into a CSV file
    with args.out as f:
        csv_writer = csv.writer(f, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for w, c in hist:
            csv_writer.writerow([w, c])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int,
                        help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r',
                            encoding='UTF-8'), help='input file path')
    parser.add_argument('-o', dest='out', type=argparse.FileType('w',
                        encoding='UTF-8'), help='output file path', default='output.csv')
    parser.add_argument('-b', dest='blob_size', type=int,
                        help='blob size', default=1024)
    args = parser.parse_args()

    main(args)
