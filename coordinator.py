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

queue_out = queue.Queue()
data_array = queue.Queue()  # array that stores results from mapping and reducing

# def split_data(string):
#     max_size = 1024
#     header = { 'type' : 'json_msg', 'size' : len(string), 'value' : '' }
#     header_len = len(json.dumps(header))
#     max_chunk_size = max_size - header_len
#     print(max_chunk_size)
#     l = []
#     for i in range(0, len(string), max_chunk_size):
#         msg = { 'type' : 'json_msg', 'size' : len(string), 'value' : string[i:i+max_chunk_size] }
#         l.append(json.dumps(msg))
#         print(len(json.dumps(msg)))
#         assert len(json.dumps(msg)) == 1024
#     return l


def proccess_msg(message_json):
    message = json.loads(message_json)
    if(message['task'] == 'register'):
        return
    data_array.put(message['value'])
    get_new_msg()


def get_new_msg():
    if(data_array.qsize() >= 2):
        new_message = []
        new_message.append(data_array.get())
        new_message.append(data_array.get())
        queue_out.put(json.dumps(
            {'task': 'reduce_request', 'value': new_message}))
    else:
        logger.info('REDUCE COMPLETED')


def parse_msg(msg):
    msg_len = len(msg)

    #return '0'*(7-msg_len) + str(msg_len) + msg

    if msg_len < 10:
        return '000000' + str(msg_len) + msg
    elif msg_len < 100:
        return '00000' + str(msg_len) + msg
    elif msg_len < 1000:
        return '0000' + str(msg_len) + msg
    elif msg_len < 10000:
        return '000' + str(msg_len) + msg
    elif msg_len < 100000:
        return '00' + str(msg_len) + msg
    elif msg_len < 1000000:
        return '0' + str(msg_len) + msg
    # elif msg_len < 10000:
    #     return '00' + str(msg_len) + msg
    # elif msg_len < 100000:
    #     return '0' + str(msg_len) + msg
    else:
        return str(msg_len) + msg

    


async def handle_echo(reader, writer):
    while True:
        start = time.time()
        data = await reader.read(7)
        addr = writer.get_extra_info('peername')

        logger.info('Received (size of json str): %r ' % data.decode())

        cur_size = 0
        total_size = int(data.decode())
        final_str = ''

        while (total_size - cur_size) >= 1024 :
            data = await reader.read(1024)
            final_str = final_str + data.decode()
            cur_size += len(data)

        data = await reader.read(total_size - cur_size)
        final_str = final_str + data.decode()

        logger.info('Received: %r ' % final_str )
        message = final_str

        proccess_msg(message)

        message = queue_out.get()

        logger.debug("Sending: %r " % parse_msg(message))
        writer.write(parse_msg(message).encode())

        await writer.drain()

    logger.debug("Close the client socket")
    writer.close()

def main(args):
    datastore = []

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
            queue_out.put(json.dumps({'task': 'map_request', 'value': blob}))

    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(handle_echo, 'localhost', args.port, loop=loop)
    server = loop.run_until_complete(coro)

    logger.debug('Number of blobs: %s (%s)', len(datastore), queue_out.qsize())

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
