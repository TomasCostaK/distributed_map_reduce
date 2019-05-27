# coding: utf-8

import logging
import argparse
import asyncio

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')

async def handle_echo(reader, writer):
    while True:
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print("Received %r from %r" % (message, addr))

        print("Send: %r" % message)
        writer.write(data)
        await writer.drain()

    print("Close the client socket")
    writer.close()

def main(args):
    datastore = []
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
            logger.debug('Blob: %s', blob)
            datastore.append(blob)

    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(handle_echo, 'localhost', args.port, loop=loop)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int, help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file', type=argparse.FileType('r'), help='file path')
    parser.add_argument('-b', dest ='blob_size', type=int, help='blob size', default=1024)
    args = parser.parse_args()
    
    main(args)

