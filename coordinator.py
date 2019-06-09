# coding: utf-8

import csv
import logging
import argparse
import asyncio
import queue
import time
import json
import signal

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('coordinator')

connectionsMap = {}
# queue_out = queue.Queue()

class Coordinator():
    def __init__(self, id_c, file_path, blob_size, file_out):
        self.id = id_c
        self.datastore = []
        self.connectionsMap = {}
        self.data_array = data_array  # array that stores results from mapping and reducing
        self.lost_msgs = queue.Queue()
        self.last_reduced = False
        self.start_time = None
        self.file_path = file_path
        self.file_read = False
        self.blob_size = blob_size
        self.file_out = file_out
        self.i_am_main = False
        self.pending_coordinator_request = False

    def full_state(self):
        return { 'datastore' : self.datastore, 
                 'connectionsMap' : self.connectionsMap, 
                 'data_array' : list(self.data_array.queue), 
                 'file_read' : self.file_read,
                 'start_time' : self.start_time
                 }

    def restore_state(self, state):
        logger.debug('Restoring state')
        self.datastore = state['datastore']
        self.connectionsMap = state['connectionsMap']
        [self.data_array.put(i) for i in state['data_array']]
        self.file_read = state['file_read']
        self.start_time = state['start_time']

    def read_file(self):
        # load txt file and divide it into blobs
        with self.file_path as f:
            while True:
                blob = f.read(self.blob_size)
                if not blob:
                    break
                # This loop is used to not break word in half
                while not str.isspace(blob[-1]):
                    ch = f.read(1)
                    if not ch:
                        break
                    blob += ch
                logger.debug('\nBlob: %s', blob)
                self.datastore.append(blob)

        self.file_read = True
        logger.debug('Number of blobs: %s', len(self.datastore))

    def print_to_file(self):
        hist = self.data_array.get()
        # store final histogram into a CSV file
        with self.file_out as f:
            csv_writer = csv.writer(f, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for w, c in hist:
                csv_writer.writerow([w, c])

    def proccess_msg(self, message_json):
        message = json.loads(message_json)
        if(message['task'] == 'register') and self.start_time is None:
            self.start_time = time.time()
            return
        elif message['task'] == 'attempt_main':
            if message['value'] == self.id: # i am sending this to myself
                self.i_am_main = True
                self.pending_coordinator_request = True
                return
            else:
                self.pending_coordinator_request = True
                return
        elif message['task'] == 'map_reply' or message['task'] == 'reduce_reply':
            self.data_array.put(message['value'])
            return
        elif message['task'] == 'coordinator_reply':
            self.restore_state(message['value'])
            return
        # return self.scheduler()

    def scheduler(self):
        if self.pending_coordinator_request:
            result = { 'task': 'coordinator_reply', 'value': self.full_state() }
            self.pending_coordinator_request = False
            return result

        #Tambem nao queremos fazer muito mais porque senao perdemos muito trabalho
        queueSize = self.data_array.qsize()
        lost_msgs_size = self.lost_msgs.qsize()
        if(len(self.datastore) > 0): # blobs available
            new_message = self.datastore.pop() # get blob from out queue
            result = {'task': 'map_request', 'value': new_message}
            return result
        elif(lost_msgs_size > 0):
            result = self.lost_msgs.get()
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
            if not self.last_reduced and queueSize > 0:
                new_message = []
                new_message.append(self.data_array.get())
                result = {'task': 'reduce_request', 'value': new_message}
                self.last_reduced = True
                return result
            if self.start_time is not None: # there are clients connected
                end = time.time()
                logger.info('TIME TAKEN: %f (s)', end-self.start_time)
                result = {'task': 'done', 'value': 'done'}
                return result


    def parse_msg(self, msg):
        msg_len = len(msg)
        return '0'*(7-len(str(int(msg_len)))) + str(msg_len) + msg

    async def register(self, host, port, loop):
        try:
            reader, writer = await asyncio.open_connection(host, port, loop=loop) # open connection
        # except (ConnectionError, ConnectionRefusedError, TimeoutError):
        # except Exception:
        except:
            self.i_am_main = True
            return

        msg = { 'task' : 'attempt_main', 'value' : self.id }
        msg_json = json.dumps(msg)        
        parsed_msg = self.parse_msg(msg_json)

        while True:
            if self.i_am_main: # i am already the main coordinator
                logger.debug('BREAK1')
                break

            # send request to main coordinator
            writer.write(parsed_msg.encode())
            await writer.drain()

            # receive data
            try: 
                data = await reader.read(7)
            except ConnectionResetError:
                # if not data:
                logger.debug('BREAK2')
                break # become main coordinator

            if not data:
                logger.debug('BREAK2.1')
                break # become main coordinator

            addr = writer.get_extra_info('peername')
            
            logger.debug('Size %s', data.decode())

            cur_size = 0
            total_size = int(data.decode())
            final_str = ''

            while (total_size - cur_size) >= 1024 :
                data = await reader.read(1024)
                if not data:
                    logger.debug('BREAK3')
                    break # become main coordinator

                final_str = final_str + data.decode()
                cur_size += len(data)

            if cur_size != total_size:
                data = await reader.read(total_size - cur_size)

            if not data:
                logger.debug('BREAK4')
                break # become main coordinator

            final_str = final_str + data.decode()

            # logger.info('Received: %r ' % final_str )
            logger.info('(REGISTER) Received from: %s ', addr )

            message = final_str
            self.proccess_msg(message)

            await asyncio.sleep(1)

        self.i_am_main = True
        return

    async def handle_client(self, reader, writer):

        while True:
            data = await reader.read(7)
            addr = writer.get_extra_info('peername')
            
            if not data or data == '':
                logger.debug("THE MONKEY KILLED: %s", addr)
                lostMsg = connectionsMap.get(addr)
                logger.debug("LOST MESSAGE: %s", lostMsg)
                if lostMsg != None:
                    self.lost_msgs.put(lostMsg)
                # logger.debug("Close the client socket")
                # writer.close()
                break

            cur_size = 0
            total_size = int(data.decode())
            final_str = ''

            while (total_size - cur_size) >= 1024 :
                data = await reader.read(1024)
                final_str = final_str + data.decode()
                cur_size += len(data)

            # if cur_size != total_size:
            #     data = await reader.read(total_size - cur_size)

            data = await reader.read(total_size - cur_size)
            final_str = final_str + data.decode()

            # logger.info('Received: %r ' % final_str )
            logger.info('Received from: %s ', addr )

            message = final_str
            self.proccess_msg(message)

            to_send = self.scheduler()
            # logger.debug(to_send)
            if to_send is not None:
                if connectionsMap.get(addr) != None:
                    connectionsMap.pop(addr)
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

def close_server(loop, server):
    loop.stop()
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

def main(args):

    coordinator = Coordinator(args.coordinator_id, args.file, args.blob_size, args.out)

    # register
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coordinator.register('localhost', args.port, loop))
    loop.close()

    loop = asyncio.new_event_loop()
    coro = asyncio.start_server(coordinator.handle_client, 'localhost', args.port, loop=loop)
    server = loop.run_until_complete(coro)

    if not coordinator.file_read:
        coordinator.read_file()

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    close_server(loop, server)
    coordinator.print_to_file()

    # # Close the server
    # server.close()
    # loop.run_until_complete(server.wait_closed())
    # loop.close()

    # print("data_array size: ", data_array.qsize())
    # hist = data_array.get()
    # # store final histogram into a CSV file
    # with args.out as f:
    #     csv_writer = csv.writer(f, delimiter=',',
    #                             quotechar='"', quoting=csv.QUOTE_MINIMAL)

    #     for w, c in hist:
    #         csv_writer.writerow([w, c])


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
    parser.add_argument('-i', dest='coordinator_id', type=int,
                        help='coordinator id', default=1)
    args = parser.parse_args()


main(args)
