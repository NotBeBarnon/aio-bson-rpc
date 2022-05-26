# -*- coding: utf-8 -*-
'''
JSON & BSON codecs and the SocketQueue class which uses them.
'''
from socket import error as socket_error
from struct import unpack

import asyncio
from bsonrpc.exceptions import (
    BsonRpcError, DecodingError, EncodingError, FramingError)

__license__ = 'http://mozilla.org/MPL/2.0/'


class BSONCodec(object):
    '''
    Encode/Decode message to/from BSON format.

    Pros:
      * Explicit type for binary data
          * No string piggypacking.
          * No size penalties.
      * Explicit type for datetime.
    Cons:
      * No top-level arrays -> no batch support.
    '''

    def __init__(self, custom_codec_implementation=None):
        if custom_codec_implementation is not None:
            self._loads = custom_codec_implementation.loads
            self._dumps = custom_codec_implementation.dumps
        else:
            # Use implementation from pymongo or from pybson
            import bson
            if hasattr(bson, 'BSON'):
                # pymongo
                self._loads = lambda raw: bson.BSON.decode(bson.BSON(raw))
                self._dumps = lambda msg: bytes(bson.BSON.encode(msg))
            else:
                # pybson
                self._loads = bson.loads
                self._dumps = bson.dumps

    def loads(self, b_msg):
        try:
            return self._loads(b_msg)
        except Exception as e:
            raise DecodingError(e)

    def dumps(self, msg):
        try:
            return self._dumps(msg)
        except Exception as e:
            raise EncodingError(e)

    def extract_message(self, raw_bytes):
        rb_len = len(raw_bytes)
        if rb_len < 4:
            return None, raw_bytes
        try:
            msg_len = unpack('<i', raw_bytes[:4])[0]
            if msg_len < 5:
                raise FramingError('Minimum valid message length is 5.')
            if rb_len < msg_len:
                return None, raw_bytes
            else:
                return raw_bytes[:msg_len], raw_bytes[msg_len:]
        except Exception as e:
            raise FramingError(e)

    def into_frame(self, message_bytes):
        return message_bytes


class JSONCodec(object):
    '''
    Encode/Decode messages to/from JSON format.
    '''

    def __init__(self, extractor, framer, custom_codec_implementation=None):
        self._extractor = extractor
        self._framer = framer
        if custom_codec_implementation is not None:
            self._loads = custom_codec_implementation.loads
            self._dumps = custom_codec_implementation.dumps
        else:
            import json
            self._loads = json.loads
            self._dumps = json.dumps


    def loads(self, b_msg):
        try:
            return self._loads(b_msg.decode('utf-8'))
        except Exception as e:
            raise DecodingError(e)

    def dumps(self, msg):
        try:
            return self._dumps(msg,
                               separators=(',', ':'),
                               sort_keys=True).encode('utf-8')
        except Exception as e:
            raise EncodingError(e)

    def extract_message(self, raw_bytes):
        try:
            return self._extractor(raw_bytes)
        except Exception as e:
            raise FramingError(e)

    def into_frame(self, message_bytes):
        try:
            return self._framer(message_bytes)
        except Exception as e:
            raise FramingError(e)


class SocketQueue(object):
    '''
    SocketQueue is a duplex Queue connected to a given socket and
    internally takes care of the conversion chain:

    python-data <-> queue-interface <-> codec <-> socket <-:net:-> peer node.
    '''

    BUFSIZE = 4096

    SHUT_RDWR = 2

    def __init__(self, reader, writer, codec, loop):
        '''
        :param reader:
        :type writer:
        :param codec: Codec converting python data to/from binary data
        :type codec: BSONCodec or JSONCodec
        :param loop:
        '''
        self._reader = reader
        self._writer = writer
        self._codec = codec
        self._loop = loop if loop else asyncio.get_event_loop()
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self.__receiver_task = loop.create_task(self._read())
        self._closed = False

    async def _read(self):
        buffer = b''
        while not self._closed:
            try:
                chunk = await self._reader.read(1024)
                print(f"客户端收到消息：{chunk}")
                buffer = await self._to_queue(buffer + chunk)
                if chunk == b'':
                    break
            except DecodingError as e:
                await self._msg_queue.put(e)
            except Exception as e:
                await self._msg_queue.put(e)
                break
        await self._msg_queue.put(None)
        self._closed = True
        self._writer.close()

    @property
    def is_closed(self):
        '''
        :property: bool -- Closed by peer node or with ``close()``
        '''
        return self._closed

    def close(self):
        '''
        Close this queue and the underlying socket.
        '''
        self._closed = True
        if self._writer:
            self._writer.close()
        self._writer = self._reader = None

    async def put(self, item, timeout=None):
        """
        向socket写消息
        :param item: dict
        :param timeout: 超时时间
        """
        if self._closed:
            raise BsonRpcError('Attempt to put items to closed queue.')
        msg_bytes = self._codec.into_frame(self._codec.dumps(item))
        self._writer.write(msg_bytes)
        await asyncio.wait([self._writer.drain()], timeout=timeout)

    async def get(self) -> dict:
        """
        从队列获取一条消息
        :return: dict
        """
        return await self._msg_queue.get()

    async def _to_queue(self, bbuffer) -> bytes:
        """
         输入数据流，解析出json/bson rpc对象，放到queue
        :param buffer: bytes
        :return: 返回剩余bytes
        """
        b_msg, bbuffer = self._codec.extract_message(bbuffer)
        while b_msg is not None:
            await self._msg_queue.put(self._codec.loads(b_msg))
            b_msg, bbuffer = self._codec.extract_message(bbuffer)
        return bbuffer

