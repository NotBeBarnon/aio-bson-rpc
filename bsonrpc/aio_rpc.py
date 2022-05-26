# -*- coding: utf-8 -*-
'''
Main module providing BSONRpc and JSONRpc.
'''
import asyncio
import re

import six
from loguru import logger

from bsonrpc.aio_socket_queue import BSONCodec, JSONCodec, SocketQueue
from bsonrpc.definitions import Definitions, RpcErrors
from bsonrpc.dispatcher import Dispatcher
from bsonrpc.exceptions import ResponseTimeout
from bsonrpc.framing import JSONFramingRFC7464
from bsonrpc.options import DefaultOptionsMixin, MessageCodec
from bsonrpc.util import PeerProxy

__license__ = 'http://mozilla.org/MPL/2.0/'



class DefaultServices(object):
    _request_handlers = {}

    _notification_handlers = {}

# 请求标识 => { 事件(event)， 返回值(value)， 异常(exception) }
# id_to_request = {"event": {"value": 123, "exception": "xxx"}}
# id_to_request = {}
class RpcBase(DefaultOptionsMixin):

    def __init__(self, reader, writer, codec, protocol, protocol_version, services=None, loop=None, **options):
        assert (hasattr(services, '_request_handlers') and
                hasattr(services, '_notification_handlers'))
        for key, value in options.items():
            setattr(self, key, value)
        self.definitions = Definitions(protocol,
                                       protocol_version,
                                       self.no_arguments_presentation)
        self.services = services
        self.socket_queue = SocketQueue(reader, writer, codec, loop)
        # 请求标识 => { 事件(event)， 返回值(value)， 异常(exception) }
        # id_to_request = {"event": {"value": 123, "exception": "xxx"}}
        self.id_to_request = {}
        self.loop = loop if loop else asyncio.get_event_loop()
        self.dispatcher = Dispatcher(self)

    @property
    def is_closed(self):
        '''
        :property: bool -- Closed by peer node or with ``close()``
        '''
        return self.socket_queue.is_closed

    async def invoke_request(self, method_name, *args, **kwargs):
        '''
        Invoke RPC Request.
        '''
        rec = re.compile(r'^_*timeout$')
        to_keys = sorted(filter(lambda x: rec.match(x), kwargs.keys()))
        if to_keys:
            timeout = kwargs[to_keys[0]]
            del kwargs[to_keys[0]]
        else:
            timeout = None
        msg_id = six.next(self.id_generator)
        try:
            msg: dict = self.definitions.request(
                        msg_id, method_name, args, kwargs)

            await self.socket_queue.put(msg)    # 把请求内容通过socket传给服务端

            request_event = asyncio.Event()
            self.id_to_request[msg_id] = dict(
                event=request_event,
                value=None,
                exception=None
            )
            await asyncio.wait([request_event.wait()], timeout=timeout)
            if not request_event.is_set():
                del self.id_to_request[msg_id]
                raise ResponseTimeout(u'Waiting response expired.')
            result = self.id_to_request[msg_id]['value']
            exception = self.id_to_request[msg_id]['exception']
            if exception:
                raise exception

        except Exception as e:
            raise e
        return result

    def invoke_notification(self, method_name, *args, **kwargs):
        '''
        Send an RPC Notification.
        '''
        self.socket_queue.put(
            self.definitions.notification(method_name, args, kwargs))

    def get_peer_proxy(self, requests=None, notifications=None, timeout=None):
        '''
        Get a RPC peer proxy object.
        '''
        return PeerProxy(self, requests, notifications, timeout)

    def close(self):
        '''
        Close the connection and stop the internal dispatcher.
        '''
        # Closing the socket queue causes the dispatcher to close also.
        self.socket_queue.close()


class BSONRpc(RpcBase):
    '''
    BSON RPC Connector. Follows closely `JSON-RPC 2.0`_ specification
    with only few differences:

    * Batches are not supported since BSON does not support top-level lists.
    * Keyword 'jsonrpc' has been replaced by 'bsonrpc'

    Connects via socket to RPC peer node. Provides access to the services
    provided by the peer node and makes local services available for the peer.

    To use BSONRpc you need to install ``pymongo``-package
    (see requirements.txt)

    .. _`JSON-RPC 2.0`: http://www.jsonrpc.org/specification
    '''

    #: Protocol name used in messages
    protocol = 'bsonrpc'

    #: Protocol version used in messages
    protocol_version = '2.0'

    def __init__(self, reader, writer, services=None, loop=None, **options):
        """
        :param reader: StreamReader
        :param writer: StreamWriter
        :param loop: event loop
        :param services: @server_class
        """
        self.codec = MessageCodec.BSON
        if not services:
            services = DefaultServices()
        cci = options.get('custom_codec_implementation', None)
        super(BSONRpc, self).__init__(
            reader, writer,
            codec=BSONCodec(custom_codec_implementation=cci),
            protocol=self.protocol,
            protocol_version=self.protocol_version,
            services=services,
            loop=loop if loop else asyncio.get_event_loop(),
            **options)


class JSONRpc(RpcBase):
    """JSON RPC Connector. Implements the `JSON-RPC 2.0`_ specification.

    Connects via socket to RPC peer node. Provides access to the services
    provided by the peer node. Optional ``services`` parameter will take an
    object of which methods are accessible to the peer node.

    Various methods of JSON message framing are available for the stream
    transport.

    .. _`JSON-RPC 2.0`: http://www.jsonrpc.org/specification
    """

    #: Protocol name used in messages
    protocol = 'jsonrpc'

    #: Protocol version used in messages
    protocol_version = '2.0'

    #: Default choice for JSON Framing
    framing_cls = JSONFramingRFC7464

    def __init__(self, reader, writer, services=None, loop=None, **options):
        """
        :param reader: StreamReader
        :param writer: StreamWriter
        :param loop: event loop
        :param services: @server_class
        """
        self.codec = MessageCodec.JSON

        cci = options.get('custom_codec_implementation', None)
        if not services:
            services = DefaultServices()
        super(JSONRpc, self).__init__(
            reader,
            writer,
            codec=JSONCodec(self.framing_cls.extract_message,
                            self.framing_cls.into_frame,
                            custom_codec_implementation=cci),
            protocol=self.protocol,
            protocol_version=self.protocol_version,
            services=services,
            loop=loop if loop else asyncio.get_event_loop(),
        )
