# -*- coding: utf-8 -*-
'''
Dispatcher for RPC Objects. Routes messages and executes services.
'''
from loguru import logger
from bsonrpc.definitions import RpcErrors
import asyncio

__license__ = 'http://mozilla.org/MPL/2.0/'


class Dispatcher(object):
    """服务端处理客户端的请求和客户端处理服务端的响应"""
    def __init__(self, rpc):
        '''
        :param rpc: Rpc parent object.
        :type rpc: RpcBase
        '''

        self.rpc = rpc
        self.loop = self.rpc.loop if self.rpc.loop else asyncio.get_event_loop()
        self.loop.create_task(self._handle_data())

    def __getattr__(self, name):
        return getattr(self.rpc, name)

    async def _handle_data(self):
        """处理socket_queue里数据，包括服务端处理客户端的请求和客户端处理服务端的响应"""

        while 1:
            msg = await self.rpc.socket_queue.get()
            print(f"收到数据{msg}")
            try:
                if not msg or not isinstance(msg, dict):
                    self.rpc.socket_queue.close()
                    break
                # 客户端处理服务端返回的响应
                if "result" in msg or "error" in msg:
                    self._handle_response(msg)
                    continue

                # 服务端执行客户端请求
                await self._handle_request(msg)
            except Exception as e:
                logger.error('{}'.format(str(e)))

    async def _handle_request(self, msg):
        """
        服务端处理客户端的请求和通知
        :param msg: dict
        """
        if 'id' in msg:
            response = await self._execute_request(msg)
            await self.rpc.socket_queue.put(response)
        else:
            # await self._execute_notification(msg)
            # todo 通知处理待完成
            pass

    def _handle_response(self, msg):
        """
        处理收到的响应，设置事件标识。
        :param msg: dict
        """
        msg_id = msg['id']
        if msg_id in self.rpc.id_to_request:
            self.rpc.id_to_request[msg_id]['event'].set()
            if 'result' in msg:
                self.rpc.id_to_request[msg_id]['value'] = msg['result']
            elif 'error' in msg:
                self.rpc.id_to_request[msg_id]['exception'] = msg['error']

    async def _execute_request(self, msg):
        """服务端执行客户端请求"""
        msg_id = msg["id"]
        method_name = msg["method"]
        args, kwargs = self._get_params(msg)
        try:
            method = self.rpc.services._request_handlers.get(method_name)
            if method:
                response = await method(self.rpc.services, self.rpc, *args, **kwargs)
                return self.rpc.definitions.ok_response(msg_id, response)
            else:
                return self.rpc.definitions.error_response(msg_id, RpcErrors.method_not_found)
        except Exception as e:
            logger.error(e)
            return self.rpc.definitions.error_response(
                msg_id, RpcErrors.server_error, str(e))

    @staticmethod
    def _get_params(msg):
        if 'params' not in msg:
            return [], {}
        params = msg['params']
        if isinstance(params, list):
            return params, {}
        if isinstance(params, dict):
            return [], params
        return [params], {}




