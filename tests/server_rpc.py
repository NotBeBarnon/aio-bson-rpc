# -*- coding: utf-8 -*-
# @Time    : 2022/4/7:11:10
# @Author  : fzx
# @Description :


import asyncio

from bsonrpc import service_class, rpc_request
from bsonrpc.aio_rpc import JSONRpc, BSONRpc


@service_class
class ServerServices(object):

    @rpc_request
    async def test(self,_, name):
      return f"test testtesttesttest:{name}"


async def on_connected(reader, writer):
    JSONRpc(reader, writer, services=ServerServices)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = asyncio.start_server(on_connected, "10.64.73.48", "6000", loop=loop)
    loop.create_task(server)
    loop.run_forever()
