# -*- coding: utf-8 -*-
# @Time    : 2022/4/7:11:12
# @Author  : fzx
# @Description :

import asyncio

from bsonrpc.aio_rpc import JSONRpc, BSONRpc


async def on_connected():
    reader, writer = await asyncio.open_connection("10.64.73.48", "6000")
    rpc = JSONRpc(reader, writer)
    peer = rpc.get_peer_proxy(timeout=30)
    while 1:
        res1 = await peer.test("test 1")
        res2 = await peer.test("test 2")
        print(res1)
        print(res2)
        await asyncio.sleep(2)

if __name__ == '__main__':
    asyncio.run(on_connected())

