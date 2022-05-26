# aiobsonrpc

[bsonrpc](https://github.com/seprich/py-bson-rpc) 异步重构. 

Python 3.5+

## Getting Started

### Installing

```
pip install aio-bson-rpc
```

### Example

Server

```python
import asyncio

from bsonrpc import service_class, rpc_request
from bsonrpc.aio_rpc import JSONRpc, BSONRpc


@service_class
class ServerServices(object):

    @rpc_request
    async def test(self,_, name):
      return f"test :{name}"


async def on_connected(reader, writer):
    JSONRpc(reader, writer, services=ServerServices)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = asyncio.start_server(on_connected, "10.64.73.48", "6000", loop=loop)
    loop.create_task(server)
    loop.run_forever()
```

Client

```python
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
```


使用BSON协议时，直接在实例化时将JSONRpc改成BSONRpc