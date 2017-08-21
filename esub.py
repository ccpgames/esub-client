"""esub client.

Usage:
    esub [options] <key>

Options:
    --data DATA, -d DATA     Rep a waiting sub with data (- for stdin)
    --token TOKEN, -t TOKEN  Sub token to use
    --host HOST, -H HOST     esub server node [default: localhost]
    --port PORT, -P PORT     esub server port [default: 8090]
    --psub, -p               sub with a persistent sub
    --timeout SECONDS        optional timeout to use
    --shared, -s             if the psub is shared
    --debug, -D              enable debugging, show stack traces
"""


import os
import sys
import time
import asyncio
import traceback
import pkg_resources

import requests
import websockets
from docopt import docopt


__ALL__ = ["node_ip", "sub", "rep"]
__version__ = pkg_resources.get_distribution("esub").version

TOKEN = os.environ.get("ESUB_TOKEN")
PROTOCOL = os.environ.get("ESUB_PROTOCOL", "http")
WS_PROTOCOL = os.environ.get("ESUB_WEBSOCKET_PROTOCOL", "ws")
CLUSTER = os.environ.get("ESUB_SERVICE_HOST", "localhost")
PORT = int(os.environ.get("ESUB_SERVICE_PORT", 8090))
HEADERS = {
    "User-Agent": "esub {}".format(
        pkg_resources.get_distribution("esub").version
    ),
}
RETRIES = int(os.environ.get("ESUB_REQUEST_RETRIES") or 0)
CONFIRM = bool(os.environ.get("ESUB_CONFIRM_RECEIPT") not in (None, "", "0"))
PING_FREQUENCY = int(os.environ.get("ESUB_PING_FREQUENCY") or 60) * 0.9


class Cache(object):
    """Holds the last known node IP (and its session) in memory."""

    timestamp = None
    ip_addr = None
    address = None
    session = None


def node_addr(node=None):
    """(re)creates a node's session and returns its full address."""

    addr = "{}://{}:{}".format(PROTOCOL, node or CLUSTER, PORT)

    if Cache.address != addr:
        if Cache.session is not None:
            Cache.session.close()
        Cache.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            max_retries=RETRIES,
            pool_connections=10,
            pool_maxsize=100,
        )
        Cache.session.mount("http://", adapter)
        Cache.session.mount("https://", adapter)

    Cache.address = addr
    return Cache.address


def node_ip(cache=10):
    """Fetches an available esub node IP.

    Args:
        cache: float seconds to cache results for

    Returns:
        string node IP
    """

    now = time.time()

    if Cache.timestamp and cache > 0 and now - Cache.timestamp < cache:
        return Cache.ip_addr

    info_url = "{}/info".format(node_addr())

    res = Cache.session.get(info_url, timeout=2)
    res.raise_for_status()

    Cache.ip_addr = res.json().get("ip")
    Cache.timestamp = now

    return Cache.ip_addr


def sub(key, token=None, node=None, timeout=None):
    """Sub to a key.

    Args:
        key: string sub key
        token: optional token, fallback to env ESUB_TOKEN
        node: optional specific node, fallback to ESUB_SERVICE_HOST
        timeout: optional timeout

    Returns:
        bytes
    """

    token = token or TOKEN
    node = node_addr(node or CLUSTER)

    if token is None:
        url = "{}/sub/{}".format(node, key)
    else:
        url = "{}/sub/{}?token={}".format(node, key, token)

    res = Cache.session.get(url, timeout=timeout)
    res.raise_for_status()
    return res.content


def rep(key, data, token=None, node=None, timeout=None):
    """Reply to a sub.

    Args:
        key: string sub key
        data: bytes data to POST
        token: optional string token, fallback to ESUB_TOKEN
        node: optional specific node, fallback to ESUB_SERVICE_HOST
        timeout: optional POST timeout
    """

    token = token or TOKEN
    node = node_addr(node or CLUSTER)

    if token is None:
        url = "{}/rep/{}".format(node, key)
    else:
        url = "{}/rep/{}?token={}".format(node, key, token)

    res = Cache.session.post(url, data=data, timeout=timeout)
    res.raise_for_status()


def psub(key, token=None, node=None, timeout=None, callback=None,
         shared=False, loop=None):
    """Subscribe to a persistent sub.

    Args:
        key: string sub key
        token: optional string token, fallback to ESUB_TOKEN
        node: optional specific node, fallback to ESUB_SERVICE_HOST
        timeout: optional timeout
        callback: function to call with each message received, default print
        shared: boolean if this psub should be shared or exclusive
        loop: existing event loop to use, or calls asyncio.get_event_loop()
    """

    token = token or TOKEN
    url = "{}://{}:{}/psub/{}{}{}".format(
        WS_PROTOCOL,
        node or CLUSTER,
        PORT,
        key,
        "?" * int(bool(token or timeout or shared)),
        "&".join(arg for arg in [
            "token={}".format(token) if token else "",
            "shared=1" if shared else "",
            "timeout={}".format(timeout) if timeout else "",
        ] if arg),
    )

    if callback is None:
        print("persistent sub to {}".format(url))
        # `callback = print` works as well but doesn't feel nearly as good
        callback = lambda *a, **kw: print(*a, **kw)

    loop = loop or asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait_for(receive(url, callback), timeout))


@asyncio.coroutine
def keepalive(websocket):
    """Keep our websocket alive by sending unsolicited PONGs."""

    while True:
        yield from asyncio.sleep(PING_FREQUENCY)
        yield from websocket.pong()


async def receive(url, callback):
    """Async websocket receive into the specified callback.

    The callback function should be quick and never error.
    """

    async with websockets.connect(url) as websocket:
        if not CONFIRM:
            pinger = asyncio.ensure_future(
                keepalive(websocket),
                loop=websocket.loop,
            )
        try:
            while True:
                message = await websocket.recv()
                callback(message)
                if CONFIRM:
                    await websocket.send("ok")
        except:
            if not CONFIRM:
                pinger.cancel()
            websocket.close()
            raise


def cli():
    """Command line entry point."""

    settings = docopt(
        __doc__,
        version="esub {}".format(__version__),
    )

    try:
        _cli(settings)
    except KeyboardInterrupt:
        raise SystemExit("Interrupted")
    except Exception as error:
        if settings["--debug"]:
            print("".join(traceback.format_exception(*sys.exc_info())))
        else:
            print(error)


def _cli(settings):
    """Command line logic."""

    global PORT
    PORT = settings["--port"]

    kwargs = {
        "token": settings["--token"],
        "node": settings["--host"],
    }

    if settings["--timeout"]:
        kwargs["timeout"] = int(settings["--timeout"])

    if settings["--data"]:
        if settings["--psub"]:
            print("warning: redundant flag --psub provided")

        if settings["--data"] == "-":
            data = sys.stdin.read()
        else:
            data = settings["--data"]

        rep(settings["<key>"], data, **kwargs)

    elif settings["--psub"]:
        kwargs["shared"] = settings["--shared"]
        psub(settings["<key>"], **kwargs)

    else:
        print(sub(settings["<key>"], **kwargs))


if __name__ == "__main__":
    cli()
