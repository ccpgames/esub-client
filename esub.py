"""esub client.

Usage:
    esub [options] <key>

Options:
    --data DATA, -d DATA     Rep a waiting sub with data (- for stdin)
    --token TOKEN, -t TOKEN  Sub token to use
    --host HOST, -H HOST     esub server node [default: localhost]
    --port PORT, -P PORT     esub server port [default: 8090]
    --psub, -p               sub with a persistent sub
    --prep, -r               rep with a persistent rep
    --timeout SECONDS        optional timeout to use
    --shared, -s             if the psub is shared
    --debug, -D              enable debugging, show stack traces
"""


import os
import sys
import json
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


def rep(key, data, token=None, node=None, timeout=None, psub=False):
    """Reply to a sub.

    Args:
        key: string sub key
        data: bytes data to POST
        token: optional string token, fallback to ESUB_TOKEN
        node: optional specific node, fallback to ESUB_SERVICE_HOST
        timeout: optional POST timeout
        psub: optional boolean to prefer sending to a psub
    """

    token = token or TOKEN

    url = "{}/rep/{}{}{}".format(
        node_addr(node or CLUSTER),
        key,
        "?" * int(bool(token or psub)),
        "&".join(arg for arg in [
            "token={}".format(token) if token else "",
            "psub=1" if psub else "",
        ] if arg),
    )

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


def prep(key=None, token=None, node=None, psub=False, func=None, timeout=None,
         loop=None, callback=None):
    """Establish a persistent rep websocket connection.

    Args:
        key: sub ID to send all reps to
        token: auth token to use with all reps
        node: esub node to connect to
        psub: boolean if all reps should send to psubs
        func: function to call per message send, must return an
              iterator of (key, token, psub, data) for each message.
              can also be a tuple or a list to send all items as data.
        timeout: optional integer seconds to timeout all publish calls with
        loop: existing event loop to use, or calls asyncio.get_event_loop()
        callback: callback function to run after confirmation. the
                  function must accept two args, the confirmed data,
                  and the reply data. both strings.
    """

    url = "{}://{}:{}/prep".format(WS_PROTOCOL, node or CLUSTER, PORT)
    if hasattr(func, "__iter__"):
        _iter = func
        func = lambda: iter((key, token, psub, x) for x in _iter)

    loop = loop or asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait_for(
        publish(url, func, sub=key, token=token, psub=psub, callback=callback),
        timeout=timeout,
    ))


@asyncio.coroutine
def keepalive(websocket):
    """Keep our websocket alive by sending unsolicited PONGs."""

    while True:
        yield from asyncio.sleep(PING_FREQUENCY)
        yield from websocket.pong()


async def publish(url, func, sub=None, token=None, psub=False, callback=None):
    """Async websocket publish from the specified function."""

    if callback is None:
        callback = lambda x, y: print("{!r}: {}".format(x, y))

    async with websockets.connect(url, max_size=None) as websocket:
        try:
            for msg_sub, msg_token, msg_psub, msg_data in func():

                await websocket.send(json.dumps({
                    "key": sub or msg_sub,
                    "token": token or msg_token or TOKEN,
                    "psub": psub or msg_psub,
                    "data": msg_data,
                }))

                if CONFIRM:
                    msg = await websocket.recv()
                    callback(msg_data, msg)

        except Exception:
            websocket.close()
            raise


async def receive(url, callback):
    """Async websocket receive into the specified callback.

    The callback function should be quick and never error.
    """

    async with websockets.connect(url, max_size=None) as websocket:
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

        kwargs["psub"] = settings["--psub"]

        if settings["--data"] == "-":
            def _from_stdin():
                while True:
                    try:
                        yield (
                            settings["<key>"],
                            settings["--token"],
                            settings["--psub"],
                            input(),
                        )
                    except (KeyboardInterrupt, EOFError):
                        raise SystemExit

            data = _from_stdin
        elif settings["--prep"]:
            data = settings["--data"].split(",")
        else:
            data = settings["--data"]

        if settings["--prep"]:
            prep(settings["<key>"], func=data, **kwargs)
        else:
            rep(settings["<key>"], data, **kwargs)

    elif settings["--psub"]:
        kwargs["shared"] = settings["--shared"]
        psub(settings["<key>"], **kwargs)

    else:
        print(sub(settings["<key>"], **kwargs))


if __name__ == "__main__":
    cli()
