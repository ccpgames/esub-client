"""esub python client library."""


import os
import time
import pkg_resources

import requests


__ALL__ = ["node_ip", "sub", "rep"]

TOKEN = os.environ.get("ESUB_TOKEN")
PROTOCOL = os.environ.get("ESUB_PROTOCOL", "http")
CLUSTER = os.environ.get("ESUB_SERVICE_HOST", "localhost")
PORT = int(os.environ.get("ESUB_SERVICE_PORT", 8090))
HEADERS = {
    "User-Agent": "esub {}".format(
        pkg_resources.get_distribution("esub").version
    ),
}


class Cache(object):
    """Holds the last known node IP in memory."""

    content = None
    timestamp = None


def node_addr(node=None):
    """Return a node's full address."""

    return "{}://{}:{}".format(PROTOCOL, node or CLUSTER, PORT)


def node_ip(cache=10):
    """Fetches an available esub node IP.

    Args:
        cache: float seconds to cache results for

    Returns:
        string node IP
    """

    now = time.time()

    if cache > 0 and Cache.content and now - Cache.timestamp < cache:
        return Cache.content

    res = requests.get("{}/info".format(node_addr()), timeout=2)
    res.raise_for_status()
    ipaddr = res.json().get("ip")
    Cache.content = ipaddr
    Cache.timestamp = now
    return ipaddr


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

    res = requests.get(url, timeout=timeout)
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

    res = requests.post(url, data=data, timeout=timeout)
    res.raise_for_status()
