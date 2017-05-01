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
