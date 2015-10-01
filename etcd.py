# coding=utf-8
import requests

__author__ = 'zephyre'


# 缓存的配置信息
_cached_result = {}


def get_service(etcd_url, service_name, alias):
    """
    获得服务的入口信息（host:port）

    所谓服务，是指etcd的服务发现部分的数据，比如：http://etcd:2379/v2/keys/backends/mongo?recursive=true
    上面一条数据中，记录了一组MongoDB服务器的入口地址。之所以是一组而不是一个，是为了高可用扩展的需求。

    上述服务的service_name为mongo。默认情况下，如果将该服务读入系统配置，其配置名就是services.mongo。
    在这里，我们可以对其取一个别名，比如mongodb-alias，那么该服务在系统配置中的键就是services.mongodb-alias

    :param service_name: 服务名称
    :param alias: 服务别名
    :return:
    """
    url = '%s/v2/keys/backends/%s' % (etcd_url, service_name)

    def get_service_entry(entry):
        """
        获得单一服务器地址

        比如，entry数据为：
        {"key":"/backends/mongo/ec6f787e91762f28741d01d6e0e7841c395f5f3ddcfa07db1c873e3a7ad6e6b3",
        "value":"192.168.100.2:31001","expiration":"2015-07-07T01:46:31.757230168Z","ttl":18,
        "modifiedIndex":5703768,"createdIndex":5703768}

        则返回一个tuple：("ec6f787e91762f28741d01d6e0e7841c395f5f3ddcfa07db1c873e3a7ad6e6b3", {"host": "192.168.100.2",
        "port": 31001})
        这在复制集的情况下很有用
        """
        key = entry['key'].split('/')[-1]
        value = entry['value'].split(':')
        host = value[0]
        port = int(value[1])

        return key, {'host': host, 'port': port}

    try:
        nodes = requests.get(url).json()['node']['nodes']
        return {alias: dict(map(get_service_entry, nodes))}
    except (KeyError, IndexError, ValueError):
        return None


def get_conf(etcd_url, conf_name, alias):
    """
    从etcd数据库获得配置信息

    :param etcd_url:
    :param conf_name:
    :param alias:
    :return:
    """

    def build_conf(node, node_alias=None):
        """
        获得配置信息

        :param node:
        :param node_alias:
        :return:
        """
        current_key = node_alias or node['key'].split('/')[-1]
        if 'dir' in node and node['dir'] and 'nodes' in node and node['nodes']:
            m = {}
            for n in node['nodes']:
                m.update(build_conf(n))
            return {current_key: m}
        elif 'value' in node:
            return {current_key: node['value']}
        else:
            return None

    url = '%s/v2/keys/project-conf/%s?recursive=true' % (etcd_url, conf_name)
    data = requests.get(url).json()
    if 'node' not in data:
        return None
    else:
        return build_conf(data['node'], node_alias=alias)


def _build_tuple(val):
    """
    如果val形如(name, alias)，则返回(name, alias)
    如果val为一个字符串，比如service，则返回(service, service)
    其它情况：抛出ValueError
    """
    if isinstance(val, tuple) and len(val) == 2:
        return val
    elif isinstance(val, basestring):
        return val, val
    else:
        raise ValueError


def info(etcd_url, service_names=None, conf_names=None, cache_key=None, force_refresh=False):
    """
    给定服务键名和配置键名，获得所有的信息
    :param etcd_url: etcd服务器的地址。
    :param service_names: 服务列表。具有两种形式。1. 指定键名：[ "rabbitmq", "mongodb" ]。
    2. 指定键名和别名：[ ("rabbitmq-node1", "rabbitmq"), ("mongodb-master", "mongodb") ]。这两种形式可以混合使用。
    :param conf_names: 配置信息列表。和服务列表类似，也具有具有别名和不具有别名这两种形式。
    :param cache_key: 如果指定了cache_key：取得的数据会被缓存，同时今后也可以根据cache_key取出
    :param force_refresh: 强制刷新缓存
    :return:
    """
    if not force_refresh and cache_key and cache_key in _cached_result:
        return _cached_result[cache_key]

    def merge_dicts(*dict_args):
        """
        Given any number of dicts, shallow copy and merge into a new dict,
        precedence goes to key value pairs in latter dicts.
        """
        result_map = {}
        for dictionary in dict_args:
            result_map.update(dictionary)
        return result_map

    services = merge_dicts(
        *filter(lambda v: v, [get_service(etcd_url, *_build_tuple(entry)) for entry in (service_names or [])]))
    conf = merge_dicts(*filter(lambda v: v, [get_conf(etcd_url, *_build_tuple(entry)) for entry in (conf_names or [])]))
    result = merge_dicts({'services': services}, conf)

    if cache_key:
        _cached_result[cache_key] = result

    return result


if __name__ == '__main__':
    import sys

    the_url = sys.argv[1]

    get_service(the_url, 'mongo-production', 'mongo')
    get_conf(the_url, 'yunkai-dev', 'yunkai')
    info(the_url, [('redis-main', 'redis'), ('mongo-production', 'mongo'), 'nexus', 'fake'],
         [('yunkai-dev', 'yunkai'), 'smscenter', 'fake'])
