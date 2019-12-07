#!/usr/bin/env python
# -*- coding:utf-8 _*-  
"""
@author  : Lin Luo / Bruce Lilu
@time    : 2019/12/7
@contact : 15869300264@163.com
"""
from redis import Redis as r
from redis import ConnectionPool
from json import loads, dumps


class Redis(object):
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, password: str = '', **kwargs):
        self._pool = ConnectionPool(host=host, port=port, db=db, password=password, **kwargs)
        self._redis = r(connection_pool=self._pool)

    @property
    def redis(self):
        return self._redis

    @property
    def pool(self):
        return self._pool


class BaseChecker(object):
    def __init__(self, redis):
        """

        """
        self._pre_key = ''
        self._redis = redis
        self._max_cycle_seconds = None

    def _format_key(self, key: str) -> str:
        """
        计算键值的字符串
        :param key:
        :return:
        """
        return '%s:%s' % (self._pre_key, key.replace(':', '.'))

    def _get(self, key: str) -> dict or None:
        """

        :param key:
        :return:
        """
        value = self._redis.get(self._format_key(key))
        if value is not None:
            value = loads(value)
        return value

    def _set(self, key: str, value: object):
        """

        :param key:
        :param value:
        :return:
        """
        self._redis.set(self._format_key(key), dumps(value), ex=self._max_cycle_seconds)
