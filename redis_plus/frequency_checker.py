#!/usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author  : Lin Luo / Bruce Lilu
@time    : 2019/12/7
@contact : 15869300264@163.com
"""
from copy import deepcopy
from time import time

from redis_plus.redis import BaseChecker

FREQUENCY_PARAM = {
    'PRE_KEY': 'frequency_checker',
    'MAX_TIMES': [
        {
            'max_times': 10,
            'cycle_seconds': 3600
        },
        {
            'max_times': 2,
            'cycle_seconds': 180
        },
        {
            'max_times': 5,
            'cycle_seconds': 300
        }
    ]
}


class FrequencyChecker(BaseChecker):
    def __init__(self, redis, ip_param: dict = None):
        super().__init__(redis)
        ip_param = ip_param if ip_param is not None else deepcopy(FREQUENCY_PARAM)
        self._check_ip_param(ip_param)
        self._pre_key = ip_param['PRE_KEY'].replace(':', '.')
        self._max_times = ip_param['MAX_TIMES']
        self._max_cycle_seconds = self._get_max_cycle_seconds()
        self._length = len(self._max_times)

    @staticmethod
    def _check_ip_param(ip_param):
        """

        :param ip_param:
        :return:
        """
        if ip_param.get('PRE_KEY') is None or not isinstance(ip_param.get('PRE_KEY'), str):
            raise KeyError('string key PRE_KEY is excepted')
        if ip_param.get('MAX_TIMES') is None or not isinstance(ip_param.get('MAX_TIMES'), list):
            raise KeyError('list key MAX_TIMES is excepted')
        for item in ip_param.get('MAX_TIMES'):
            if item.get('max_times') is None or not isinstance(item.get('max_times'), int):
                raise KeyError('integer key max_times is excepted')
            if item.get('cycle_seconds') is None or not isinstance(item.get('cycle_seconds'), int):
                raise KeyError('integer key cycle_seconds is excepted')

    def _get_max_cycle_seconds(self):
        """

        :return:
        """
        return max([i.get('cycle_seconds') for i in self._max_times])

    def check(self, key: str) -> bool:
        """
        根据键值检查是否超过查询阈值
        :param key: 需要查询的键值
        :return: True，没超过阈值；False，超过阈值
        """
        value = self._get(key)
        if value is None:
            # 如果不存在，则创建键值，并返回True
            self._add(key)
            return True
        else:
            # 如果存在，则更新值并传入方法中进行计算
            value['items'].append(int(time()))
            flag = self._calc(value)
            self._set(key, value)
            return flag

    def _format_key(self, key: str) -> str:
        """
        计算键值的字符串
        :param key:
        :return:
        """
        return '%s:%s' % (self._pre_key, key.replace(':', '.'))

    def _add(self, key: str):
        """
        新建键值
        :param key:
        :return:
        """
        value = {
            'max': [1] * self._length,
            'items': [int(time())]
        }
        self._set(key, value)

    def _calc(self, value: dict) -> bool:
        """
        计算是否超过阈值
        :param value:
        :return:
        """
        tmp_sub = [0] * self._length
        # 直接循环计算当前库中存储的值与阈值之间的差，是否超过阈值，如果没有超过，所有计数+1，并直接返回True
        for i in range(self._length):
            tmp_sub[i] = value['max'][i] - self._max_times[i]['max_times']
            value['max'][i] += 1
        if max(tmp_sub) < 0:
            return True
        # 存在计数超过阈值的情况
        new_max = [0] * self._length
        # 获取当前时间戳
        current = value['items'][-1]
        for i in range(len(value['items']) - 1, -1, -1):
            # 逆向循环计算items列表中的时间戳
            if current - self._max_cycle_seconds > value['items'][i]:
                # 如果时间戳差值超过最大的计算周期，删除该值及其左边的所有值，并退出循环
                del value['items'][:i + 1]
                break
            for j in range(self._length):
                # 循环计算计数数量
                if current - self._max_times[j]['cycle_seconds'] < value['items'][i]:
                    new_max[j] += 1
        # 更新值
        value['max'] = new_max
        # 计算新的计数是否超过阈值
        tmp_sub = [value['max'][i] - self._max_times[i]['max_times'] for i in range(self._length)]
        if max(tmp_sub) <= 0:
            return True
        else:
            return False
