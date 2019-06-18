#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Hunter
# Email: crhuazai@163.com
# Date: 2019/6/18
# Project: ops_scripts
# FileName: redis-sync

import redis
import threading
import time


def redis_string(redis_from, redis_to, k):
    # string处理
    try:
        v = redis_from.get(k)
        redis_to.set(k, v)
    except Exception as e:
        print(e)
    return 0


def redis_list(redis_from, redis_to, k):
    # list处理
    try:
        v = redis_from.lrange(k, 0, -1)
        redis_to.lpush(k, v)
    except Exception as e:
        print(e)
    return 0


def redis_set(redis_from, redis_to, k):
    # set处理
    try:
        v = redis_from.smembers(k)
        for i in v:
            redis_to.sadd(k, i)
    except Exception as e:
        print(e)
    return 0


def redis_hash(redis_from, redis_to, k):
    # hash处理
    try:
        keys = redis_from.hkeys(k)
        for key in keys:
            v = redis_from.hget(k, key)
            redis_to.hset(k, key, v)
    except Exception as e:
        print(e)

    return 0


def redis_zset(redis_from, redis_to, k):
    # zset处理
    try:
        n = 0
        v = redis_from.zrange(k, 0, -1)
        for i in v:
            n += 1
            redis_to.zadd(k, {i: n})
    except Exception as e:
        print(e)
    return 0


def thread_main(key_list, redis_from, redis_to):
    print('start before', time.strftime('%Y%m%d %H:%M:%S'),threading.current_thread().getName())
    for k in key_list:
        date_type = redis_from.type(k).decode('utf-8')
        if date_type == 'string':
            redis_string(redis_from, redis_to, k)
        elif date_type == 'list':
            redis_list(redis_from, redis_to, k)
        elif date_type == 'set':
            redis_set(redis_from, redis_to, k)
        elif date_type == 'hash':
            redis_hash(redis_from, redis_to, k)
        elif date_type == 'zset':
            redis_zset(redis_from, redis_to, k)
        else:
            print("Unkown type", k)
    print('end before', time.strftime('%Y%m%d %H:%M:%S'),threading.current_thread().getName())


if __name__ == '__main__':
    cnt = 0
    print("startime", time.strftime('%Y%m%d %H:%M:%S'))
    for db in range(0, 16):
        redis_from = redis.StrictRedis(host='172.16.1.225', port=16379,
                                       password='', db=db)
        redis_to = redis.StrictRedis(host='172.16.1.225', port=19000, db=db)
        start = 0
        end = start + 10000
        while start <= len(redis_from.keys()):
            # print('treading before', time.strftime('%Y%m%d %H:%M:%S'))
            # 每个线程处理10000个key, 可调整
            t = threading.Thread(target=thread_main, name=end,args=(redis_from.keys()[start:end],redis_from,redis_to,))
            # print('treading after', time.strftime('%Y%m%d %H:%M:%S'))
            # print('start before', time.strftime('%Y%m%d %H:%M:%S'))
            t.start()
            # print('start after', time.strftime('%Y%m%d %H:%M:%S'))
            start = end
            end += 10000
        cnt += 1
    print(cnt)
    print('endtime', time.strftime('%Y%m%d %H:%M:%S'))
