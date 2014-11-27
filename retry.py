#!/usr/bin/env python
# encoding: utf-8


"""
    retry decorator
    
    ~~~~~~~~
    retry.py
"""

import functools
from time import sleep
from logging import Logger



class NotCallable(Exception):
    pass


def run_callback(cb):
    func = cb.get('callback')
    if not func or not callable(func):
        raise NotCallable
    args = cb.get('args', ())
    kwargs = cb.get('kwargs', {})
    try:
        return func(*args, **kwargs)
    except:
        return None


def get_retry_wait_time(which_time, interval):
    """计算每次retry时间的方法
       `y = kx + c`
    @param which_time  retry的当前次数
    @param interval    retry的时间间隔基数
    """
    k, c = 1, 0
    wait_t = k * which_time * interval + c
    return wait_t


def retry_on_errors(times=2, exceptions=(), raise_exc=True, default=False, logger=None, print_info=False,
        wait_func=get_retry_wait_time, wait_interval=0, cbs=()):
    """
    @param times           retry的次数
    @param exceptions      此类异常会retry, 
                           times为-1 retry并且exceptions为(), 或者捕获的异常在exceptions中, retry无限次
    @param raise_exc       retry次数用完后是否抛出异常
    @param default         retry结束不抛出异常的话，返回的默认值
    @param logger          logger如果为Logger实例， 记录日志
    @param wait_func       获取retry等待时间的函数, 每次等待时间如何计算， 自己发挥。
    @param wait_interval   等待时长
    @param cbs             [{'callback': func, 'args': xxx, '**kwargs': xxx}, ]
                           抛出异常时的回调函数列表
    """
    def func(f):
        @functools.wraps(f)
        def _func(*args, **kwargs):
            t = 0
            is_logger = isinstance(logger, Logger)
            callable_func = callable(wait_func)
            while 1:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    t += 1
                    ename = e.__class__
                    exc_hit = ename in exceptions or exceptions == ()
                    retry = exc_hit and (times==-1 or t<=times)
                    if retry:
                        over = "over" if t == times else ""
                        msg = "call {0} error: {1}, retry [{2}] {3}".format(f.__name__, e, t, over)
                        is_logger and logger.exception(msg)
                        if print_info:
                            print(msg)

                        if callable_func:
                            try:
                                wait_time = wait_func(t, wait_interval)
                            except:
                                wait_time = 0
                            sleep(wait_time)
                        continue

                    for cb in cbs:
                        run_callback(cb)

                    if raise_exc:
                        raise e
                    return default

        return _func
    return func

retry_anyway = functools.partial(retry_on_errors, times=-1)


if __name__ == '__main__':
    @retry_on_errors(times=10, raise_exc=False, print_info=True)
    def f():
        print 1/0

    print f()
