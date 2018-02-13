#!/usr/bin/env python
from gevent.monkey import patch_all
patch_all()
from gevent.queue import Queue
from gevent import spawn
import sys
from urllib3 import PoolManager
import logging

READ_TIMEOUT = 5
WORKERS = 2

def worker_producer(fname, taskq):
    for line in open(fname):
        url = line.strip()
        if url:
            taskq.put(url)


def worker_consumer(taskq, conn_pool, resultq):
    while True:
        url = taskq.get()
        if url is None:
            return
        else:
            clen = None
            ctype = None
            try:
                res = conn_pool.request('HEAD', url, retries=False, timeout=READ_TIMEOUT)
                clen = res.getheader('content-length')
                ctype = res.getheader('content-type')
                print('[%s]: %s | %s' % (url, clen, ctype))
                resultq.put((url, True, clen, ctype, None))
            except Exception as ex:
                logging.error('Fail to process [%s]: %s' % (url, str(ex)))
                resultq.put((url, False, None, None, str(ex)))


def worker_saver(resultq):
    with open('var/ok.txt', 'w') as ok_out:
        with open('var/fail.txt', 'w') as fail_out:
            while True:
                res = resultq.get()
                if res is None:
                    return
                else:
                    url, status, clen, ctype, error = res
                    if status:
                        ok_out.write('%s|%s|%s\n' % (url, clen, ctype))
                        ok_out.flush()
                    else:
                        fail_out.write('%s|%s\n' % (url, error))
                        fail_out.flush()
       

def main():
    urls_file = sys.argv[1]
    taskq = Queue(maxsize=WORKERS * 10)
    resultq = Queue(maxsize=WORKERS * 10)
    producer = spawn(worker_producer, urls_file, taskq)
    pool = []
    conn_pool = PoolManager() 
    saver = spawn(worker_saver, resultq)
    for x in range(WORKERS):
        th = spawn(worker_consumer, taskq, conn_pool, resultq)
        pool.append(th)
    producer.join()
    [taskq.put(None) for x in range(WORKERS)]
    for th in pool:
        th.join()
    resultq.put(None)




if __name__ == '__main__':
    main()
