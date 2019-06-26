from gevent import monkey; monkey.patch_all()

import time, os, sys, inspect
from pickle import loads as pickle_loads, dumps as pickle_dumps
from dill import loads as dill_loads, dumps as dill_dumps
from redis import Redis
import gevent as g
from gevent import sleep
from hashlib import md5
from base64 import b64encode, b64decode
from functools import partial
import traceback

STOPPED, SCHEDULED, RUNNING, SUCCESS, FAILURE = range(5)
WORKQ = "jobs-work-q"
ACTIVEQ = "jobs-active-q"
ACKQ    = "jobs-ack-list"

class Job:
    jid = 0
    def __init__(self, fun, retries=3):
        self._jid = Job.jid
        self._job_id = None
        Job.jid += 1
        self.fun = dill_dumps(fun)
        self.funname = fun.__name__
        self.retries = retries
        self.state = SCHEDULED
        self.args = []
        self.kwargs = {}
        self.jobkey_cached = "jobs-results:cache-{}".format(self._hash())
        self.jobkey = "job-results:job-{}".format(self.job_id)
        self.result = None
        self.error = None
        self.start_time = None
        self.done_time = None
        self.last_modified_time = None # should it be a list of prev handling times?
        self.in_process = False
        self.memoized = True
        self.timeout = 0

    @property
    def job_id(self):
        if not self._job_id:
            self._job_id = "{}-{}".format(time.time(), self._jid)
        return self._job_id

    def getfn(self):
        return dill_loads(self.fun)

    def _hash(self):
        data = str(self.fun)+str(self.args)+str(self.kwargs)
        thehash = md5()
        thehash.update(data.encode())
        return thehash.hexdigest()
    
    def __call__(self):
        return self
    
    def __str__(self):
        return str(self.__dict__)
    
    __repr__ = __str__

def job_dumps(job):
    return dill_dumps(job)

def job_loads(data):
    return dill_loads(data)

def job_to_success(job):
    job.state = SUCCESS
    now = time.time()
    job.last_modified_time = now
    job.done_time = now
    return job

def job_set_result(job, value, redis_connection):
    job.result = value
    print("setting result to : ", value)
    dumped_value = dill_dumps(value)
    redis_connection.set(job.jobkey, dumped_value)
    redis_connection.set(job.jobkey_cached, dumped_value)
    return job

def job_get_result(job, redis_connection):
    val = None
    try:
        if job.memoized and redis_connection.exists(job.jobkey_cached):
            val = dill_loads(redis_connection.get(job.jobkey_cached))
        else:
            val = dill_loads(redis_connection.get(job.jobkey))
    except Exception as e:
        print("[-] error getting job result: ", e)
    return val

def job_to_failure(job):
    job.retries -= 1
    job.state = FAILURE
    job.error = str(traceback.format_exc())
    job.last_modified_time = time.time()
    return job


def new_job(fun, *args, **kwargs):
    job = Job(fun=fun)
    job.state = STOPPED
    job.args = args
    job.kwargs = kwargs

    return job

def on_error(job):
    print("error: ", job)

def on_success(job):
    print("success: ", job)

class TaskQueue:
    def __init__(self):
        self.r = Redis()

    def schedule_fun(self, fun, *args, **kwargs):
        return self.schedule(new_job(fun, *args, **kwargs))
    
    def schedule(self, job):
        job.state = SCHEDULED
        print(self.r.lpush(WORKQ, job_dumps(job)))

    def _move_job_from_workq_to_activeq(self):
        self.r.brpoplpush(WORKQ, ACTIVEQ)

    def get_worker_job(self):
        if self.r.llen(ACTIVEQ) == 0:
            self._move_job_from_workq_to_activeq()
        activejob = self.r.lpop(ACTIVEQ)
        job = job_loads(activejob)
        return job

class BaseWorker:
    wid = 0

class GeventWorker(BaseWorker):
    def __init__(self, queue, greenlet=True):
        self.q = queue
        self.wid = BaseWorker.wid + 1
        BaseWorker.wid += 1
        self.greenlet = greenlet

    def work(self):
        jn = 0
        print("Starting worker")
        def execute_job_in_greenlet(job):
            fn = job.getfn()
            args = job.args
            kwargs = job.kwargs
            if job.timeout == 0:
                f = g.spawn(fn, *args, **kwargs)
            else:
                print("executing with timeout: ", )
                try:
                    f = g.with_timeout(job.timeout, fn, *args, *kwargs)
                    f.link_exception(partial(self.on_job_error, job))
                    f.link_value(partial(self.on_job_success, job))
                except g.Timeout as e:
                    print("timeout happened.")
                    self.on_job_error(job, None)

        while True:
            jn += 1
            print("jn # ", jn)
            job = self.q.get_worker_job()
            fn = job.getfn()
            args, kwargs = job.args, job.kwargs
            print("executing fun: {} and memoized {}".format(job.funname, job.memoized))
            if job.memoized and self.q.r.exists(job.jobkey_cached):
                val = job_get_result(job, self.q.r)
                job = job_to_success(job)
                job = job_set_result(job, val, self.q.r)
                continue 
            
            if self.greenlet and not job.in_process:
                execute_job_in_greenlet(job)
            else:
                try:
                    res = fn(*args, **kwargs)
                except Exception as e:
                    print("[-]Exception in worker: ", e)
                    self.on_job_error(job, None)
                else:
                    job = job_to_success(job)
                    job_set_result(job, res, self.q.r)
                    # remember cache key.

    def on_job_error(self, job, g):
        print("[-] Job failed\n", job)
        job = job_to_failure(job)
        if job.retries > 0:
            print("Scheduling again for retry")
            self.q.schedule(job)

    def on_job_success(self, job, g):
        job = job_to_success(job)
        print("[+] Job succeeded\n", job)
        value = g.value
        print(value)
        job_set_result(job, value, self.q.r)

class WorkersMgr:
    def __init__(self, workerclass, queue):
        self.workers = {}
        self._workerclass = workerclass
        self.q = queue
    def new_worker(self):
        w = self._workerclass(self.q)
        self.workers[w.wid] = w
        return w
    
    def start_new_worker(self):
        w = self.new_worker()
        return g.spawn(w.work)

def produce():
    q = TaskQueue()

    def longfn1():
        sleep(0)
        return "ok"
    bglongfn1 = new_job(longfn1)
    q.schedule(bglongfn1)

    def longfn2(username, lang="en"):
        sleep(0)
        print("hi ", username, lang)
        return username, lang

    # # bglongfn2 = new_job(longfn2)
    q.schedule_fun(longfn2, username="ahmed", lang="en")

    def fail1():
        sleep(1)
        raise ValueError("errr")
    
    q.schedule_fun(fail1)

    def fail2Timeout():
        sleep(5)

    failingjob = new_job(fail2Timeout)
    failingjob.timeout = 1
    q.schedule(failingjob)



    q.schedule_fun(longfn2, username="dmdm", lang="ar")

if __name__ == "__main__":
    q = TaskQueue()

    argv = sys.argv
    if argv[1] == "producer":
        count = 100
        if len(argv) > 2:
            count = int(argv[2])
        for i in range(count):
            produce()
    elif argv[1] == "worker":
        wm = WorkersMgr(GeventWorker, q)
        nworkers = 4
        if len(argv) > 2:
            nworkers = argv[2]
        futures = []
        for w in range(int(nworkers)):
            f = wm.start_new_worker()
            futures.append(f)
        g.joinall(futures, raise_error=False)
    elif argv[1] == "clean":
        q.r.flushall()