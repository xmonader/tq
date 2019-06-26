from gevent import monkey; monkey.patch_all()
from uuid import uuid4
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
WORKQ = "tq:jobs-work-q"
ACTIVEQ = "tq:jobs-active-q"
ACKQ    = "tq:jobs-ack-list"

class Job:
    def __init__(self, fun, retries=3):
        self._job_id = None
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
        self.worker_id = None

    @property
    def job_id(self):
        if not self._job_id:
            self._job_id = "job-{}".format(str(uuid4()))
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

def prepare_to_reschedule(job):
    job.worker_id = None
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
        return self.r.brpoplpush(WORKQ, ACTIVEQ)

    def get_worker_job(self):
        # if self.r.llen(ACTIVEQ) == 0:
        activejob = self._move_job_from_workq_to_activeq()
        job = job_loads(activejob)
        return job

def get_worker_last_seen_key_from_wid(worker_id):
    return "tq:{}-last_seen".format(worker_id)

class WorkerIdMixin:

    @property
    def worker_id(self):
        if not self._worker_id:
            self._worker_id = "worker-{}".format(str(uuid4()))
        return self._worker_id
    
    @property
    def worker_last_seen_key(self):
        return get_worker_last_seen_key_from_wid(self.worker_id)




class GeventWorker(WorkerIdMixin):
    def __init__(self, queue, greenlet=True):
        self.q = queue
        self.greenlet = greenlet
        self._worker_id = None

    def work(self):
        jn = 0
        print("Starting worker: # ", self.worker_id)
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
            self.q.r.set(self.worker_last_seen_key, time.time())
            g.sleep(1)
            jn += 1
            print("jn # ", jn)
            job = self.q.get_worker_job()
            job.worker_id = self.worker_id
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
        self.WORKER_DEAD_TIMEOUT = 2 #  seconds.
        self.start_reaping_deadworkers()


    def new_worker(self):
        w = self._workerclass(self.q)
        self.workers[w.worker_id] = w
        return w
    
    def start_new_worker(self):
        w = self.new_worker()
        return g.spawn(w.work)
    
    def start_reaping_deadworkers(self):
        return g.spawn(self._reaping_deadworkers)
    
    def _reaping_deadworkers(self):
        while True:
            print("reaping...")
            for job_dumped in self.q.r.lrange(ACTIVEQ, 0, -1):
                job = job_loads(job_dumped)
                print("checking for job: ", job)
                job_worker_id = job.worker_id
                if job_worker_id is None:
                    continue
                last_seen_key = get_worker_last_seen_key_from_wid(job_worker_id)
                last_seen = self.q.r.get(last_seen_key)
                print("LAST SEEN FOR WORKER {} is {} ".format(job_worker_id, last_seen))
                if time.time() - int(last_seen) > 2:
                    print("WORKER {} is dead".format(job.worker_id))
                    self.workers.pop(job.worker_id, None)
                    job = prepare_to_reschedule(job)
                    self.q.schedule(job)
            sleep(1)

def produce():
    q = TaskQueue()

    # def longfn1():
    #     sleep(0)
    #     return "ok"
    # bglongfn1 = new_job(longfn1)
    # q.schedule(bglongfn1)

    # def longfn2(username, lang="en"):
    #     sleep(0)
    #     print("hi ", username, lang)
    #     return username, lang

    # # # bglongfn2 = new_job(longfn2)
    # q.schedule_fun(longfn2, username="ahmed", lang="en")

    # def fail1():
    #     sleep(1)
    #     raise ValueError("errr")
    
    # q.schedule_fun(fail1)

    # def fail2Timeout():
    #     sleep(5)

    # failingjob1 = new_job(fail2Timeout)
    # failingjob1.timeout = 1
    # q.schedule(failingjob1)


    def fail3Timeout():
        for i in range(10):
            print("a very long job")
            sleep(1)

    failingjob2 = new_job(fail3Timeout)
    failingjob2.timeout = 40
    failingjob2.in_process = True
    failingjob2.memoized = False
    q.schedule(failingjob2)

    # q.schedule_fun(longfn2, username="dmdm", lang="ar")

if __name__ == "__main__":
    q = TaskQueue()

    argv = sys.argv
    if argv[1] == "producer":
        count = 100
        if len(argv) > 2:
            count = int(argv[2])
        for i in range(count):
            produce()
    elif argv[1] == "producemany":
        count = 100
        if len(argv) > 2:
            count = int(argv[2])
        for i in range(count):
            produce()
            sleep(1)
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