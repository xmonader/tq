from gevent import monkey; monkey.patch_all()
import gevent as g
from gevent import sleep
from uuid import uuid4
import re
import time, os, sys, inspect
from pickle import loads as pickle_loads, dumps as pickle_dumps
from dill import loads as dill_loads, dumps as dill_dumps
from redis import Redis
from hashlib import md5
from base64 import b64encode, b64decode
from functools import partial
import traceback
import enum

class JobState(enum.Enum):
    STOPPED = "STOPPED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING" 
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
 
WAITING_Q = "tq:queue:waiting-jobs"
WORKER_QUEUE_ID_PATTERN = "tq:queue:worker-(.+)?"

class Job:
    def __init__(self, fun, retries=3):
        self._job_id = None
        self.fun = dill_dumps(fun)
        self.fun_name = fun.__name__
        self.retries = retries
        self.state = JobState.STOPPED
        self.args = []
        self.kwargs = {}
        self.result = None
        self.error = None
        self.start_time = None
        self.done_time = None
        self.last_modified_time = None # should it be a list of prev handling times?
        self.in_process = False
        self.memoized = True
        self.timeout = 0
        self.worker_id = None
        self.safe_to_collect = False


    @property
    def job_result_key(self):
        return "tq:results:{}".format(self.job_id)

    @property
    def job_result_key_cached(self):
        return "tq:cache-results:{}".format(self._hash())
    
    @property
    def job_id(self):
        if not self._job_id:
            self._job_id = "{}".format(str(uuid4()))
        return self._job_id

    @property
    def job_key(self):
        return "tq:job:{}".format(self.job_id)

    def getfn(self):
        return dill_loads(self.fun)

    def _hash(self):
        data = str(self.fun)+str(self.args)+str(self.kwargs)
        thehash = md5()
        thehash.update(data.encode())
        return thehash.hexdigest()

    def is_done(self):
        return self.state in [JobState.SUCCESS, JobState.FAILURE]
    
    def is_safe_to_collect(self):
        return self.safe_to_collect and self.state == JobState.SUCCESS or (self.retries == 0 and self.state == JobState.FAILURE)


    def __call__(self):
        return self
    
    def __str__(self):
        return str(self.__dict__)
    
    __repr__ = __str__

def new_job(fun, *args, **kwargs):
    job = Job(fun=fun)
    job.state = JobState.STOPPED
    job.args = args
    job.kwargs = kwargs

    return job

def on_error(job):
    print("error: ", job)

def on_success(job):
    print("success: ", job)

class TaskQueueManager:
    def __init__(self):
        self.r = Redis()

    def schedule_fun(self, fun, *args, **kwargs):
        return self.schedule(new_job(fun, *args, **kwargs))
    
    def schedule(self, job):
        job.state = JobState.SCHEDULED
        self.save_job(job)
        print(self.r.lpush(WAITING_Q, job.job_key))
        return job

    def _move_job_from_waiting_q_to_worker_q(self, worker_q):
        return self.r.brpoplpush(WAITING_Q, worker_q)

    def get_worker_job_from_worker_queue(self, worker_q):
        activejob = self._move_job_from_waiting_q_to_worker_q(worker_q)
        return activejob
    
    def job_dumps(self, job):
        return dill_dumps(job)

    def job_loads(self, data):
        return dill_loads(data)

    def job_to_success(self, job):
        job.state = JobState.SUCCESS
        now = time.time()
        job.last_modified_time = now
        job.done_time = now
        return job

    def prepare_to_reschedule(self, job):
        job.worker_id = None
        return job

    def set_job_result(self, job, value):
        job.result = value
        print("setting result to : ", value)
        dumped_value = dill_dumps(value)
        self.r.set(job.job_result_key, dumped_value)
        self.r.set(job.job_result_key_cached, dumped_value)
        self.save_job(job)
        return job

    def get_job_result_from_redis(self, job):
        try:
            if self.r.exists(job.job_result_key_cached):
                val = dill_loads(self.r.get(job.job_result_key_cached))
            else:
                val = dill_loads(self.r.get(job.job_result_key))
        except Exception as e:
            raise e

    def get_job_result(self, job, raise_exception=False):
        if raise_exception and job.state == JobState.FAILURE:
            job.safe_to_collect = True
            self.save_job(job)
            raise RuntimeError(job.error)

        job = tm.get_job(job.job_key)
        val = None
        try:
            if job.memoized and self.r.exists(job.job_result_key_cached):
                val = dill_loads(self.r.get(job.job_result_key_cached))
            else:
                val = dill_loads(self.r.get(job.job_result_key))
        except Exception as e:
            print("[-] error getting job result: ", e)
        job.safe_to_collect = True
        self.save_job(job)
        return val
    
    def wait_job(self, job):
        job = tm.get_job(job.job_key)

        while True:
            if job.state in [JobState.SUCCESS, JobState.FAILURE]:
                break
            job = tm.get_job(job.job_key)
            print(job.state, job.job_key)

            sleep(1)

    def save_job(self, job):
        self.r.set(job.job_key, self.job_dumps(job))

    def get_job(self, job_key):
        return self.job_loads(self.r.get(job_key))

    def job_to_failure(self, job):
        job.retries -= 1
        job.state = JobState.FAILURE
        job.error = str(traceback.format_exc())
        job.last_modified_time = time.time()
        return job

    def clean_job_from_worker_queue(self, jobkey, worker_queue):
        self.r.lrem(worker_queue, jobkey)

    def get_jobs_of_worker(self, worker_id):
        return self.r.lrange("tq:queue:worker:{}".format(worker_id), 0, -1)

    def get_jobs_of_worker_queue(self, worker_queue):
        return self.r.lrange(worker_queue, 0, -1)

    def get_all_jobs_keys(self):
        return self.r.keys("tq:job:*")

    def get_worker_queues(self):
        return self.r.keys("tq:queue:worker*")

    def get_worker_last_seen(self, worker_id):
        last_seen_key = "tq:worker-{}-last_seen".format(worker_id)
        last_seen = self.tm.r.get(last_seen_key).decode()
        return last_seen

class WorkerIdMixin:

    @property
    def worker_id(self):
        if not self._worker_id:
            self._worker_id = str(uuid4())
        return self._worker_id
    
    @property
    def worker_last_seen_key(self):
        return "tq:worker-{}-last_seen".format(self.worker_id)
    
    @property
    def worker_queue_key(self):
        return "tq:queue:worker-{}".format(self.worker_id)




class GeventWorker(WorkerIdMixin):
    def __init__(self, taskqueuemanager, greenlet=True):
        self.tm= taskqueuemanager
        self.greenlet = greenlet
        self._worker_id = None

    def send_heartbeat(self):
        self.tm.r.set(self.worker_last_seen_key, time.time())

    def work(self):
        jn = 0
        print("Starting worker: # ", self.worker_id)
        def execute_job_in_greenlet(job):
            fn = job.getfn()
            args = job.args
            kwargs = job.kwargs
            if job.timeout == 0:
                f = g.spawn(fn, *args, **kwargs)
                f.link_exception(partial(self.on_job_error, job))
                f.link_value(partial(self.on_job_success, job))
            else:
                print("executing with timeout: ", )
                try:
                    f = g.with_timeout(job.timeout, fn, *args, *kwargs)
                    f.link_exception(partial(self.on_job_error, job))
                    f.link_value(partial(self.on_job_success, job))
                except g.Timeout as e:
                    print("timeout happened.")
                    self.on_job_error(job, None)
            # self.tm.save_job(job)

        while True:
            self.send_heartbeat()
            g.sleep(1)
            jn += 1
            print("jn # ", jn)
            jobkey = self.tm.get_worker_job_from_worker_queue(self.worker_queue_key)
            if not jobkey:
                continue
            job = self.tm.get_job(jobkey)
            job.worker_id = self.worker_id
            job.state = JobState.RUNNING
            job.start_time = time.time()
            self.tm.save_job(job)
            fn = job.getfn()
            args, kwargs = job.args, job.kwargs
            print("executing fun: {} and memoized {}".format(job.fun_name, job.memoized))
            if job.memoized and self.tm.r.exists(job.job_result_key_cached):
                val = self.tm.get_job_result(job)
                job = self.tm.job_to_success(job)
                job = self.tm.set_job_result(job, val)
                self.tm.save_job(job)
                continue 
            else:
                if self.greenlet and not job.in_process:
                    execute_job_in_greenlet(job)
                else:
                    try:
                        res = fn(*args, **kwargs)
                    except Exception as e:
                        print("[-]Exception in worker: ", e)
                        self.on_job_error(job, None)

                    else:
                        job = self.tm.job_to_success(job)
                        self.tm.set_job_result(job, res)
                        self.tm.save_job(job)
                        # remember cache key.

    def on_job_error(self, job, g):
        print("[-] Job failed\n", job)
        job = self.tm.job_to_failure(job)
        self.tm.save_job(job)
        if job.retries > 0:
            print("Scheduling again for retry")
            self.tm.schedule(job)

    def on_job_success(self, job, g):
        job = self.tm.job_to_success(job)
        print("[+] Job succeeded\n", job)
        value = g.value
        print(value)
        self.tm.set_job_result(job, value)
        self.tm.save_job(job)

    
class WorkersMgr:
    WORKER_DEAD_TIMEOUT = 60 # seconds
    def __init__(self, workerclass, taskqueuemanager, cleanjobs=False):
        self._workerclass = workerclass
        self.tm= taskqueuemanager
        self.start_reaping_deadworkers()
        if cleanjobs:
            self.start_cleaning_jobs()


    def new_worker(self):
        w = self._workerclass(self.tm)
        return w
    
    def start_new_worker(self):
        w = self.new_worker()
        return g.spawn(w.work)
    
    def start_reaping_deadworkers(self):
        return g.spawn(self._reaping_deadworkers)
    
    def start_cleaning_jobs(self):
        return g.spawn(self._clean_jobs)

    def _clean_jobs(self):
        while True:
            print("cleaning jobs....")
            jobkeys = self.tm.get_all_jobs_keys()
            for jobkey in jobkeys:
                job = self.tm.get_job(jobkey)
                if job.is_safe_to_collect():
                    print("[INFO] job completed .. cleaning up the {} job".format(jobkey))
                    self.tm.r.delete(jobkey)
            g.sleep(1)
        
    def _reaping_deadworkers(self):
        while True:
            print("reaping...")
            worker_queues = self.tm.get_worker_queues()
            print("work queues", worker_queues)
            
            for wq in worker_queues:
                for jobkey in self.tm.get_jobs_of_worker(wq):
                    job = self.tm.get_job(jobkey)
                    print("JOB STATE: ", job.state)
                    if job.is_safe_to_collect():
                        print("[INFO] job completed .. cleaning up the {} queue".format(wq))
                        self.tm.clean_job_from_worker_queue(jobkey, wq)

                    job_worker_id = re.findall(WORKER_QUEUE_ID_PATTERN , wq.decode())[0]
                    print(job_worker_id)

                    last_seen = self.tm.get_worker_last_seen(job_worker_id)
                    print("LAST SEEN FOR WORKER {} is {} ".format(job_worker_id, last_seen))
                    ## CHECK WORKER_DEAD_TIMEOUT + activejob timeout too calculation
                    if time.time() - float(last_seen) > WorkersMgr.WORKER_DEAD_TIMEOUT: 
                        print("WORKER {} is dead".format(job.worker_id))
                        job = self.tm.prepare_to_reschedule(job)
                        self.tm.schedule(job)
            sleep(1)

tm = TaskQueueManager()