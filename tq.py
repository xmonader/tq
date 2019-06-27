from gevent import monkey; monkey.patch_all()
from uuid import uuid4
import re
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
WAITING_Q = "tq:queue:waiting-jobs"
WORKER_QUEUE_ID_PATTERN = "tq:queue:worker-(.+)?"

class Job:
    def __init__(self, fun, retries=3):
        self._job_id = None
        self.fun = dill_dumps(fun)
        self.funname = fun.__name__
        self.retries = retries
        self.state = STOPPED
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
    
    def __call__(self):
        return self
    
    def __str__(self):
        return str(self.__dict__)
    
    __repr__ = __str__

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


class TaskQueueManager:
    def __init__(self):
        self.r = Redis()

    def schedule_fun(self, fun, *args, **kwargs):
        return self.schedule(new_job(fun, *args, **kwargs))
    
    def schedule(self, job):
        job.state = SCHEDULED
        self.job_save(job)
        print(self.r.lpush(WAITING_Q, job.job_key))


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
        job.state = SUCCESS
        now = time.time()
        job.last_modified_time = now
        job.done_time = now
        return job

    def prepare_to_reschedule(self, job):
        job.worker_id = None
        return job

    def job_set_result(self, job, value):
        job.result = value
        print("setting result to : ", value)
        dumped_value = dill_dumps(value)
        self.r.set(job.job_result_key, dumped_value)
        self.r.set(job.job_result_key_cached, dumped_value)
        return job

    def job_get_result(self, job):
        val = None
        try:
            if job.memoized and self.r.exists(job.job_result_key_cached):
                val = dill_loads(self.r.get(job.job_result_key_cached))
            else:
                val = dill_loads(self.r.get(job.job_result_key))
        except Exception as e:
            print("[-] error getting job result: ", e)
        return val

    def job_save(self, job):
        self.r.set(job.job_key, self.job_dumps(job))

    def job_get(self, job_key):
        return self.job_loads(self.r.get(job_key))

    def job_to_failure(self, job):
        job.retries -= 1
        job.state = FAILURE
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
            self.tm.job_save(job)

        while True:
            self.send_heartbeat()
            g.sleep(1)
            jn += 1
            print("jn # ", jn)
            jobkey = self.tm.get_worker_job_from_worker_queue(self.worker_queue_key)
            if not jobkey:
                continue
            job = self.tm.job_get(jobkey)
            job.worker_id = self.worker_id
            job.state = RUNNING
            self.tm.job_save(job)
            fn = job.getfn()
            args, kwargs = job.args, job.kwargs
            print("executing fun: {} and memoized {}".format(job.funname, job.memoized))
            if job.memoized and self.tm.r.exists(job.job_result_key_cached):
                val = self.tm.job_get_result(job)
                job = self.tm.job_to_success(job)
                job = self.tm.job_set_result(job, val)
                self.tm.job_save(job)
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
                        self.tm.job_set_result(job, res)
                        self.tm.job_save(job)
                        # remember cache key.

    def on_job_error(self, job, g):
        print("[-] Job failed\n", job)
        job = self.tm.job_to_failure(job)
        self.tm.job_save(job)
        if job.retries > 0:
            print("Scheduling again for retry")
            self.tm.schedule(job)

    def on_job_success(self, job, g):
        job = self.tm.job_to_success(job)
        print("[+] Job succeeded\n", job)
        value = g.value
        print(value)
        self.tm.job_set_result(job, value)
        self.tm.job_save(job)

    
class WorkersMgr:
    WORKER_DEAD_TIMEOUT = 60 # seconds
    def __init__(self, workerclass, taskqueuemanager):
        self._workerclass = workerclass
        self.tm= taskqueuemanager
        self.start_reaping_deadworkers()
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
                job = self.tm.job_get(jobkey)
                if job.state == SUCCESS or (job.retries == 0 and job.state == FAILURE):
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
                    job = self.tm.job_get(jobkey)
                    print("JOB STATE: ", job.state)
                    if job.state == SUCCESS or (job.retries == 0 and job.state == FAILURE):
                        print("[INFO] job completed .. cleaning up the {} queue".format(wq))
                        self.tm.clean_job_from_worker_queue(jobkey, wq)

                    job_worker_id = re.findall(WORKER_QUEUE_ID_PATTERN , wq.decode())[0]
                    print(job_worker_id)

                    last_seen = self.tm.get_worker_last_seen(job_worker_id)
                    print("LAST SEEN FOR WORKER {} is {} ".format(job_worker_id, last_seen))
                    if time.time() - float(last_seen) > WorkersMgr.WORKER_DEAD_TIMEOUT:
                        print("WORKER {} is dead".format(job.worker_id))
                        job = self.tm.prepare_to_reschedule(job)
                        self.tm.schedule(job)
            sleep(1)

def produce():
    q = TaskQueueManager()

    def longfn1():
        sleep(1)
        return "ok"
    bglongfn1 = new_job(longfn1)
    q.schedule(bglongfn1)

    def longfn2(username, lang="en"):
        sleep(0)
        print("hi ", username, lang)
        return username, lang

    # # bglongfn2 = new_job(longfn2)
    q.schedule_fun(longfn2, username="ahmed", lang="en")
    q.schedule_fun(longfn2, username="dmdm", lang="ar")

    def fail1():
        sleep(1)
        raise ValueError("errr")
    
    q.schedule_fun(fail1)

    # def fail2Timeout():
    #     sleep(5)

    # failingjob1 = new_job(fail2Timeout)
    # failingjob1.timeout = 1
    # q.schedule(failingjob1)


    # def fail3Timeout():
    #     for i in range(5):
    #         print("a very long job")
    #         sleep(1)

    # failingjob2 = new_job(fail3Timeout)
    # failingjob2.timeout = 40
    # failingjob2.in_process = True
    # failingjob2.memoized = False
    # q.schedule(failingjob2)


if __name__ == "__main__":
    tm = TaskQueueManager()

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
        wm = WorkersMgr(GeventWorker, tm)
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
    elif argv[1] == "info":
        jobskeys = tm.get_all_jobs_keys()
        print("Jobs: ", len(jobskeys))
        print("Job #\t\t\t\t\t\t State\t\t Retries ")
        for jobkey in jobskeys:
            job = tm.job_get(jobkey)
            print(job.job_key, "\t", job.state, "\t\t", job.retries)