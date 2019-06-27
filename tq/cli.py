import gevent as g
from gevent import sleep
from gevent import monkey; monkey.patch_all()

from tq import GeventWorker, TaskQueueManager, WorkersMgr, new_job
from terminaltables import AsciiTable 
import time, os, sys, inspect
import click


@click.group()
def cli():
    pass 

@click.command()
@click.option("--number", "-n", default=4, help="number of workers")
def workers(number=4):
    tm = TaskQueueManager()
    wm = WorkersMgr(GeventWorker, tm)
    futures = []
    for w in range(int(number)):
        f = wm.start_new_worker()
        futures.append(f)
    g.joinall(futures, raise_error=False)

@click.command()
def info():
    tm = TaskQueueManager()
    headers = [["JID", "State", "Retries", "Function", "WID"]]
    jobskeys = tm.get_all_jobs_keys()
    jobs = [tm.get_job(jobkey) for jobkey in jobskeys]
    rows = [[job.job_id, job.state.value, job.retries, job.fun_name, job.worker_id] for job in jobs]
    if rows:
        table_data = headers + rows
        t = AsciiTable(title="TQ info # {} jobs".format(len(rows)), table_data=table_data)
        print(t.table)
    else:
        print("TQ info not found.")

@click.command()
def clean():
    print("cleaning up all `tq:*` keys")
    tm = TaskQueueManager()
    keys = tm.r.keys("tq:*")
    for k in keys:
        tm.r.delete(k)

def fake_test_jobs():
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

    def fail2Timeout():
        sleep(5)

    failingjob1 = new_job(fail2Timeout)
    failingjob1.timeout = 1
    q.schedule(failingjob1)


    def fail3Timeout():
        for i in range(5):
            print("a very long job")
            sleep(1)

    failingjob2 = new_job(fail3Timeout)
    failingjob2.timeout = 40
    failingjob2.in_process = True
    failingjob2.memoized = False
    q.schedule(failingjob2)

@click.command()
@click.option("--count", "-c", default=5, help="generate fake test jobs")
def fakejobs(count=5):
    for i in range(count):
        fake_test_jobs()
        sleep(1)


cli.add_command(info)
cli.add_command(clean)
cli.add_command(fakejobs)
cli.add_command(workers)
