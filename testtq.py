from tq import *


def f():
    from time import sleep
    sleep(5)
    return "Yea"

job = tm.schedule_fun(f)

from time import sleep

tm.wait_job(job)
print("DONE: ", tm.get_job_result(job))