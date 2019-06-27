# tq

`tq` is taskqueue to process your long expensive functions in the background based on `redis` and `gevent`


## how it works

```python
from tq import *
from time import sleep

def f():
    # functions are self contained.
    from time import sleep
    sleep(5)
    return "Yea"

job = tm.schedule_fun(f) # async execute

tm.wait_job(job) # if u want the value need to block on it until it's available
print("DONE: ", tm.get_job_result(job))

```

## design

- uses queues
    - main queue for pushing jobs id
    - for each worker the taskqueuemanager fans out jobs from main queue to a queue per worker atomically using `brpoplpush`
- allows retrying for jobs (user specified param. default is 3)
- in case of a dead worker with job isn't declared as processed until the worker `ack`s
- jobs are declared safe to collect by the garbage collector if user already accessed `value` or `exception` attributes
- `get_job_result_from_redis` to retrieve the results values from redis directly even after the job gets reaped


### streams
I'd like to investigate the usage of redis streams in the future


## benchmarks

TBD