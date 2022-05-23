#!/usr/bin/env python
# -*- coding: utf-8 -*-
import prefect
from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import Local, GitHub
from prefect.executors import DaskExecutor
import cloudpickle
import pandas as pd
import requests
import json
from flask_socketio import send, emit

FLOW_NAME = "test-state-handler"
storage = Local()

def my_state_handler(obj, old_state, new_state):
    msg = "\nCalling my custom state handler on {0}:\n{1} to {2}\n"
    print(msg.format(obj, old_state.result, new_state))
    return new_state

def post_my_state_handler(obj, old_state, new_state):
    #requests.post("http://localhost:5000/test-state", json=json.loads(str({"task": "task", "state" : str(new_state.serialize())})))
    msg = "Calling my custom state handler on {0} : {1} to {2}"
    print({'data': msg.format(obj, old_state, new_state)})
    requests.post("http://localhost:5000/test-state", json.dumps({'data': msg.format(obj, old_state, new_state)}))

@task(log_stdout=True, name="add_data", state_handlers=[post_my_state_handler])
def add(x, y):
    return x + y

@task(log_stdout=True, name="add_data", state_handlers=[post_my_state_handler])
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello world!")

with Flow(name=FLOW_NAME) as flow:
    hello_task()

flow.run()
    