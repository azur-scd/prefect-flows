from flask import Flask, Response,jsonify, abort, render_template,request,url_for,request,session, copy_current_request_context
from flask_json import FlaskJSON, JsonError, json_response, as_json
from flask_cors import CORS, cross_origin
from flask_socketio import SocketIO, emit, disconnect
from threading import Lock
import pandas as pd
import requests
import json
import os
import subprocess

async_mode = None
app = Flask(__name__)
FlaskJSON(app)
CORS(app)
socket_ = SocketIO(app, async_mode=async_mode)
thread = None
thread_lock = Lock()

class Installer:
   def run(command):
        # Some code here
        proc = subprocess.Popen(
            [ 'powershell.exe', command ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return proc

def file_path(relative_path):
    folder = os.path.dirname(os.path.abspath("__file__"))
    path_parts = relative_path.split("/")
    new_path = os.path.join(folder, *path_parts)
    return new_path

@app.route('/')
def index():
    flows_py = []
    for root, dirs, files in os.walk("flows"):
        for file in files:
            if file == "flow.py":
                print(root)
                #flows_py.append(os.path.join(root, file))
                flows_py.append(root.partition("\\")[2])
    return render_template('flows_ui.html', flows_py=flows_py,len=len(flows_py),async_mode=socket_.async_mode)


"""@app.route('/')
def index():
    return render_template('test.html', async_mode=socket_.async_mode)"""


@socket_.on('my_event', namespace='/test')
def test_message(message):
    print(message)
    emit('my_response',
         {'data': message['data']})

@app.route('/', methods = ['POST'])
@socket_.on('my_response', namespace='/test')
def get_state():
    message = request.get_json(force=True)
    print(message)
    #emit('my_response', {'data':message}, broadcast=True, namespace='/test')
    emit('my_response', message, broadcast=True, namespace='/test')
    return message

@app.route('/runflow', methods = ['POST'])
def runflow():
    request_data = json.loads(request.data)
    flow_py_name = request_data.get('flow_py_name')
    #subprocess.call("prefect run -p flows/bso_theses/flow.py",shell=True)
    #subprocess.run(["prefect","run","-p","flows/bso_theses/flow.py"], shell=True)
    installer = Installer()
    installer.run("C:/Users/geoffroy/Docker/prefect-flows/venv_prefect/Scripts/python.exe prefect run -p ../../flows/bso_theses/flow.py")


if __name__ == '__main__':
    socket_.run(app,debug=True,port=5000,host="0.0.0.0") 
    #usage console : python app.py puis prefect run -p flows/bso_theses/flow.py