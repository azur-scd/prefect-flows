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
from subprocess import Popen, PIPE, STDOUT,  CalledProcessError

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

# correspond à socket.on('connect', function(){} dans flow_ui.html
@socket_.on('my_event', namespace='/test')
def test_message(message):
    print(message)
    emit('my_response',
         {'data': message['data']})

# correspond à socket.on('my_response', function(data) {} dans flow_ui.html
@app.route('/', methods = ['POST'])
@socket_.on('my_response', namespace='/test')
def get_state():
    message = request.get_json(force=True)
    print(message)
    #emit('my_response', {'data':message,'counter':out}, broadcast=True, namespace='/test')
    emit('my_response', message, broadcast=True, namespace='/test')
    return message

# tentative de lancement par CLI du flow
@app.route('/runflow', methods = ['POST'])
def runflow():
    request_data = json.loads(request.data)
    flow_py_name = request_data.get('flow_py_name')
    """process = Popen("prefect run -p flows/{0}/flow.py".format(flow_py_name), shell=True, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        try:
            for line in iter(process.stdout.readline, b''):
                print(line.decode("utf-8").strip())
            
        except CalledProcessError as e:
            print(f"{str(e)}")"""
    try:
        subprocess.check_output("C:/Users/geoffroy/Docker/prefect-flows/venv_prefect/Scripts/python.exe","prefect","run","-p","flows/flow_de_test/flow.py",shell=True,stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
    #subprocess.run(["prefect","run","-p","flows/bso_theses/flow.py"], shell=True)
    #installer = Installer()
    #installer.run("C:/Users/geoffroy/Docker/prefect-flows/venv_prefect/Scripts/python.exe prefect run -p ../../flows/bso_theses/flow.py")


if __name__ == '__main__':
    socket_.run(app,debug=True,port=5000,host="0.0.0.0") 
    #usage console : python app.py puis prefect run -p flows/bso_theses/flow.py