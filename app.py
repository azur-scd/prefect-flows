from flask import Flask, Response,jsonify, abort, render_template,request,url_for,request,session, copy_current_request_context
from flask_json import FlaskJSON, JsonError, json_response, as_json
from flask_cors import CORS, cross_origin
from flask_socketio import SocketIO, emit, disconnect
from threading import Lock
import pandas as pd
import requests
import json

async_mode = None
app = Flask(__name__)
FlaskJSON(app)
CORS(app)
socket_ = SocketIO(app, async_mode=async_mode)
thread = None
thread_lock = Lock()

@app.route('/')
def index():
    return render_template('test.html', async_mode=socket_.async_mode)


@socket_.on('my_event', namespace='/test')
def test_message(message):
    print(message)
    emit('my_response',
         {'data': message['data']})


"""@socket_.on('my_broadcast_event', namespace='/test')
def test_broadcast_message(message):
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my_response',
         {'data': message['data'], 'count': session['receive_count']},
         broadcast=True)


@socket_.on('disconnect_request', namespace='/test')
def disconnect_request():
    @copy_current_request_context
    def can_disconnect():
        disconnect()

    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my_response',
         {'data': 'Disconnected!', 'count': session['receive_count']},
         callback=can_disconnect)"""



@app.route('/test-state', methods = ['POST'])
@socket_.on('my_response', namespace='/test')
def get_state():
    message = request.get_json(force=True)
    print(message)
    #emit('my_response', {'data':message}, broadcast=True, namespace='/test')
    emit('my_response', message, broadcast=True, namespace='/test')
    return message


if __name__ == '__main__':
    socket_.run(app,debug=True,port=5000,host="0.0.0.0") 