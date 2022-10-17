# importing in modules 
import os
import shutil
from flask import Flask, request, render_template, redirect, url_for
from flask import send_from_directory, Response, send_file
from flask import session, request, copy_current_request_context
from flask_socketio import SocketIO, emit, disconnect
from flask_session import Session
from werkzeug.utils import secure_filename
import sys
from datetime import datetime
from time import sleep
import subprocess
from uuid import uuid4
import csv
import json
import base64

# Setting Variables
app = Flask(__name__,static_folder="static/",template_folder="templates/")
SESSION_TYPE = 'filesystem'
app.config.from_object(__name__)
socketio = SocketIO(app,logger=False, engineio_logger=True,ping_timeout=600)
Session(app)
app.secret_key = "secret key"
ALLOWED_EXTENSIONS = set(['csv'])
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
path = os.getcwd()

# Parsing the filename and returning data back
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Home route that acts different based on if its a Get or Post request
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            print(file.filename)
            if not os.path.isdir(os.path.join(path, 'upload',session['number'],'input','input')):
                os.makedirs(os.path.join(path, 'upload',session['number'],'input','input'))
            if not os.path.isdir(os.path.join(path, 'upload',session['number'],'input','evictions')):
                os.makedirs(os.path.join(path, 'upload',session['number'],'input','evictions'))
            if not os.path.isdir(os.path.join(path, 'upload',session['number'],'input','mortgage_foreclosures')):
                os.makedirs(os.path.join(path, 'upload',session['number'],'input','mortgage_foreclosures'))
            if not os.path.isdir(os.path.join(path, 'upload',session['number'],'input','tax_lien_foreclosures')):
                os.makedirs(os.path.join(path, 'upload',session['number'],'input','tax_lien_foreclosures') )
            filename = secure_filename(file.filename)
            if file.filename == "evictions.csv":
                save_location = os.path.join(path, 'upload',session['number'],'input','evictions', filename)
            elif file.filename == "mortgage_foreclosures.csv":
                save_location = os.path.join(path, 'upload',session['number'],'input','mortgage_foreclosures', filename)
            elif file.filename == "tax_lien_foreclosures.csv":
                save_location = os.path.join(path, 'upload',session['number'],'input','tax_lien_foreclosures', filename)
            else:
                socketio.emit('logerror',{"errormessage":"Wrong File Name","loginfo": "You must upload a file that is named either 'evictions.csv' or 'mortgage_foreclosures.csv' or 'tax_lien_foreclosures.csv'."+ "<br>"})
            file.save(save_location)
            socketio.emit('uploadcomplete')
    else:
        session['number'] = str(uuid4())
    return render_template('index.html', async_mode=socketio.async_mode), 200


# A Websocket to remove that tmp and upload folder after the prediction is finished.
@socketio.event
def killdata():
    shutil.rmtree('./upload/'+str(session['number']))

# The websocket that starts the run.py prediction model
@socketio.event
def run_tool():

    # Checking to see if the tool is already running. If it is then we let the user know.
    if "toolstatus" in session:
        if session["toolstatus"] == "running":
            toolstatuscheck = "running"
            emit('toolAR',{"loginfo":"The tool is already running. Please wait till the tool is finished or refresh the browser to start over."})
    else:
        toolstatuscheck = "not running"

    # Checking to make sure the upload folder exists
    if toolstatuscheck == "not running":
        if not os.path.isdir("upload/"+str(session['number'])+'/input/'):
            emit('logerror',{"errormessage":"File Missing","loginfo": "No file is selected. Please select a file using the 'Browse'."+ "<br>"})
            inputcheck = "fail"
        else:
            inputcheck = "success"
    else:
        inputcheck = "fail"
    try:
        if inputcheck == "success":
            session["toolstatus"] = "running"
            os.environ["PYTHONUNBUFFERED"] = "1"
            emit('clearoutput')
            emit('loadicon')
            # starting realtime pipe to websocket
            with subprocess.Popen(["python","cli/load_data.py","upload/"+str(session['number'])+'/input/'],stdout=subprocess.PIPE,shell=False,bufsize=1,universal_newlines=True) as process:
                for linestdout in process.stdout:
                    linestdout = linestdout.rstrip()
                    try:
                        emit('logTool',{"loginfo": linestdout+ "<br>"})
                        print(linestdout)
                    except Exception as e:
                        exception_type, exception_object, exception_traceback = sys.exc_info()
                        filename = exception_traceback.tb_frame.f_code.co_filename
                        line_number = exception_traceback.tb_lineno
                        exceptstring = str(exception_type).replace("<","").replace(">","")
                        reason = "ERROR: "+ str(exception_object)+"<br>\
                        Exception type: "+ str(exceptstring)+"<br>\
                        File name: "+ str(filename)+"<br>\
                        Line number: "+ str(line_number)+"<br>"
                        emit('logTool',{"loginfo": str(reason)+ "<br>"})
                        print(str(reason))
            zipdir = os.path.join(path,'upload',session['number'],'output_data')
            desdrive = os.path.join(path,'upload',session['number'],"results")
            shutil.make_archive(base_name=desdrive, format='zip', base_dir="./",root_dir=zipdir)

            emit('showzip')
            session["toolstatus"] = "stopped"

    except Exception as e:
        session["toolstatus"] = "stopped"
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        exceptstring = str(exception_type).replace("<","").replace(">","")
        reason = "ERROR: "+ str(exception_object)+"<br>\
        Exception type: "+ str(exceptstring)+"<br>\
        File name: "+ str(filename)+"<br>\
        Line number: "+ str(line_number)+"<br>"

        emit('logTool',{"loginfo": str(reason)+ "<br>"})
        

@app.route('/sendFile')
def sendFile():
    with open("upload/"+str(session['number'])+"/results.zip", 'rb') as f:
            data = f.readlines()
    return Response(data, headers={
        'Content-Type': 'application/zip',
        'Content-Disposition': 'attachment; filename=%s;' % "output_data"
    })

# A disconnection socket, may or may not be used
@socketio.on('disconnect')
def test_disconnect():
    print('Client disconnected', request.sid)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=os.environ.get("PORT"), threaded=True)
    socketio.run(app,host="0.0.0.0", port=os.environ.get("PORT"), threaded=True)
