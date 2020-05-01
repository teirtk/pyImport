import time
import importlib
import pkgutil
import psycopg2
import config
import os
from flask import Flask, render_template, request
from waitress import serve
import glob
import io

UPLOAD_FOLDER = 'uploads'

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


def __init__():
    for f in glob.glob(UPLOAD_FOLDER+'/*'):
        os.unlink(f)


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/upload/<modname>', methods=['POST'])
def upload_file(modname):
    f = request.files['file']
    filename = os.path.join(app.config['UPLOAD_FOLDER'], f.filename)
    f.save(filename)
    return plugins[modname].process(filename)


if __name__ == "__main__":
    __init__()
    plugins = { 
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("mod_")
    }
    conn = psycopg2.connect(database=config.db["db"], user=config.db["user"],
                            password=config.db["passwd"], host=config.db["host"], port=config.db["port"])
    cur = conn.cursor()
    for id in plugins:
        cur.execute(plugins[id].SQL)
    conn.commit()
    conn.close()
    serve(app, host='0.0.0.0', port=5000)
   
