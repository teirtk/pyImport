import time
import importlib
import pkgutil
import psycopg2
import config
import os
from flask import Flask, render_template, request
from waitress import serve


UPLOAD_FOLDER = 'uploads'

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/upload/<modname>', methods=['POST'])
def upload_file(modname):
    f = request.files['file']
    filename = os.path.join(app.config['UPLOAD_FOLDER'], f.filename)
    f.save(filename)
    return plugins[modname].process(filename, conn)


if __name__ == "__main__":
    plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("mod_")
    }
    for id in plugins:
        print(plugins[id].SQL)
    conn = psycopg2.connect(database=config.db["db"], user=config.db["user"],
                            password=config.db["passwd"], host=config.db["host"], port=config.db["port"])
    cur = conn.cursor()
    for id in plugins:
        cur.execute(plugins[id].SQL)
    serve(app, host='0.0.0.0', port=5000)
    conn.close()
