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


ALLOWED_EXTENSIONS = {'.xls', '.xlsx'}
app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = '/tmp'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


def __init__():
    try:
        os.makedirs(app.config['UPLOAD_FOLDER'])
    except FileExistsError:
        pass
    for f in glob.glob(app.config['UPLOAD_FOLDER']+'/*'):
        os.unlink(f)

    conn = psycopg2.connect(database=config.db["db"], user=config.db["user"],
                            password=config.db["passwd"], host=config.db["host"], port=config.db["port"])
    cur = conn.cursor()
    for id in plugins:
        cur.execute(plugins[id].SQL)
    conn.commit()
    conn.close()


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/upload/<modname>', methods=['POST'])
def upload_file(modname):

    f = request.files['file']
    _, file_extension = os.path.splitext(f.filename)
    if file_extension.lower() in ALLOWED_EXTENSIONS:
        filename = os.path.join(app.config['UPLOAD_FOLDER'], f.filename)
        f.save(filename)
        return plugins[modname].process(filename)
    return "Tập tin sai định dạng\n"


if __name__ == "__main__":
    plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("mod_")
    }
    __init__()
    port = int(os.environ.get("PORT", 5000))
    serve(app, host='0.0.0.0', port=port)
