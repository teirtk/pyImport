import os
import glob
import importlib
import pkgutil
from flask import Flask, render_template, request
from waitress import serve
from psycopg2 import pool
import config


ALLOWED_EXTENSIONS = {'.xls', '.xlsx'}
app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

postgreSQL_pool = pool.ThreadedConnectionPool(1, 10, \
    database=config.db["db"], user=config.db["user"], \
    password=config.db["passwd"], host=config.db["host"], port=config.db["port"])


def __init__():
    try:
        os.makedirs(app.config['UPLOAD_FOLDER'])
    except FileExistsError:
        pass
    for file in glob.glob(app.config['UPLOAD_FOLDER']+'/*'):
        if os.path.isfile(file):
            os.unlink(file)

    conn = postgreSQL_pool.getconn()
    cur = conn.cursor()
    for idx in config.ext:
        cur.execute(config.ext[idx]["sql"])
        conn.commit()
    postgreSQL_pool.putconn(conn)


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/upload/<modname>', methods=['POST'])
def upload_file(modname):

    file = request.files['file']
    _, file_extension = os.path.splitext(file.filename)
    if file_extension.lower() in ALLOWED_EXTENSIONS:
        filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filename)
        return plugins[modname].process(filename, postgreSQL_pool)
    return "Tập tin sai định dạng\n"


if __name__ == "__main__":
    plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("mod_")
    }
    __init__()
    serve(app, host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
    if postgreSQL_pool:
        postgreSQL_pool.closeall()
