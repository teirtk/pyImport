import os
import glob
import atexit
from flask import Flask, g, render_template, request, make_response
from waitress import serve
import config

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

postgreSQL_pool = config.pgPool


def init():
    try:
        os.makedirs(app.config['UPLOAD_FOLDER'])
    except FileExistsError:
        pass
    for file in glob.glob(app.config['UPLOAD_FOLDER']+'/*'):
        if os.path.isfile(file):
            os.unlink(file)
    conn = postgreSQL_pool.getconn()
    with conn.cursor() as cur:
        for idx in config.ext:
            cur.execute(config.ext[idx]["sql"])
            conn.commit()
    postgreSQL_pool.putconn(conn)


def get_db():
    if 'db' not in g:
        g.db = postgreSQL_pool.getconn()
    return g.db


@atexit.register
def onexit():
    if postgreSQL_pool:
        postgreSQL_pool.closeall()


@app.teardown_appcontext
def close_conn(e=None):
    db = g.pop('db', None)
    if db is None:
        return
    if e is None:
        db.commit()
    else:
        db.rollback()
    postgreSQL_pool.putconn(db)


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/upload/<modname>', methods=['POST'])
def upload_file(modname):

    # file = request.files['file']
    # _, file_extension = os.path.splitext(file.filename)
    # if file_extension.lower() in ALLOWED_EXTENSIONS:
    #     filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    #     file.save(filename)
    #     return plugins[modname].process(filename, postgreSQL_pool)
    # return "Tập tin sai định dạng\n"
    file = request.files['file']

    save_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    current_chunk = int(request.form['dzchunkindex'])

    # If the file already exists it's ok if we are appending to it,
    # but not if it's new file that would overwrite the existing one
    if os.path.exists(save_path) and current_chunk == 0:
        # 400 and 500s will tell dropzone that an error occurred and show an error
        return make_response(('File already exists', 400))

    try:
        with open(save_path, 'ab') as f:
            f.seek(int(request.form['dzchunkbyteoffset']))
            f.write(file.stream.read())
    except OSError:
        # log.exception will include the traceback so we can see what's wrong
        print('Could not write to file')
        return make_response(("Not sure why,"
                              " but we couldn't write the file to disk", 500))

    total_chunks = int(request.form['dztotalchunkcount'])

    if current_chunk + 1 == total_chunks:
        # This was the last chunk, the file should be complete and the size we expect
        if os.path.getsize(save_path) != int(request.form['dztotalfilesize']):
            print(f"File {file.filename} was completed, "
                  f"but has a size mismatch."
                  f"Was {os.path.getsize(save_path)} but we"
                  f" expected {request.form['dztotalfilesize']} ")
            return make_response(('Size mismatch', 500))
        result = config.plugins[modname].process(save_path, get_db())
        os.unlink(save_path)
        return make_response(result, 200)

    return make_response((f"Chunk upload successful {modname}", 200))


if __name__ == "__main__":
    init()
    serve(app, host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
