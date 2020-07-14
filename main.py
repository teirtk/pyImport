import os
import atexit
import tempfile
from flask import Flask, g, request, make_response
from waitress import serve
import config
from mod import caytrong, channuoi, dichbenh

tmpDir = tempfile.TemporaryDirectory()
app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = tmpDir.name
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

postgreSQL_pool = config.pgPool


def init():
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
    tmpDir.cleanup()
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
    html = """
    <!DOCTYPE html>
<html>

<head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <title></title>
    <meta name="robots" content="noindex, nofollow">
    <meta name="googlebot" content="noindex, nofollow">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>File Uploader - truongvucuong</title>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/open-iconic/1.1.1/font/css/open-iconic-bootstrap.min.css"
        rel="stylesheet">

    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css"
        integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">
    <style>
        /* prefixed by https://autoprefixer.github.io (PostCSS: v7.0.26, autoprefixer: v9.7.3) */

        .btn:focus,
        .upload-btn:focus {
            outline: 0 !important
        }

        body,
        html {
            height: 90%;
            background-color: #4791d2
        }

        body {
            text-align: center
        }

        .row {
            margin-top: 2em
        }

        .upload-btn {
            color: #fff;
            background-color: #f89406;
            border: 0;
            margin-top: 20px
        }

        .upload-btn.active,
        .upload-btn:active,
        .upload-btn:focus,
        .upload-btn:hover {
            color: #fff;
            background-color: #fa8900;
            border: 0
        }

        .card {
            -webkit-box-shadow: 0 0 1px transparent;
            box-shadow: 0 0 1px transparent;
            width: 70%
        }

        #format,
        label {
            color: #f0f8ff
        }

        h8 {
            padding-bottom: 30px;
            color: #b8bdc1
        }

        h2 {
            margin-top: 15px;
            color: #68757e
        }

        .panel {
            padding-top: 20px;
            padding-bottom: 20px
        }

        .dz-preview,
        #upload-input {
            display: none
        }

        .oi-cloud-upload:before {
            font-size: 5em
        }

        @media (min-width:768px) {
            .main-container {
                width: 100%
            }
        }

        @media (min-width:992px) {
            .container {
                width: 650px
            }
        }

        .highlight {
            -webkit-box-shadow: inset 0 0 0 4px #2098d1, 0 0 1px transparent;
            box-shadow: inset 0 0 0 4px #2098d1, 0 0 1px transparent
        }
    </style>

</head>

<body>
    <div class="container">
        <div class="row">
            <label class="col-sm-4">Dữ liệu nhập</label>
            <div class="col" id="format">
                <div class="form-check form-check-inline">
                    <input class="form-check-input" type="radio" name="format" id="inlineRadio1" value="caytrong"
                        checked>
                    <label class="form-check-label" for="inlineRadio1">Cây trồng</label>
                </div>
                <div class="form-check form-check-inline">
                    <input class="form-check-input" type="radio" name="format" id="inlineRadio2" value="dichbenh">
                    <label class="form-check-label" for="inlineRadio2">Dịch bệnh</label>
                </div>
                <div class="form-check form-check-inline">
                    <input class="form-check-input" type="radio" name="format" id="inlineRadio3" value="channuoi">
                    <label class="form-check-label" for="inlineRadio3">Chăn nuôi</label>
                </div>
            </div>
            <div class="col-12">
                <div class="card card-default rounded mx-auto" id="my-dropzone" class="dropzone">
                    <div class="card-body">
                        <span class="oi oi-cloud-upload" aria-hidden="true"></span>
                        <h2>File Uploader</h2>
                        <h8>Thả vào đây file .xls hoặc .xlsx</h8>
                        <div class="progress">
                            <div class="progress-bar bg-info" role="progressbar" id="progress-bar"></div>
                        </div>
                        <button class="btn btn-lg upload-btn" for="upload-input" type="button" id="upload-btn">Upload
                            File</button>
                    </div>
                </div>
            </div>
            <div class="col">
            </div>
        </div>
        <div class="row">
            <div class="col-12">
                <textarea class="form-control" id="history" rows="7" readonly></textarea>
            </div>
        </div>
    </div>
    <script src="https://code.jquery.com/jquery-3.4.1.slim.min.js"
        integrity="sha384-J6qa4849blE2+poT4WnyKhv5vZF5SrPo0iEjwBvKU7imGFAV0wwj1yYfoRSJoZ+n"
        crossorigin="anonymous"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js"
        integrity="sha384-wfSDF2E50Y2D1uUdj0O3uMBJnjuUD4Ih7YwaYd1iqfktj0Uod8GCExl3Og8ifwB6"
        crossorigin="anonymous"></script>
    <script type="application/javascript"
        src="https://cdnjs.cloudflare.com/ajax/libs/dropzone/5.7.0/min/dropzone.min.js">
        </script>
    <script>
        let uploadProgress = []
        let progressBar = document.getElementById('progress-bar')
        function initializeProgress(numFiles) {
            uploadProgress = []
            for (let i = numFiles; i > 0; i--) {
                uploadProgress.push(0)
            }
        }

        function updateProgress() {
            let total = Object.keys(uploadProgress).reduce((tot, key) => tot + uploadProgress[key], 0) / Object.keys(uploadProgress).length
            total = Math.round(total)
            progressBar.style.width = total + '%'
        }

        let myDropzone = new Dropzone("div#my-dropzone", {
            method: "POST",
            url: "/upload/",
            timeout: 180000,
            acceptedFiles: "application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            paramName: "file", 
            clickable: document.getElementById("upload-btn"),
            chunking: true,
            previewsContainer: null,
            parallelUploads: 5,
            forceChunking: true,
            maxFilesize: 1025, // megabytes
            chunkSize: 5000000, // bytes
            addedfile: function (file) {
                this.options.url = "/upload/" + document.querySelector("input[name=format]:checked").value;
                uploadProgress[file.upload.uuid] = 0
            },
            uploadprogress: function (file, progress, bytesSent) {
                uploadProgress[file.upload.uuid] = progress
                updateProgress()
            },
            success: function () {
                var args = Array.prototype.slice.call(arguments);
                document.getElementById("history").value = args[0].xhr.responseText + document.getElementById("history").value
            },
            sending: function (file, xhr, formData) {
                /*Called just before each file is sent*/
                xhr.ontimeout = (() => {
                    /*Execute on case of timeout only*/
                    console.log('Server Timeout')
                });
            },
            queuecomplete: function () {
                uploadProgress = []
                progressBar.style.width = '0%'
            }
        });
        Dropzone.options.myDropzone = false;
        // This is useful when you want to create the
        // Dropzone programmatically later

        // Disable auto discover for all elements:
        Dropzone.autoDiscover = false;       
    </script>
</body>

</html>
    """
    return html


@app.route('/upload/<modname>', methods=['POST'])
def upload_file(modname):
    file = request.files['file']

    save_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    current_chunk = int(request.form['dzchunkindex'])

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
            os.unlink(save_path)
            return make_response(('Size mismatch', 500))
        result = "Sai định dạng"
        if modname == "caytrong":
            result = caytrong.process(save_path, get_db())
        elif modname == "channuoi":
            result = channuoi.process(save_path, get_db())
        elif modname == "dichbenh":
            result = dichbenh.process(save_path, get_db())
        os.unlink(save_path)
        return make_response(result, 200)

    return make_response((f"Chunk upload successful {modname}", 200))


if __name__ == "__main__":
    init()
    serve(app, host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
