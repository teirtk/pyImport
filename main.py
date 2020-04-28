import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import importlib
import pkgutil
import re
import psycopg2
import config
import argparse
import os

conn = None


class Watcher:

    def __init__(self, dir="Pandas_In"):
        self.observer = Observer()
        self.DIRECTORY_TO_WATCH = dir
        for f in ([os.path.join(f.path, "data") for f in os.scandir(dir) if f.is_dir()]):
            with open(f, 'w'):
                pass

    def run(self):
        print("Watching directory: ", self.DIRECTORY_TO_WATCH)
        event_handler = Handler()
        self.observer.schedule(
            event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        global conn
        if event.is_directory:
            return None

        elif event.event_type == "created":
            # Take any action here when a file is first created.
            if event.src_path.endswith(".xls") or event.src_path.endswith(".xlsx"):
                match = re.search(r"\\(.+)\\", event.src_path)
                if match:
                    mod_name = "mod_"+match.group(1).lower()
                    plugins[mod_name].process(event.src_path, conn)


if __name__ == "__main__":
    plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith("mod_")
    }
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--withdb", help="Ghi dữ liệu vào postgres, edit thông số kết nối trong config.py", action="store_true")
    args = parser.parse_args()
    config.withdb = args.withdb
    if config.withdb:
        conn = psycopg2.connect(database=config.db["db"], user=config.db["user"],
                                password=config.db["passwd"], host=config.db["host"], port=config.db["port"])
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS caytrong (data jsonb);")
        cur.execute("CREATE TABLE IF NOT EXISTS dichbenh (loai character varying(20),nhom character varying(20),svgh character varying(50),gdst character varying(100),dtnhiemnhe numeric,dtnhiemtb numeric,dtnhiemnang numeric,dttong numeric,dtmattrang numeric,dtsokytruoc numeric,dtphongtru numeric,phanbo character varying(100),fdate date NOT NULL,tdate date NOT NULL,mdpb1 numeric,mdpb2 numeric,mdcao1 numeric,mdcao2 numeric);")
    w = Watcher()
    w.run()
    if config.withdb:
        conn.close()
