import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import importlib
import pkgutil
import re
import psycopg2
discovered_plugins = []
cur = None


class Watcher:

    def __init__(self, dir='Pandas_In'):
        self.observer = Observer()
        self.DIRECTORY_TO_WATCH = dir

    def run(self):
        event_handler = Handler()
        self.observer.schedule(
            event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print('Error')

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            match = re.search(r'\\(.+)\\', event.src_path)
            if match:
                mod_name = 'mod_'+match.group(1).lower()
                discovered_plugins[mod_name].process(event.src_path, conn,'dichbenh')


if __name__ == '__main__':
    discovered_plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith('mod_')
    }
    conn = psycopg2.connect(database='postgres', user='postgres',
                            password='b7ec6b08dc1f4f3383663c0ecdf5dda7', host='localhost', port='5432')
    w = Watcher()
    w.run()
    conn.close()
