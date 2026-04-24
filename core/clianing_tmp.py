import atexit
import shutil
import tempfile

def cleanup_chrome_temp():
    shutil.rmtree('/tmp/.com.google.Chrome.*', ignore_errors=True)

atexit.register(cleanup_chrome_temp)