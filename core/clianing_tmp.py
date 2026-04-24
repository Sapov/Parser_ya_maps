import shutil

def cleanup_chrome_temp():
    shutil.rmtree('/tmp/.com.google.Chrome.*', ignore_errors=True)

