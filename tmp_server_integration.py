import subprocess
import time
import requests
import os
import signal

if __name__ == '__main__':
    proc = subprocess.Popen([os.sys.executable, 'web.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        # wait a bit for server to start
        time.sleep(2)
        r = requests.get('http://127.0.0.1:5000/', timeout=5)
        print('status:', r.status_code)
        print('len:', len(r.text))
        print(r.text[:400].replace('\n', '\\n'))
    except Exception as e:
        print('error', type(e).__name__, e)
        out, err = proc.communicate(timeout=1)
        print('server stdout:', out.decode(errors='replace')[:1000])
        print('server stderr:', err.decode(errors='replace')[:1000])
    finally:
        try:
            proc.terminate()
        except Exception:
            pass
        proc.wait(timeout=3)
