#!/usr/bin/env python3
"""Start and stop the entire local development stack; no Docker required."""
import argparse, os, shutil, signal, subprocess, sys, time
from pathlib import Path
root=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser()
parser.add_argument('--python',default=str(root/'.venv/bin/python'))
args=parser.parse_args()
python=str(Path(args.python).absolute())
if not Path(python).exists():
    raise SystemExit('Create .venv and install backend requirements first; see README.md.')
if not (root/'.env').exists():
    subprocess.run([sys.executable,str(root/'scripts/configure.py')],check=True)
env=os.environ.copy()
for line in (root/'.env').read_text().splitlines():
    if line and not line.startswith('#'):
        key,value=line.split('=',1);env[key]=value
runtime=root/'.runtime'
runtime.mkdir(exist_ok=True)
env.update(REDIS_URL='redis://127.0.0.1:16379/0',DATA_ROOT=str(runtime),SPARK_MASTER='local[2]',SPARK_PARTITIONS='4',SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=python,CSRF_TRUSTED_ORIGINS='http://127.0.0.1:5173,http://localhost:5173')
# Prefer a supported Java 17 runtime on macOS when one is installed.
if sys.platform=='darwin':
    java=subprocess.run(['/usr/libexec/java_home','-v','17'],capture_output=True,text=True)
    if java.returncode==0:env['JAVA_HOME']=java.stdout.strip()
for tool in ['redis-server','npm','java']:
    if not shutil.which(tool):raise SystemExit(f'{tool} is required; see README.md.')
subprocess.run([python,'manage.py','migrate','--noinput'],cwd=root/'backend',env=env,check=True)
commands=[('redis',['redis-server','--bind','127.0.0.1','--port','16379','--save','','--appendonly','no','--dir',str(runtime)],root),('api',[python,'manage.py','runserver','127.0.0.1:8000','--noreload'],root/'backend'),('worker',[str(Path(python).parent/'celery'),'-A','config','worker','--pool=solo','--concurrency=1','--loglevel=WARNING'],root/'backend'),('web',['npm','run','dev','--','--port','5173','--strictPort'],root/'frontend')]
processes=[]
logs=[]
def cleanup(*_):
    for process in reversed(processes):
        if process.poll() is None:
            try:os.killpg(process.pid,signal.SIGTERM)
            except ProcessLookupError:pass
    for process in processes:
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL)
    for log in logs:log.close()
    raise SystemExit(0)
signal.signal(signal.SIGINT,cleanup)
signal.signal(signal.SIGTERM,cleanup)
try:
    for name,command,cwd in commands:
        log=(runtime/f'{name}.log').open('a');logs.append(log)
        processes.append(subprocess.Popen(command,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True))
    print('RegexFlow: http://127.0.0.1:5173/\nLogs: .runtime/*.log\nPress Ctrl+C to stop all services.',flush=True)
    while True:
        time.sleep(1)
        for i,process in enumerate(processes):
            if process.poll() is not None:
                print(f'{commands[i][0]} stopped. Check .runtime/{commands[i][0]}.log',flush=True)
                cleanup()
finally:
    for process in processes:
        if process.poll() is None:process.terminate()
