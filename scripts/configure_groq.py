#!/usr/bin/env python3
"""Enter your own Groq key locally; input is hidden and never printed."""
import getpass, os, subprocess, sys
from pathlib import Path
root=Path(__file__).resolve().parent.parent
path=root/'.env'
if not path.exists():
    subprocess.run([sys.executable,str(root/'scripts/configure.py')],check=True)
print('Create your own key at https://console.groq.com/keys and stay on the Free plan.')
print('This enables real Groq requests for sample data. Only transformation instructions are sent, not table rows.')
key=getpass.getpass('Groq API Key (hidden; Enter to cancel): ').strip()
if not key:
    raise SystemExit('Cancelled. Configuration unchanged.')
if len(key)<16 or any(c.isspace() for c in key):
    raise SystemExit('Invalid key format. Configuration unchanged.')
updates={'LLM_PROVIDER':'groq','GROQ_API_KEY':key,'GROQ_MODEL':'openai/gpt-oss-20b','DEMO_USE_LIVE_AI':'1'}
lines=path.read_text().splitlines();seen=set()
for i,line in enumerate(lines):
    name=line.split('=',1)[0]
    if name in updates:
        lines[i]=name+'='+updates[name];seen.add(name)
for name,value in updates.items():
    if name not in seen:lines.append(name+'='+value)
# An exclusive private temporary file avoids exposing keys during replacement.
tmp=path.with_name('.env.groq.tmp')
fd=os.open(tmp,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
with os.fdopen(fd,'w') as stream:stream.write('\n'.join(lines)+'\n')
os.replace(tmp,path)
print('Saved privately. Run: docker compose up -d --force-recreate backend worker')
print('Then: docker compose exec worker python check_ai.py')
