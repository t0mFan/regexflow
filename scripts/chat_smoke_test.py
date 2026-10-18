#!/usr/bin/env python3
"""Real HTTP + configured LLM + Spark test with synthetic demo data."""
import argparse,http.cookiejar,json,time,urllib.request,urllib.error
parser=argparse.ArgumentParser();parser.add_argument('--url',default='http://localhost:8080');parser.add_argument('--output',default='chat-verification.json');args=parser.parse_args()
origin=args.url.rstrip('/');base=origin+'/api';jar=http.cookiejar.CookieJar();client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar));token=''
def request(path,data=None):
 headers={'Content-Type':'application/json','Origin':origin}
 if data is not None:headers['X-CSRFToken']=token
 req=urllib.request.Request(base+path,data=json.dumps(data,ensure_ascii=False).encode() if data is not None else None,headers=headers)
 with client.open(req,timeout=30) as res:return json.load(res)
def wait(job):
 deadline=time.monotonic()+360;previous=None
 while time.monotonic()<deadline:
  job=request('/jobs/'+job['id']+'/')
  state=(job['status'],job['stage'])
  if state!=previous:print(job['id'][:8],*state,flush=True);previous=state
  if job['status'] not in ['QUEUED','RUNNING']:return job
  time.sleep(1)
 raise AssertionError('Job did not finish within six minutes')
def rows(job):
 page=request('/jobs/'+job['id']+'/results/');result=list(page['rows'])
 for number in range(2,page['page_count']+1):result.extend(request('/jobs/'+job['id']+'/results/?page='+str(number))['rows'])
 return result
print('This test sends synthetic table samples to your configured AI provider.',flush=True)
token=request('/bootstrap/')['csrf_token']
source=wait(request('/jobs/',{'kind':'ingest','demo':True}));assert source['status']=='SUCCESS',source
source_rows=rows(source);assert len(source_rows)==250
context=request('/jobs/'+source['id']+'/ai-context/');assert 0<len(context['sample_rows'])<=8
assert len(json.dumps(context,ensure_ascii=False).encode())<=6000
first=wait(request('/jobs/',{'kind':'transform','dataset':source['id'],'prompt':'Set all Notes values to test and trim Name.'}));assert first['status']=='SUCCESS',first
first_rows=rows(first);assert len(first_rows)==250
assert all(r['Notes']=='test' and r['Name']==r['Name'].strip() for r in first_rows)
question=wait(request('/jobs/',{'kind':'transform','dataset':first['id'],'prompt':'Change it'}));assert question['status']=='NEEDS_INPUT',question
assert question['metadata']['reply']
try:request('/jobs/'+question['id']+'/results/')
except urllib.error.HTTPError as exc:assert exc.code==409
else:raise AssertionError('An unclear instruction published a result')
followup=wait(request('/jobs/',{'kind':'transform','dataset':first['id'],'prompt':'Set all Notes values to clarified. Leave everything else unchanged.','messages':[{'role':'user','content':'Change it'},{'role':'assistant','content':question['metadata']['reply'][:1000]}]}));assert followup['status']=='SUCCESS',followup
followup_rows=rows(followup);assert all(r['Notes']=='clarified' and r['Name']==r['Name'].strip() for r in followup_rows)
selected=wait(request('/jobs/',{'kind':'transform','dataset':followup['id'],'prompt':'Keep only the Name column without changing its values.'}));assert selected['status']=='SUCCESS',selected
selected_rows=rows(selected);assert len(selected_rows)==250 and all(set(row)=={'Name'} and row['Name'] for row in selected_rows)
assert rows(source)==source_rows,'Original data changed'
assert selected['metadata']['original_dataset']==source['id']
report={'unified_prompt':'PASS','sample_context':'PASS','multi_step_edit':'PASS','unclear_instruction':'NEEDS_INPUT; no result published','clarification_followup':'PASS','chained_result':'PASS','column_selection':'PASS','original_unchanged':'PASS','rows_checked':250,'source_id':source['id'],'result_id':selected['id'],'provider':first['metadata']['provider']}
with open(args.output,'w') as out:json.dump(report,out,indent=2,ensure_ascii=False)
print(json.dumps(report,indent=2,ensure_ascii=False))
