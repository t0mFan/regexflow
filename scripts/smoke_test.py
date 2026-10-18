import argparse,json,time,urllib.request,http.cookiejar
parser=argparse.ArgumentParser()
parser.add_argument('--url',default='http://localhost:8080')
args=parser.parse_args()
jar=http.cookiejar.CookieJar()
client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
base=args.url.rstrip('/')+'/api'
def req(path,data=None):
 headers={'Content-Type':'application/json','Origin':args.url.rstrip('/')}
 if data is not None:headers['X-CSRFToken']=token
 request=urllib.request.Request(base+path, data=json.dumps(data).encode() if data is not None else None, headers=headers)
 with client.open(request) as response:return json.load(response)
token=req('/bootstrap/')['csrf_token']
def wait(job):
 for _ in range(90):
  latest=req('/jobs/'+job['id']+'/')
  if latest['status'] in ['SUCCESS','FAILED','CANCELLED']:
   assert latest['status']=='SUCCESS',latest
   return latest
  time.sleep(.5)
 raise AssertionError('Job did not finish')
ingest=wait(req('/jobs/',{'kind':'ingest','demo':True}))
assert ingest['metadata']['rows']==250
result=wait(req('/jobs/',{'kind':'transform','dataset':ingest['id'],'operation':'replace','columns':['Email'],'prompt':'find email addresses','replacement':'REDACTED'}))
page=req('/jobs/'+result['id']+'/results/?page=1')
assert all(row['Email']=='REDACTED' for row in page['rows'])
assert page['total_rows']==250
extract=wait(req('/jobs/',{'kind':'transform','dataset':ingest['id'],'operation':'extract','columns':['Notes'],'prompt':'find email addresses'}))
page=req('/jobs/'+extract['id']+'/results/')
assert all('@example.com' in row['Notes'] and 'Contact' not in row['Notes'] for row in page['rows'])
normalize=wait(req('/jobs/',{'kind':'transform','dataset':ingest['id'],'operation':'normalize','columns':['Name'],'prompt':'lowercase and trim'}))
page=req('/jobs/'+normalize['id']+'/results/')
assert all(row['Name']==row['Name'].strip().lower() for row in page['rows'])
print(json.dumps({'ingest':'PASS','replace':'PASS','extract':'PASS','normalize':'PASS','rows':250,'pipeline':'HTTP -> Django -> Redis -> Celery -> Spark -> paginated API'},indent=2))
