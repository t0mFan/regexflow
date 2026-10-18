#!/usr/bin/env python
"""Live unified-table assistant check with synthetic table values, no S3."""
import json,os
os.environ.setdefault('DJANGO_SETTINGS_MODULE','config.settings')
import django
django.setup()
from django.conf import settings
from pipeline.assistant import make_table_plan
from pipeline.engine import spark_session
from pipeline.table_engine import execute_table_plan
provider=os.getenv('LLM_PROVIDER','openai');spark=None
print(f'Checking unified table assistant via {provider}; synthetic table samples will be sent to this provider.',flush=True)
try:
    rows=[{'ID':'1','Name':'  Alice  ','Email':'alice@example.com','Notes':'old'},{'ID':'2','Name':' Bob ','Email':'bob@example.com','Notes':'hello'}]
    context={'dataset_id':'synthetic-chat-check-v1','columns':list(rows[0]),'total_rows':2,'sample_rows':rows,'truncated':False}
    prompt='Set all Notes values to test and trim Name.'
    first,hit=make_table_plan(prompt,context)
    repeated,cache_hit=make_table_plan(prompt,context)
    if not cache_hit or repeated!=first:raise ValueError('Cache verification failed.')
    if first['status']!='ready':raise ValueError('A clear multi-step instruction was not understood.')
    spark=spark_session();frame=spark.createDataFrame(rows)
    output,_=execute_table_plan(frame,first);result=output.orderBy('ID').collect()
    if [r.Notes for r in result]!=['test','test'] or [r.Name for r in result]!=['Alice','Bob']:raise ValueError('The generated plan failed the sample-value checks.')
    context2={**context,'dataset_id':'synthetic-chat-check-v1-step2','sample_rows':[r.asDict() for r in result]}
    unclear,_=make_table_plan('Change it',context2)
    if unclear['status']!='clarify' or unclear['steps']:raise ValueError('An unclear instruction was executed instead of requesting clarification.')
    follow,_=make_table_plan('Set all Notes values to done. Leave everything else unchanged.',context2,[{'role':'user','content':'Change it'},{'role':'assistant','content':unclear['message']}])
    continued,_=execute_table_plan(output,follow)
    final=continued.orderBy('ID').collect()
    if [r.Notes for r in final]!=['done','done'] or [r.Name for r in final]!=['Alice','Bob']:raise ValueError('Follow-up editing did not preserve the previous edits.')
    report={'provider':provider,'table_samples':'PASS','unified_prompt':'PASS','automatic_multi_step_plan':'PASS','clarification_without_editing':'PASS','followup_on_current_result':'PASS','cache':'PASS','data':'synthetic examples only'}
    (settings.DATA_ROOT/'ai-verification.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
except Exception as exc:
    print(str(exc) if isinstance(exc,ValueError) else 'AI check failed. Check key, model permissions, quota, and service availability.')
    raise SystemExit(1)
finally:
    if spark is not None:spark.stop()
