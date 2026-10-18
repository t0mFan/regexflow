import json, tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from django.test import TestCase, SimpleTestCase, Client, override_settings
from pipeline.models import Job
from pipeline.planner import validate_pattern, validate_plan, make_plan
from pipeline.engine import spark_session, transform, materialize, excel_to_csv

class PatternTests(SimpleTestCase):
    def test_email_is_valid(self):
        self.assertEqual(validate_pattern(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
    def test_rejects_dangerous_patterns(self):
        for value in ["(a+)+$", "(a|aa)+$", r"(a)\1", "(?=abc)", "[", "a{9999}"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_pattern(value)
    def test_rejects_unapproved_normalization(self):
        with self.assertRaises(ValueError):
            validate_plan({"pattern":"", "normalization":"exec", "explanation":"bad"}, "normalize")
    def test_demo_is_explicit_and_restricted(self):
        plan, hit = make_plan("find email addresses", "replace", True)
        self.assertIn("Demo", plan["explanation"])
        with self.assertRaises(ValueError):
            make_plan("find phone numbers", "replace", True)
    @patch('pipeline.planner.OpenAI')
    @patch('pipeline.planner.cache')
    def test_cache_hit_avoids_llm(self, mocked_cache, mocked_llm):
        mocked_cache.return_value.get.return_value=json.dumps({"pattern":"abc", "normalization":"", "explanation":"test"})
        plan, hit=make_plan("find abc", "replace")
        self.assertTrue(hit)
        mocked_llm.assert_not_called()

class APITests(TestCase):
    def setUp(self):
        self.client.get('/api/bootstrap/')
        self.owner=self.client.session['owner']
    def test_cross_session_isolation(self):
        job=Job.objects.create(owner=self.owner,kind='ingest')
        other=Client()
        self.assertEqual(other.get(f'/api/jobs/{job.id}/').status_code,404)
        self.assertEqual(other.get(f'/api/jobs/{job.id}/results/').status_code,404)
    @override_settings(DEMO_ENABLED=True)
    @patch('pipeline.views.execute_job.apply_async')
    def test_submission_returns_202_without_doing_work(self, mocked):
        response=self.client.post('/api/jobs/',data=json.dumps({'kind':'ingest','demo':True}),content_type='application/json')
        self.assertEqual(response.status_code,202)
        self.assertEqual(response.json()['status'],'QUEUED')
        mocked.assert_called_once()
    @override_settings(DEMO_ENABLED=False)
    def test_disabled_demo(self):
        response=self.client.post('/api/jobs/',data=json.dumps({'kind':'ingest','demo':True}),content_type='application/json')
        self.assertEqual(response.status_code,403)
    def test_csrf_enforced(self):
        client=Client(enforce_csrf_checks=True)
        response=client.post('/api/jobs/',data='{}',content_type='application/json')
        self.assertEqual(response.status_code,403)
    def test_cancel_sets_flag(self):
        job=Job.objects.create(owner=self.owner,kind='ingest')
        response=self.client.post(f'/api/jobs/{job.id}/cancel/')
        job.refresh_from_db()
        self.assertEqual(response.status_code,200)
        self.assertTrue(job.cancel_requested)
    def test_results_page_bounds_and_empty(self):
        with tempfile.TemporaryDirectory() as root, override_settings(DATA_ROOT=Path(root)):
            job=Job.objects.create(owner=self.owner,kind='ingest',status='SUCCESS')
            directory=Path(root,str(job.id),'pages');directory.mkdir(parents=True)
            (directory/'manifest.json').write_text(json.dumps({'columns':['A'],'pages':[], 'rows':0}))
            self.assertEqual(self.client.get(f'/api/jobs/{job.id}/results/').json()['rows'],[])
            self.assertEqual(self.client.get(f'/api/jobs/{job.id}/results/?page=2').status_code,404)
    @patch('pipeline.views.execute_job.apply_async',side_effect=RuntimeError('broker unavailable'))
    @override_settings(DEMO_ENABLED=True)
    def test_broker_failure_does_not_leave_queued_job(self, mocked):
        response=self.client.post('/api/jobs/',data=json.dumps({'kind':'ingest','demo':True}),content_type='application/json')
        self.assertEqual(response.status_code,503)
        self.assertEqual(Job.objects.latest('created_at').status,'FAILED')

class TaskTests(TestCase):
    @patch('pipeline.tasks.spark_session')
    def test_cancelled_job_does_not_start_spark(self, spark):
        from pipeline.tasks import execute_job
        job=Job.objects.create(owner='test',kind='ingest',cancel_requested=True)
        execute_job.run(str(job.id))
        job.refresh_from_db()
        self.assertEqual(job.status,'CANCELLED')
        spark.assert_not_called()
    @patch('pipeline.tasks.spark_session',side_effect=RuntimeError('secret-access-key'))
    def test_external_error_details_are_not_exposed(self, spark):
        from pipeline.tasks import execute_job
        job=Job.objects.create(owner='test',kind='ingest')
        execute_job.run(str(job.id))
        job.refresh_from_db()
        self.assertEqual(job.status,'FAILED')
        self.assertNotIn('secret',job.error)

class SparkTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark=spark_session()
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        super().tearDownClass()
    def frame(self):
        return self.spark.createDataFrame([('1','  ALICE  ','alice@example.com'),('2',' Bob ',None),('3','Carol','nothing')],['ID','Name','Email']).repartition(2)
    def test_replace_literal_value_and_null(self):
        plan={'pattern':validate_pattern(r'[A-Za-z]+@example\.com')}
        rows=transform(self.frame(),['Email'],'replace',plan,r'$1\value').orderBy('ID').collect()
        self.assertEqual(rows[0].Email,r'$1\value')
        self.assertIsNone(rows[1].Email)
        self.assertEqual(rows[2].Email,'nothing')
    def test_extract_and_normalize(self):
        out=transform(self.frame(),['Email'],'extract',{'pattern':r'[A-Za-z]+@example\.com'},'').orderBy('ID').collect()
        self.assertEqual(out[0].Email,'alice@example.com')
        self.assertIsNone(out[2].Email)
        out=transform(self.frame(),['Name'],'normalize',{'normalization':'lower'},'').orderBy('ID').collect()
        self.assertEqual(out[0].Name,'alice')
    def test_paginated_materialization_and_unicode(self):
        frame=self.spark.createDataFrame([(str(i),'中文') for i in range(251)],['ID','text']).repartition(2)
        with tempfile.TemporaryDirectory() as root:
            meta=materialize(frame,root)
            manifest=json.loads(Path(root,'manifest.json').read_text())
            rows=[row for name in manifest['pages'] for row in json.loads(Path(root,name).read_text())]
            self.assertEqual(meta['rows'],251)
            self.assertEqual(len({r['ID'] for r in rows}),251)
            self.assertTrue(all(len(json.loads(Path(root,name).read_text()))<=100 for name in manifest['pages']))
            self.assertEqual(rows[0]['text'],'中文')
    def test_stream_excel(self):
        from openpyxl import Workbook
        import csv
        with tempfile.TemporaryDirectory() as root:
            source=Path(root,'test.xlsx');out=Path(root,'test.csv')
            wb=Workbook();ws=wb.active;ws.append(['Name','Email']);ws.append(['Alice','alice@example.com']);wb.save(source)
            excel_to_csv(source,out)
            with out.open() as stream:
                self.assertEqual(list(csv.reader(stream))[1],['Alice','alice@example.com'])

class CredentialTests(TestCase):
    @patch('pipeline.security.cache')
    @patch.dict('os.environ', {'CREDENTIAL_ENCRYPTION_KEY':'MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA='})
    def test_vault_ciphertext_is_scoped_and_roundtrips(self, mocked):
        from pipeline.security import vault_put, vault_get
        data={'access_key':'example-key','secret_key':'private-secret','bucket':'my-bucket'}
        token=vault_put('owner',data)
        key,ttl,value=mocked.return_value.setex.call_args.args
        self.assertEqual(ttl,86400)
        self.assertEqual(key,f'vault:owner:{token}')
        self.assertNotIn(b'private-secret',value)
        mocked.return_value.get.return_value=value
        self.assertEqual(vault_get('owner',token),data)
    @patch('pipeline.views.vault_put',return_value='connection-id')
    @patch('pipeline.views.s3_client')
    def test_s3_credentials_never_returned(self, s3, vault):
        self.client.get('/api/bootstrap/')
        response=self.client.post('/api/connections/',data=json.dumps({'access_key':'example-key','secret_key':'private-secret','bucket':'my-bucket','region':'us-east-1'}),content_type='application/json')
        self.assertEqual(response.status_code,200)
        self.assertNotIn('private-secret',response.content.decode())
        s3.return_value.list_objects_v2.assert_called_once_with(Bucket='my-bucket',MaxKeys=1)
    @patch('pipeline.views.s3_client')
    def test_s3_error_is_sanitized(self, s3):
        from botocore.exceptions import ClientError
        s3.return_value.list_objects_v2.side_effect=ClientError({'Error':{'Code':'InvalidAccessKeyId','Message':'private-secret'}},'ListObjectsV2')
        response=self.client.post('/api/connections/',data=json.dumps({'access_key':'example-key','secret_key':'private-secret','bucket':'my-bucket'}),content_type='application/json')
        self.assertEqual(response.status_code,400)
        self.assertNotIn('private-secret',response.content.decode())

@patch.dict('os.environ',{'LLM_PROVIDER':'groq','GROQ_API_KEY':'fake-groq-test-key','GROQ_MODEL':'openai/gpt-oss-20b'})
class GroqTests(SimpleTestCase):
    def response(self,model,plan,reason='stop'):
        response=MagicMock()
        response.choices=[MagicMock(finish_reason=reason,message=MagicMock(content=json.dumps(plan)))]
        model.return_value.chat.completions.create.return_value=response
        return response
    @patch('pipeline.planner.cache')
    @patch('pipeline.planner.OpenAI')
    def test_groq_calls_only_groq_and_validates_plan(self,model,redis):
        redis.return_value.get.return_value=None
        self.response(model,{'pattern':'abc','normalization':'','explanation':'match abc'})
        plan,hit=make_plan('find abc','replace')
        self.assertEqual(plan['pattern'],'abc');self.assertFalse(hit)
        self.assertEqual(model.call_args.kwargs['base_url'],'https://api.groq.com/openai/v1')
        self.assertEqual(model.call_args.kwargs['api_key'],'fake-groq-test-key')
        kwargs=model.return_value.chat.completions.create.call_args.kwargs
        self.assertTrue(kwargs['response_format']['json_schema']['strict'])
        self.assertEqual(kwargs['model'],'openai/gpt-oss-20b')
        model.return_value.responses.create.assert_not_called()
        redis.return_value.setex.assert_called_once()
    @patch('pipeline.planner.cache')
    @patch('pipeline.planner.OpenAI')
    def test_missing_groq_key_does_not_fall_back_to_openai(self,model,redis):
        redis.return_value.get.return_value=None
        with patch.dict('os.environ',{'GROQ_API_KEY':'','OPENAI_API_KEY':'fake-paid-key'}):
            with self.assertRaisesRegex(ValueError,'GROQ_API_KEY'):
                make_plan('find abc','replace')
        model.assert_not_called()
    @patch('pipeline.planner.cache')
    @patch('pipeline.planner.OpenAI')
    def test_truncated_groq_output_is_not_cached(self,model,redis):
        redis.return_value.get.return_value=None
        self.response(model,{'pattern':'abc','normalization':'','explanation':'test'},'length')
        with self.assertRaisesRegex(ValueError,'did not complete'):
            make_plan('find abc','replace')
        redis.return_value.setex.assert_not_called()
    @patch('pipeline.planner.cache')
    @patch('pipeline.planner.OpenAI')
    def test_unsafe_groq_pattern_is_not_cached(self,model,redis):
        redis.return_value.get.return_value=None
        self.response(model,{'pattern':'(a+)+','normalization':'','explanation':'test'})
        with self.assertRaises(ValueError):make_plan('find abc','replace')
        redis.return_value.setex.assert_not_called()
    @patch('pipeline.planner.cache')
    @patch('pipeline.planner.OpenAI')
    def test_provider_caches_are_separate(self,model,redis):
        redis.return_value.get.return_value=None
        self.response(model,{'pattern':'abc','normalization':'','explanation':'test'})
        make_plan('find abc','replace')
        groq_key=redis.return_value.get.call_args.args[0]
        model.return_value.responses.create.return_value.output_text=json.dumps({'pattern':'abc','normalization':'','explanation':'test'})
        with patch.dict('os.environ',{'LLM_PROVIDER':'openai','OPENAI_API_KEY':'fake-openai-test-key'}):
            make_plan('find abc','replace')
        openai_key=redis.return_value.get.call_args.args[0]
        self.assertNotEqual(groq_key,openai_key)

class MeaningValidationTests(SimpleTestCase):
    def test_rejects_explicitly_unsupported_request(self):
        plan={'supported':False,'reason':'固定值赋值不是格式标准化，请使用匹配与替换。','pattern':'','normalization':'','explanation':''}
        with self.assertRaisesRegex(ValueError,'固定值赋值'):
            validate_plan(plan,'normalize')
    def test_extraction_cannot_silently_apply_normalization(self):
        with self.assertRaisesRegex(ValueError,'cannot also'):
            validate_plan({'pattern':'name','normalization':'trim','explanation':'test'},'extract')
    def test_preset_does_not_turn_unrecognized_request_into_lowercase(self):
        with self.assertRaises(ValueError):
            make_plan('把所有notes转换为test','normalize',True)
    def test_preset_trim_does_not_change_case(self):
        plan,_=make_plan('只去除两端空格','normalize',True)
        self.assertEqual(plan['normalization'],'trim')
    def test_no_match_extraction_stops_before_nulling_column(self):
        from pipeline.engine import matching_stats
        frame=MagicMock();frame.rdd.mapPartitions.return_value.fold.return_value=(250,0)
        with self.assertRaisesRegex(ValueError,'avoid turning the entire column into nulls'):
            matching_stats(frame,['Name'],'extract',{'pattern':'name'})
    def test_no_match_replacement_provides_warning(self):
        from pipeline.engine import matching_stats
        frame=MagicMock();frame.rdd.mapPartitions.return_value.fold.return_value=(250,0)
        stats=matching_stats(frame,['Name'],'replace',{'pattern':'name'})
        self.assertEqual(stats['matched_cells'],0)
        self.assertTrue(stats['warnings'])
    def test_empty_extraction_does_not_fail(self):
        from pipeline.engine import matching_stats
        frame=MagicMock();frame.rdd.mapPartitions.return_value.fold.return_value=(0,0)
        self.assertEqual(matching_stats(frame,['Name'],'extract',{'pattern':'name'})['populated_cells'],0)
    def test_column_label_is_not_a_name_pattern(self):
        from pipeline.engine import count_matches_partition,transform_partition
        from pyspark.sql import Row
        source=[Row(Name='John Doe'),Row(Name='张三'),Row(Name=None)]
        plan={'pattern':r'[\s\S]+'}
        self.assertEqual(list(count_matches_partition(iter(source),['Name'],'extract',plan)),[(2,2)])
        output=list(transform_partition(iter(source),['Name'],'extract',plan,''))
        self.assertEqual([r['Name'] for r in output],['John Doe','张三',None])

@patch.dict('os.environ',{'LLM_PROVIDER':'groq','GROQ_API_KEY':'fake-groq-test-key'})
class ContextPlannerTests(SimpleTestCase):
    @patch('pipeline.planner.cache')
    @patch('pipeline.planner.OpenAI')
    def test_target_column_names_are_in_prompt_and_cache_key(self,model,redis):
        redis.return_value.get.return_value=None
        model.return_value.chat.completions.create.return_value.choices=[MagicMock(finish_reason='stop',message=MagicMock(content=json.dumps({'pattern':r'[\s\S]+','normalization':'','explanation':'保留姓名列的值','supported':True,'reason':''})))]
        make_plan('找到文本中的name','extract',columns=['Name'])
        first_key=redis.return_value.get.call_args.args[0]
        request=model.return_value.chat.completions.create.call_args.kwargs
        context=json.loads(request['messages'][1]['content'])
        self.assertEqual(context['selected_columns'],['Name'])
        self.assertEqual(context['instruction'],'找到文本中的name')
        make_plan('找到文本中的name','extract',columns=['Notes'])
        self.assertNotEqual(first_key,redis.return_value.get.call_args.args[0])
