import ast
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import requests
from rasterio.transform import Affine

import data_contract as D
import request_control as N
import vngis_2024 as V
import v6_runtime as R
import verify_pilot as Q
from test_pipeline import admin, records, complete_progress


class ScientificTests(unittest.TestCase):
    def test_fetch_plan_queries_only_active_phase_sources(self):
        collection = Mock()
        collection.filterBounds.return_value = collection
        collection.filterDate.return_value = collection
        collection.size.return_value = 1
        fc = Mock();fc.size.return_value=1
        with patch.object(V.ee,'ImageCollection',return_value=collection) as source, patch.object(V.ee,'List',side_effect=lambda x:x), patch.object(V.ee,'Dictionary',side_effect=lambda x:x), patch.object(V,'ee_getinfo',side_effect=lambda x:x):
            _, day = V.fetch_plan(fc, 'geom', 'day')
            self.assertEqual({c.args[0] for c in source.call_args_list},{V.S2_COLLECTION})
            self.assertEqual(day[1]['indices_count'],1)
            source.reset_mock()
            _, night = V.fetch_plan(fc, 'geom', 'night')
            self.assertEqual({c.args[0] for c in source.call_args_list},{V.VIIRS_A,V.VIIRS_B})
            self.assertEqual(night[1]['viirs'],V.VIIRS_A)

    def test_original_v6_scientific_functions_are_unchanged(self):
        root = Path(__file__).resolve().parents[1]
        functions = {n.name: n for n in ast.parse((root/'vngis_2024.py').read_text()).body
                     if isinstance(n, ast.FunctionDef)}
        for line in (root/'tests/v6_scientific.sha256').read_text().splitlines():
            name, expected = line.split()
            if name == 'task1_all_months':
                current = functions[name]
                self.assertEqual(current.args.args[-1].arg,'months')
                current.args.args.pop()
                current.args.defaults.clear()
                loop = next(n for n in current.body if isinstance(n,ast.For))
                self.assertEqual(ast.dump(loop.iter),ast.dump(ast.parse('MONTHS if months is None else months',mode='eval').body))
                loop.iter = ast.Name(id='MONTHS',ctx=ast.Load())
            actual = hashlib.sha256(ast.dump(functions[name], include_attributes=False).encode()).hexdigest()
            self.assertEqual(actual, expected, name)

    def test_pilot_does_not_increase_workers_and_rejects_quality_reduction(self):
        code = 'import vngis_2024 as v; print(v.N_WORKERS)'
        env = {**os.environ, 'VNGIS_MODE': 'pilot', 'VNGIS_PILOT_N': '8', 'VNGIS_WORKERS': '1'}
        result = subprocess.run([os.sys.executable, '-c', code], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), '1')
        for name, value in [('VNGIS_DAY_BANDS', '6'), ('VNGIS_DAY_FORMAT', 'int16')]:
            result = subprocess.run([os.sys.executable, '-c', code], env={**env, name: value}, capture_output=True)
            self.assertNotEqual(result.returncode, 0)


class RateTests(unittest.TestCase):
    def setUp(self):
        logger = patch.object(R.log, 'disabled', True)
        logger.start(); self.addCleanup(logger.stop)
        self.now = 0.
        self.event = Mock()
        self.event.wait.side_effect = self.advance
        self.gate = N.RequestGate(1, 2, lambda: None, self.event, clock=lambda: self.now)

    def advance(self, seconds):
        self.now += seconds
        return False

    def error(self, code=429, header='7'):
        response = requests.Response()
        response.status_code = code
        response.headers['Retry-After'] = header
        return requests.HTTPError(response=response)

    def test_retry_after_seconds_and_http_date(self):
        self.assertEqual(N.retry_after('7'), 7)
        self.assertEqual(N.retry_after('Thu, 08 Oct 2026 04:00:10 GMT', datetime(2026,10,8,4,0,0,tzinfo=timezone.utc)), 10)
        self.assertIsNone(N.retry_after('invalid'))
        self.assertLessEqual(N.retry_delay(10), 120)

    def test_429_cooldown_is_shared_and_semaphore_released_during_backoff(self):
        def defer(service, delay, throttled):
            self.assertTrue(self.gate.semaphore.acquire(blocking=False))
            self.gate.semaphore.release()
            original(service, delay, throttled)
        original = self.gate.defer
        operation = Mock(side_effect=[self.error(), 'success'])
        with patch.object(self.gate, 'defer', side_effect=defer):
            self.assertEqual(self.gate.call('Earth Engine', operation, 2), 'success')
        self.assertGreaterEqual(self.now, 7)
        self.assertEqual(self.gate.throttles['Earth Engine'], 1)
        self.gate.defer('Image download', 11, True)
        before = self.now
        with self.gate.slot('Earth Engine'):
            self.assertGreaterEqual(self.now-before, 11)

    def test_ee_and_image_download_share_start_rate(self):
        with self.gate.slot('Earth Engine'):
            pass
        with self.gate.slot('Image download'):
            self.assertGreaterEqual(self.now, .5)

    def test_exhausted_429_is_bounded_and_stop_interrupts_wait(self):
        operation = Mock(side_effect=self.error(header='1'))
        with self.assertRaisesRegex(RuntimeError, '6/6'):
            self.gate.call('Earth Engine', operation, 6)
        self.assertEqual(operation.call_count, 6)
        self.assertEqual(self.gate.throttles['Earth Engine'], 6)
        stopped = N.RequestGate(1, 2, Mock(side_effect=V.StopRequested()), threading.Event())
        with self.assertRaises(V.StopRequested):
            stopped.wait(100)

    def test_drive_commands_use_throttle_flags_and_do_not_retry_permission_errors(self):
        with patch.object(V, 'REQUEST_GATE', self.gate), patch.object(R.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', '403 forbidden')) as run:
            R.rclone_run(['lsjson', 'gdrive:sample'])
            self.assertEqual(run.call_count, 1)
            argv = run.call_args.args[0]
            self.assertEqual(argv[argv.index('--tpslimit')+1], '2.0')
            self.assertEqual(argv[argv.index('--transfers')+1], '2')
            self.assertEqual(argv[argv.index('--checkers')+1], '4')

    def test_drive_retry_after_and_successful_command_are_not_replayed(self):
        results = [subprocess.CompletedProcess([],1,'','HTTP 429\nRetry-After: 8'),
                   subprocess.CompletedProcess([],0,'success','Recovered from 429')]
        with patch.object(V,'REQUEST_GATE',self.gate),patch.object(R.subprocess,'run',side_effect=results) as run:
            self.assertEqual(R.rclone_run(['moveto','source','destination']).returncode,0)
            self.assertEqual(run.call_count,2)
        self.assertGreaterEqual(self.now,8)

    def test_download_429_honors_header_without_logging_signed_url(self):
        image = Mock()
        image.getDownloadURL.return_value = 'https://example.invalid/?token=secret'
        first = requests.Response()
        first.status_code = 429
        first.headers['Retry-After'] = '9'
        first._content = b'throttled'
        second = requests.Response()
        second.status_code = 200
        second._content = b'x'*256
        with patch.object(V, 'REQUEST_GATE', self.gate), patch.object(V, '_http_get', side_effect=[first, second]):
            self.assertEqual(V.fetch_geotiff_bytes(image, 'region', 20), b'x'*256)
        self.assertGreaterEqual(self.now, 9)
        self.assertEqual(image.getDownloadURL.call_args.args[0]['scale'], 20)


class FakeDrive:
    def __init__(self, root, local):
        self.root, self.local = Path(root), Path(local)
        self.base = 'fake:VNGISDash_2024_PILOT'
        self.downloads = []
        self.uploads = []
        self.root.mkdir(parents=True)

    def fetch(self, rel, dest, optional=False):
        source, dest = self.root/rel, Path(dest)
        if not source.exists() and optional:
            return None
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
        self.downloads.append(rel)
        return dest

    def metadata(self, path, relative):
        return dict(Path=str(relative), Size=path.stat().st_size, ModTime=str(path.stat().st_mtime_ns),
                    Hashes={'md5': hashlib.md5(path.read_bytes()).hexdigest()})

    def listing(self, rel):
        root = self.root/rel
        return [self.metadata(p, p.relative_to(root)) for p in sorted(root.rglob('*')) if p.is_file()]

    def stat(self, rel, **kwargs):
        path = self.root/rel
        return self.metadata(path, rel) if path.is_file() else None

    def put(self, path, rel, backup=True):
        destination = self.root/rel
        if destination.exists() and backup:
            saved = self.root/f'_control/backups/{len(self.uploads)}/{rel}'
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(destination, saved)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, str(destination)+'.part')
        Path(str(destination)+'.part').replace(destination)
        self.uploads.append(rel)

    def pull_history(self):
        for rel in ['_control/status', '_control/parts']:
            for entry in self.listing(rel):
                self.fetch(f"{rel}/{entry['Path']}", self.local/rel/entry['Path'])

    def flush_outbox(self):
        pass


class EngineTests(unittest.TestCase):
    def setUp(self):
        logger = patch.object(R.log, 'disabled', True)
        logger.start(); self.addCleanup(logger.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.a = admin(('VNM.1.2_1',))
        self.gid = self.a.iloc[0].GID_3
        self.drive = FakeDrive(root/'remote', root/'local')
        self.engine = R.Engine(self.a, self.drive)
        for obj, name, value in [(V,'MIN_FREE_GB',0), (V,'PREFLIGHT',False), (V,'MAX_ATTEMPTS',1),
                                 (V,'MONTH_THREADS',2), (V,'N_WORKERS',2), (V,'UPLOAD_EVERY_SEC',300)]:
            if hasattr(obj, name):
                patcher = patch.object(obj,name,value)
                patcher.start(); self.addCleanup(patcher.stop)
        for name in ['communes_fc']:
            patcher = patch.object(V,name,Mock());patcher.start();self.addCleanup(patcher.stop)
        patcher = patch.object(V.ee.Filter,'eq',return_value=Mock());patcher.start();self.addCleanup(patcher.stop)
        patcher = patch.dict(os.environ,{'VNGIS_MIN_FREE_GB':'0'});patcher.start();self.addCleanup(patcher.stop)
        V.STOP_EVENT.clear();V.STOP_REASON[0]=None

    def write_image(self, month, kind='day'):
        ctx = self.engine.contexts[self.gid]
        path = self.drive.root/ctx['rel_'+kind+'_dir']/(V.day_name(ctx,month) if kind == 'day' else V.night_name(ctx,month))
        self.image_bytes(path, 20 if kind == 'day' else 500)
        return path

    @staticmethod
    def image_bytes(path, scale):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        bands = D.DAY_BANDS if scale == 20 else ['avg_rad','cf_cvg']
        V.write_tif(str(path),np.ones((len(bands),4,4),dtype='float64'),
                    Affine(scale/111319.49079327357,0,105,0,-scale/111319.49079327357,21),'EPSG:4326',-9999,bands)

    def fill_day(self):
        for m in D.MONTHS:
            self.write_image(m)
        path = self.drive.root/'CSV/day_indices.csv'
        path.parent.mkdir(exist_ok=True)
        records(self.a).to_csv(path,index=False)

    def plan(self, fc, geom, kind):
        return 1,{m:dict(image_count=1,indices_count=1,s2_window=0,viirs=V.VIIRS_A) for m in D.MONTHS}

    def fake_download(self,image,region,scale,path,label):
        self.image_bytes(path,scale)

    def test_inventory_migrates_with_backup_and_does_not_trust_old_done(self):
        self.fill_day()
        original = (self.drive.root/'CSV/day_indices.csv').read_bytes()
        self.engine.inventory()
        backups = list((self.drive.root/'_control/backups').rglob('day_indices.csv'))
        self.assertEqual(backups[0].read_bytes(),original)
        self.assertEqual(list(pd.read_csv(self.drive.root/'CSV/day_indices.csv')),D.COLUMNS['day'])
        self.write_image(5).unlink()
        table = self.engine.inventory()
        self.assertEqual(table.loc[table.month.eq(5),'day_image'].iloc[0],'pending')
        with self.assertRaises(RuntimeError):
            D.require_day_complete(table)
        self.assertEqual(len(table),12)

    def test_corrupt_file_cache_is_reused_until_file_changes(self):
        self.fill_day()
        self.write_image(4).write_bytes(b'corrupt')
        self.engine.inventory()
        self.assertEqual(self.engine.table.loc[3,'day_image'],'failed')
        with patch.object(D,'validate_image',wraps=D.validate_image) as decode:
            self.engine.inventory()
            decode.assert_not_called()
        self.write_image(4)
        with patch.object(D,'validate_image',wraps=D.validate_image) as decode:
            self.engine.inventory()
            self.assertEqual(decode.call_count,1)
        D.require_day_complete(self.engine.table)

    def test_interrupted_inventory_checkpoints_and_only_decodes_remaining_files(self):
        self.fill_day()
        with patch.object(V,'check_stop',side_effect=[None,None,V.StopRequested()]):
            with self.assertRaises(V.StopRequested):
                self.engine.inventory()
        cache = self.drive.root/'_control/validated_images.json'
        self.assertEqual(len(json.loads(cache.read_text())),2)
        with patch.object(D,'validate_image',wraps=D.validate_image) as decode:
            self.engine.inventory()
            self.assertEqual(decode.call_count,10)
        D.require_day_complete(self.engine.table)

    def test_missing_indices_only_queries_missing_month_and_keeps_valid_values(self):
        self.fill_day()
        rows = records(self.a).iloc[1:]
        rows.to_csv(self.drive.root/'CSV/day_indices.csv',index=False)
        self.engine.inventory()
        def task(fc,months):
            self.assertEqual(months,[1])
            incoming = records(self.a).iloc[[0]].copy()
            incoming[D.DAY_METRICS] *= 100
            return incoming.rename(columns={'GID_3':'GID_3','MONTH':'MONTH'}).to_dict('records')
        with patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'task1_all_months',side_effect=task),patch.object(V,'download_tif') as download:
            self.assertTrue(self.engine.run_phase('day'))
            download.assert_not_called()
        self.assertEqual(self.engine.frames['day'].iloc[0].BLUE_mean,100)
        self.assertEqual(self.engine.frames['day'].iloc[1].BLUE_mean,2)

    def test_missing_image_only_downloads_that_month(self):
        self.fill_day();self.write_image(7).unlink()
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'task1_all_months') as indices,patch.object(V,'download_tif',side_effect=self.fake_download) as download:
            self.assertTrue(self.engine.run_phase('day'))
            indices.assert_not_called()
            self.assertEqual(download.call_count,1)
        self.engine.inventory()
        D.require_day_complete(self.engine.table)

    def test_confirmed_no_source_keeps_twelve_blank_rows(self):
        self.engine.inventory()
        plan = {m:dict(image_count=0,indices_count=0,s2_window=None) for m in D.MONTHS}
        with patch.object(V,'fetch_plan',return_value=(1,plan)),patch.object(V,'task1_all_months') as task,patch.object(V,'download_tif') as download:
            self.assertTrue(self.engine.run_phase('day'))
            task.assert_not_called();download.assert_not_called()
        self.engine.inventory()
        D.require_day_complete(self.engine.table)
        self.assertTrue(self.engine.table.day_image.eq('no_source').all())
        self.assertTrue(self.engine.frames['day'][D.DAY_METRICS].isna().all().all())

    def test_no_source_is_not_inferred_from_failed_requests_and_night_is_blocked(self):
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=RuntimeError('HTTP 429 quota exhausted')):
            self.assertFalse(self.engine.run_phase('day'))
        self.assertTrue(self.engine.table.day_image.eq('failed').all())
        self.assertFalse(self.engine.sources)
        with patch.object(V,'fetch_plan') as query,patch.object(V,'task3_all_months') as task:
            with self.assertRaises(RuntimeError):
                self.engine.run_phase('night')
            query.assert_not_called();task.assert_not_called()

    def test_actual_day_then_night_pipeline_and_verifier(self):
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'night_image'),patch.object(V,'task1_all_months',return_value=records(self.a).to_dict('records')),patch.object(V,'task3_all_months',return_value=records(self.a,'night')),patch.object(V,'download_tif',side_effect=self.fake_download):
            self.assertTrue(self.engine.run_phase('day'))
            self.assertTrue(self.engine.table.night_image.eq('pending').all())
            self.engine.inventory()
            self.assertTrue(self.engine.run_phase('night'))
        self.engine.inventory()
        checks = Q.verify_commune(self.drive.root,self.gid,{},self.engine.frames['day'],self.engine.frames['night'])
        self.assertTrue(all(level == 'PASS' for _,level,_ in checks),checks)

    def test_asset_missing_gid_remains_failed_not_skipped(self):
        self.engine.inventory()
        full = admin(tuple([self.gid]+[f'VNM.9.{i}_1' for i in range(11162)]))
        with patch.object(V,'ee_getinfo',return_value=list(full.GID_3)[1:]):
            with self.assertRaisesRegex(ValueError,'thiếu 1'):
                self.engine.check_asset(full)
        self.assertTrue(self.engine.table.day_image.eq('failed').all())

    def test_failed_attempt_limit_survives_restart(self):
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=RuntimeError('HTTP 429 exhausted')):
            self.assertFalse(self.engine.run_phase('day'))
        resumed = R.Engine(self.a,self.drive)
        resumed.inventory()
        with patch.object(V,'fetch_plan') as query:
            self.assertFalse(resumed.run_phase('day'))
            query.assert_not_called()

    def test_recover_valid_legacy_parts_if_csv_missing(self):
        part = self.drive.root/'_control/parts/day_legacy.jsonl'
        part.parent.mkdir(parents=True)
        part.write_text(''.join(json.dumps(r)+'\n' for r in records(self.a).to_dict('records')))
        self.engine.inventory()
        self.assertTrue(self.engine.table.day_indices.eq('done').all())
        self.assertEqual(len(self.engine.frames['day']),12)

    def test_main_stops_after_day_failure_and_reports_final_sync_failure(self):
        fake = Mock()
        fake.drive.stat.return_value=None
        fake.root = self.drive.local
        fake.table = complete_progress(self.a)
        fake.run_phase.return_value=False
        with patch.object(R,'Engine',return_value=fake),patch.object(V,'build_admin_table',return_value=self.a),patch.object(V,'load_targets',return_value=self.a),patch.object(V,'init_earth_engine'),patch.object(V,'setup_logging'),patch.object(V,'install_signal_handlers'):
            self.assertEqual(R.main(V),1)
            fake.run_phase.assert_called_once_with('day')
            fake.checkpoint.side_effect=RuntimeError('Drive quota')
            self.assertEqual(R.main(V,step='inventory'),1)

    def test_fatal_download_stop_is_a_failure_not_a_manual_stop(self):
        fake=Mock();fake.drive.stat.return_value=None;fake.root=self.drive.local
        fake.inventory.side_effect=V.StopRequested()
        with patch.object(R,'Engine',return_value=fake),patch.object(V,'build_admin_table',return_value=self.a),patch.object(V,'load_targets',return_value=self.a),patch.object(V,'setup_logging'),patch.object(V,'install_signal_handlers'),patch.object(V,'STOP_REASON',['fatal']):
            self.assertEqual(R.main(V),1)

    def test_atomic_upload_failure_preserves_outbox_snapshot(self):
        drive = R.Drive('gdrive:pilot',self.drive.local)
        path = self.drive.local/'snapshot.csv'
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text('old valid data')
        with patch.object(drive,'_put',side_effect=RuntimeError('quota')):
            with self.assertRaises(RuntimeError):
                drive.put(path,'CSV/day_indices.csv')
        temporary = path.with_suffix('.part');temporary.write_text('new data');temporary.replace(path)
        jobs = list((drive.local/'_control/outbox').glob('*/manifest.json'))
        self.assertEqual(len(jobs),1)
        self.assertEqual((jobs[0].parent/'payload').read_text(),'old valid data')
        with patch.object(drive,'_put') as upload:
            drive.flush_outbox()
            self.assertEqual(upload.call_args.args[1],'CSV/day_indices.csv')
            self.assertTrue(upload.call_args.args[2])
        self.assertFalse(list((drive.local/'_control/outbox').glob('*/manifest.json')))

    def test_repeated_replacements_keep_distinct_backups(self):
        drive = R.Drive('gdrive:pilot',self.drive.local)
        with patch.object(drive,'stat',return_value={'Size':1}),patch.object(drive,'command',return_value='') as command:
            drive._put('local.csv','CSV/day_indices.csv')
            drive._put('local.csv','CSV/day_indices.csv')
        targets = [call.args[0][2] for call in command.call_args_list if '/backups/' in call.args[0][2]]
        self.assertEqual(len(targets),2)
        self.assertNotEqual(targets[0],targets[1])

    def test_processing_interruption_resumes_without_recomputing_valid_metrics(self):
        self.engine.inventory()
        def stop_on_third(image,region,scale,path,label):
            if label.endswith('/3'):
                V.request_stop('deadline')
                raise V.StopRequested()
            self.fake_download(image,region,scale,path,label)
        with patch.object(V,'MAX_ATTEMPTS',3),patch.object(V,'MONTH_THREADS',1),patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'task1_all_months',return_value=records(self.a).to_dict('records')),patch.object(V,'download_tif',side_effect=stop_on_third):
            with self.assertRaises(V.StopRequested):
                self.engine.run_phase('day')
        self.engine.checkpoint(force=True)
        V.STOP_EVENT.clear();V.STOP_REASON[0]=None
        resumed = R.Engine(self.a,self.drive)
        resumed.inventory()
        with patch.object(V,'MAX_ATTEMPTS',3),patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'task1_all_months') as indices,patch.object(V,'download_tif',side_effect=self.fake_download) as download:
            self.assertTrue(resumed.run_phase('day'))
            indices.assert_not_called()
            self.assertEqual(download.call_count,10)

    def test_verifier_rejects_empty_run(self):
        with patch('sys.argv',['verify_pilot.py','--root',str(self.drive.root)]),patch('builtins.print'):
            with self.assertRaises(SystemExit) as result:
                Q.main()
        self.assertEqual(result.exception.code,1)


if __name__ == '__main__':
    unittest.main()
