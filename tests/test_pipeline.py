import json
from pathlib import Path
import tempfile
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch, Mock

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import Affine

import batch_config as B
import colab_export as C
import data_contract as D
import process_exports as P
import vngis_2024 as V
import verify_pilot as Q


def admin(gids=('VNM.1.2_1', 'VNM.1.10_1')):
    return pd.DataFrame([dict(GID_3=g, NAME_3='Xã ' + g, TYPE_3='Xa', GID_2='VNM.1_1',
                              NAME_2='Huyện', GID_1='VNM.1_1', NAME_1='Tỉnh') for g in gids])


def records(a, kind='day'):
    rows = []
    for gid in a['GID_3']:
        for m in range(1, 13):
            row = dict(GID_3=gid, YEAR=2024, MONTH=m)
            row.update({c: float(m) for c in D.DAY_METRICS if kind == 'day'})
            if kind == 'night':
                row.update({c: float(m) for c in D.NIGHT_METRICS})
                row['TIME'] = f'2024-{m:02d}'
                if m == 1:
                    row['TNL_MOM_GROWTH_PCT'] = np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def complete_progress(a):
    images = {(k, g, m): ('done', '') for k in ['day', 'night']
              for g in a['GID_3'] for m in range(1, 13)}
    return D.progress(a, {k: records(a, k) for k in ['day', 'night']}, images)


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.a = admin()

    def test_schema_legacy_migration_natural_order_and_names_by_gid(self):
        old = records(self.a).iloc[::-1].copy()
        old['NAME_3'] = 'wrong legacy name'
        frame = D.normalize(old, self.a, 'day')
        self.assertEqual(list(frame.columns), D.PREFIX + D.DAY_METRICS)
        self.assertEqual(frame['gid_3'].drop_duplicates().tolist(), self.a['GID_3'].tolist())
        self.assertEqual(frame.iloc[0]['name_3'], self.a.iloc[0]['NAME_3'])
        self.assertEqual(frame.groupby('gid_3')['month'].apply(list).iloc[0], list(range(1, 13)))
        self.assertTrue(frame['BLUE_mean'].eq(frame['month']).all())

    def test_missing_month_becomes_blank_and_blocks_night(self):
        old = records(self.a)
        old = old[old['MONTH'] != 5]
        frame = D.normalize(old, self.a, 'day')
        self.assertEqual(len(frame), 24)
        self.assertTrue(frame.loc[frame['month'].eq(5), D.DAY_METRICS].isna().all().all())
        table = complete_progress(self.a)
        table.loc[table['month'].eq(5), 'day_indices'] = 'pending'
        with self.assertRaisesRegex(RuntimeError, 'Chặn phần đêm'):
            D.require_day_complete(table)

    def test_no_source_requires_evidence_and_keeps_nan(self):
        blank = D.normalize(pd.DataFrame(), self.a, 'day').iloc[0]
        self.assertEqual(D.metric_state(blank, 'day')[0], 'pending')
        self.assertEqual(D.metric_state(blank, 'day', 0)[0], 'no_source')
        images = {(k, g, m): ('done', '') for k in ['day', 'night'] for g in self.a.GID_3 for m in D.MONTHS}
        sources = {(g, m, 'day_indices'): 0 for g in self.a.GID_3 for m in D.MONTHS}
        table = D.progress(self.a, {'day': pd.DataFrame(), 'night': records(self.a, 'night')}, images, sources)
        D.require_day_complete(table)
        self.assertTrue(table.day_indices.eq('no_source').all())

    def test_duplicates_wrong_year_and_unknown_gid(self):
        rows = records(self.a)
        self.assertEqual(len(D.normalize(pd.concat([rows, rows]), self.a, 'day')), 24)
        conflict = rows.iloc[[0]].copy()
        conflict['BLUE_mean'] = 100
        with self.assertRaisesRegex(ValueError, 'trùng khóa'):
            D.normalize(pd.concat([rows, conflict]), self.a, 'day')
        for c, v in [('YEAR', 2023), ('MONTH', 13), ('GID_3', 'unknown')]:
            wrong = rows.copy()
            wrong.loc[0, c] = v
            with self.assertRaises(ValueError):
                D.normalize(wrong, self.a, 'day')

    def test_preserve_valid_old_values_and_only_repair_missing(self):
        old = records(self.a).iloc[1:].copy()
        new = records(self.a)
        new[D.DAY_METRICS] *= 100
        result = D.merge_valid(old, new, self.a, 'day')
        self.assertEqual(result.iloc[0].BLUE_mean, 100)
        self.assertEqual(result.iloc[1].BLUE_mean, 2)

    def test_merge_night_into_initially_empty_table(self):
        frame = D.merge_valid(pd.DataFrame(), records(self.a, 'night'), self.a, 'night')
        self.assertEqual(frame.iloc[0].TIME, '2024-01')
        self.assertEqual(len(frame), 24)

    def test_boundary_count_and_duplicate_gid_fail_explicitly(self):
        with self.assertRaisesRegex(ValueError, '11,136'):
            D.administrative_table(self.a)
        with self.assertRaisesRegex(ValueError, 'mã trùng'):
            D.administrative_table(pd.concat([self.a, self.a]), expected=None)

    def test_night_columns_and_first_month_growth_nan_is_valid(self):
        frame = D.normalize(records(self.a, 'night'), self.a, 'night')
        self.assertEqual(list(frame), D.PREFIX + D.NIGHT_METRICS)
        self.assertEqual(D.metric_state(frame.iloc[0], 'night')[0], 'done')

    def test_gate_rejects_missing_month_duplicate_or_partial_state(self):
        table = complete_progress(self.a)
        D.require_day_complete(table)
        for wrong in [table.iloc[1:], pd.concat([table, table.iloc[[0]]])]:
            with self.assertRaises(RuntimeError):
                D.require_day_complete(wrong)
        for state in ['pending', 'running', 'failed']:
            wrong = table.copy()
            wrong.loc[0, 'day_image'] = state
            with self.assertRaises(RuntimeError):
                D.require_day_complete(wrong)

    def test_resume_interrupted_work_but_recheck_done(self):
        prev = complete_progress(self.a)
        prev.loc[0, 'day_image'] = 'running'
        frames = {k: records(self.a, k) for k in ['day', 'night']}
        resumed = D.progress(self.a, frames, {}, previous=prev)
        self.assertEqual(resumed.iloc[0].day_image, 'failed')
        self.assertEqual(resumed.iloc[1].day_image, 'pending')
        recovered = D.progress(self.a, frames, {('day', self.a.iloc[0].GID_3, 1): ('done', '')}, previous=prev)
        self.assertEqual(recovered.iloc[0].day_image, 'done')


class ImageAndRunnerTests(unittest.TestCase):
    def test_verifier_checks_real_sample_and_rejects_empty_run(self):
        a = admin(('VNM.1.2_1',))
        gid = a.iloc[0].GID_3
        ctx = V.build_ctx(a.iloc[0].to_dict())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'CSV').mkdir()
            for kind in ['day', 'night']:
                D.normalize(records(a, kind), a, kind).to_csv(root/'CSV'/f'{kind}_indices.csv', index=False)
                for m in D.MONTHS:
                    rel = ctx['rel_day_dir' if kind == 'day' else 'rel_night_dir']
                    name = V.day_name(ctx, m) if kind == 'day' else V.night_name(ctx, m)
                    path = root/rel/name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    bands, d = (10, B.D20) if kind == 'day' else (2, B.D500)
                    V.write_tif(str(path), np.ones((bands,4,4), dtype='float64'),
                                Affine(d,0,105,0,-d,21), 'EPSG:4326', -9999, [])
            command = [sys.executable, 'verify_pilot.py', '--root', tmp, '--gids', gid]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            (root/ctx['rel_day_dir']/V.day_name(ctx, 2)).write_bytes(b'corrupt')
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
        with tempfile.TemporaryDirectory() as tmp:
            self.assertNotEqual(subprocess.run([sys.executable,'verify_pilot.py','--root',tmp], capture_output=True).returncode, 0)

    def test_decode_bad_band_crs_resolution_corruption_and_night_dtype(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'day.tif'
            def write(count=10, scale=B.D20, crs='EPSG:4326', dtype='float64'):
                V.write_tif(str(path), np.ones((count, 4, 4), dtype=dtype),
                            Affine(scale, 0, 105, 0, -scale, 21), crs, -9999, [])
            write()
            self.assertEqual(D.validate_image(path, 'day')[0], 'done')
            for kw in [{'count': 6}, {'scale': B.D500}, {'crs': 'EPSG:3857'}]:
                write(**kw)
                self.assertEqual(D.validate_image(path, 'day')[0], 'failed')
            write(count=2, scale=B.D500, dtype='float32')
            self.assertEqual(D.validate_image(path, 'night')[0], 'failed')
            path.write_bytes(b'not a GeoTIFF')
            self.assertEqual(D.validate_image(path, 'day')[0], 'failed')

    def test_cut_worker_writes_correct_pixels(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, out = Path(tmp) / 'source.tif', Path(tmp) / 'out.tif'
            arr = np.arange(160, dtype='float64').reshape(10, 4, 4)
            V.write_tif(str(path), arr, Affine(B.D20, 0, 105, 0, -B.D20, 21), 'EPSG:4326', -9999, D.DAY_BANDS)
            from shapely.geometry import box, mapping
            result = P._cut_worker(dict(gid='test', win=(0, 4, 0, 4), X0=105, Y0=21, d=B.D20,
                tiles=[dict(path=str(path), ro=0, co=0)], polys=[mapping(box(105, 21-4*B.D20, 105+4*B.D20, 21))],
                bands=D.DAY_BANDS, out=str(out)))
            self.assertTrue(result[1], result)
            with rasterio.open(out) as src:
                np.testing.assert_array_equal(src.read(), arr)

    def test_group_files_ignores_unrelated_exports(self):
        listing = [dict(Path='day_img_202401_w0-0000000000-0000004096.tif'),
                   dict(Path='night_img_202401.tif'), dict(Path='plan_day.json')]
        result = P.group_files(listing, 'day_img_202401_w0')
        self.assertEqual(set(result), {(0, 4096)})

    def test_old_batch_done_cannot_skip_missing_output(self):
        P.SOURCES = {}
        with patch.object(P, 'group_files', return_value={}), patch.object(P, 'checkpoint'):
            result = P.process_group('day_img_202401_w0', 'day', 1, ['gid'], [], {'day': {}, 'night': {}},
                                     {'day_img_202401_w0'})
        self.assertEqual(result, 'waiting')

    def test_verified_image_can_skip_group_without_export_or_geometry(self):
        with patch.object(P, 'load_geoms') as geoms:
            result = P.process_group('day_img_202401_w0', 'day', 1, ['gid'], [],
                                     {'day': {'gid': {1}}, 'night': {}}, set())
        self.assertEqual(result, 'done')
        geoms.assert_not_called()

    def test_optional_fetch_does_not_hide_permission_failure(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(P.subprocess, 'run') as run:
            run.return_value = Mock(returncode=1, stderr='403 Forbidden')
            with self.assertRaisesRegex(RuntimeError, '403'):
                P.fetch_optional('gdrive:file', str(Path(tmp) / 'file'))
            run.return_value = Mock(returncode=1, stderr='object not found')
            self.assertIsNone(P.fetch_optional('gdrive:file', str(Path(tmp) / 'file')))

    def test_atomic_upload_backups_old_csv_and_stops_on_backup_failure(self):
        with patch.object(P, 'fetch_optional', return_value='/tmp/old.csv'), patch.object(V, '_rclone', return_value=True) as call:
            P.upload_atomic('/tmp/new.csv', 'CSV/day_indices.csv')
            commands = [args[0][0] for args, _ in call.call_args_list]
            self.assertEqual(commands, ['copyto', 'copyto', 'moveto'])
            self.assertIn('/_control/backups/', call.call_args_list[0].args[0][2])
        with patch.object(P, 'fetch_optional', return_value='/tmp/old.csv'), patch.object(V, '_rclone', return_value=False) as call:
            with self.assertRaisesRegex(RuntimeError, 'sao lưu'):
                P.upload_atomic('/tmp/new.csv', 'CSV/day_indices.csv')
            self.assertEqual(call.call_count, 1)

    def test_night_submission_blocked_before_any_task_or_image_creation(self):
        with patch.object(C, 'inventory', return_value=complete_progress(admin()).iloc[1:]), patch.object(C.ee.batch.Export.table, 'toDrive') as table, patch.object(C.ee.batch.Export.image, 'toDrive') as image:
            for submit in [C.submit_night_csv, C.submit_night_images]:
                with self.assertRaises(RuntimeError):
                    submit()
            table.assert_not_called()
            image.assert_not_called()

    def test_night_image_with_no_source_is_not_exported(self):
        table = complete_progress(admin())
        table['night_image'] = 'pending'
        collection = Mock()
        collection.size.return_value.getInfo.return_value = 0
        with patch.object(C, 'require_day_complete', return_value=table), patch.object(C, '_viirs_col', return_value=collection), patch.object(C.ee.Geometry, 'Rectangle'), patch.object(C.ee.batch.Export.image, 'toDrive') as export:
            C.submit_night_images(months=[1])
        export.assert_not_called()

    def test_active_export_not_duplicated_even_with_force(self):
        task = Mock()
        with patch.object(C, '_existing_tasks', return_value={'x': 'RUNNING'}):
            C._start(task, 'x', force=True)
        task.start.assert_not_called()

    def test_completed_export_reused_and_absent_export_resubmitted(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(C, '_out_dir', return_value=tmp), patch.object(C, '_existing_tasks', return_value={'x': 'COMPLETED'}):
            Path(tmp, 'x.csv').write_text('GID_3\ntest\n')
            task = Mock()
            C._start(task, 'x')
            task.start.assert_not_called()
            Path(tmp, 'x.csv').unlink()
            C._start(task, 'x')
            task.start.assert_called_once()

    def test_completed_corrupt_export_resubmitted(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(C, '_out_dir', return_value=tmp), patch.object(C, '_existing_tasks', return_value={'day_img_202401_w0': 'COMPLETED'}):
            Path(tmp, 'day_img_202401_w0.tif').write_bytes(b'broken')
            task = Mock()
            C._start(task, 'day_img_202401_w0')
            task.start.assert_called_once()

    def test_processing_night_gate_runs_before_night_worker(self):
        table = complete_progress(admin())
        table.loc[0, 'day_indices'] = 'failed'
        with patch('sys.argv', ['process_exports.py', 'night']), patch.object(P, 'setup'), patch.object(P, 'inventory', side_effect=lambda: setattr(P, 'PROGRESS', table) or table), patch.object(P, 'night_csv') as csv, patch.object(P, 'run_images') as images, patch.object(V, 'MAX_RUNTIME_SEC', 0):
            self.assertEqual(P.main(), 1)
            csv.assert_not_called()
            images.assert_not_called()

    def test_failed_image_step_preserves_failure_exit_status(self):
        table = complete_progress(admin())
        table.loc[0, 'day_image'] = 'pending'
        with patch('sys.argv', ['process_exports.py', 'day']), patch.object(P, 'setup'), patch.object(P, 'inventory', side_effect=lambda: setattr(P, 'PROGRESS', table) or table), patch.object(P, 'day_csv', return_value=False), patch.object(P, 'run_images', return_value=1), patch.object(V, 'MAX_RUNTIME_SEC', 0):
            self.assertEqual(P.main(), 1)


class LocalDriveIntegrationTests(unittest.TestCase):
    """Run the real inventory/migration/gates with filesystem-backed fake transport."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.drive = self.root / 'drive'
        self.target = self.drive / 'VNGISDash_2024'
        self.export = self.drive / 'VNGIS_EXPORT_2024'
        self.a = admin(('VNM.1.2_1',))
        self.target.mkdir(parents=True)
        self.export.mkdir()
        self.patches = []
        for obj, name, value in [(V, 'LOCAL_ROOT', str(self.root/'outputs')),
                                 (V, 'REMOTE_BASE', 'gdrive:VNGISDash_2024'),
                                 (P, 'WORK', str(self.root/'work')), (P, 'ADMIN', self.a),
                                 (P, 'EXP', 'gdrive:VNGIS_EXPORT_2024'), (P, 'PROGRESS', None),
                                 (P, 'GEOMS', {}),
                                 (P, 'SOURCES', {}), (P, 'IMAGE_CACHE', {}),
                                 (P, 'TASK_STATES', {}), (V, 'MIN_FREE_GB', 0)]:
            self.patches.append(patch.object(obj, name, value))
        self.patches.append(patch.object(V, 'ADMIN_BY_GID', {r['GID_3']: r for r in self.a.to_dict('records')}))
        self.patches.extend([patch.object(P, 'fetch', side_effect=self.fetch),
                             patch.object(P, 'fetch_optional', side_effect=self.optional),
                             patch.object(P, 'rclone_json', side_effect=self.listing),
                             patch.object(V, '_rclone', side_effect=self.rclone)])
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)
        P.CTX.clear()
        P.SAFE2GID.clear()
        for row in self.a.to_dict('records'):
            ctx = V.build_ctx(row)
            P.CTX[row['GID_3']] = ctx
            P.SAFE2GID[ctx['safe_gid3']] = row['GID_3']
        self.gid = self.a.iloc[0].GID_3
        self.ctx = P.CTX[self.gid]
        (self.target/'CSV').mkdir()
        records(self.a).to_csv(self.target/'CSV/day_indices.csv', index=False)
        plan = {self.gid: [[1, 1, 1] for _ in D.MONTHS]}
        (self.export/'plan_day.json').write_text(json.dumps(plan))
        for month in D.MONTHS:
            self.write_image(month)

    def remote(self, name):
        return self.drive / name.split(':', 1)[1]

    def fetch(self, name, dest):
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.remote(name), dest)
        return dest

    def optional(self, name, dest):
        return self.fetch(name, dest) if self.remote(name).is_file() else None

    def listing(self, name, *extra):
        import fnmatch
        path = self.remote(name)
        pattern = extra[extra.index('--include')+1] if '--include' in extra else '*'
        if not path.is_dir():
            return []
        files = path.rglob('*') if '-R' in extra else path.glob('*')
        return [dict(Path=str(p.relative_to(path)), Size=p.stat().st_size,
                     ModTime=str(p.stat().st_mtime_ns), Hashes={})
                for p in files if p.is_file() and fnmatch.fnmatch(p.name, pattern)]

    def rclone(self, args, **kwargs):
        action, src, dst = args[:3]
        if action not in ('copyto', 'moveto'):
            raise AssertionError(f'Unexpected/destructive remote operation: {args}')
        a = self.remote(src) if src.startswith('gdrive:') else Path(src)
        b = self.remote(dst)
        b.parent.mkdir(parents=True, exist_ok=True)
        if action == 'copyto':
            shutil.copyfile(a, b)
        else:
            a.replace(b)
        return True

    def write_image(self, month):
        path = self.target/self.ctx['rel_day_dir']/V.day_name(self.ctx, month)
        path.parent.mkdir(parents=True, exist_ok=True)
        V.write_tif(str(path), np.ones((10,4,4), dtype='float64'),
                    Affine(B.D20, 0, 105, 0, -B.D20, 21), 'EPSG:4326', -9999, D.DAY_BANDS)
        return path

    def test_migration_backup_validation_cache_and_repair_after_interruption(self):
        original = (self.target/'CSV/day_indices.csv').read_bytes()
        table = P.inventory()
        D.require_day_complete(table)
        frame = pd.read_csv(self.target/'CSV/day_indices.csv')
        self.assertEqual(list(frame), D.COLUMNS['day'])
        backups = list((self.target/'_control/backups').rglob('day_indices.csv'))
        self.assertEqual(backups[0].read_bytes(), original)
        with patch.object(D, 'validate_image', wraps=D.validate_image) as check:
            D.require_day_complete(P.inventory())
            check.assert_not_called()  # same size/mtime/hash, already fully decoded
        self.assertEqual(len(list((self.target/'_control/backups').rglob('day_indices.csv'))), 1)
        broken = self.write_image(2)
        broken.write_bytes(b'corrupt')
        table = P.inventory()
        self.assertEqual(table.loc[table.month.eq(2), 'day_image'].iloc[0], 'failed')
        with self.assertRaises(RuntimeError):
            D.require_day_complete(table)
        self.write_image(2)
        D.require_day_complete(P.inventory())

    def test_csv_missing_month_only_repaired_without_redownloading_valid_images(self):
        rows = records(self.a)
        rows = rows[rows.MONTH != 3]
        rows.to_csv(self.target/'CSV/day_indices.csv', index=False)
        self.assertEqual(P.inventory().loc[lambda x: x.month.eq(3), 'day_indices'].iloc[0], 'pending')
        incoming = records(self.a)
        incoming[D.DAY_METRICS] *= 100
        with patch.object(P, '_download_csvs', return_value=[str(self.export/'day.csv')]):
            incoming.to_csv(self.export/'day.csv', index=False)
            P.day_csv()
        with patch.object(D, 'validate_image', wraps=D.validate_image) as check:
            D.require_day_complete(P.inventory())
            check.assert_not_called()
        frame = pd.read_csv(self.target/'CSV/day_indices.csv')
        self.assertEqual(frame.loc[frame.month.eq(1), 'BLUE_mean'].iloc[0], 1)
        self.assertEqual(frame.loc[frame.month.eq(3), 'BLUE_mean'].iloc[0], 300)

    def test_false_done_and_missing_file_are_not_trusted(self):
        self.write_image(7).unlink()
        progress_dir = self.target/'_control'
        progress_dir.mkdir()
        complete_progress(self.a).to_csv(progress_dir/'progress.csv', index=False)
        (progress_dir/'batch_done.txt').write_text('day_img_202407_w0\n')
        table = P.inventory()
        self.assertEqual(table.loc[table.month.eq(7), 'day_image'].iloc[0], 'pending')
        with self.assertRaises(RuntimeError):
            D.require_day_complete(table)

    def test_real_image_runner_repairs_one_missing_month_and_retains_export(self):
        from shapely.geometry import mapping, box
        self.write_image(7).unlink()
        P.inventory()
        source = self.export/'day_img_202407_w0.tif'
        V.write_tif(str(source), np.full((10,4,4), 7, dtype='float64'),
                    Affine(B.D20,0,105,0,-B.D20,21), 'EPSG:4326', -9999, D.DAY_BANDS)
        geom = mapping(box(105,21-4*B.D20,105+4*B.D20,21))
        (self.export/'communes_l3.geojson').write_text(json.dumps(dict(type='FeatureCollection',
            features=[dict(type='Feature',properties={'GID_3': self.gid}, geometry=geom)])))
        P.TASK_STATES['day_img_202407_w0'] = 'COMPLETED'
        first_before = (self.target/self.ctx['rel_day_dir']/V.day_name(self.ctx,1)).read_bytes()
        with patch.object(P, 'refresh_task_states'):
            self.assertEqual(P.run_images('day'), 0)
        D.require_day_complete(P.inventory())
        with rasterio.open(self.target/self.ctx['rel_day_dir']/V.day_name(self.ctx,7)) as src:
            np.testing.assert_array_equal(src.read(), np.full((10,4,4),7,dtype='float64'))
        self.assertEqual((self.target/self.ctx['rel_day_dir']/V.day_name(self.ctx,1)).read_bytes(), first_before)
        self.assertTrue(source.exists())

    def test_confirmed_no_source_has_blank_csv_and_does_not_block_next_phase(self):
        plan = {self.gid: [[1,1,1] for _ in D.MONTHS]}
        plan[self.gid][11] = [0,0,0]
        (self.export/'plan_day.json').write_text(json.dumps(plan))
        self.write_image(12).unlink()
        rows = records(self.a)
        rows.loc[rows.MONTH.eq(12), D.DAY_METRICS] = np.nan
        rows.to_csv(self.target/'CSV/day_indices.csv', index=False)
        rows['SOURCE_COUNT'] = [1]*11 + [0]
        rows.to_csv(self.export/'day.csv', index=False)
        table = P.inventory()
        self.assertEqual(table.iloc[11].day_image, 'no_source')
        self.assertEqual(table.iloc[11].day_indices, 'pending')
        with patch.object(P, '_download_csvs', return_value=[str(self.export/'day.csv')]):
            P.day_csv()
        table = P.inventory()
        D.require_day_complete(table)
        self.assertEqual(table.iloc[11].day_indices, 'no_source')
        frame = pd.read_csv(self.target/'CSV/day_indices.csv')
        self.assertEqual(len(frame), 12)
        self.assertTrue(frame.iloc[11][D.DAY_METRICS].isna().all())


if __name__ == '__main__':
    unittest.main()
