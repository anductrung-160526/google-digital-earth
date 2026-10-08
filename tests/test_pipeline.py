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

import data_contract as D
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
        with self.assertRaisesRegex(ValueError, '11,163'):
            D.administrative_table(self.a)
        with self.assertRaisesRegex(ValueError, 'mã trùng'):
            D.administrative_table(pd.concat([self.a, self.a]), expected=None)

    def test_full_gadm_scope_is_accepted_and_old_count_is_rejected(self):
        self.assertEqual(D.EXPECTED_COMMUNES, 11163)
        full = admin(tuple(f'VNM.1.{i}_1' for i in range(1, 11164)))
        self.assertEqual(len(D.administrative_table(full)), 11163)
        with self.assertRaisesRegex(ValueError, '11,136.*11,163'):
            D.administrative_table(full.iloc[:11136])

    def test_full_gadm_scope_still_rejects_duplicates_and_missing_names(self):
        full = admin(tuple(f'VNM.1.{i}_1' for i in range(1, 11164)))
        duplicate = full.copy()
        duplicate.loc[1, 'GID_3'] = duplicate.loc[0, 'GID_3']
        with self.assertRaisesRegex(ValueError, 'mã trùng'):
            D.administrative_table(duplicate)
        blank = full.copy()
        blank.loc[1, 'NAME_3'] = ''
        with self.assertRaisesRegex(ValueError, 'thông tin hành chính trống'):
            D.administrative_table(blank)

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
