"""Inventory and scheduling around the v6 scientific routines.

Only successfully verified/uploaded artifacts become done. JSONL is a checkpoint,
not proof of an artifact's existence. No national run executes on import.
"""
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import data_contract as D
import vngis_2024 as V
from request_control import retry_delay, status_code

log = logging.getLogger('vngis')
_drive_lock = threading.Lock()


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def safe_error(exc):
    # HTTP libraries can include a signed download URL in exception messages.
    text = re.sub(r'https?://\S+', '[URL omitted]', str(exc))
    text = re.sub(r'(?i)(token|signature|key|authorization)\s*[=:]\s*\S+', r'\1=[omitted]', text)
    return f'{type(exc).__name__}: {text[:300]}'


def rclone_run(args, timeout=1200, allow_stopped=False):
    """All Drive commands share pacing/serialization and bounded application retries."""
    common = V.RCLONE_COMMON
    for attempt in range(V.REQUEST_ATTEMPTS):
        with V.REQUEST_GATE.slot('Google Drive', allow_stopped), _drive_lock:
            result = subprocess.run(['rclone', *args, *common], capture_output=True,
                                    text=True, timeout=timeout)
        throttled = bool(re.search(r'\b429\b|rateLimitExceeded|userRateLimitExceeded', result.stderr, re.I))
        if result.returncode == 0:
            if throttled:
                V.REQUEST_GATE.defer('Google Drive', 0, True)
            return result
        transient = throttled or bool(re.search(r'\b(500|502|503|504)\b', result.stderr))
        if not transient or attempt == V.REQUEST_ATTEMPTS - 1:
            if throttled:
                V.REQUEST_GATE.defer('Google Drive', 0, True)
                log.error('Google Drive: hết %s lần thử vì giới hạn request', V.REQUEST_ATTEMPTS)
            return result
        header = re.search(r'(?im)^\s*Retry-After:\s*([^\r\n]+)', result.stderr)
        V.REQUEST_GATE.defer('Google Drive', retry_delay(attempt, header[1] if header else None), throttled)
    raise RuntimeError('Không có lượt rclone')


class Drive:
    def __init__(self, base=None, local=None):
        self.base = base or V.REMOTE_BASE
        self.local = Path(local or V.LOCAL_ROOT)
        self.backup_stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' + uuid.uuid4().hex[:8]

    def command(self, args, optional=False, allow_stopped=False):
        result = rclone_run(args, allow_stopped=allow_stopped)
        if result.returncode:
            if optional and re.search(r'directory not found|object not found|file not found', result.stderr, re.I):
                return None
            raise RuntimeError(f'Google Drive: {args[0]} thất bại (mã {result.returncode}); '
                               'kiểm tra quyền, kết nối hoặc quota')
        return result.stdout

    def fetch(self, rel, dest, optional=False):
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        text = self.command(['copyto', f'{self.base}/{rel}', str(dest)], optional)
        return dest if text is not None else None

    def listing(self, rel):
        raw = self.command(['lsjson', f'{self.base}/{rel}', '-R', '--files-only', '--hash'], optional=True)
        return json.loads(raw) if raw is not None else []

    def stat(self, rel, allow_stopped=False):
        raw = self.command(['lsjson', f'{self.base}/{rel}', '--stat', '--hash'], optional=True,
                           allow_stopped=allow_stopped)
        return json.loads(raw) if raw is not None else None

    def _put(self, path, rel, backup=True):
        # This is also the final flush after STOP; new scientific work stays stopped.
        if backup and self.stat(rel, allow_stopped=True) is not None:
            backup_id = self.backup_stamp + '_' + uuid.uuid4().hex[:8]
            self.command(['copyto', f'{self.base}/{rel}',
                          f'{self.base}/_control/backups/{backup_id}/{rel}'], allow_stopped=True)
        temporary = f'{self.base}/_control/staging/{uuid.uuid4().hex}.part'
        self.command(['copyto', str(path), temporary], allow_stopped=True)
        self.command(['moveto', temporary, f'{self.base}/{rel}'], allow_stopped=True)

    def put(self, path, rel, backup=True):
        # Keep an immutable local outbox so --sync-only can retry a failed final upload.
        outbox = self.local/'_control/outbox'/uuid.uuid4().hex
        outbox.mkdir(parents=True, exist_ok=True)
        payload = outbox/'payload'
        path = Path(path)
        if path.suffix in {'.csv', '.tif', '.json'}:
            # These writers replace files atomically; a hard link preserves the old inode.
            try:
                os.link(path, payload)
            except OSError:
                shutil.copyfile(path, payload)
        else:
            shutil.copyfile(path, payload)
        (outbox/'manifest.json').write_text(json.dumps(dict(base=self.base, rel=rel, backup=backup)))
        self._put(payload, rel, backup)
        shutil.rmtree(outbox)

    def flush_outbox(self):
        for path in sorted((self.local/'_control/outbox').glob('*/manifest.json'), key=lambda p: p.stat().st_mtime_ns):
            job = json.loads(path.read_text())
            if job['base'] != self.base:
                raise ValueError('Outbox thuộc thư mục Drive khác; không tự đổi đích upload')
            self._put(path.parent/'payload', job['rel'], job['backup'])
            shutil.rmtree(path.parent)

    def pull_history(self):
        for sub in ['_control/status', '_control/parts']:
            log.info('KIỂM KÊ: đọc lịch sử %s', sub)
            for entry in self.listing(sub):
                if entry['Path'].endswith('.jsonl'):
                    self.fetch(f"{sub}/{entry['Path']}", self.local/sub/entry['Path'])


def fingerprint(entry):
    return dict(size=entry.get('Size'), mtime=entry.get('ModTime'),
                hashes=entry.get('Hashes', {}), version=D.VALIDATION_VERSION)


def read_jsonl(path):
    lines = Path(path).read_text(encoding='utf-8').splitlines(keepends=True)
    for index, line in enumerate(lines):
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            if index == len(lines) - 1 and not line.endswith('\n'):
                log.warning('Bỏ dòng cuối JSONL chưa ghi xong: %s', Path(path).name)
            else:
                raise ValueError(f'JSONL hỏng: {Path(path).name}, dòng {index + 1}') from None


class Engine:
    def __init__(self, admin, drive=None):
        self.admin = D.administrative_table(admin, expected=None)
        self.drive = drive or Drive()
        self.root = self.drive.local
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.sync_lock = threading.Lock()
        self.contexts = {r['gid_3']: V.build_ctx({k.upper(): v for k, v in r.items()})
                         for r in self.admin.to_dict('records')}
        self.mapping = {c['safe_gid3']: g for g, c in self.contexts.items()}
        self.sources = {}
        self.cache = {}
        self.images = {}
        self.frames = {}
        self.table = None
        self.positions = {}
        self.history = {}
        self.dirty = set()
        self.last_sync = time.monotonic()
        self.run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' + V.RUN_ID + '_' + uuid.uuid4().hex[:8]

    def save_cache(self, cache):
        path = self.root/'_control/validated_images.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix('.json.part')
        temporary.write_text(json.dumps(cache), encoding='utf-8')
        temporary.replace(path)
        self.drive.put(path, '_control/validated_images.json', backup=False)

    def scan_images(self):
        old = self.cache.copy()
        current = {}
        self.images = {}
        new_checks, reused, dirty = 0, 0, False
        last_save = time.monotonic()
        every = max(1, int(os.getenv('VNGIS_INVENTORY_CHECKPOINT_EVERY', '250')))
        seen = set()
        try:
            for kind in ['day', 'night']:
                log.info('KIỂM KÊ %s: đang liệt kê Drive', kind)
                listing = self.drive.listing(kind.title())
                log.info('KIỂM KÊ %s: %s file, bắt đầu đối chiếu', kind, len(listing))
                for i, entry in enumerate(listing, 1):
                    match = D.IMAGE_RE.match(Path(entry['Path']).name)
                    if not match or match[2] != kind or match[1] not in self.mapping:
                        continue
                    V.check_stop()
                    key = (kind, self.mapping[match[1]], int(match[3]))
                    rel = f"{kind.title()}/{entry['Path']}"
                    if key in seen:
                        self.images[key] = 'failed', 'Có nhiều file cùng xã/tháng'
                        continue
                    seen.add(key)
                    fp = fingerprint(entry)
                    previous = old.get(rel, {})
                    if previous.get('fingerprint') == fp and previous.get('state') in {'done', 'failed'}:
                        state, error = previous['state'], previous.get('error', '')
                        reused += 1
                    else:
                        log.info('KIỂM KÊ %s %s/%s: tải %s (%.1f MB)', kind, i, len(listing), rel,
                                 entry.get('Size', 0)/1e6)
                        size = entry.get('Size', 0)
                        if shutil.disk_usage(self.root).free < size + 512 * 1024 ** 2:
                            raise RuntimeError('Không đủ dung lượng kiểm tra ảnh; cache đã hoàn tất sẽ được giữ')
                        start = time.monotonic()
                        path = self.root/'_control/validation'/Path(rel).name
                        self.drive.fetch(rel, path)
                        try:
                            log.info('KIỂM KÊ: đã tải, đang đọc mọi block GeoTIFF')
                            state, error = D.validate_image(path, kind)
                        finally:
                            path.unlink(missing_ok=True)
                        new_checks += 1
                        dirty = True
                        log.info('KIỂM KÊ: %s trong %.1fs; mới %s, dùng cache %s',
                                 state, time.monotonic()-start, new_checks, reused)
                    current[rel] = dict(fingerprint=fp, state=state, error=error)
                    self.images[key] = state, error
                    if dirty and (new_checks % every == 0 or time.monotonic()-last_save >= 120):
                        self.save_cache({**old, **current})
                        dirty, last_save = False, time.monotonic()
                    elif reused and reused % every == 0:
                        log.info('KIỂM KÊ: đã tái sử dụng %s kết quả kiểm tra', reused)
            self.save_cache(current)
            self.cache = current
            dirty = False
        finally:
            if dirty:
                self.cache = {**old, **current}
                try:
                    self.save_cache(self.cache)
                except Exception as exc:
                    log.error('Không lưu được cache giữa chừng: %s', safe_error(exc))

    def inventory(self):
        log.info('KIỂM KÊ: đọc tiến độ, nguồn và cache cũ')
        previous = self.drive.fetch('_control/progress.csv', self.root/'_control/progress.csv', True)
        prior = pd.read_csv(previous) if previous else None
        source = self.drive.fetch('_control/source_counts.csv', self.root/'_control/source_counts.csv', True)
        self.sources = D.read_sources(source) if source else {}
        if set(g for g, _, _ in self.sources) - set(self.contexts):
            raise ValueError('Bằng chứng nguồn chứa GID ngoài phạm vi thư mục')
        cache = self.drive.fetch('_control/validated_images.json', self.root/'_control/validated_images.json', True)
        self.cache = json.loads(cache.read_text()) if cache else {}
        self.drive.pull_history()
        for path in sorted((self.root/'_control/status').glob('status_*.jsonl')):
            for record in read_jsonl(path):
                if 'gid_3' in record:
                    self.history[record['gid_3']] = record
                    for item in record.get('source_counts', []):
                        self.sources[record['gid_3'], int(item['month']), item['field']] = item['count']
        self.scan_images()
        for kind in ['day', 'night']:
            log.info('KIỂM KÊ: đọc CSV %s và parts v6', kind)
            rel = f'CSV/{kind}_indices.csv'
            path = self.drive.fetch(rel, self.root/rel, True)
            original = pd.read_csv(path) if path else pd.DataFrame()
            frame = D.normalize(original, self.admin, kind)
            parts = [r for p in sorted((self.root/'_control/parts').glob(f'{kind}_*.jsonl')) for r in read_jsonl(p)]
            if parts:
                # Ignore measurements already backed by a valid CSV, preserving them exactly.
                states = D.metric_states(frame, kind, self.sources)
                rows = D.aliases(pd.DataFrame(parts))
                selected = [states.get((str(r.gid_3), int(r.month)), ('pending', ''))[0] not in D.TERMINAL
                            for r in rows.itertuples()]
                frame = D.merge_valid(frame, rows.loc[selected], self.admin, kind, self.sources)
            self.frames[kind] = frame
            if path is None or not D.same_table(frame, original):
                self.dirty.add(kind)
        self.table = D.progress(self.admin, self.frames, self.images, self.sources, prior)
        self.table['updated_at'] = stamp()
        self.positions = {(r.gid_3, int(r.month)): i for i, r in enumerate(self.table.itertuples())}
        self.checkpoint(force=True)
        for field in D.FIELDS:
            log.info('KIỂM KÊ %s: %s', field, self.table[field].value_counts().to_dict())
        return self.table

    def checkpoint(self, force=False):
        if not force and time.monotonic()-self.last_sync < V.UPLOAD_EVERY_SEC:
            return
        with self.sync_lock, self.lock:
            if not force and time.monotonic()-self.last_sync < V.UPLOAD_EVERY_SEC:
                return
            for kind in sorted(self.dirty):
                rel = f'CSV/{kind}_indices.csv'
                path = self.root/rel
                V._write_csv(self.frames[kind], str(path))
                self.drive.put(path, rel, backup=True)
            self.dirty.clear()
            sources = pd.DataFrame([dict(gid_3=g, year=D.YEAR, month=m, field=f, count=n)
                                    for (g, m, f), n in sorted(self.sources.items())],
                                   columns=D.KEY+['field', 'count'])
            for rel, frame in [('_control/source_counts.csv', sources), ('_control/progress.csv', self.table)]:
                V._write_csv(frame, str(self.root/rel))
                self.drive.put(self.root/rel, rel, backup=False)
            self.save_cache(self.cache)
            for sub in ['status', 'parts']:
                for path in (self.root/'_control'/sub).glob(f'*_{self.run}.jsonl'):
                    self.drive.put(path, f'_control/{sub}/{path.name}', backup=False)
            self.last_sync = time.monotonic()
            log.info('CHECKPOINT: đã lưu CSV, nguồn, cache và tiến độ xã–tháng lên Drive')

    def update(self, gid, month, field, state, error=''):
        with self.lock:
            i = self.positions[gid, month]
            self.table.loc[i, [field, field+'_error', 'updated_at']] = [state, error, stamp()]
            self.write_status(gid, month)

    def write_status(self, gid, month=None, include_sources=False):
        rows = self.table.iloc[[self.positions[gid, m] for m in D.MONTHS]]
        info = dict(gid_3=gid, run_id=V.RUN_ID, version='v6-resume',
                    phase_attempts=self.history.get(gid, {}).get('phase_attempts', {}),
                    status='done' if rows[D.FIELDS].isin(D.TERMINAL).all().all() else 'partial',
                    finished_at=stamp())
        if month is not None:
            info['month_status'] = self.table.iloc[self.positions[gid, month]].to_dict()
        if include_sources:
            info['source_counts'] = [dict(month=m, field=f, count=self.sources[gid,m,f])
                                    for m in D.MONTHS for f in D.FIELDS if (gid,m,f) in self.sources]
        self.history[gid] = {**self.history.get(gid, {}), **info}
        path = self.root/f'_control/status/status_{self.run}.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(info, ensure_ascii=False)+'\n')

    def needed(self, gid, field):
        with self.lock:
            rows = self.table.iloc[[self.positions[gid, m] for m in D.MONTHS]]
            return list(rows.loc[~rows[field].isin(D.TERMINAL), 'month'].astype(int))

    def add_metrics(self, gid, kind, incoming, months):
        normalized = D.normalize(incoming, self.admin.loc[self.admin.gid_3.eq(gid)], kind)
        with self.lock:
            for month in months:
                row = normalized.loc[normalized.month.eq(month)].iloc[0]
                field = kind+'_indices'
                count = self.sources.get((gid, month, field))
                state, error = D.metric_state(row, kind, count)
                if state == 'pending' and count and count > 0:
                    state, error = 'failed', 'Có nguồn nhưng phép tính không trả chỉ số tháng này'
                i = self.positions[gid, month]
                if state in D.TERMINAL and self.table.loc[i, field] not in D.TERMINAL:
                    metrics = D.DAY_METRICS if kind == 'day' else D.NIGHT_METRICS
                    self.frames[kind].loc[i, metrics] = row[metrics].to_numpy()
                    self.dirty.add(kind)
                    path = self.root/f'_control/parts/{kind}_{self.run}.jsonl'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with path.open('a', encoding='utf-8') as fh:
                        fh.write(row.to_json(force_ascii=False)+'\n')
                self.update(gid, month, field, state, error)

    def check_asset(self, full_admin):
        gids = V.ee_getinfo(V.communes_fc.aggregate_array('GID_3'))
        expected = set(D.administrative_table(full_admin).gid_3)
        actual = set(gids)
        missing, extra = expected-actual, actual-expected
        if missing or extra or len(gids) != len(actual):
            message = f'Asset không khớp GADM: thiếu {len(missing)}, ngoài phạm vi {len(extra)}, trùng {len(gids)-len(actual)}; thiếu mẫu {sorted(missing)[:10]}'
            for gid in missing & set(self.contexts):
                for month in D.MONTHS:
                    self.update(gid, month, 'day_image', 'failed', message)
            self.checkpoint(force=True)
            raise ValueError(message)

    def process(self, gid, kind):
        V.check_stop()
        if kind == 'night':
            D.require_day_complete(self.table)
        ctx = self.contexts[gid].copy()
        fc = V.communes_fc.filter(V.ee.Filter.eq('GID_3', gid))
        geom = fc.geometry()
        ctx['geom'] = geom
        missing = {field: self.needed(gid, kind+'_'+field) for field in ['image', 'indices']}
        if not any(missing.values()):
            return
        for suffix, months in missing.items():
            for month in months:
                self.update(gid, month, kind+'_'+suffix, 'running')
        try:
            n, plan = V.fetch_plan(fc, geom, kind)
            if n != 1:
                raise ValueError(f'Asset phải có đúng một feature cho GID {gid}; có {n}')
            with self.lock:
                for month, entry in plan.items():
                    for suffix in ['image', 'indices']:
                        field = kind+'_'+suffix
                        count = entry[suffix+'_count']
                        self.sources[gid, month, field] = count
                        if month in missing[suffix] and count == 0:
                            if suffix == 'indices':
                                i = self.positions[gid, month]
                                state, error = D.metric_state(self.frames[kind].iloc[i], kind, 0)
                                self.update(gid, month, field, state, error)
                            else:
                                image_state = self.images.get((kind, gid, month))
                                if image_state and image_state[0] == 'failed':
                                    self.update(gid, month, field, 'failed', 'Nguồn rỗng nhưng file ảnh hiện có hỏng/trùng')
                                else:
                                    self.update(gid, month, field, 'no_source')
                self.write_status(gid, include_sources=True)
            months = self.needed(gid, kind+'_indices')
            sourced = [m for m in months if plan[m]['indices_count'] > 0]
            if sourced:
                try:
                    if kind == 'day':
                        props = V.task1_all_months(fc, months=sourced)
                        incoming = pd.DataFrame([{**r, 'YEAR': D.YEAR} for r in props])
                    else:
                        # All sourced months are needed to preserve v6 rolling/pct_change semantics.
                        incoming = V.task3_all_months(geom, ctx['gid1'], ctx['name1'], gid, ctx['cname_full'])
                    self.add_metrics(gid, kind, incoming, sourced)
                    self.checkpoint()
                except Exception as exc:
                    for month in sourced:
                        self.update(gid, month, kind+'_indices', 'failed', safe_error(exc))
            jobs = self.needed(gid, kind+'_image')
            with ThreadPoolExecutor(max_workers=V.MONTH_THREADS) as pool:
                futures = {pool.submit(self.process_image, gid, kind, m, ctx, plan[m]): m for m in jobs}
                for future in as_completed(futures):
                    month = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        self.update(gid, month, kind+'_image', 'failed', safe_error(exc))
        except Exception as exc:
            for suffix in ['image', 'indices']:
                for month in self.needed(gid, kind+'_'+suffix):
                    self.update(gid, month, kind+'_'+suffix, 'failed', safe_error(exc))
        finally:
            self.checkpoint()

    def process_image(self, gid, kind, month, ctx, entry):
        V.check_stop()
        if entry['image_count'] == 0:
            return
        rel = str(Path(ctx['rel_'+kind+'_dir'])/(V.day_name(ctx, month) if kind == 'day' else V.night_name(ctx, month)))
        image = (V.day_image(month, entry['s2_window'], ctx['geom']).select(V.DAY_BANDS_ALL)
                 if kind == 'day' else V.night_image(month, entry['viirs'], ctx['geom']))
        path = self.root/'_control/candidates'/Path(rel).name
        min_free = V._env('VNGIS_MIN_FREE_GB', 6.0, float) * 1024 ** 3
        if shutil.disk_usage(self.root).free < min_free:
            raise RuntimeError('Dung lượng đĩa dưới ngưỡng VNGIS_MIN_FREE_GB; không bắt đầu tải ảnh mới')
        V.download_tif(image, ctx['geom'], 20 if kind == 'day' else 500, str(path), f'[{gid}] {kind}/{month}')
        state, error = D.validate_image(path, kind)
        if state != 'done':
            raise ValueError(error)
        self.drive.put(path, rel, backup=True)
        stat = self.drive.stat(rel, allow_stopped=True)
        if stat is None:
            raise RuntimeError('File vừa upload không tồn tại trên Drive')
        with self.lock:
            self.cache[rel] = dict(fingerprint=fingerprint(stat), state='done', error='')
            self.images[kind, gid, month] = 'done', ''
            self.update(gid, month, kind+'_image', 'done')
        path.unlink()
        self.checkpoint()

    def preflight(self, kind):
        if kind == 'night':
            D.require_day_complete(self.table)
        gid = next((g for g in self.contexts if self.needed(g, kind+'_image') or self.needed(g, kind+'_indices')), None)
        if gid is None:
            return
        fc = V.communes_fc.filter(V.ee.Filter.eq('GID_3', gid))
        geom = fc.geometry()
        _, plan = V.fetch_plan(fc, geom, kind)
        month = next((m for m in D.MONTHS if plan[m]['image_count'] > 0), None)
        if month is None:
            log.info('PREFLIGHT %s: không có nguồn tại xã mẫu; worker sẽ xác minh no_source', kind)
            return
        region = geom.centroid(maxError=1).buffer(1500)
        image = (V.day_image(month, plan[month]['s2_window'], geom).select(V.DAY_BANDS_ALL)
                 if kind == 'day' else V.night_image(month, plan[month]['viirs'], geom)).clip(region)
        scale = 20 if kind == 'day' else 500
        whole = V.fetch_geotiff_bytes(image, region, scale)
        import rasterio
        with rasterio.MemoryFile(whole) as memory, memory.open() as source:
            if source.count != (10 if kind == 'day' else 2) or source.crs.to_epsg() != 4326:
                raise ValueError('PREFLIGHT: số kênh/CRS sai')
            array, transform = source.read(), source.transform
        if V.PREFLIGHT_TILE_TEST:
            V.TILING_OK[0] = False
            parts = [V.fetch_geotiff_bytes(image, rect, scale) for rect in V._split_bbox(V._bbox(region), 2)]
            tiled, tiled_transform, *_ = V.mosaic_tiles(parts)
            V.compare_on_grid(array, transform, tiled, tiled_transform)
            V.TILING_OK[0] = True
        log.info('PREFLIGHT %s: đạt; không truy vấn ảnh của giai đoạn khác', kind)

    def run_phase(self, kind):
        if kind == 'night':
            D.require_day_complete(self.table)
        fields = [kind+'_image', kind+'_indices']
        if self.table[fields].isin(D.TERMINAL).all().all():
            return True
        exhausted = [g for g in self.contexts if any(self.needed(g, f) for f in fields)
                     and self.history.get(g, {}).get('phase_attempts', {}).get(kind, 0) >= V.MAX_ATTEMPTS]
        if len(exhausted) == sum(any(self.needed(g, f) for f in fields) for g in self.contexts):
            log.error('Giai đoạn %s đã hết số lần thử; không gửi preflight/request mới', kind)
            return False
        if V.PREFLIGHT:
            self.preflight(kind)
        while True:
            V.check_stop()
            jobs = []
            for gid in self.contexts:
                if not any(self.needed(gid, f) for f in fields):
                    continue
                attempts = self.history.get(gid, {}).get('phase_attempts', {}).get(kind, 0)
                if attempts < V.MAX_ATTEMPTS:
                    jobs.append(gid)
            if not jobs:
                break
            log.info('GIAI ĐOẠN %s: còn %s xã cần xử lý', kind, len(jobs))
            with ThreadPoolExecutor(max_workers=V.N_WORKERS) as pool:
                futures = {}
                def attempt_process(gid):
                    V.check_stop()
                    with self.lock:
                        previous = self.history.setdefault(gid, {})
                        counts = previous.setdefault('phase_attempts', {})
                        counts[kind] = counts.get(kind, 0)+1
                        self.write_status(gid)
                    self.process(gid, kind)
                for gid in jobs:
                    futures[pool.submit(attempt_process, gid)] = gid
                for future in as_completed(futures):
                    future.result()
            self.checkpoint(force=True)
        complete = self.table[fields].isin(D.TERMINAL).all().all()
        if not complete:
            log.error('Giai đoạn %s còn thiếu/lỗi sau giới hạn thử; dừng nối lượt tự động', kind)
        return bool(complete)


def main(module, step='run', sync_only=False):
    global V
    V = module
    V.setup_logging()
    V.install_signal_handlers()
    timer = None
    engine = None
    watcher = None
    watcher_stop = threading.Event()
    code = 1
    if V.MAX_RUNTIME_SEC > 0:
        timer = threading.Timer(V.MAX_RUNTIME_SEC, V.request_stop, args=('deadline',))
        timer.daemon = True
        timer.start()
    try:
        if sync_only:
            Drive().flush_outbox()
            log.info('sync-only: đã gửi nốt outbox, không ghi đè bằng CSV cũ ngoài outbox')
            return 0
        admin = V.build_admin_table()
        targets = V.load_targets(admin)
        engine = Engine(targets)
        engine.drive.flush_outbox()
        if engine.drive.stat('_control/STOP') is not None:
            return 130
        def watch_stop():
            while not watcher_stop.wait(V.DRIVE_STOP_POLL_SEC):
                try:
                    if engine.drive.stat('_control/STOP') is not None:
                        V.request_stop('drive_stop')
                        return
                except V.StopRequested:
                    return
                except Exception as exc:
                    log.warning('Không kiểm tra được STOP trên Drive: %s', safe_error(exc))
        watcher = threading.Thread(target=watch_stop, daemon=True, name='drive-stop')
        watcher.start()
        engine.inventory()
        if step == 'inventory':
            return 0
        V.init_earth_engine()
        engine.check_asset(admin)
        if V._env('VNGIS_RETRY_FAILED', False, bool):
            for gid in engine.contexts:
                engine.history.setdefault(gid, {}).setdefault('phase_attempts', {}).pop('day', None)
        V.TILING_OK[0] = False  # Only successful preflight verifies the tiling grid.
        if not engine.run_phase('day'):
            return 1
        # Read actual uploaded files and CSVs again before admitting the night phase.
        engine.inventory()
        D.require_day_complete(engine.table)
        if V._env('VNGIS_RETRY_FAILED', False, bool):
            for gid in engine.contexts:
                engine.history.setdefault(gid, {}).setdefault('phase_attempts', {}).pop('night', None)
        V.TILING_OK[0] = False
        code = 0 if engine.run_phase('night') else 1
        return code
    except V.StopRequested:
        return {'deadline': 3, 'fatal': 1}.get(V.STOP_REASON[0], 130)
    except Exception as exc:
        log.error('LỖI: %s', safe_error(exc))
        return 1
    finally:
        watcher_stop.set()
        if watcher:
            watcher.join(timeout=5)
        if timer:
            timer.cancel()
        if engine is not None and engine.table is not None:
            try:
                engine.checkpoint(force=True)
                for path in (engine.root/'_control/logs').glob('*.log'):
                    engine.drive.put(path, f'_control/logs/{path.name}', backup=False)
            except Exception as exc:
                log.error('Không lưu được checkpoint cuối lượt: %s', safe_error(exc))
                return 1
