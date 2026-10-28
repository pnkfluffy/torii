import asyncio
from collections import Counter
import errno
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from coordinator import control_api, mcp, problems
from coordinator.host import HostClient, _reap_launchers, alive, host_state
from coordinator.service import Service
from coordinator.store import Store
from tests.support import read_jsonl, settle
from tests.test_host import FakeTelegram, HostTestsSupport
from tests.test_codex_secrets_goals import SERVER


class HostLogTests(HostTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        source = self.binary.read_text()
        source = source.replace("if '--append-system-prompt' in sys.argv:",
                                """sid=Path(sid).stem
native=Path(os.environ['CLAUDE_CONFIG_DIR'])/'projects'/'host-logs'
native.mkdir(parents=True,exist_ok=True)
with (native/(sid+'.jsonl')).open('a') as stream:
    stream.write(json.dumps({'type':'user','sessionId':sid})+'\\n')
if '--append-system-prompt' in sys.argv:""")
        source = source.replace("    while prompt=='torii-host-wait'", """    if prompt in ('torii-host-logs-recorded-group-zombie','torii-host-logs-recorded-group-live'):
        import subprocess
        subprocess.Popen([sys.executable,str(root/'recorded-group.py'),
                          'live' if prompt=='torii-host-logs-recorded-group-live' else 'zombie'],stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        while not (root/'recorded-group-ready.json').exists():
            time.sleep(.01)
        if prompt=='torii-host-logs-recorded-group-live':
            while True:
                time.sleep(.02)
    if prompt in ('torii-host-logs-group-survivor','torii-host-logs-group-survivor-success'):
        import subprocess
        descendant=subprocess.Popen([sys.executable,str(root/'group-survivor.py')],stdin=subprocess.DEVNULL,
                                    stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        (root/('group-survivor-'+str(os.getpid()))).write_text(str(descendant.pid))
        if prompt=='torii-host-logs-group-survivor':
            while True:
                time.sleep(.02)
    if prompt=='torii-host-logs-independent-hold':
        (root/('independent-hold-ready-'+str(os.getpid()))).touch()
        while not (root/('independent-hold-release-'+str(os.getpid()))).exists():
            time.sleep(.02)
        os._exit(0)
    while prompt=='torii-host-wait'""")
        self.binary.write_text(source)
        self.store = Store(self.state)
        self.add_named_account(self.store)
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('home', -100, 1, 'Home', str(self.root)))
            self.store.put('coordinator_home_topic', 'home')
        self.services = []

    async def asyncTearDown(self):
        for service, task in reversed(self.services):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        _reap_launchers()
        finished = self.root / 'finished-hosts'
        for directory in (self.state / 'hosts').glob('*'):
            client = HostClient(directory)
            try:
                if client.state == 'running':
                    await client.stop()
            finally:
                client.detach()
            pid = directory / 'pid'
            if (directory / 'exit.json').is_file() and pid.is_file():
                def gone():
                    _reap_launchers()
                    return not alive(pid.read_text())
                await self.until(gone)
                finished.mkdir(exist_ok=True)
                directory.chmod(0o700)
                directory.rename(finished / directory.name)
        self.store.close()
        await super().asyncTearDown()

    def job(self):
        return self.store.task_create('home', 'Host logs', worktree=str(self.root))

    def worker(self, job, prompt='finish'):
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,created,updated)
                VALUES (?,?,?,?,?,?,1,1)''',
                (job['id'], 'home', 'claude', prompt, str(self.root), '{}')).lastrowid
        return worker

    def row(self, worker):
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())

    def directory(self, worker):
        row = self.row(worker)
        return self.state / 'hosts' / (row['host'] or 'worker-' + str(worker))

    def start_service(self):
        service = Service(self.store, FakeTelegram(), self.runner({'claude': str(self.binary)}), self.root)
        task = asyncio.create_task(service.run())
        self.services.append((service, task))
        return service, task

    async def stop_service(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def observed(self, predicate, seconds=6):
        deadline = time.monotonic() + seconds
        while not predicate() and time.monotonic() < deadline:
            await asyncio.sleep(.01)
        return predicate()

    async def finished(self, worker):
        await self.until(lambda: self.row(worker)['status'] == 'done')
        directory = self.directory(worker)
        await self.until(lambda: (directory / 'exit.json').exists())
        def gone():
            _reap_launchers()
            return not alive((directory / 'pid').read_text().strip())
        await self.until(gone)
        return directory

    async def test_worker_logs_resume_receipts_and_job_close(self):
        job = self.job()
        worker = self.worker(job)
        service, task = self.start_service()
        directory = await self.finished(worker)
        self.assertTrue(await self.observed(lambda: not any((directory / name).exists()
                        for name in ('events.jsonl', 'writes.jsonl'))), 'Finished worker journals were not deleted')
        self.assertTrue((directory / 'stderr.log').is_file())
        self.assertTrue((directory / 'spec.json').is_file())
        saved = self.row(worker)
        self.assertEqual(Path(json.loads(saved['result'])['log_path']).resolve(),
                         (directory / 'stderr.log').resolve())
        client = HostClient.attach(directory)
        self.assertEqual(client.state, 'exited')
        self.assertIsNone(await client.next_event())
        self.assertEqual(list(client.history(1000)), [])
        self.assertEqual(list(client.history_after(0)), [])
        self.assertEqual(client.written_lines(), [])
        self.assertIsNone(client.past_request(1))
        self.assertIsNone(client.past_response(1))
        self.assertFalse(client.past_echo('message'))
        self.assertFalse(client.past_user('message'))
        self.assertEqual(client.max_request_id(), 0)
        with self.store.db:
            result = control_api.call(self.store, 'workers.steer', {'worker': worker, 'prompt': 'resume finish'})
        self.assertTrue(result.ok, result.text)
        await self.until(lambda: self.row(worker)['host'] != saved['host'])
        resumed = await self.finished(worker)
        self.assertTrue(await self.observed(lambda: not any((resumed / name).exists()
                        for name in ('events.jsonl', 'writes.jsonl'))), 'Resumed worker journals were not deleted')
        self.assertEqual(self.row(worker)['session'], saved['session'])
        inputs = read_jsonl(self.root / 'inputs.jsonl')
        self.assertEqual([item['event']['message']['content'].endswith('resume finish')
                          for item in inputs if 'session' in item], [False, True])
        with self.store.db:
            closed = await control_api.call_async(self.store, 'tasks.update', {'task': job['number'], 'status': 'done'}, topic='home')
        self.assertTrue(closed.ok, closed.text)
        self.assertTrue(await self.observed(lambda: not directory.exists() and not resumed.exists()),
                        'Closed job kept worker stderr folders')
        self.assertFalse(task.done())

    async def test_failed_worker_logs_survive_until_round_closes(self):
        job, worker, directory, service = await self._finished_host_without_service()
        with self.store.db:
            self.store.put('close_clears_space', True)
        result = json.loads(self.row(worker)['result'])
        variants = (
            ('success false', {'success': False, 'failure_code': None, 'status': 'done'}, 'done'),
            ('failure code', {'success': True, 'failure_code': 'execution_failed', 'status': 'done'}, 'done'),
            ('interrupted status', {'success': True, 'failure_code': None, 'status': 'interrupted'}, 'interrupted'),
        )
        for name, fields, status in variants:
            with self.subTest(reason=name):
                result.update(fields)
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET result=?,status=? WHERE id=?',
                                          (json.dumps(result), status, worker))
                    self.store.db.execute("UPDATE messages SET text=? WHERE source_worker=? AND kind='worker_result'",
                                          (json.dumps(result), worker))
                await service.host_logs.clean({'worker': worker})
                self.assertTrue((directory / 'events.jsonl').is_file(),
                                'A failed worker lost its event log before its job round closed')
                self.assertTrue((directory / 'writes.jsonl').is_file(),
                                'A failed worker lost its write log before its job round closed')
        await self._finish_worker_temp_cleanup(service, worker)
        self.store.task_update(job['id'], status='done')
        with self.assertLogs('coordinator.host_logs', level='INFO') as recorded:
            await service.host_logs.clean({'worker': worker})
        self.assertFalse(directory.exists(), 'A closed failed worker kept its host folder: %r' % recorded.output)

    async def test_failed_worker_logs_are_reclaimed_when_a_new_round_opens(self):
        job, worker, directory, service = await self._finished_host_without_service()
        result = json.loads(self.row(worker)['result'])
        result.update(success=False, failure_code='execution_failed')
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?', (json.dumps(result), worker))
            self.store.db.execute("UPDATE messages SET text=? WHERE source_worker=? AND kind='worker_result'",
                                  (json.dumps(result), worker))
        await service.host_logs.clean({'worker': worker})
        self.assertTrue((directory / 'events.jsonl').is_file())
        self.assertTrue((directory / 'writes.jsonl').is_file())
        self.store.task_update(job['id'], status='done')
        origin = self.store.message_save('home', 'owner', 'Open the next round')
        self.store.wait_open_round(job['id'], origin['id'], opened_by='owner')
        await service.host_logs.clean({'worker': worker})
        self.assertFalse((directory / 'events.jsonl').exists(),
                         'An older failed worker kept its event log after a new round opened')
        self.assertFalse((directory / 'writes.jsonl').exists(),
                         'An older failed worker kept its write log after a new round opened')
        self.assertTrue((directory / 'stderr.log').is_file())
        self.assertTrue(directory.is_dir())

    async def test_failed_run_logs_survive_a_successful_resume_until_round_closes(self):
        job, worker, failed_directory, service = await self._finished_host_without_service()
        failed_result = json.loads(self.row(worker)['result'])
        failed_result.update(success=False, failure_code='execution_failed')
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?',
                                  (json.dumps(failed_result), worker))
            self.store.db.execute("UPDATE messages SET text=? WHERE source_worker=? AND kind='worker_result'",
                                  (json.dumps(failed_result), worker))
        steered = control_api.call(self.store, 'workers.steer',
                                   {'worker': worker, 'prompt': 'resume after failure'}, topic='home')
        self.assertTrue(steered.ok, steered.text)
        await service.controls_once()
        await service.workers.run(self.row(worker), self.store.topic('home'))
        resumed_directory = await self.finished(worker)
        self.assertNotEqual(resumed_directory, failed_directory)
        self.assertTrue(json.loads(self.row(worker)['result'])['success'])
        await self._finish_worker_temp_cleanup(service, worker)
        with self.store.db:
            self.store.put('close_clears_space', True)
        await service.host_logs.clean({'worker': worker})
        await service.host_logs.clean()
        self.assertTrue((failed_directory / 'events.jsonl').is_file(),
                        'A successful resume hid the failed run journal before round close')
        self.assertTrue((failed_directory / 'writes.jsonl').is_file(),
                        'A successful resume hid the failed run writes before round close')
        self.assertFalse(resumed_directory.exists(), 'A successful resumed run kept its host folder')
        self.store.task_update(job['id'], status='done')
        await service.host_logs.clean({'task': job['id']})
        self.assertFalse(failed_directory.exists(), 'A closed round kept the failed run host folder')

    async def test_active_worker_is_retained_before_ownership_recovery(self):
        worker = self.worker(self.job())
        name = 'worker-%d-resume-7' % worker
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',host=? WHERE id=?", (name, worker))
        directory = self.state / 'hosts' / name
        directory.mkdir(parents=True)
        (directory / 'job.plist').write_bytes(b'')
        (directory / 'spec.json').write_text('{}')
        service = self._host_log_service()
        with patch.object(service.host_logs, '_recover') as recover:
            await service.host_logs.clean({'worker': worker})
        recover.assert_not_awaited()

    async def test_legacy_worker_folder_with_multiple_versions_is_removed_after_job_closes(self):
        job = self.job()
        name = 'worker-532.2.2.2'
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers
                (id,task,topic,provider,prompt,cwd,workspace,created,updated,status,result,host)
                VALUES (532,?,?,?,?,?,'{}',1,1,'done',?,?)''',
                (job['id'], 'home', 'claude', 'finish', str(self.root), json.dumps({'success': True}), name))
        directory = self.state / 'hosts' / name
        directory.mkdir(parents=True)
        (directory / 'launch-failed.json').write_text('true')
        self.store.task_update(job['id'], status='done')
        service = self._host_log_service()

        await service.host_logs.clean({'worker': 532})

        self.assertFalse(directory.exists(), 'A closed worker kept its legacy host folder')

    async def test_legacy_active_worker_folder_is_kept_as_worker_active(self):
        worker = self.worker(self.job())
        name = 'worker-%d.2.2.2' % worker
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',host=? WHERE id=?", (name, worker))
        directory = self.state / 'hosts' / name
        directory.mkdir(parents=True)
        service = self._host_log_service()

        await service.host_logs.clean({'worker': worker})
        _, whole, reason = service.host_logs._retention(directory)

        self.assertFalse(whole)
        self.assertEqual(reason, 'worker_active')
        self.assertTrue(directory.is_dir(), 'An active worker lost its legacy host folder')

    def test_invalid_worker_folder_names_stay_unknown_kind(self):
        service = self._host_log_service()
        for name in ('worker-0.1', 'worker-5x'):
            with self.subTest(name=name):
                directory = self.state / 'hosts' / name
                directory.mkdir(parents=True)
                _, whole, reason = service.host_logs._retention(directory)
                self.assertFalse(whole)
                self.assertEqual(reason, 'unknown_kind')

    async def test_running_and_restart_reattached_worker_stay_until_final(self):
        worker = self.worker(self.job(), 'torii-host-wait')
        service, task = self.start_service()
        await self.until(lambda: self.row(worker)['pid'] is not None and service.session is not None)
        directory = self.directory(worker)
        pid = self.row(worker)['pid']
        await self.stop_service(task)
        self.assertTrue(alive(pid))
        self.assertTrue((directory / 'events.jsonl').is_file())
        self.assertTrue((directory / 'writes.jsonl').is_file())
        service, task = self.start_service()
        await self.until(lambda: worker in service.worker_tasks and service.session is not None)
        self.assertEqual(self.row(worker)['pid'], pid)
        self.assertEqual(host_state(directory), 'running')
        self.assertTrue((directory / 'events.jsonl').is_file())
        (self.root / ('release-' + str(pid))).touch()
        await self.finished(worker)
        self.assertTrue(await self.observed(lambda: not (directory / 'events.jsonl').exists()),
                        'Reattached worker kept its log after final result')
        self.assertFalse(task.done())

    async def test_unsettled_steer_keeps_log_until_receipt_settles(self):
        worker = self.worker(self.job(), 'torii-host-wait')
        service, task = self.start_service()
        await self.until(lambda: self.row(worker)['pid'] is not None)
        with self.store.db:
            request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'receipt waiting'})
            self.store.service_request_settle(request, 'sending')
        (self.root / ('release-' + str(self.row(worker)['pid']))).touch()
        directory = await self.finished(worker)
        await self.until(lambda: not self.store.db.execute(
            "SELECT 1 FROM service_requests WHERE op='_cleanup.host_logs' AND state='queued'").fetchone())
        await asyncio.sleep(.1)
        self.assertTrue((directory / 'events.jsonl').is_file())
        self.assertTrue((directory / 'writes.jsonl').is_file())
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                              (request,)).fetchone()[0], 'sending')
        directory.chmod(0o500)
        self.store.service_request_settle(request, 'done', {'receipt': 'received'})
        self.assertTrue(await self.observed(lambda: not (directory / 'events.jsonl').exists()),
                        'Settled receipt did not release the worker log')
        self.assertFalse(task.done())

    async def test_replaced_parent_folder_goes_current_folder_stays(self):
        service, task = self.start_service()
        await self.until(lambda: service.session is not None)
        message = self.store.message_save('home', 'owner', 'initialize parent')
        await self.until(lambda: self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()[0] == 'received')
        await self.until(lambda: not service.session.busy)
        previous = service.session
        old = previous.process.directory
        native = previous.session_id
        await previous.stop()
        await self.until(lambda: service.session is not None and service.session is not previous)
        current = service.session.process.directory
        self.assertNotEqual(current, old)
        self.assertEqual(service.session.session_id, native)
        await self.until(lambda: not self.store.db.execute(
            "SELECT 1 FROM service_requests WHERE op='_cleanup.host_logs' "
            "AND json_extract(params,'$.host')=? AND state='queued'", (old.name,)).fetchone())
        await service.host_logs.queue.join()
        self.assertTrue(await self.observed(lambda: not old.exists()), 'Replaced parent folder was not deleted')
        self.assertTrue((current / 'events.jsonl').is_file())
        self.assertEqual(self.store.get('coordinator_host')['id'], current.name)
        self.assertFalse(task.done())

    async def test_deleted_worker_restart_has_no_replay_or_error(self):
        worker = self.worker(self.job())
        service, task = self.start_service()
        directory = await self.finished(worker)
        self.assertTrue(await self.observed(lambda: not (directory / 'events.jsonl').exists()),
                        'Worker log was not deleted before restart')
        await self.stop_service(task)
        with self.assertLogs('coordinator', level=logging.INFO) as captured:
            service, task = self.start_service()
            await self.until(lambda: service.session is not None)
            await asyncio.sleep(.15)
        self.assertNotIn(worker, service.worker_tasks)
        self.assertEqual(self.row(worker)['status'], 'done')
        self.assertEqual([record.getMessage() for record in captured.records if record.levelno >= logging.ERROR], [])
        self.assertEqual(len([item for item in read_jsonl(self.root / 'inputs.jsonl') if 'session' in item]), 1)
        self.assertFalse(task.done())

    async def test_last_worker_after_job_close_removes_all_stderr_folders(self):
        job = self.job()
        first, last = self.worker(job), self.worker(job, 'torii-host-wait')
        service, task = self.start_service()
        first_directory = await self.finished(first)
        await self.until(lambda: self.row(last)['pid'] is not None)
        self.store.task_update(job['id'], status='dropped')
        await self.until(lambda: not self.store.db.execute(
            "SELECT 1 FROM service_requests WHERE op='_cleanup.host_logs' AND state='queued'").fetchone())
        self.assertTrue(first_directory.exists())
        self.assertTrue((first_directory / 'stderr.log').is_file())
        last_directory = self.directory(last)
        (self.root / ('release-' + str(self.row(last)['pid']))).touch()
        self.assertTrue(await self.observed(lambda: not first_directory.exists() and not last_directory.exists()),
                        'Last worker completion did not remove every closed-job stderr folder')
        self.assertEqual(self.row(last)['status'], 'done')
        self.assertFalse(task.done())

    async def test_closed_job_retains_host_metadata_until_worker_temp_cleanup_finishes(self):
        self._host_log_service()
        from coordinator import worker_storage
        from tests.fake_opencode import install_fake_opencode
        from tests.test_opencode_runtime import ACCOUNT
        base = self.root / 'private-worker-temp'
        base.mkdir()
        key_home = self.root / 'dummy-opencode-account'
        key_home.mkdir()
        (key_home / 'key').write_text('dummy-only\n')
        (key_home / 'key').chmod(0o600)
        binary = install_fake_opencode(self.root / 'fake-opencode')
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_binary', str(binary))
        self.store.put('opencode_enabled', True)
        real_remove = worker_storage._remove_tree
        with patch('coordinator.worker_storage.temp_base', return_value=base), \
                patch('coordinator.providers.platform', 'linux'), \
                patch('coordinator.opencode_accounts.account_home', return_value=key_home), \
                patch('coordinator.opencode_runtime.managed_preference_paths', return_value=(), create=True):
            for provider in ('claude', 'opencode'):
                with self.subTest(provider=provider):
                    entered, release = threading.Event(), threading.Event()
                    targets = []
                    service, task, child = None, None, None
                    worker = self.worker(self.job(), 'torii-host-wait')
                    job = self.row(worker)['task']
                    provider_release = self.root / ('temp-interop-release-' + str(worker))
                    with self.store.db:
                        self.store.db.execute('UPDATE workers SET provider=? WHERE id=?', (provider, worker))
                    (binary.parent / 'scenario.json').write_text(json.dumps({
                        'scratch_files': True, 'release': str(provider_release)}))
                    def blocked(path):
                        if any(Path(path).resolve().parent == target.resolve().parent and
                               Path(path).name.startswith(target.name + '.deleting-') for target in targets):
                            entered.set()
                            release.wait()
                        return real_remove(path)
                    with patch('coordinator.worker_storage._remove_tree', side_effect=blocked):
                        try:
                            service = Service(self.store, FakeTelegram(), self.runner({
                                'claude': str(self.binary), 'opencode': str(binary)}), self.root)
                            task = asyncio.create_task(service.run())
                            self.services.append((service, task))
                            await self.until(lambda: self.row(worker)['pid'] is not None)
                            child = self.row(worker)['pid']
                            directory = self.directory(worker)
                            host_pid = (directory / 'pid').read_text().strip()
                            spec = json.loads((directory / 'spec.json').read_text())
                            self.assertIs(spec.get('worker_temp'), True)
                            scratch = Path(spec['env']['TMPDIR'])
                            targets.append(scratch)
                            if provider == 'opencode':
                                targets.append(Path(spec['env']['HOME']) / 'Library/Caches')
                                await self.until(lambda: all((path / 'scratch').is_file() for path in targets))
                            else:
                                (scratch / 'interop-proof').write_text('worker scratch')
                                self.assertEqual(service.workers.storage.state(worker)['path'], str(scratch))
                            self.store.task_update(job, status='done')
                            if provider == 'claude':
                                (self.root / ('release-' + str(child))).touch()
                            else:
                                provider_release.touch()
                            await self.until(lambda: self.row(worker)['status'] == 'done')
                            def host_gone():
                                _reap_launchers()
                                return not alive(host_pid)
                            await self.until(host_gone)
                            if provider == 'opencode':
                                self.assertIsNone(service.workers.storage.state(worker)['path'])
                            self.assertTrue(await self.observed(entered.is_set),
                                            'Worker temp cleanup never reached its held delete')
                            self.assertTrue(await self.observed(lambda: not (directory / 'events.jsonl').exists()
                                                              and not (directory / 'writes.jsonl').exists()),
                                            'Temp cleanup blocked finished worker journal deletion')
                            self.assertTrue((directory / 'spec.json').is_file(),
                                            'Closed-job host metadata disappeared before worker temp cleanup finished')
                            self.assertTrue((directory / 'stderr.log').is_file())
                            self.assertTrue(service.workers.storage.state(worker)['pending'])
                            self.assertFalse(release.is_set())
                            self.assertTrue((directory / 'spec.json').is_file())
                            release.set()
                            self.assertTrue(await self.observed(lambda: not directory.exists()),
                                            'Completed temp cleanup did not revisit its retained closed-job host')
                            self.assertFalse(service.workers.storage.state(worker)['pending'])
                            self.assertTrue(all(not path.exists() for path in targets))
                            self.assertFalse(task.done())
                        finally:
                            provider_release.touch()
                            if child is not None:
                                (self.root / ('release-' + str(child))).touch()
                            release.set()
                            if service is not None:
                                await service.workers_once()
                                pending = list(service.workers.storage.tasks.values())
                                if pending:
                                    await asyncio.gather(*pending, return_exceptions=True)
                            if task is not None:
                                await self.stop_service(task)

    async def test_shared_delete_bounds_steps_per_turn_and_resumes_cancelled_work(self):
        self._host_log_service()
        from coordinator import worker_storage
        self.assertTrue(callable(getattr(worker_storage, 'delete', None)),
                        'Host and worker temp cleanup do not share the bounded delete')
        directory = self.state / 'batch-delete-tree'
        directory.mkdir()
        for index in range(128):
            (directory / str(index)).write_bytes(b'x' * 64)
        quarantine = self.state / 'batch-delete-quarantine'
        loop = asyncio.get_running_loop()
        run_once, real_stat, real_scandir = loop._run_once, os.stat, os.scandir
        steps, counts = Counter(), Counter()
        turn = 0
        def counted_turn():
            nonlocal turn
            turn += 1
            run_once()
        def counted_stat(path, *args, **options):
            if options.get('dir_fd') is not None:
                steps[turn] += 1
            return real_stat(path, *args, **options)
        class Entries:
            def __init__(self, entries):
                self.entries = entries
            def __iter__(self):
                return self
            def __next__(self):
                steps[turn] += 1
                return next(self.entries)
            def close(self):
                self.entries.close()
        def counted_scandir(path):
            entries = real_scandir(path)
            return Entries(entries) if isinstance(path, int) else entries
        with patch.object(loop, '_run_once', side_effect=counted_turn), \
                patch('coordinator.worker_storage.os.stat', side_effect=counted_stat), \
                patch('coordinator.worker_storage.os.scandir', side_effect=counted_scandir):
            deleting = asyncio.create_task(worker_storage.delete(directory, quarantine, counts))
            try:
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                self.assertGreater(counts['bytes'], 0, 'Bounded delete made no progress before cancellation')
                deleting.cancel()
                settled, = await asyncio.gather(deleting, return_exceptions=True)
                self.assertIsInstance(settled, asyncio.CancelledError)
                self.assertFalse(directory.exists())
                detached, = quarantine.iterdir()
                self.assertTrue(detached.is_dir(), 'Cancellation did not preserve detached deletion work')
                self.assertGreater(len(list(detached.iterdir())), 0)
                await worker_storage.delete(detached, quarantine, counts)
            finally:
                if not deleting.done():
                    deleting.cancel()
                    await asyncio.gather(deleting, return_exceptions=True)
        self.assertFalse(any(quarantine.iterdir()))
        self.assertEqual(counts['bytes'], 128 * 64)
        self.assertEqual(counts['folders'], 1)
        self.assertGreater(len(steps), 8)
        self.assertGreater(max(steps.values()), 1, 'Shared delete still yields after every filesystem step')
        self.assertLessEqual(max(steps.values()), 8, 'Shared delete exceeded its bounded work per loop turn')
        print('Shared host delete proof: files=128 turns=%d max_steps_per_turn=%d bytes=%d cancelled_then_resumed=True' % (
            len(steps), max(steps.values()), counts['bytes']))

    async def test_startup_many_tiny_quarantined_folders_yields_once_per_entry(self):
        self._host_log_service()
        quarantine = self.state / 'hosts/.deleting'
        quarantine.mkdir(parents=True)
        names = ['tiny-' + str(index) for index in range(64)]
        for name in names:
            (quarantine / name).mkdir()
        loop = asyncio.get_running_loop()
        run_once, real_scandir = loop._run_once, os.scandir
        turn, entries, visits = 0, Counter(), Counter()
        def counted_turn():
            nonlocal turn
            turn += 1
            run_once()
        class Entries:
            def __init__(self, iterator):
                self.iterator = iterator
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.iterator.close()
            def __iter__(self):
                return self
            def __next__(self):
                entry = next(self.iterator)
                entries[turn] += 1
                visits[entry.name] += 1
                return entry
        def counted_scandir(path):
            iterator = real_scandir(path)
            return Entries(iterator) if not isinstance(path, int) and Path(path).resolve() == quarantine.resolve() else iterator
        with patch.object(loop, '_run_once', side_effect=counted_turn), \
                patch('coordinator.host_logs.os.scandir', side_effect=counted_scandir):
            service, task = self.start_service()
            self.assertTrue(await self.observed(lambda: not any(quarantine.iterdir())),
                            'Service startup did not finish its tiny interrupted deletes')
        self.assertEqual(visits, Counter({name: 1 for name in names}))
        self.assertLessEqual(max(entries.values()), 1,
                             'Startup processed multiple tiny quarantined folders in one event loop turn')
        self.assertEqual(len(entries), len(names))
        self.assertFalse(task.done())
        print('Tiny quarantine startup proof: folders=%d turns=%d max_entries_per_turn=%d' % (
            len(visits), len(entries), max(entries.values())))

    async def test_finished_worker_logs_are_cleaned(self):
        worker = self.worker(self.job())
        service, task = self.start_service()
        directory = await self.finished(worker)
        self.assertTrue(await self.observed(lambda: not (directory / 'events.jsonl').exists() and
                                             not (directory / 'writes.jsonl').exists()),
                        'Finished worker logs were not cleaned up')
        self.assertFalse((directory / 'writes.jsonl').exists())
        before = mcp.tools_hash()
        self.assertNotIn('cleanup_host_logs', {tool['name'] for tool in mcp.tools_list()})
        self.assertEqual(mcp.tools_hash(), before)
        self.assertFalse(task.done())

    async def test_thousand_missing_hosts_survive_deletions_during_index_cleanup(self):
        job = self.job()
        workers = [self.worker(job) for _ in range(1000)]
        hosts = self.state / 'hosts'
        hosts.mkdir(parents=True)
        names = ['worker-' + str(worker) for worker in workers]
        with self.store.db:
            self.store.db.executemany("UPDATE workers SET status='done' WHERE id=?", [(worker,) for worker in workers])
            for name, worker in zip(names, workers):
                self.store.host_log_register(name, worker)
        cleaner = self._host_log_service().host_logs
        halfway = asyncio.Event()
        deleted = asyncio.Event()
        forget = cleaner._forget
        visited = []

        def forget_and_signal(directory):
            forget(directory)
            visited.append(directory.name)
            if len(visited) == 500:
                halfway.set()

        async def delete_during_cleanup():
            await halfway.wait()
            self.assertFalse(cleaning.done())
            self.assertEqual(len(visited), 500)
            with self.store.db:
                self.store.db.executemany('DELETE FROM host_log_hosts WHERE name=?',
                                          [(name,) for name in names[500::2]])
            self.store.db.execute('BEGIN IMMEDIATE')
            self.store.put('synthetic_host_index_rollback', True)
            self.store.db.rollback()
            deleted.set()

        with patch.object(cleaner, '_forget', side_effect=forget_and_signal):
            cleaning = asyncio.create_task(cleaner.clean())
            deleting = asyncio.create_task(delete_during_cleanup())
            try:
                await asyncio.wait_for(asyncio.gather(cleaning, deleting), 5)
            finally:
                cleaning.cancel()
                deleting.cancel()
                await asyncio.gather(cleaning, deleting, return_exceptions=True)
        self.assertTrue(deleted.is_set())
        self.assertCountEqual(visited, names)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM host_log_hosts').fetchone()[0], 0)

    async def test_stale_host_index_cleanup_survives_concurrent_store_commit(self):
        job = self.job()
        workers = [self.worker(job) for _ in range(4)]
        hosts = self.state / 'hosts'
        hosts.mkdir(parents=True)
        names = []
        for worker in workers:
            name = 'worker-' + str(worker)
            (hosts / name).mkdir()
            self.store.host_log_register(name, worker)
            names.append(name)
        for name in names[1:]:
            (hosts / name).rmdir()
        self.assertEqual([row['name'] for row in self.store.db.execute(
            'SELECT name FROM host_log_hosts').fetchall()], names)
        cleaner = self._host_log_service().host_logs
        scanned = asyncio.Event()
        committed = asyncio.Event()
        remember = cleaner._remember

        def remember_and_signal(directories):
            remember(directories)
            if directories:
                scanned.set()

        cleaner._remember = remember_and_signal

        async def commit_during_cleanup():
            await scanned.wait()
            with self.store.db:
                self.store.put('host_log_cursor_commit', True)
            committed.set()
            self.store.db.execute('BEGIN IMMEDIATE')
            self.store.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)",
                                  ('host_log_cursor_rollback', 'true'))
            self.store.db.rollback()

        cleaning = asyncio.create_task(cleaner.clean())
        committing = asyncio.create_task(commit_during_cleanup())
        await asyncio.gather(cleaning, committing)
        self.assertTrue(committed.is_set())
        self.assertTrue((hosts / names[0]).is_dir())
        self.assertTrue(all(not (hosts / name).exists() for name in names[1:]))
        self.assertEqual([row['name'] for row in self.store.db.execute(
            'SELECT name FROM host_log_hosts').fetchall()], [names[0]])

    async def test_closed_codex_host_cleanup_removes_empty_secret_names_setting(self):
        job = self.job()
        worker = self.worker(job)
        with self.store.db:
            self.store.db.execute("UPDATE workers SET provider='codex',status='done',result=? WHERE id=?",
                                  (json.dumps({'success': True}), worker))
            self.store.put('host_log_secret_names:' + str(worker), [])
        directory = self.directory(worker)
        directory.mkdir(parents=True)
        (directory / 'launch-failed.json').write_text('true')
        (directory / 'spec.json').write_text('{"secret_names":[]}')
        self.store.task_update(job['id'], status='done')
        service = self._host_log_service()

        await service.host_logs.clean({'worker': worker})

        self.assertFalse(directory.exists())
        self.assertIsNone(self.store.get('host_log_secret_names:' + str(worker)))

    async def test_codex_reopened_job_keeps_historical_secret_names(self):
        job = self.job()
        worker = self.worker(job)
        with self.store.db:
            self.store.db.execute("UPDATE workers SET provider='codex' WHERE id=?", (worker,))
        binary = self.root / 'fake-codex'
        binary.write_text(SERVER.replace('SID', repr(str(uuid.uuid4()))).replace('ROOT', repr(str(self.root))))
        binary.chmod(0o700)
        service = Service(self.store, FakeTelegram(),
                          self.runner({'claude': str(self.binary), 'codex': str(binary)}), self.root)
        task = asyncio.create_task(service.run())
        self.services.append((service, task))
        directory = await self.finished(worker)
        self.assertTrue(json.loads(self.row(worker)['result'])['success'])
        spec = directory / 'spec.json'
        record = json.loads(spec.read_text())
        record['secret_names'] = ['HISTORICAL_NAME']
        spec.write_text(json.dumps(record))
        self.store.task_update(job['id'], status='done')
        self.assertTrue(await self.observed(lambda: not directory.exists()),
                        'Closed Codex job kept its finished host folder')
        saved = self.row(worker)
        self.store.put('codex_enabled', False)
        self.store.task_update(job['id'], status='open')
        with patch.dict(os.environ, {'HISTORICAL_NAME': 'synthetic-inherited'}):
            with self.store.db:
                result = control_api.call(self.store, 'workers.steer', {'worker': worker, 'prompt': 'blocked resume'})
            self.assertTrue(result.ok, result.text)
            await self.until(lambda: json.loads(self.row(worker)['result'] or '{}').get('success') is False)
            await self._finish_worker_temp_cleanup(service, worker)
            failed = self.row(worker)
            self.store.put('codex_enabled', True)
            with self.store.db:
                result = control_api.call(self.store, 'workers.steer', {'worker': worker, 'prompt': 'resume'})
            self.assertTrue(result.ok, result.text)
            await self.until(lambda: self.row(worker)['host'] != failed['host'])
            resumed = await self.finished(worker)
        self.assertTrue(json.loads(self.row(worker)['result'])['success'])
        self.assertEqual(self.row(worker)['session'], saved['session'])
        resumed_spec = json.loads((resumed / 'spec.json').read_text())
        self.assertEqual(resumed_spec['secret_names'], ['HISTORICAL_NAME'])
        self.assertNotIn('HISTORICAL_NAME', resumed_spec['env'])

    async def prepare_closed(self, count):
        job = self.job()
        workers = [self.worker(job) for _ in range(count)]
        service = Service(self.store, FakeTelegram(), self.runner({'claude': str(self.binary)}), self.root)
        for worker in workers:
            await service.workers.run(self.row(worker), self.store.topic('home'))
        directories = [await self.finished(worker) for worker in workers]
        self.store.task_update(job['id'], status='done')
        return directories

    @patch('coordinator.providers.platform', 'linux')
    async def test_invalid_process_or_failed_ownership_keeps_logs(self):
        directory, = await self.prepare_closed(1)
        pid = directory / 'pid'
        original = pid.read_text()
        pid.write_text('invalid')
        service, task = self.start_service()
        await self.until(lambda: service.session is not None)
        await asyncio.sleep(.2)
        self.assertTrue((directory / 'events.jsonl').is_file())
        await self.stop_service(task)
        pid.write_text(original)
        owner = directory / 'owner.json'
        owner.write_text(json.dumps({'cleanup': 'failed'}))
        service, task = self.start_service()
        await self.until(lambda: service.session is not None)
        await asyncio.sleep(.2)
        self.assertTrue((directory / 'events.jsonl').is_file())
        await self.stop_service(task)
        owner.rename(self.root / 'failed-owner-fixture.json')
        service, task = self.start_service()
        self.assertTrue(await self.observed(lambda: not directory.exists()),
                        'Valid process proof did not release the closed worker folder')
        self.assertFalse(task.done())

    async def test_startup_backlog_yields_and_shutdown_leaves_work(self):
        from coordinator import worker_storage
        self.assertEqual(getattr(worker_storage, 'DELETE_BATCH', None), 8,
                         'Shared host deletion does not enforce its eight-step cooperative work bound')
        directories = await self.prepare_closed(12)
        identities = {}
        for directory in directories:
            info = directory.stat()
            identities[info.st_dev, info.st_ino] = directory.name
            for index in range(500):
                (directory / ('extra-' + str(index))).touch()
        ticks, samples, partial = [], [], []
        removed = Counter()
        progressed = asyncio.Event()
        real_unlink = os.unlink
        running = True
        first = None
        def counted_unlink(path, *args, **options):
            owner = None
            if isinstance(path, str) and path.startswith('extra-') and options.get('dir_fd') is not None:
                info = os.fstat(options['dir_fd'])
                owner = identities.get((info.st_dev, info.st_ino))
            result = real_unlink(path, *args, **options)
            if owner is not None:
                removed[owner] += 1
            return result
        async def heartbeat():
            nonlocal first
            while running:
                ticks.append(time.monotonic())
                samples.append(sum(removed.values()))
                if removed and first is None:
                    first = next(iter(removed))
                progress = removed[first] if first is not None else 0
                if 0 < progress < 500 and (not partial or progress != partial[-1]):
                    partial.append(progress)
                    if len(partial) >= 3:
                        progressed.set()
                await asyncio.sleep(0)
        beat = asyncio.create_task(heartbeat())
        try:
            with patch('coordinator.worker_storage.os.unlink', new=counted_unlink), \
                    self.assertLogs('coordinator', level='INFO') as captured:
                service, task = self.start_service()
                try:
                    await asyncio.wait_for(progressed.wait(), 6)
                except asyncio.TimeoutError:
                    self.fail('Startup cleanup did not yield between partial deletions of its first folder')
                self.assertFalse(task.done(), 'Service ended before partial startup cleanup could be cancelled')
                self.assertLess(removed[first], 500, 'Startup cleanup drained its first folder before cancellation')
                before_stop = sum(removed.values())
                stopping = time.monotonic()
                await self.stop_service(task)
                stopped = time.monotonic() - stopping
            self.assertGreaterEqual(len(partial), 3, 'Concurrent coroutine did not observe partial deletion progress')
            work = [b - a for a, b in zip(samples, samples[1:])]
            self.assertGreater(max(work), 0, 'Startup cleanup made no progress between concurrent coroutine turns')
            self.assertLessEqual(max(work), worker_storage.DELETE_BATCH,
                                 'Startup cleanup exceeded its bounded work between concurrent coroutine turns')
            self.assertLessEqual(sum(removed.values()) - before_stop, worker_storage.DELETE_BATCH,
                                 'Shutdown kept deleting after startup cleanup was cancelled')
            remaining = sum(directory.exists() for directory in directories)
            self.assertGreater(remaining, 0,
                            'Shutdown waited for the complete backlog')
            quarantine = self.state / 'hosts/.deleting'
            self.assertTrue(quarantine.is_dir() and any(
                path.is_dir() and any(entry.name.startswith('extra-') for entry in path.iterdir())
                for path in quarantine.iterdir()),
                            'Shutdown did not preserve partial detached deletion work')
            print('Host backlog proof: partial=%s max_unlinks_per_turn=%d shutdown_unlinks=%d '
                  'heartbeat_max_gap=%.6fs stop=%.6fs remaining=%d' % (
                      partial, max(work), sum(removed.values()) - before_stop,
                      max(b - a for a, b in zip(ticks, ticks[1:])), stopped, remaining))
            summaries = [record.getMessage() for record in captured.records if 'host logs cleanup' in record.getMessage()]
            self.assertGreaterEqual(len(summaries), 1)
            self.assertTrue(all('folders=' in line and 'logs=' in line and 'bytes=' in line and 'kept=' in line
                                for line in summaries))
        finally:
            running = False
            await beat

    async def test_interrupted_delete_readonly_and_symlink_are_recovered(self):
        chmod = os.chmod
        def available_chmod(path, mode, **options):
            if options.get('follow_symlinks') is False:
                raise NotImplementedError('No non-following chmod on this platform')
            return chmod(path, mode, **options)
        permissions = patch('os.chmod', side_effect=available_chmod)
        permissions.start()
        self.addCleanup(permissions.stop)
        directory, = await self.prepare_closed(1)
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'precious').write_text('keep')
        (directory / 'outside-link').symlink_to(outside, target_is_directory=True)
        readonly = directory / 'readonly'
        readonly.mkdir()
        (readonly / 'file').write_text('remove')
        (readonly / 'file').chmod(0o400)
        readonly.chmod(0o500)
        for index in range(2000):
            (directory / ('extra-' + str(index))).touch()
        service, task = self.start_service()
        moved = await self.observed(lambda: not directory.exists())
        await self.stop_service(task)
        self.assertTrue(moved, 'Finished folder was not renamed for deletion')
        quarantine = self.state / 'hosts' / '.deleting'
        self.assertTrue(any(quarantine.iterdir()), 'Interrupted deletion did not retain resumable work')
        service, task = self.start_service()
        loop = asyncio.get_running_loop()
        run_once = loop._run_once
        turns, longest = 0, 0
        def counted_turn():
            nonlocal turns, longest
            started = time.monotonic()
            run_once()
            turns += 1
            longest = max(longest, time.monotonic() - started)
        with patch.object(loop, '_run_once', side_effect=counted_turn):
            complete = await self.observed(lambda: not any(quarantine.iterdir()))
        remaining = sum(len(files) + len(directories) for _, directories, files in os.walk(quarantine))
        loops = sorted(candidate.get_coro().cr_frame.f_locals.get('name', '')
                       for candidate in asyncio.all_tasks() if candidate.get_coro().cr_frame is not None
                       and candidate.get_coro().__qualname__ == 'Service.supervise')
        print('Interrupted host delete proof: complete=%s remaining=%d turns=%d longest_turn=%.6f queue=%s loops=%r' % (
            complete, remaining, turns, longest,
            service.host_logs.queue.qsize() if service.host_logs.queue is not None else None, loops))
        self.assertTrue(complete, 'Startup did not finish interrupted deletion: %r' % service.host_logs.retry)
        self.assertEqual((outside / 'precious').read_text(), 'keep')
        self.assertEqual(sorted(path.name for path in outside.iterdir()), ['precious'])
        self.assertFalse(task.done())


    def _host_log_service(self):
        service = Service(self.store, FakeTelegram(), self.runner({'claude': str(self.binary)}), self.root)
        self.assertIsNotNone(getattr(service, 'host_logs', None),
                             'Host log cleanup is missing its tracked service task')
        self.addCleanup(getattr(service.host_logs, 'close', lambda: None))
        return service

    def _delayed_host_teardown(self, worker, ready, release, nonchild=False):
        real_popen = subprocess.Popen
        code = '''import os,sys,time
from pathlib import Path
from coordinator.host import main
directory,ready,release=map(Path,sys.argv[1:4])
if sys.argv[4]=='nonchild' and os.fork():
    deadline=time.monotonic()+10
    while time.monotonic()<deadline and not ((directory/'sock').exists() and (directory/'child.pid').exists()):
        time.sleep(.005)
    os._exit(0)
main(directory)
ready.touch()
while not release.exists():
    time.sleep(.01)
'''
        def launching(argv, *args, **kwargs):
            if (isinstance(argv, list) and argv[1:4] == ['-m', 'coordinator', 'host'] and
                    Path(argv[4]).name == 'worker-' + str(worker)):
                argv = [argv[0], '-c', code, argv[4], str(ready), str(release),
                        'nonchild' if nonchild else 'child']
            return real_popen(argv, *args, **kwargs)
        return patch('coordinator.host.subprocess.Popen', side_effect=launching)

    @patch('coordinator.providers.platform', 'linux')
    async def test_final_host_exit_revisits_a_quiet_open_job_after_delayed_teardown(self):
        self._host_log_service()
        for reattach in (False, True):
            with self.subTest(reattach_nonchild=reattach):
                job = self.job()
                worker = self.worker(job, 'torii-host-wait')
                ready = self.root / ('host-teardown-ready-' + str(worker))
                release = self.root / ('host-teardown-release-' + str(worker))
                host_pid = None
                task = None
                try:
                    with self._delayed_host_teardown(worker, ready, release, nonchild=reattach):
                        service, task = self.start_service()
                        await self.until(lambda: self.row(worker)['pid'] is not None and service.session is not None)
                        directory = self.directory(worker)
                        host_pid = int((directory / 'pid').read_text())
                        if reattach:
                            with self.assertRaises(ChildProcessError):
                                os.waitpid(host_pid, os.WNOHANG)
                            await self.stop_service(task)
                            service, task = self.start_service()
                            await self.until(lambda: worker in service.worker_tasks and service.session is not None)
                            self.assertEqual(int((directory / 'pid').read_text()), host_pid)
                        (self.root / ('release-' + str(self.row(worker)['pid']))).touch()
                        await self.until(lambda: ready.exists() and self.row(worker)['status'] == 'done')
                        await self.until(lambda: not self.store.db.execute(
                            "SELECT 1 FROM service_requests WHERE op='_cleanup.host_logs' AND state='queued'").fetchone())
                        await service.host_logs.queue.join()
                        await asyncio.sleep(1.2)
                        self.assertTrue(alive(host_pid))
                        self.assertTrue((directory / 'events.jsonl').is_file())
                        self.assertTrue((directory / 'writes.jsonl').is_file())
                        self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
                        release.touch()
                        self.assertTrue(await self.observed(lambda: not any(
                            (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl'))),
                                        'A quiet final worker kept its journals after the recorded host exited: ' +
                                        repr([dict(row) for row in self.store.db.execute(
                                            "SELECT code,detail FROM problems WHERE area='hosts'")]))
                        self.assertFalse((directory / 'writes.jsonl').exists())
                        self.assertTrue((directory / 'stderr.log').is_file())
                        self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
                        self.assertEqual(self.row(worker)['status'], 'done')
                        await service.host_logs.queue.join()
                        self.assertNotIn(directory.name, service.host_logs.watch_hosts)
                        self.assertNotIn(directory.name, service.host_logs.exited)
                        self.assertIsNotNone(self.store.db.execute(
                            'SELECT 1 FROM host_log_hosts WHERE name=?', (directory.name,)).fetchone())
                        self.store.task_update(job['id'], status='done')
                        self.assertTrue(await self.observed(lambda: not directory.exists()),
                                        'Closing a job after releasing exit witnesses kept its stderr folder')
                        self.assertFalse(task.done())
                finally:
                    release.touch()
                    if host_pid is not None:
                        def gone():
                            _reap_launchers()
                            return not alive(host_pid)
                        await self.until(gone)
                    if task is not None:
                        await self.stop_service(task)

    @patch('coordinator.providers.platform', 'linux')
    async def test_service_stop_closes_pending_native_exit_watches(self):
        self._host_log_service()
        worker = self.worker(self.job(), 'torii-host-wait')
        ready = self.root / 'watch-close-ready'
        release = self.root / 'watch-close-release'
        host_pid = None
        task = None
        try:
            with self._delayed_host_teardown(worker, ready, release):
                service, task = self.start_service()
                await self.until(lambda: self.row(worker)['pid'] is not None)
                directory = self.directory(worker)
                host_pid = int((directory / 'pid').read_text())
                (self.root / ('release-' + str(self.row(worker)['pid']))).touch()
                await self.until(lambda: ready.exists() and self.row(worker)['status'] == 'done')
                self.assertTrue(await self.observed(lambda: bool(getattr(service.host_logs, 'watches', {}))),
                                'A live final host did not register a native exit watch: ' +
                                repr([dict(row) for row in self.store.db.execute(
                                    "SELECT code,detail FROM problems WHERE area='hosts'")]))
                watched = list(service.host_logs.watches.values())
                self.assertTrue((directory / 'events.jsonl').is_file())
                self.assertTrue(alive(host_pid))
                await settle(self.stop_service(task))
                self.assertFalse(service.host_logs.watches)
                for _, descriptor, queue in watched:
                    if queue is not None:
                        self.assertTrue(queue.closed)
                    with self.assertRaises(OSError):
                        os.fstat(descriptor)
                self.assertTrue(alive(host_pid), 'Stopping the cleanup watcher waited for or stopped its host')
                self.assertTrue((directory / 'events.jsonl').is_file())
        finally:
            release.touch()
            if host_pid is not None:
                def gone():
                    _reap_launchers()
                    return not alive(host_pid)
                await self.until(gone)
            if task is not None:
                await self.stop_service(task)

    @patch('coordinator.providers.platform', 'linux')
    async def test_native_exit_watch_failure_backs_off_then_resumes_cleanup(self):
        self._host_log_service()
        from coordinator import host_logs
        worker = self.worker(self.job(), 'torii-host-wait')
        ready = self.root / 'watch-failure-ready'
        release = self.root / 'watch-failure-release'
        native = host_logs.select if host_logs.sys.platform == 'darwin' else os
        name = 'kqueue' if host_logs.sys.platform == 'darwin' else 'pidfd_open'
        original = getattr(native, name)
        attempts = []
        def failing_once(*args, **kwargs):
            attempts.append(args)
            if len(attempts) == 1:
                raise OSError(errno.EMFILE, 'Synthetic native exit watch registration failure')
            return original(*args, **kwargs)
        host_pid = None
        task = None
        try:
            with self._delayed_host_teardown(worker, ready, release):
                service, task = self.start_service()
                await self.until(lambda: self.row(worker)['pid'] is not None and service.session is not None)
                directory = self.directory(worker)
                host_pid = int((directory / 'pid').read_text())
                with patch.object(native, name, side_effect=failing_once), \
                        patch('coordinator.host_logs.time', return_value=1000.0, create=True) as clock:
                    (self.root / ('release-' + str(self.row(worker)['pid']))).touch()
                    await self.until(lambda: ready.exists() and self.row(worker)['status'] == 'done')
                    self.assertTrue(await self.observed(lambda: directory.name in service.host_logs.retry),
                                    'A failed native exit watch lost its durable retry')
                    await service.host_logs.queue.join()
                    self.assertEqual(service.host_logs.retry[directory.name], {'attempt': 1, 'due': 1060.0})
                    self.assertEqual(self.store.get('host_log_delete_retries'), service.host_logs.retry)
                    self.assertEqual(len(attempts), 1)
                    clock.return_value = 1059.0
                    await service.host_logs.clean({'worker': worker})
                    await service.host_logs.clean({'retry': True})
                    self.assertEqual(len(attempts), 1, 'Native exit watch registration ignored its retry deadline')
                    self.assertTrue((directory / 'events.jsonl').is_file())
                    clock.return_value = 1060.0
                    service.host_logs.request({'retry': True})
                    await service.host_logs.queue.join()
                    self.assertEqual(len(attempts), 2)
                    self.assertTrue(service.host_logs.watches)
                    release.touch()
                    self.assertTrue(await self.observed(lambda: not any(
                        (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl'))),
                                    'The recovered native exit watch did not finish cleanup')
                    self.assertNotIn(directory.name, service.host_logs.retry)
                    self.assertFalse(task.done())
        finally:
            release.touch()
            if host_pid is not None:
                def gone():
                    _reap_launchers()
                    return not alive(host_pid)
                await self.until(gone)
            if task is not None:
                await self.stop_service(task)

    async def test_owned_worker_success_kills_its_survivor_before_log_cleanup(self):
        self._host_log_service()
        worker = self.worker(self.job(), 'torii-host-logs-group-survivor-success')
        (self.root / 'group-survivor.py').write_text('import time\ntime.sleep(120)\n')
        service, task = self.start_service()
        directory = await self.finished(worker)
        provider = int((directory / 'child.pid').read_text())
        descendant = int((self.root / ('group-survivor-' + str(provider))).read_text())
        await self.until(lambda: not alive(descendant))
        self.assertTrue(json.loads(self.row(worker)['result'])['success'])
        if sys.platform == 'darwin':
            self.assertEqual(json.loads((directory / 'spec.json').read_text())['ownership'], 'coalition')
            self.assertEqual(json.loads((directory / 'owner.json').read_text())['cleanup'], 'ok')
        self.assertTrue(await self.observed(lambda: not any(
            (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl'))))
        self.assertTrue((directory / 'stderr.log').is_file())
        self.assertFalse(task.done())

    @patch('coordinator.providers.platform', 'linux')
    async def test_last_group_survivor_exit_revisits_a_quiet_final_worker(self):
        self._host_log_service()
        job = self.job()
        worker = self.worker(job, 'torii-host-logs-group-survivor')
        survivor = self.root / 'group-survivor.py'
        survivor.write_text('''import os,time
from pathlib import Path
root=Path(__file__).parent
(root/('group-grandchild-ready-'+str(os.getpid()))).touch()
while not (root/('group-grandchild-release-'+str(os.getpid()))).exists():
    time.sleep(.02)
''')
        pids = []
        descendant = None
        task = None
        try:
            service, task = self.start_service()
            await self.until(lambda: self.row(worker)['pid'] is not None)
            directory = self.directory(worker)
            host_pid = int((directory / 'pid').read_text())
            child_pid = self.row(worker)['pid']
            pids.extend((host_pid, child_pid))
            roster = self.root / ('group-survivor-' + str(child_pid))
            await self.until(roster.exists)
            descendant = int(roster.read_text())
            pids.append(descendant)
            await self.until((self.root / ('group-grandchild-ready-' + str(descendant))).exists)
            self.assertEqual(os.getpgid(descendant), child_pid)
            real_stop = HostClient.stop
            async def preserve_group(client):
                if client.directory != directory:
                    await real_stop(client)
            with patch.object(HostClient, 'stop', preserve_group):
                os.kill(host_pid, signal.SIGKILL)
                os.kill(child_pid, signal.SIGKILL)
                await self.until(lambda: self.row(worker)['status'] == 'done')
                await self.until(lambda: not self.store.db.execute(
                    "SELECT 1 FROM service_requests WHERE op='_cleanup.host_logs' AND state='queued'").fetchone())
                await service.host_logs.queue.join()
                _reap_launchers()
                self.assertFalse(alive(host_pid))
                await self.until(lambda: not alive(child_pid))
                self.assertFalse(alive(child_pid))
                self.assertTrue(alive(descendant))
                self.assertTrue((directory / 'events.jsonl').is_file())
                self.assertTrue((directory / 'writes.jsonl').is_file())
                self.store.task_update(job['id'], status='done')
                (self.root / ('group-grandchild-release-' + str(descendant))).touch()
                self.assertTrue(await self.observed(lambda: not any(
                    (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl'))),
                                'The last process-group survivor exited without revisiting its final worker')
                self.assertFalse((directory / 'writes.jsonl').exists())
                self.assertFalse(directory.exists())
                self.assertEqual(self.store.task_get(job['id'])['status'], 'done')
                self.assertFalse(task.done())
        finally:
            if descendant is not None:
                (self.root / ('group-grandchild-release-' + str(descendant))).touch()
            for pid in pids:
                try:
                    if alive(pid):
                        os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if task is not None:
                await self.stop_service(task)

    def _recorded_group_fixture(self):
        helper = self.root / 'recorded-group.py'
        helper.write_text('''import json,os,sys,time
from pathlib import Path
root=Path(__file__).parent
group=os.getpgrp()
child=os.fork()
if child==0:
    if sys.argv[1]=='live':
        while not (root/'recorded-group-exit').exists():
            time.sleep(.01)
    os._exit(0)
os.setpgid(0,0)
(root/'recorded-group-ready.json').write_text(json.dumps({'keeper':os.getpid(),'child':child,'group':group}))
while not (root/'recorded-group-reap').exists():
    time.sleep(.01)
os.waitpid(child,0)
(root/'recorded-group-reaped').touch()
''')
        return self.root / 'recorded-group-ready.json'

    async def _release_recorded_group(self):
        (self.root / 'recorded-group-exit').touch()
        (self.root / 'recorded-group-reap').touch()
        roster = self.root / 'recorded-group-ready.json'
        if roster.exists():
            value = json.loads(roster.read_text())
            await self.until((self.root / 'recorded-group-reaped').exists)
            await self.until(lambda: not alive(value['keeper']) and not alive(value['child']))

    @patch('coordinator.providers.platform', 'linux')
    async def test_owned_unreaped_recorded_group_is_proved_terminal_without_waiting_for_reap(self):
        service = self._host_log_service()
        from coordinator import process_ownership as ownership
        ready = self._recorded_group_fixture()
        job = self.job()
        worker = self.worker(job, 'torii-host-logs-recorded-group-zombie')
        try:
            await asyncio.wait_for(service.workers.run(self.row(worker), self.store.topic('home')), 30)
            directory = await self.finished(worker)
            self.assertTrue(ready.exists())
            record = json.loads(ready.read_text())
            self.assertEqual(record['group'], int((directory / 'child.pid').read_text()))
            self.assertNotEqual(os.getpgid(record['keeper']), record['group'])
            self.assertTrue(alive(record['keeper']))
            self.assertTrue(alive(record['child']))
            self.assertFalse((self.root / 'recorded-group-reap').exists())
            try:
                os.killpg(record['group'], 0)
            except PermissionError:
                signal_check = 'denied'
            else:
                signal_check = 'allowed'
            try:
                terminal = await ownership.group_members(record['group'])
            except OSError as error:
                self.fail('An owned unreaped recorded group could not read its terminal roster: ' + str(error))
            self.assertEqual(terminal, ((), True),
                             'An owned unreaped recorded group could not prove its terminal roster')
            await service.host_logs.clean({'worker': worker})
            self.assertTrue(all(not (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl')),
                            'A terminal unreaped recorded group kept its journals after signal zero was denied')
            self.assertTrue((directory / 'stderr.log').is_file())
            self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
            self.assertTrue(alive(record['child']))
            self.assertFalse((self.root / 'recorded-group-reap').exists())
            print('Host unreaped recorded group proof: signal0=%s terminal_roster=True holder_live=True reaped=False' %
                  signal_check)
        finally:
            await self._release_recorded_group()

    @patch('coordinator.providers.platform', 'linux')
    async def test_denied_live_recorded_group_uses_watches_and_unreadable_metadata_backs_off(self):
        self._host_log_service()
        from coordinator import process_ownership as ownership
        ready = self._recorded_group_fixture()
        job = self.job()
        worker = self.worker(job, 'torii-host-logs-recorded-group-live')
        real_killpg = os.killpg
        task = None
        pids = []
        child_pid = None
        denied = []
        def denied_group(group, number):
            if number == 0 and group == child_pid:
                denied.append(group)
                raise PermissionError(errno.EPERM, 'Synthetic denied recorded group')
            return real_killpg(group, number)
        try:
            service, task = self.start_service()
            await self.until(lambda: self.row(worker)['pid'] is not None and ready.exists())
            directory = self.directory(worker)
            record = json.loads(ready.read_text())
            host_pid = int((directory / 'pid').read_text())
            child_pid = self.row(worker)['pid']
            pids.extend((host_pid, child_pid))
            self.assertEqual(record['group'], child_pid)
            real_stop = HostClient.stop
            async def preserve_group(client):
                if client.directory != directory:
                    await real_stop(client)
            with patch.object(HostClient, 'stop', preserve_group), \
                    patch('coordinator.host_logs.os.killpg', side_effect=denied_group), \
                    patch('coordinator.host_logs.time', return_value=1000.0, create=True), \
                    patch.object(ownership, 'group_members', side_effect=OSError(errno.EACCES, 'Unreadable roster')):
                os.kill(host_pid, signal.SIGKILL)
                os.kill(child_pid, signal.SIGKILL)
                await self.until(lambda: self.row(worker)['status'] == 'done')
                self.assertTrue(await self.observed(lambda: directory.name in service.host_logs.retry),
                                'A denied recorded group with unreadable metadata lost its durable retry')
                await service.host_logs.queue.join()
                self.assertEqual(service.host_logs.retry[directory.name], {'attempt': 1, 'due': 1060.0})
                self.assertEqual(self.store.get('host_log_delete_retries'), service.host_logs.retry)
                self.assertTrue(denied, 'The recorded-group signal permission check was not denied')
                self.assertTrue(alive(record['child']))
                self.assertTrue((directory / 'events.jsonl').is_file())
            with patch('coordinator.host_logs.os.killpg', side_effect=denied_group), \
                    patch('coordinator.host_logs.time', return_value=1060.0, create=True):
                service.host_logs.request({'retry': True})
                await service.host_logs.queue.join()
                self.assertTrue(any(key[2] == record['child'] for key in service.host_logs.watches),
                                'A denied live recorded group did not watch its surviving member')
                self.assertNotIn(directory.name, service.host_logs.retry)
                self.assertTrue((directory / 'events.jsonl').is_file())
                self.store.task_update(job['id'], status='done')
                (self.root / 'recorded-group-exit').touch()
                self.assertTrue(await self.observed(lambda: not any(
                    (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl'))),
                                'A denied recorded group did not clean up after its watched member ended')
                self.assertTrue(alive(record['child']))
                self.assertFalse((self.root / 'recorded-group-reap').exists())
                self.assertFalse(directory.exists())
                self.assertEqual(self.store.task_get(job['id'])['status'], 'done')
        finally:
            await self._release_recorded_group()
            for pid in pids:
                if alive(pid):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            if task is not None:
                await self.stop_service(task)

    async def test_duplicate_exit_notifications_coalesce_without_losing_an_active_pass_followup(self):
        job, _, first, service = await self._finished_host_without_service()
        _, _, second, _ = await self._finished_host_without_service(job)
        cleaner = service.host_logs
        first_target, second_target = {'host': first.name}, {'host': second.name}
        for _ in range(100):
            cleaner.request(first_target)
            cleaner.request(second_target)
        self.assertEqual(len(cleaner.pending), 2, 'Duplicate process exits queued one cleanup pass per process')
        entered = asyncio.Event()
        release = asyncio.Event()
        visits = []
        original = cleaner.clean
        async def cleaning(target):
            visits.append(target)
            if len(visits) == 1:
                entered.set()
                await release.wait()
            await original(target)
        task = None
        try:
            with patch.object(cleaner, 'clean', side_effect=cleaning):
                task = asyncio.create_task(cleaner.run())
                await asyncio.wait_for(entered.wait(), 6)
                for _ in range(100):
                    cleaner.request(first_target)
                    cleaner.request(second_target)
                self.assertEqual(cleaner.queue.qsize(), 2,
                                 'An active cleanup did not retain exactly one followup and one unrelated host')
                release.set()
                await asyncio.wait_for(cleaner.queue.join(), 6)
                self.assertEqual(visits, [first_target, second_target, first_target],
                                 'A process exit during cleanup was lost or repeated')
                self.assertTrue(all(not (directory / name).exists() for directory in (first, second)
                                    for name in ('events.jsonl', 'writes.jsonl')))
        finally:
            release.set()
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    def _marker_fsync_counter(self):
        from coordinator import host_logs
        scope = threading.local()
        calls = []
        real_write = host_logs._write
        real_fsync = os.fsync
        def writing(*args, **kwargs):
            scope.active = True
            try:
                return real_write(*args, **kwargs)
            finally:
                scope.active = False
        def syncing(descriptor):
            if getattr(scope, 'active', False):
                calls.append((threading.get_ident(), os.fstat(descriptor).st_size))
            return real_fsync(descriptor)
        return calls, writing, syncing

    async def _finished_host_without_service(self, job=None):
        job = job or self.job()
        worker = self.worker(job)
        service = self._host_log_service()
        await service.workers.run(self.row(worker), self.store.topic('home'))
        directory = await self.finished(worker)
        await self._finish_worker_temp_cleanup(service, worker)
        return job, worker, directory, service

    async def _finish_worker_temp_cleanup(self, service, worker):
        while service.workers.storage.tasks:
            await asyncio.gather(*list(service.workers.storage.tasks.values()))
            await asyncio.sleep(0)
        if service.workers.storage.state(worker)['pending']:
            await service.workers.storage.cleanup(worker)
        self.assertEqual(service.workers.storage.state(worker)['pending'], 0)

    async def test_failed_provider_launch_records_an_ended_host_and_is_reclaimed(self):
        job = self.job()
        worker = self.worker(job)
        service = Service(self.store, FakeTelegram(), self.runner({'claude': str(self.binary)}), self.root)
        self.binary.chmod(0o600)
        try:
            await service.workers.run(self.row(worker), self.store.topic('home'))
        finally:
            self.binary.chmod(0o700)
        row = self.row(worker)
        directory = self.directory(worker)
        self.assertEqual(row['status'], 'done')
        self.assertFalse(json.loads(row['result'])['success'])
        self.assertTrue((directory / 'spec.json').is_file())
        self.assertTrue(await self.observed(lambda: (directory / 'launch-failed.json').is_file()),
                        'A failed provider spawn left no explicit ended-host record')
        self.assertIs(json.loads((directory / 'launch-failed.json').read_text()), True)
        self.store.task_update(job['id'], status='done')
        service, task = self.start_service()
        self.assertTrue(await self.observed(lambda: not directory.exists()),
                        'A recorded failed launch kept its closed-job host folder')
        self.assertFalse(task.done())

    @patch('coordinator.providers.platform', 'linux')
    async def test_host_launcher_oserror_records_failure_without_starting_a_process(self):
        job = self.job()
        worker = self.worker(job)
        service = self._host_log_service()
        real_popen = subprocess.Popen
        def failed_host_launch(argv, *args, **kwargs):
            if list(argv[1:4]) == ['-m', 'coordinator', 'host']:
                raise OSError('Synthetic launcher failure')
            return real_popen(argv, *args, **kwargs)
        with patch('coordinator.host.subprocess.Popen', side_effect=failed_host_launch):
            await service.workers.run(self.row(worker), self.store.topic('home'))
        directory = self.directory(worker)
        self.assertTrue((directory / 'spec.json').is_file())
        self.assertFalse((directory / 'pid').exists())
        self.assertFalse((directory / 'child.pid').exists())
        self.assertTrue((directory / 'launch-failed.json').is_file(),
                        'A failed host launcher left no explicit safe failure record')
        self.assertIs(json.loads((directory / 'launch-failed.json').read_text()), True)
        self.assertFalse(json.loads(self.row(worker)['result'])['success'])
        self.store.task_update(job['id'], status='done')
        service, task = self.start_service()
        self.assertTrue(await self.observed(lambda: not directory.exists()),
                        'A recorded failed host launcher kept its closed-job folder')
        self.assertFalse(task.done())

    @patch('coordinator.providers.platform', 'linux')
    async def test_abrupt_host_death_requires_both_recorded_processes_to_be_gone(self):
        for survivor in ('host', 'child'):
            with self.subTest(survivor=survivor):
                worker = self.worker(self.job(), 'torii-host-logs-independent-hold')
                service = self._host_log_service()
                from coordinator import process_ownership as ownership
                cleanup = None
                run = asyncio.create_task(service.workers.run(self.row(worker), self.store.topic('home')))
                directory = None
                host_pid = child_pid = None
                stop_guard = None

                async def recorded_group_ended(wait_for_host=False):
                    while True:
                        if wait_for_host:
                            _reap_launchers()
                            if alive(host_pid):
                                await asyncio.sleep(.02)
                                continue
                        try:
                            os.killpg(child_pid, 0)
                        except ProcessLookupError:
                            return
                        except PermissionError:
                            pass
                        try:
                            members, ended = await ownership.group_members(child_pid)
                        except OSError as error:
                            if error.errno != errno.EAGAIN:
                                raise
                        else:
                            if ended:
                                self.assertEqual(members, ())
                                return
                        await asyncio.sleep(.02)

                try:
                    await self.until(lambda: self.row(worker)['pid'] is not None)
                    directory = self.directory(worker)
                    host_pid = int((directory / 'pid').read_text())
                    child_pid = int((directory / 'child.pid').read_text())
                    await self.until(lambda: (self.root / ('independent-hold-ready-' + str(child_pid))).is_file())
                    if survivor == 'child':
                        real_stop = HostClient.stop
                        async def preserve_owned_child(client):
                            if client.directory != directory.resolve():
                                return await real_stop(client)
                        stop_guard = patch.object(HostClient, 'stop', preserve_owned_child)
                        stop_guard.start()
                    if survivor == 'host':
                        os.kill(host_pid, signal.SIGSTOP)
                        os.killpg(child_pid, signal.SIGTERM)
                        await asyncio.wait_for(recorded_group_ended(), 40)
                        self.store.worker_complete(worker, {'success': False,
                            'session_id': self.row(worker)['session'], 'error': 'Test final result before host failure'})
                    else:
                        os.kill(host_pid, signal.SIGKILL)
                        await asyncio.wait_for(asyncio.shield(run), 5)
                    self.assertFalse((directory / 'exit.json').exists())
                    self.assertTrue(alive(host_pid) if survivor == 'host' else alive(child_pid))
                    await service.host_logs.clean({'worker': worker})
                    self.assertTrue((directory / 'events.jsonl').is_file(),
                                    'Cleanup deleted a log while a recorded process was still alive')
                    self.assertTrue((directory / 'writes.jsonl').is_file())
                    if survivor == 'host':
                        os.kill(host_pid, signal.SIGKILL)
                    else:
                        os.killpg(child_pid, signal.SIGTERM)
                    await asyncio.wait_for(asyncio.shield(run), 5)
                    self.store.task_update(self.row(worker)['task'], status='done')
                    cleanup = asyncio.create_task(service.host_logs.run())
                    await asyncio.wait_for(recorded_group_ended(wait_for_host=True), 40)
                    self.assertFalse((directory / 'exit.json').exists())
                    self.assertTrue(await self.observed(lambda: not any(
                        (directory / name).exists() for name in ('events.jsonl', 'writes.jsonl'))),
                                    'Native exit notification kept the journals after both recorded processes ended')
                    self.assertFalse((directory / 'writes.jsonl').exists())
                    self.assertTrue((directory / 'spec.json').is_file())
                    await self._finish_worker_temp_cleanup(service, worker)
                    await service.host_logs.clean({'worker': worker})
                    self.assertFalse(directory.exists())
                finally:
                    if stop_guard is not None:
                        stop_guard.stop()
                    for pid, group in ((host_pid, False), (child_pid, True)):
                        if pid is not None and alive(pid):
                            try:
                                (os.killpg if group else os.kill)(pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                    if not run.done():
                        run.cancel()
                    if cleanup is not None:
                        cleanup.cancel()
                        await asyncio.gather(cleanup, return_exceptions=True)
                    await asyncio.gather(run, return_exceptions=True)

    async def test_marker_fsync_yields_and_cancellation_waits_for_its_single_write(self):
        for refused in (False, True):
            with self.subTest(fsync_fails=refused):
                job, worker, directory, service = await self._finished_host_without_service()
                loop_thread = threading.get_ident()
                started = threading.Event()
                release = threading.Event()
                fsync_threads = []
                failure = [refused]
                real_fsync = os.fsync
                client = HostClient(directory)
                expected_writes = client.written_lines()
                expected_events = list(client.history_after(0))
                self.assertTrue(expected_writes)
                self.assertTrue(expected_events)
                def gated_fsync(descriptor):
                    identity = threading.get_ident()
                    fsync_threads.append(identity)
                    started.set()
                    if identity == loop_thread:
                        raise AssertionError('Host marker fsync ran on the event loop thread')
                    if not release.wait(10):
                        raise AssertionError('Test did not release its marker fsync')
                    if failure[0]:
                        raise OSError('Synthetic marker fsync failure during cancellation')
                    real_fsync(descriptor)
                cleaning = None
                try:
                    with patch('os.fsync', side_effect=gated_fsync), \
                            patch('coordinator.host_logs.MARKER_LINK_LIMIT', 1, create=True):
                        cleaning = asyncio.create_task(service.host_logs.clean({'worker': worker}))
                        self.assertTrue(await self.observed(started.is_set), 'Cleanup never began its durable marker write')
                        self.assertEqual(len(fsync_threads), 1)
                        self.assertNotEqual(fsync_threads[0], loop_thread, 'Marker fsync blocked the event loop')
                        self.assertFalse(cleaning.done())
                        self.assertTrue((directory / 'events.jsonl').is_file())
                        self.assertTrue((directory / 'writes.jsonl').is_file())
                        cleaning.cancel()
                        for index in range(10):
                            await asyncio.sleep(0)
                        self.assertFalse(cleaning.done(), 'Cleanup cancellation left its marker writer running')
                        release.set()
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(cleaning, 3)
                        self.assertFalse((directory / 'logs-deleted.json').exists())
                        if not refused:
                            self.assertIs(json.loads((directory / 'logs-deleted.pending').read_text()), True)
                        self.assertEqual(client.written_lines(), expected_writes)
                        self.assertEqual(list(client.history_after(0)), expected_events)
                        failure[0] = False
                        await service.host_logs.clean({'worker': worker})
                        expected_syncs = 2 if refused else 1
                        self.assertEqual(len(fsync_threads), expected_syncs)
                        self.assertFalse((directory / 'events.jsonl').exists())
                        self.assertFalse((directory / 'writes.jsonl').exists())
                        await service.host_logs.clean({'worker': worker})
                        self.assertEqual(len(fsync_threads), expected_syncs, 'Cleanup rewrote an already durable ended-host marker')
                finally:
                    release.set()
                    if cleaning is not None:
                        await asyncio.gather(cleaning, return_exceptions=True)
                    client.detach()

    async def test_marker_payload_reuse_rotation_and_cross_device_fallback(self):
        job = self.job()
        directories = []
        identities = []
        template = self.store.directory / 'host-log-marker.json'
        self._host_log_service()
        calls, writing, syncing = self._marker_fsync_counter()
        with patch('coordinator.host_logs.MARKER_LINK_LIMIT', 3, create=True), \
                patch('coordinator.host_logs._write', writing), patch('os.fsync', syncing):
            for index in range(4):
                _, worker, directory, service = await self._finished_host_without_service(job)
                await service.host_logs.clean({'worker': worker})
                directories.append(directory)
                marker = directory / 'logs-deleted.json'
                record = marker.stat()
                identities.append((record.st_dev, record.st_ino))
                self.assertIs(json.loads(marker.read_text()), True)
                self.assertFalse((directory / 'events.jsonl').exists())
            self.assertEqual(identities[0], identities[1], 'Two retained hosts did not share one durable marker payload')
            self.assertEqual(identities[2], identities[3])
            self.assertNotEqual(identities[0], identities[2], 'The marker payload did not rotate at the link limit')
            self.assertEqual(len(calls), 2, 'Marker reuse fsynced one payload per host')
            outside = self.root / 'outside-marker-target'
            outside.write_text('outside marker target stays')
            template.rename(self.root / 'old-marker-template')
            template.symlink_to(outside)
            _, worker, directory, service = await self._finished_host_without_service(job)
            (directory / 'logs-deleted.pending').symlink_to(outside)
            await service.host_logs.clean({'worker': worker})
            directories.append(directory)
            self.assertFalse(template.is_symlink())
            self.assertIs(json.loads((directory / 'logs-deleted.json').read_text()), True)
            self.assertEqual(outside.read_text(), 'outside marker target stays')
            self.assertEqual(len(calls), 3)
            _, worker, directory, service = await self._finished_host_without_service(job)
            real_link = os.link
            def different_device(source, *args, **kwargs):
                if source == template.name:
                    raise OSError(errno.EXDEV, 'Synthetic different host filesystem')
                return real_link(source, *args, **kwargs)
            with patch('os.link', side_effect=different_device):
                await service.host_logs.clean({'worker': worker})
            directories.append(directory)
            self.assertIs(json.loads((directory / 'logs-deleted.json').read_text()), True)
            self.assertNotEqual((directory / 'logs-deleted.json').stat().st_ino, template.stat().st_ino)
            self.assertEqual(len(calls), 4, 'The cross-device fallback did not fsync its host-local marker')
            _, worker, directory, service = await self._finished_host_without_service(job)
            refused = []
            def link_limit(source, *args, **kwargs):
                if source == template.name and not refused:
                    refused.append(source)
                    raise OSError(errno.EMLINK, 'Synthetic filesystem link limit')
                return real_link(source, *args, **kwargs)
            with patch('os.link', side_effect=link_limit):
                await service.host_logs.clean({'worker': worker})
            directories.append(directory)
            self.assertEqual(len(refused), 1)
            self.assertEqual(len(calls), 5, 'An unexpected filesystem link limit did not rotate the payload')
            self.assertTrue(all(json.loads((folder / 'logs-deleted.json').read_text()) is True for folder in directories))
            self.assertTrue(all(size == 4 for _, size in calls))
            with self.store.db:
                closed = await control_api.call_async(self.store, 'tasks.update', {'task': job['number'], 'status': 'done'},
                                          source='cli', topic='home')
            self.assertTrue(closed.ok, closed.text)
            await service.host_logs.clean({'task': job['id']})
            self.assertTrue(all(not folder.exists() for folder in directories))
            self.assertIs(json.loads(template.read_text()), True)
            self.assertEqual(outside.read_text(), 'outside marker target stays')
            self.assertEqual(len(calls), 5)

    async def test_receipt_arriving_during_marker_fsync_keeps_journals_readable(self):
        await self._marker_fsync_interleaving('receipt')

    async def test_failed_worker_during_marker_fsync_keeps_journals_readable(self):
        await self._marker_fsync_interleaving('failure')

    async def _marker_fsync_interleaving(self, action):
        job, worker, directory, service = await self._finished_host_without_service()
        started = threading.Event()
        release = threading.Event()
        loop_thread = threading.get_ident()
        real_fsync = os.fsync
        client = HostClient(directory)
        expected_writes = client.written_lines()
        expected_events = list(client.history_after(0))
        self.assertTrue(expected_writes)
        self.assertTrue(expected_events)
        def gated_fsync(descriptor):
            started.set()
            if threading.get_ident() == loop_thread:
                raise AssertionError('Host marker fsync ran on the event loop thread')
            if not release.wait(10):
                raise AssertionError('Test did not release its marker fsync')
            real_fsync(descriptor)
        cleaning = None
        try:
            with patch('os.fsync', side_effect=gated_fsync), \
                    self.assertLogs('coordinator.host_logs', level='INFO') as recorded:
                cleaning = asyncio.create_task(service.host_logs.clean({'worker': worker}))
                self.assertTrue(await self.observed(started.is_set))
                with self.store.db:
                    if action == 'receipt':
                        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'receipt pending'})
                        self.store.service_request_settle(request, 'sending')
                    else:
                        result = json.loads(self.row(worker)['result'])
                        result.update(success=False, failure_code='execution_failed')
                        self.store.db.execute('UPDATE workers SET result=? WHERE id=?', (json.dumps(result), worker))
                        self.store.db.execute(
                            "UPDATE messages SET text=? WHERE source_worker=? AND kind='worker_result'",
                            (json.dumps(result), worker))
                release.set()
                await asyncio.wait_for(cleaning, 3)
                self.assertTrue((directory / 'events.jsonl').is_file(), 'A retention change during fsync lost its event journal')
                self.assertTrue((directory / 'writes.jsonl').is_file())
                self.assertFalse((directory / 'logs-deleted.json').exists())
                self.assertIs(json.loads((directory / 'logs-deleted.pending').read_text()), True)
                self.assertEqual(client.written_lines(), expected_writes)
                self.assertEqual(list(client.history_after(0)), expected_events)
            reason = 'receipts' if action == 'receipt' else 'failed_worker'
            self.assertTrue(any('"%s": 1' % reason in line for line in recorded.output))
            if action == 'receipt':
                self.store.service_request_settle(request, 'done', {'receipt': 'received'})
            else:
                self.store.task_update(job['id'], status='done')
            await service.host_logs.clean({'worker': worker})
            if action == 'receipt':
                self.assertFalse((directory / 'events.jsonl').exists())
                self.assertFalse((directory / 'writes.jsonl').exists())
            else:
                self.assertFalse(directory.exists())
        finally:
            release.set()
            if cleaning is not None:
                await asyncio.gather(cleaning, return_exceptions=True)
            client.detach()

    async def test_routine_retained_worker_cleanup_avoids_redundant_permission_writes_and_retention_reads(self):
        self._host_log_service()
        _, worker, directory, service = await self._finished_host_without_service()
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        retentions, permissions = [], []
        real_retention, real_fchmod = service.host_logs._retention, os.fchmod
        def counted_retention(path):
            retentions.append(path)
            return real_retention(path)
        def counted_fchmod(*args):
            permissions.append(args)
            return real_fchmod(*args)
        with patch.object(service.host_logs, '_retention', counted_retention), \
                patch('os.fchmod', counted_fchmod):
            await service.host_logs.clean({'worker': worker})
        self.assertFalse((directory / 'events.jsonl').exists())
        self.assertFalse((directory / 'writes.jsonl').exists())
        self.assertTrue((directory / 'stderr.log').is_file())
        self.assertTrue((directory / 'spec.json').is_file())
        self.assertLessEqual(len(retentions), 3, 'Routine cleanup repeated its retention query without a mutation')
        self.assertEqual(permissions, [], 'Routine cleanup rewrote already sufficient host directory permissions')

    @patch('coordinator.providers.platform', 'linux')
    async def test_startup_5000_hosts_is_one_scan_and_idle_work_does_not_repeat(self):
        job, seed, source, service = await self._finished_host_without_service()
        total = 5000
        seed_row = self.row(seed)
        records = [file for file in source.iterdir() if file.is_file()]
        directories = [source]
        worker_ids = [seed]
        with self.store.db:
            for index in range(total - 1):
                worker = self.store.db.execute('''INSERT INTO workers
                    (task,topic,provider,prompt,cwd,workspace,status,session,fresh,result,created,updated)
                    VALUES (?,?,?,?,?,?,'done',?,0,?,1,1)''',
                    (job['id'], 'home', 'claude', 'finished startup fixture', str(self.root), '{}',
                     seed_row['session'], seed_row['result'])).lastrowid
                directory = self.state / 'hosts' / ('worker-' + str(worker))
                self.store.db.execute('UPDATE workers SET host=? WHERE id=?', (directory.name, worker))
                directory.mkdir(mode=0o700)
                for record in records:
                    if record.name == 'spec.json':
                        from coordinator.worker_storage import temp_directory
                        spec = json.loads(record.read_text())
                        spec['env']['TMPDIR'] = str(temp_directory(worker))
                        (directory / record.name).write_text(json.dumps(spec))
                    else:
                        os.link(record, directory / record.name)
                directories.append(directory)
                worker_ids.append(worker)
            self.store.db.execute("UPDATE service_requests SET state='done' WHERE op='_cleanup.host_logs'")
        await service.workers.cleanup_finished()
        while service.workers.storage.tasks:
            await asyncio.gather(*list(service.workers.storage.tasks.values()))
            await asyncio.sleep(0)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM worker_temp_state WHERE pending!=0').fetchone()[0], 0)
        with self.store.db:
            self.store.db.execute("UPDATE service_requests SET state='done' WHERE op='_cleanup.host_logs'")
        visited = Counter()
        root_scans = []
        root_entries = []
        cleanup_scans = []
        entries_per_turn = Counter()
        folders_per_turn = Counter()
        sql_counts = Counter()
        index_pending = False
        def traced_sql(statement):
            nonlocal index_pending
            statement = statement.strip().upper()
            if statement.startswith('INSERT OR IGNORE INTO HOST_LOG_HOSTS'):
                sql_counts['index_rows'] += 1
                index_pending = True
            elif statement == 'COMMIT':
                sql_counts['commits'] += 1
                if index_pending:
                    sql_counts['index_commits'] += 1
                    index_pending = False
            elif statement == 'ROLLBACK':
                index_pending = False
        turn = 0
        running = True
        async def heartbeat():
            nonlocal turn
            while running:
                turn += 1
                await asyncio.sleep(0)
        worker_globs = []
        progress = asyncio.Queue()
        nonworker_boundaries = []
        real_folder = service.host_logs._folder
        real_scandir = os.scandir
        real_glob = Path.glob
        marker_fsyncs, marker_write, marker_sync = self._marker_fsync_counter()
        hosts = self.store.directory / 'hosts'
        class CountedEntries:
            def __init__(self, entries):
                self.entries = entries
            def __iter__(self):
                return self
            def __next__(self):
                entry = next(self.entries)
                root_entries.append(entry.name)
                entries_per_turn[turn] += 1
                return entry
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.close()
            def close(self):
                self.entries.close()
        def counted_scandir(path):
            entries = real_scandir(path)
            if isinstance(path, (str, bytes, Path)) and Path(path) == hosts:
                root_scans.append(path)
                cleanup = service.session is not None
                if cleanup:
                    cleanup_scans.append(path)
                return CountedEntries(entries)
            return entries
        def counted_glob(path, pattern):
            if path == hosts and pattern.startswith('worker-'):
                worker_globs.append(pattern)
            return real_glob(path, pattern)
        async def counted_folder(directory, counts, kept):
            is_worker = directory.name.startswith('worker-')
            if is_worker:
                visited[directory.name] += 1
                folders_per_turn[turn] += 1
            await real_folder(directory, counts, kept)
            if is_worker and (len(visited) % 32 == 0 or len(visited) == total):
                progress.put_nowait(len(visited))
            if is_worker and len(visited) == 32 and not nonworker_boundaries:
                nonworker_boundaries.append(len(visited))
                queued = progress.qsize()
                await counted_folder(hosts / '.deleting', counts, kept)
                self.assertEqual(progress.qsize(), queued,
                                 'A nonworker host callback repeated a completed worker progress batch')
        beat = asyncio.create_task(heartbeat())
        self.store.db.set_trace_callback(traced_sql)
        try:
            with patch('os.scandir', side_effect=counted_scandir), \
                    patch.object(hosts._accessor, 'scandir', side_effect=counted_scandir), \
                    patch.object(Path, 'glob', counted_glob), \
                    patch('coordinator.host_logs._write', marker_write), patch('os.fsync', marker_sync), \
                    patch.object(service.host_logs, '_folder', side_effect=counted_folder):
                startup_started = time.monotonic()
                task = asyncio.create_task(service.run())
                self.services.append((service, task))
                completed = 0
                while completed < total:
                    try:
                        current = await asyncio.wait_for(progress.get(), 30)
                        self.assertGreater(current, completed, 'Startup repeated a completed progress batch')
                        completed = current
                    except asyncio.TimeoutError:
                        self.fail('Startup made no progress through its 5000 completed host folders: '
                                  'visited=%d root_scans=%d root_entries=%d index_rows=%d index_commits=%d commits=%d fsyncs=%d' % (
                                      len(visited), len(root_scans), len(root_entries), sql_counts['index_rows'],
                                      sql_counts['index_commits'], sql_counts['commits'], len(marker_fsyncs)))
                startup_elapsed = time.monotonic() - startup_started
                self.assertEqual(nonworker_boundaries, [32], 'Startup did not exercise its nonworker batch boundary')
                self.assertEqual(set(visited), {directory.name for directory in directories})
                self.assertEqual(set(visited.values()), {1}, 'Startup revisited a completed host folder')
                self.assertLessEqual(len(root_scans), 2, 'Startup scanned hosts once per worker')
                self.assertLessEqual(len(root_entries), 2 * (total + 3))
                self.assertEqual(worker_globs, [], 'A per-worker glob scanned all historical hosts')
                self.assertEqual(len(cleanup_scans), 1, 'Startup did not use one background hosts scan')
                self.assertLessEqual(max(entries_per_turn.values()), 4,
                                     'Startup consumed an unbounded number of host entries in one event loop turn')
                self.assertLessEqual(max(folders_per_turn.values()), 4,
                                     'Startup inspected an unbounded number of host folders in one event loop turn')
                self.assertGreaterEqual(sql_counts['index_rows'], total - 31,
                                        'Startup did not persist its bounded completed index batches')
                self.assertLessEqual(sql_counts['index_commits'], (total + 31) // 32,
                                     'Startup committed a separate host index transaction for every folder')
                self.assertLessEqual(sql_counts['commits'], (total + 31) // 32 + 40,
                                     'Startup committed Store changes once per completed folder')
                self.assertGreaterEqual(len(marker_fsyncs), 1, 'Startup never persisted its reusable marker payload')
                self.assertLessEqual(len(marker_fsyncs), 2, 'Startup fsynced an ended marker payload once per host')
                self.assertTrue(all(not (directory / 'events.jsonl').exists() for directory in directories))
                inspected = visited.copy()
                scanned = len(root_entries)
                for index in range(20):
                    await service.workers_once()
                    await service.controls_once()
                    await asyncio.sleep(0)
                await asyncio.sleep(.25)
                self.assertEqual(visited, inspected, 'Idle worker passes repeated already deleted host work')
                self.assertEqual(len(root_entries), scanned, 'Idle worker passes scanned historical hosts')
                service.host_logs.request({'worker': worker_ids[-1]})
                await service.host_logs.queue.join()
                self.assertEqual(sql_counts['index_rows'], total + 1,
                                 'Startup lost its retained host index tail before the targeted request')
                extra = sum(visited.values()) - sum(inspected.values())
                self.assertEqual(extra, 1, 'A targeted worker event did not inspect exactly its retained host folder')
                self.assertEqual(worker_globs, [], 'A targeted worker event globbed all historical hosts')
                self.assertEqual(len(root_entries), scanned, 'A targeted worker event scanned the hosts root')
                print('Host 5000 count proof: folders=%d root_scans=%d root_entries=%d '
                      'max_entries_per_turn=%d max_folders_per_turn=%d idle_extra=%d target_extra=%d '
                      'index_commits=%d commits=%d marker_fsyncs=%d startup_seconds=%.6f' % (
                          len(visited), len(root_scans), len(root_entries), max(entries_per_turn.values()),
                          max(folders_per_turn.values()), sum(inspected.values()) - total, extra,
                          sql_counts['index_commits'], sql_counts['commits'], len(marker_fsyncs), startup_elapsed))
                self.assertFalse(task.done())
        finally:
            self.store.db.set_trace_callback(None)
            running = False
            await beat

    async def test_host_log_delete_stops_after_twenty_four_failures(self):
        job, worker, directory, service = await self._finished_host_without_service()
        self.store.task_update(job['id'], status='done')
        cleaner = service.host_logs
        with patch('coordinator.host_logs.time', return_value=1800000000.0) as clock, \
                patch('coordinator.host_logs.delete', side_effect=PermissionError('permanent refusal')) as failed:
            await cleaner.clean({'worker': worker})
            for _ in range(25):
                clock.return_value += 3600
                cleaner = type(cleaner)(self.store)
                await cleaner.clean({'retry': True})
            self.assertEqual(failed.call_count, 24)
        self.assertEqual(cleaner.retry, {})
        self.assertEqual(self.store.get('host_log_delete_retries'), {})
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE code='log-delete-failed'").fetchone()[0], 1)
        self.assertTrue(directory.is_dir())

    async def test_failed_delete_has_throttled_problem_and_durable_exponential_backoff(self):
        job, worker, directory, service = await self._finished_host_without_service()
        cleaner = service.host_logs
        real_record = problems.record
        attempts = []
        def refused(*args, **kwargs):
            attempts.append(clock.return_value)
            raise PermissionError('Synthetic filesystem refusal')
        with patch('coordinator.host_logs.time', return_value=1000.0, create=True) as clock, \
                patch('coordinator.host_logs.delete', side_effect=refused), \
                patch('coordinator.problems.record', wraps=real_record) as reported:
            await cleaner.clean({'worker': worker})
            self.assertEqual(attempts, [1000.0])
            for index in range(20):
                await cleaner.clean({'worker': worker})
            clock.return_value = 1059.999
            await cleaner.clean({'worker': worker})
            self.assertEqual(attempts, [1000.0], 'Failed deletion retried before its exact due time')
            self.assertEqual(cleaner.retry[directory.name], {'due': 1060.0, 'attempt': 1})
            self.assertEqual(self.store.get('host_log_delete_retries'), cleaner.retry)
            restarted = type(cleaner)(self.store)
            self.assertEqual(restarted.retry, cleaner.retry)
            await restarted.clean({'worker': worker})
            self.assertEqual(attempts, [1000.0], 'A cleaner restart discarded durable retry backoff')
            clock.return_value = 1060.0
            await restarted.clean({'worker': worker})
            self.assertEqual(attempts, [1000.0, 1060.0])
            self.assertEqual(restarted.retry[directory.name], {'due': 1180.0, 'attempt': 2})
            clock.return_value = 1179.999
            await restarted.clean({'worker': worker})
            self.assertEqual(attempts, [1000.0, 1060.0])
            self.assertEqual(reported.call_count, 0)
        self.assertTrue((directory / 'events.jsonl').is_file())
        self.assertTrue((directory / 'writes.jsonl').is_file())
        with patch('coordinator.host_logs.time', return_value=1180.0, create=True):
            await restarted.clean({'worker': worker})
        self.assertFalse((directory / 'events.jsonl').exists(), 'A due retry did not reclaim its log')
        self.assertNotIn(directory.name, restarted.retry)
        self.assertNotIn(directory.name, self.store.get('host_log_delete_retries', {}))

    async def test_finished_worker_can_resume_while_its_old_delete_is_blocked(self):
        worker = self.worker(self.job(), 'torii-host-wait')
        started = asyncio.Event()
        release = asyncio.Event()
        service = self._host_log_service()
        import coordinator.host_logs as host_logs
        real_delete = host_logs.delete
        async def gated_delete(path, *args, **kwargs):
            if Path(path).parent.name.startswith('worker-'):
                started.set()
                await release.wait()
            return await real_delete(path, *args, **kwargs)
        with patch('coordinator.host_logs.delete', side_effect=gated_delete):
            task = asyncio.create_task(service.run())
            self.services.append((service, task))
            try:
                await self.until(lambda: self.row(worker)['pid'] is not None)
                original_run = service.worker_tasks[worker]
                original = self.row(worker)
                (self.root / ('release-' + str(original['pid']))).touch()
                self.assertTrue(await self.observed(started.is_set), 'Finished worker cleanup did not start')
                self.assertFalse(release.is_set())
                self.assertTrue(original_run.done(), 'Worker run waited for its background host deletion')
                await settle(service.workers_once())
                with self.store.db:
                    result = control_api.call(self.store, 'workers.steer',
                                              {'worker': worker, 'prompt': 'resume while delete blocked'})
                self.assertTrue(result.ok, result.text)
                await self.until(lambda: self.row(worker)['host'] != original['host'])
                resumed = await self.finished(worker)
                self.assertNotEqual(resumed.name, original['host'])
                self.assertEqual(self.row(worker)['session'], original['session'])
                self.assertTrue(json.loads(self.row(worker)['result'])['success'])
                request = self.store.db.execute("SELECT state,result FROM service_requests "
                    "WHERE op='workers.steer' ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual((request['state'], json.loads(request['result'])['receipt']), ('done', 'resumed'))
                self.assertFalse(release.is_set(), 'The test gate opened before the resumed run completed')
                self.assertFalse(task.done())
            finally:
                release.set()

    async def test_blocked_startup_delete_does_not_hold_start_workers_or_shutdown(self):
        job, worker, directory, unused = await self._finished_host_without_service()
        self.store.task_update(job['id'], status='done')
        next_worker = self.worker(self.job(), 'torii-host-wait')
        service = self._host_log_service()
        import coordinator.host_logs as host_logs
        real_delete = host_logs.delete
        started = asyncio.Event()
        release = asyncio.Event()
        async def gated_delete(path, *args, **kwargs):
            if Path(path).resolve() == directory.resolve():
                started.set()
                await release.wait()
            return await real_delete(path, *args, **kwargs)
        with patch('coordinator.host_logs.delete', side_effect=gated_delete):
            task = asyncio.create_task(service.run())
            self.services.append((service, task))
            try:
                self.assertTrue(await self.observed(started.is_set), 'Startup host deletion did not start')
                self.assertIsNotNone(service.session, 'Startup awaited host deletion before starting its session')
                self.assertTrue(await self.observed(lambda: self.row(next_worker)['pid'] is not None),
                                'Startup awaited deletion before launching a queued worker')
                await settle(service.workers_once())
                self.assertFalse(release.is_set())
                await settle(self.stop_service(task))
                self.assertTrue(task.done())
                self.assertFalse(release.is_set(), 'Shutdown joined the blocked deletion')
                self.assertTrue(directory.exists())
            finally:
                release.set()

    async def test_nested_readonly_and_mode_zero_directories_are_reclaimed(self):
        directory, = await self.prepare_closed(1)
        readonly = directory / 'go' / 'pkg' / 'mod' / 'example' / 'v1' / 'internal'
        readonly.mkdir(parents=True)
        subfolder = readonly / 'pkg'
        subfolder.mkdir()
        (subfolder / 'file').write_text('readonly nested contents')
        (subfolder / 'file').chmod(0o400)
        subfolder.chmod(0o555)
        readonly.chmod(0o555)
        zero = directory / 'mode-zero'
        zero.mkdir()
        (zero / 'file').write_text('mode zero contents')
        zero.chmod(0o000)
        directory.chmod(0o000)
        try:
            service, task = self.start_service()
            self.assertTrue(await self.observed(lambda: not directory.exists()),
                            'Nested 0555 or mode-000 directories prevented finished-host reclamation')
            quarantine = self.state / 'hosts' / '.deleting'
            self.assertTrue(await self.observed(lambda: not quarantine.exists() or not any(quarantine.iterdir())),
                            'Nested readonly folders remained in the deletion quarantine')
            self.assertFalse(task.done())
        finally:
            for folder in (directory, readonly, subfolder, zero):
                if folder.exists():
                    folder.chmod(0o700)

    async def test_startup_owned_host_recovery_is_cancelled_with_its_child(self):
        job, worker, directory, unused = await self._finished_host_without_service()
        if not (directory / 'job.plist').exists():
            (directory / 'job.plist').write_text('Fixture ownership job')
        booted = directory / 'job-booted-out'
        if booted.exists():
            booted.rename(self.root / 'previous-job-booted-out')
        next_worker = self.worker(self.job(), 'torii-host-wait')
        started = asyncio.Event()
        release = asyncio.Event()
        stopped = asyncio.Event()
        class RecoveryChild:
            returncode = None
            terminated = False
            waited = False
            async def communicate(self):
                started.set()
                await release.wait()
                return b'[]', b''
            def terminate(self):
                self.terminated = True
                self.returncode = 0
                stopped.set()
            def kill(self):
                self.returncode = -signal.SIGKILL
                stopped.set()
            async def wait(self):
                await stopped.wait()
                self.waited = True
                return self.returncode
        child = RecoveryChild()
        real_spawn = asyncio.create_subprocess_exec
        async def spawn(*argv, **kwargs):
            if len(argv) >= 3 and argv[1] == '-c' and 'settle_ownership' in argv[2]:
                return child
            return await real_spawn(*argv, **kwargs)
        with patch('coordinator.host_logs.asyncio.create_subprocess_exec', side_effect=spawn), \
                patch('coordinator.host.recover_ownership',
                      side_effect=AssertionError('Enabled startup used synchronous ownership recovery')):
            service, task = self.start_service()
            try:
                self.assertTrue(await self.observed(started.is_set), 'Startup did not recover its ended owned host')
                self.assertIsNotNone(service.session, 'Startup awaited ownership recovery before starting its session')
                self.assertTrue(await self.observed(lambda: self.row(next_worker)['pid'] is not None),
                                'Ownership recovery held a queued worker launch')
                self.assertFalse(release.is_set())
                await settle(self.stop_service(task))
                self.assertTrue(child.terminated, 'Stopping the service left its ownership recovery child alive')
                self.assertTrue(child.waited, 'Stopping the service did not reap its ownership recovery child')
                self.assertFalse(release.is_set(), 'Shutdown waited for the blocked ownership recovery')
            finally:
                release.set()


if __name__ == '__main__':
    unittest.main()
