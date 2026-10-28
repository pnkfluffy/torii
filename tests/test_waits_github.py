from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import json
import asyncio
import os
from pathlib import Path
import pwd
import re
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp, updates
from tests.test_service import TOPIC
from tests.test_waits_review import WaitReviewSupport


SHA = 'a' * 40
RUNNING = 'b' * 40
PR = 'https://github.com/example/widget/pull/12'


def pull_request(number=12, state='OPEN', checks='PENDING', auto_merge=False):
    return {'number': number, 'url': 'https://github.com/example/widget/pull/%d' % number,
            'state': state, 'title': 'Sample change', 'mergedAt': '2026-10-01T00:00:00Z' if state == 'MERGED' else None,
            'mergeCommit': {'oid': SHA} if state == 'MERGED' else None,
            'autoMergeRequest': {'enabledAt': '2026-10-01T00:00:00Z'} if auto_merge else None,
            'commits': {'nodes': [{'commit': {'oid': RUNNING, 'statusCheckRollup': {
                'state': checks, 'contexts': {'nodes': [{'__typename': 'CheckRun', 'name': 'lint',
                    'status': 'COMPLETED' if checks in ('SUCCESS', 'FAILURE') else 'IN_PROGRESS',
                    'conclusion': 'FAILURE' if checks == 'FAILURE' else 'SUCCESS'}]}}}}]}}


class WaitGithubTests(WaitReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.store.put('waits_enabled', True)
        self.now = 10000
        self.responses = {12: pull_request()}
        self.github = AsyncMock(side_effect=self.github_reply)
        runner = self.store.code_requests
        if runner is None:
            from coordinator import github
            runner = github
        self.github_patch = patch.object(runner, 'run_github', self.github)
        self.github_patch.start()
        self.addCleanup(self.github_patch.stop)

    async def github_reply(self, argv, timeout):
        self.assertEqual(argv[:3], ['gh', 'api', 'graphql'])
        self.assertEqual(timeout, 30)
        query = next(arg[6:] for arg in argv if arg.startswith('query='))
        numbers = [int(number) for number in re.findall(r'pullRequest\(number:\s*(\d+)\)', query)]
        return {'data': {'repository': {'pr%d' % number: deepcopy(self.responses[number]) for number in numbers}}}

    def arm_pr(self, task=None, number=12, until='checks', **params):
        if task is None:
            task = self.job()
        with patch('coordinator.waits.time.time', return_value=self.now):
            wait = self.wait(task, kind='pr', ask='Continue the change',
                             pr='https://github.com/example/widget/pull/%d' % number, until=until, **params)
        return task, wait

    async def poll(self, now=None):
        with patch('coordinator.waits.time.time', return_value=self.now if now is None else now):
            await self.service.waits_once()

    def assert_opened(self, task, wait, outcome, text):
        self.assertEqual((self.store.wait_get(wait['id'])['state'], self.store.wait_get(wait['id'])['outcome']),
                         ('fired', outcome))
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['iteration']), ('open', 1))
        iteration = self.store.task_iterations(task['id'])[-1]
        callback = self.store.db.execute('SELECT * FROM messages WHERE id=?', (iteration['origin'],)).fetchone()
        self.assertEqual((iteration['opened_by'], iteration['wait'], callback['kind']), ('wait', wait['id'], 'callback'))
        self.assertIn('Job %d.1 opened: %s' % (task['number'], text), callback['text'])

    async def test_green_opens_and_wakes_once(self):
        task, wait = self.arm_pr()
        self.responses[12] = pull_request(checks='SUCCESS')
        await self.poll()
        await self.poll()
        self.assert_opened(task, wait, 'green', 'PR #12 checks passed')
        self.assertEqual(len(self.callbacks()), 1)
        self.assertTrue(self.service.wake('feed').is_set())
        self.assertEqual(self.github.await_count, 1)

    async def test_red_opens_and_queues_declared_worker(self):
        task, wait = self.arm_pr(then_provider='codex', then_prompt='Fix CI', then_model='gpt-6.1-sol',
                                 then_effort='max', then_work='research')
        self.responses[12] = pull_request(checks='FAILURE')
        await self.poll()
        self.assert_opened(task, wait, 'red', 'PR #12 CI failed (check lint)')
        workers = [dict(row) for row in self.store.db.execute('SELECT * FROM workers')]
        self.assertEqual([(row['status'], row['iteration'], row['prompt']) for row in workers], [('queued', 1, 'Fix CI')])
        self.assertIn('Worker %d queued.' % workers[0]['id'], self.callbacks()[0]['text'])

    async def test_until_merged_queue_failure_keeps_merge_ask_armed(self):
        task = self.job()
        ask = '#12 merged by the queue: note the merge sha, close job'
        with patch('coordinator.waits.time.time', return_value=self.now):
            wait = self.wait(task, kind='pr', pr=PR, until='merged', ask=ask,
                             then_provider='codex', then_prompt='Record the merge')
        reply = pull_request(checks='FAILURE')
        reply['commits']['nodes'][0]['commit']['statusCheckRollup']['contexts']['nodes'] = [
            {'__typename': 'StatusContext', 'context': 'queue/status', 'state': 'FAILURE'}]
        self.responses[12] = reply
        await self.poll()
        await self.poll(self.now + 60)
        self.assertEqual(self.callbacks(), [])
        current = self.store.wait_get(wait['id'])
        self.assertEqual((current['state'], current['outcome'], current['checks']), ('armed', None, 2))
        self.assertEqual(current['next_check'], self.now + 120)
        self.assertEqual((self.store.task_get(task['id'])['status'], self.store.task_get(task['id'])['iteration']),
                         ('waiting', 0))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 0)

    async def test_until_merged_waits_on_each_failed_check_state(self):
        failures = [(state, []) for state in ('FAILURE', 'ERROR')]
        failures += [('SUCCESS', [{'__typename': 'StatusContext', 'context': 'queue/status', 'state': state}])
                     for state in ('FAILURE', 'ERROR')]
        failures += [('PENDING', [{'__typename': 'CheckRun', 'name': 'ci', 'status': 'COMPLETED',
                                  'conclusion': conclusion}])
                     for conclusion in ('FAILURE', 'TIMED_OUT', 'CANCELLED', 'ACTION_REQUIRED',
                                        'STARTUP_FAILURE', 'STALE')]
        for number, (state, contexts) in enumerate(failures, 20):
            with self.subTest(state=state, contexts=contexts):
                task, wait = self.arm_pr(number=number, until='merged')
                reply = pull_request(number, checks=state)
                reply['commits']['nodes'][0]['commit']['statusCheckRollup']['contexts']['nodes'] = contexts
                self.responses[number] = reply
                await self.poll()
                self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
                self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')

    async def test_until_merged_failure_then_merge_keeps_merge_proof(self):
        task = self.job()
        self.store.task_update(task['id'], finish='live')
        task, wait = self.arm_pr(task, until='merged', next={'kind': 'torii_update'})
        self.responses[12] = pull_request(checks='FAILURE')
        await self.poll()
        self.responses[12] = pull_request(state='MERGED', checks='FAILURE')
        await self.poll(self.now + 60)
        current = self.store.wait_get(wait['id'])
        self.assertEqual((current['state'], current['outcome']), ('fired', 'merged'))
        self.assertEqual(current['detail']['proof']['mergeCommit']['oid'], SHA)
        self.assertEqual(len(self.callbacks()), 1)
        self.assertIn('PR #12 merged. Waiting for the declared follow-up containing %s.' % SHA[:12],
                      self.callbacks()[0]['text'])
        self.assertEqual(self.store.waits_list()[0]['detail']['commit'], SHA)

    async def test_merged_settles_merged_finish_with_round_proof(self):
        task, wait = self.arm_pr(until='merged')
        self.responses[12] = pull_request(state='MERGED')
        await self.poll()
        await self.poll()
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['iteration'], current['pr']), ('done', 0, PR))
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)
        self.assertEqual(self.store.task_iterations(task['id'])[0]['outcome'], 'done')
        self.assertEqual(self.store.wait_get(wait['id'])['outcome'], 'merged')
        self.assertEqual(self.callbacks()[0]['text'], 'Job %d is done: PR #12 merged.' % task['number'])
        self.assertEqual([row['stage'] for row in self.store.db.execute('SELECT stage FROM task_stamps')], ['merged'])
        proof = self.store.wait_get(wait['id'])['detail']['proof']
        self.assertEqual((proof['url'], proof['mergeCommit']['oid']), (PR, SHA))

    async def test_merged_live_finish_stamps_and_arms_declared_followup(self):
        task = self.job()
        self.store.task_update(task['id'], finish='live')
        task, wait = self.arm_pr(task, until='merged', next={'kind': 'torii_update'})
        self.responses[12] = pull_request(state='MERGED')
        await self.poll()
        self.assertEqual((self.store.task_get(task['id'])['status'], self.store.task_get(task['id'])['iteration']), ('waiting', 0))
        armed = self.store.waits_list()
        self.assertEqual([(item['kind'], item['detail']['commit']) for item in armed], [('torii_update', SHA)])
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'fired')
        self.assertEqual([row['stage'] for row in self.store.db.execute('SELECT stage FROM task_stamps')], ['merged'])
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)
        self.assertIn('PR #12 merged. Waiting for the declared follow-up containing %s.' % SHA[:12], self.callbacks()[0]['text'])

    async def test_merged_other_finish_or_live_without_followup_opens(self):
        for finish in ('live', 'answered', 'delivered'):
            with self.subTest(finish=finish):
                task = self.job(finish)
                self.store.task_update(task['id'], finish=finish)
                task, wait = self.arm_pr(task, until='merged')
                self.responses[12] = pull_request(state='MERGED')
                await self.poll()
                self.assert_opened(task, wait, 'merged', 'PR #12 merged')

    async def test_closed_unmerged_opens_without_a_merge_stamp(self):
        task, wait = self.arm_pr(until='merged')
        self.responses[12] = pull_request(state='CLOSED')
        await self.poll()
        self.assert_opened(task, wait, 'closed_unmerged', 'PR #12 closed without merging')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_stamps').fetchone()[0], 0)

    async def test_pending_three_hours_gives_checks_stuck(self):
        task, wait = self.arm_pr(until='merged')
        await self.poll()
        await self.poll(self.now + 3 * 3600 - 1)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        await self.poll(self.now + 3 * 3600 + 300)
        self.assert_opened(task, wait, 'checks_stuck', 'PR #12 checks have been pending for 3 hours')

    async def test_failure_backs_off_fifteen_minutes_without_firing(self):
        task, wait = self.arm_pr()
        self.github.side_effect = RuntimeError('Fixture GitHub failure')
        await self.poll()
        pending = self.store.wait_get(wait['id'])
        self.assertEqual((pending['state'], pending['next_check']), ('armed', self.now + 900))
        await self.poll(self.now + 899)
        self.assertEqual(self.github.await_count, 1)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        self.assertEqual(self.callbacks(), [])

    async def test_backoff_starts_when_the_failed_query_finishes(self):
        task, wait = self.arm_pr()

        async def failed(argv, timeout):
            self.now += 30
            raise RuntimeError('Fixture query timeout')

        self.github.side_effect = failed
        with patch('coordinator.waits.time.time', side_effect=lambda: self.now):
            await self.service.waits_once()
        self.assertEqual(self.store.wait_get(wait['id'])['next_check'], self.now + 900)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')

    async def test_many_prs_in_one_repository_use_one_call_per_due_poll(self):
        waits = []
        for number in range(12, 32):
            self.responses[number] = pull_request(number)
            waits.append(self.arm_pr(number=number)[1])
        await self.poll()
        self.assertEqual(self.github.await_count, 1)
        self.assertEqual({self.store.wait_get(item['id'])['next_check'] for item in waits}, {self.now + 60})
        await self.poll(self.now + 59)
        self.assertEqual(self.github.await_count, 1)
        await self.poll(self.now + 60)
        self.assertEqual(self.github.await_count, 2)
        await self.poll(self.now + 1800)
        self.assertEqual(self.github.await_count, 3)
        self.assertEqual({self.store.wait_get(item['id'])['next_check'] for item in waits}, {self.now + 2100})

    async def test_until_merged_and_auto_merge_do_not_reopen_on_green(self):
        first, merged = self.arm_pr(until='merged')
        second, checks = self.arm_pr(number=13)
        self.responses[12] = pull_request(checks='SUCCESS')
        self.responses[13] = pull_request(13, checks='SUCCESS', auto_merge=True)
        await self.poll()
        self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (merged, checks)], ['armed', 'armed'])
        self.assertEqual(self.callbacks(), [])
        self.responses[13] = pull_request(13, state='MERGED')
        await self.poll(self.now + 60)
        self.assertEqual(self.store.task_get(second['id'])['status'], 'done')
        self.assertEqual(self.store.task_get(first['id'])['status'], 'waiting')

    async def test_partial_graphql_errors_malformed_or_wrong_identity_never_fire(self):
        task, wait = self.arm_pr()
        for response in ({'errors': [{'message': 'Fixture quota'}]},
                         {'data': {'repository': {'pr12': None}}},
                         {'data': {'repository': {'pr12': pull_request(99, state='MERGED')}}}):
            with self.subTest(response=response):
                self.github.side_effect = None
                self.github.return_value = response
                await self.poll()
                self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
                self.assertEqual(self.store.wait_get(wait['id'])['next_check'], self.now + 900)
                self.now += 900
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')

    async def test_switch_off_makes_no_calls_no_update_work_no_signals(self):
        task, pr = self.arm_pr()
        update = self.wait(self.job('Update'), kind='torii_update', commit=SHA)
        signal = self.wait(self.job('Signal'), kind='signal', signal='service.started')
        self.store.put('waits_enabled', False)
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM waits ORDER BY id')]
        with patch('coordinator.updates.git') as git:
            await self.poll()
            await self.service.waits_started()
            await self.service.updater.once()
        result = control_api.call(self.store, 'pending.signal', {'name': 'service.started'})
        self.assertFalse(result.ok)
        self.assertEqual(result.text, "Waits aren't switched on yet.")
        git.assert_not_called()
        self.github.assert_not_awaited()
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM waits ORDER BY id')], before)
        self.assertEqual(self.callbacks(), [])

    async def test_update_waits_fire_at_start_and_after_success_using_running_commit(self):
        self.service.updater.commit = RUNNING
        first = self.job('Startup update')
        self.store.task_update(first['id'], finish='live')
        start = self.wait(first, kind='torii_update', commit=SHA)
        started = self.wait(self.job('Started signal'), kind='signal', signal='service.started')
        landed = self.wait(self.job('Landed signal'), kind='signal', signal='update.landed')
        with patch('coordinator.updates.git', return_value='') as git:
            await self.service.waits_started()
        git.assert_called_with(self.service.updater.root, 'merge-base', '--is-ancestor', SHA, RUNNING)
        self.assertEqual(self.store.task_get(first['id'])['status'], 'done')
        self.assertEqual(self.store.wait_get(start['id'])['outcome'], 'landed')
        self.assertEqual([self.store.wait_get(item['id'])['outcome'] for item in (started, landed)], ['seen', 'seen'])
        self.assertEqual(len(self.store.task_iterations(first['id'])), 1)
        proof = self.store.wait_get(start['id'])['detail']['proof']
        self.assertEqual(proof['running_commit'], RUNNING)
        second = self.job('Later update')
        self.store.task_update(second['id'], finish='live')
        later = self.wait(second, kind='torii_update', commit=SHA)
        self.store.put(updates.KEY, {'id': 'fixture-update', 'phase': 'updated', 'reported': True, 'target': RUNNING})
        with patch('coordinator.updates.git', return_value=''):
            await self.service.updater.once()
            await self.service.updater.once()
        self.assertEqual(self.store.task_get(second['id'])['status'], 'done')
        self.assertEqual(self.store.wait_get(later['id'])['detail']['proof']['update']['id'], 'fixture-update')
        self.assertEqual([row['stage'] for row in self.store.db.execute('SELECT stage FROM task_stamps')], ['live', 'live'])

    async def test_update_not_ancestor_and_failed_update_leave_wait_armed(self):
        self.service.updater.commit = RUNNING
        wait = self.wait(self.job('Absent commit'), kind='torii_update', commit=SHA)
        with patch('coordinator.updates.git', side_effect=RuntimeError('Not an ancestor')):
            await self.service.waits_started()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.store.put(updates.KEY, {'id': 'fixture-failed', 'phase': 'failed', 'reported': True, 'target': RUNNING})
        with patch('coordinator.updates.git') as git:
            await self.service.updater.once()
        git.assert_not_called()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')

    async def test_pending_signal_local_cli_fires_once_and_other_sources_refuse(self):
        from coordinator.__main__ import ctl
        task = self.job('Deploy proof')
        wait = self.wait(task, kind='signal', signal='sample.deploy')
        for source in ('mcp', 'worker', 'telegram'):
            result = control_api.call(self.store, 'pending.signal', {'name': 'sample.deploy'}, source=source)
            self.assertFalse(result.ok)
        with self.assertRaisesRegex(ValueError, 'Unknown tool: pending_signal'):
            mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                'name': 'pending_signal', 'arguments': {'name': 'sample.deploy'}}}, topic=TOPIC)
        self.assertNotIn('pending_signal', {tool['name'] for tool in mcp.tools_list()})
        args = SimpleNamespace(operation='call', op='pending.signal', topic=None, pairs=['name=sample.deploy'])
        output = StringIO()
        with patch('coordinator.__main__.calling_app', return_value='Fixture program'), redirect_stdout(output):
            self.assertEqual(ctl(args, self.store), 0)
        self.assertEqual(json.loads(output.getvalue())['data']['fired'], [wait['id']])
        self.assert_opened(task, wait, 'seen', 'Signal sample.deploy received')
        self.assertTrue(control_api.call(self.store, 'pending.signal', {'name': 'sample.deploy'}).ok)
        self.assertEqual(len(self.callbacks()), 1)

    async def test_failed_wait_transaction_does_not_block_other_prs(self):
        first, bad = self.arm_pr()
        second, good = self.arm_pr(number=13)
        self.responses[12] = pull_request(checks='SUCCESS')
        self.responses[13] = pull_request(13, checks='SUCCESS')
        original = self.store.wait_fire

        def fire(identifier, **params):
            if identifier == bad['id']:
                original(identifier, **params)
                raise RuntimeError('Fixture failure after writing')
            return original(identifier, **params)

        with patch.object(self.store, 'wait_fire', side_effect=fire):
            await self.poll()
        self.assertEqual(self.github.await_count, 1)
        self.assertEqual(self.store.wait_get(bad['id'])['state'], 'armed')
        self.assertEqual(self.store.task_get(first['id'])['status'], 'waiting')
        self.assertEqual(len(self.store.task_iterations(first['id'])), 1)
        self.assert_opened(second, good, 'green', 'PR #13 checks passed')
        self.assertEqual(len(self.callbacks()), 1)
        self.assertEqual(self.store.wait_get(bad['id'])['next_check'], self.now + 900)

    async def test_shared_pr_wait_settles_each_finish_without_an_extra_round(self):
        merged = self.job('Merge finish')
        live = self.job('Live finish')
        self.store.task_update(live['id'], finish='live')
        merged, first = self.arm_pr(merged, until='merged', next={'kind': 'torii_update'}, key='shared-pr')
        live, joined = self.arm_pr(live, until='merged', next={'kind': 'torii_update'}, key='shared-pr')
        self.assertEqual(first['id'], joined['id'])
        self.responses[12] = pull_request(state='MERGED')
        await self.poll()
        self.assertEqual(self.github.await_count, 1)
        self.assertEqual([self.store.task_get(task['id'])['status'] for task in (merged, live)], ['done', 'waiting'])
        self.assertEqual([self.store.task_get(task['id'])['iteration'] for task in (merged, live)], [0, 0])
        self.assertEqual([(item['kind'], len(item['jobs'])) for item in self.store.waits_list()], [('torii_update', 1)])

    async def test_each_wait_has_its_own_due_time_in_a_repository_batch(self):
        first, older = self.arm_pr()
        await self.poll()
        self.now += 30
        second, newer = self.arm_pr(number=13)
        self.responses[13] = pull_request(13)
        await self.poll()
        self.assertEqual(self.github.await_count, 2)
        self.responses[12] = pull_request(checks='SUCCESS')
        self.responses[13] = pull_request(13, checks='SUCCESS')
        await self.poll(self.now + 30)
        self.assertEqual(self.github.await_count, 3)
        self.assertEqual(self.store.task_get(first['id'])['status'], 'open')
        self.assertEqual(self.store.task_get(second['id'])['status'], 'waiting')
        await self.poll(self.now + 60)
        self.assertEqual(self.github.await_count, 4)
        self.assertEqual(self.store.wait_get(newer['id'])['state'], 'fired')

    async def test_one_query_per_repository_and_no_rest_calls(self):
        first, a = self.arm_pr()
        second = self.job('Other repository')
        with patch('coordinator.waits.time.time', return_value=self.now):
            b = self.wait(second, kind='pr', pr='https://github.com/example/other/pull/13')
        other = pull_request(13)
        other['url'] = 'https://github.com/example/other/pull/13'
        self.responses[13] = other
        await self.poll()
        self.assertEqual(self.github.await_count, 2)
        self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (a, b)], ['armed', 'armed'])
        self.assertTrue(all(call.args[0][:3] == ['gh', 'api', 'graphql'] for call in self.github.await_args_list))

    async def test_disabling_while_github_is_running_discards_the_observation(self):
        task, wait = self.arm_pr()
        self.responses[12] = pull_request(state='MERGED')

        async def disabled(argv, timeout):
            result = await self.github_reply(argv, timeout)
            self.store.put('waits_enabled', False)
            return result

        self.github.side_effect = disabled
        await self.poll()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        self.assertEqual(self.callbacks(), [])

    async def test_incomplete_merge_proof_never_settles(self):
        task, wait = self.arm_pr(until='merged')
        for missing in ('title', 'mergedAt', 'mergeCommit'):
            self.responses[12] = pull_request(state='MERGED')
            self.responses[12][missing] = None
            await self.poll()
            self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
            self.assertEqual(self.store.wait_get(wait['id'])['next_check'], self.now + 900)
            self.now += 900
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')

    async def test_legacy_status_context_failure_names_the_check(self):
        task, wait = self.arm_pr()
        reply = pull_request(checks='FAILURE')
        reply['commits']['nodes'][0]['commit']['statusCheckRollup']['contexts']['nodes'] = [
            {'__typename': 'StatusContext', 'context': 'legacy-ci', 'state': 'ERROR'}]
        self.responses[12] = reply
        await self.poll()
        self.assert_opened(task, wait, 'red', 'PR #12 CI failed (check legacy-ci)')

    async def test_update_declared_live_check_opens_and_queues_worker(self):
        task = self.job('Live check')
        self.store.task_update(task['id'], finish='live')
        wait = self.wait(task, kind='torii_update', commit=SHA,
                         then_provider='codex', then_prompt='Check the running release')
        self.service.updater.commit = RUNNING
        with patch('coordinator.updates.git', return_value=''):
            await self.service.waits_started()
        self.assert_opened(task, wait, 'landed', 'Torii update containing %s is running' % SHA[:12])
        worker = self.store.db.execute('SELECT status,iteration FROM workers').fetchone()
        self.assertEqual(tuple(worker), ('queued', 1))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_stamps').fetchone()[0], 0)

    async def test_update_hook_also_fires_after_recovered_success(self):
        self.service.updater.commit = RUNNING
        await self.service.updater.waits_changed(started=True)
        task = self.job('Recovered update')
        self.store.task_update(task['id'], finish='live')
        wait = self.wait(task, kind='torii_update', commit=SHA)
        self.store.put(updates.KEY, {'id': 'fixture-recovery', 'phase': 'restart', 'target': RUNNING})

        def recover(store, *args, **params):
            store.put(updates.KEY, {'id': 'fixture-recovery', 'phase': 'updated', 'target': RUNNING, 'reported': True})
            return True

        with patch('coordinator.updates.watch.process_identity', return_value='fixture-process'), \
                patch('coordinator.updates.watch.watchdog_alive', return_value=False), \
                patch('coordinator.updates.claim_recovery', side_effect=lambda store, record: record), \
                patch('coordinator.updates.watch.stop_installation'), \
                patch('coordinator.updates.recover_tools', return_value={}), \
                patch('coordinator.updates.recover', side_effect=recover), \
                patch('coordinator.updates.git', return_value=''):
            await self.service.updater.once()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'fired')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'done')

    async def test_reattached_signal_fires_after_connect_and_not_after_failed_connect(self):
        wait = self.wait(self.job('Attachment observer'), kind='signal', signal='worker.reattached')
        worker = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',account_alias='fixture',session=? WHERE id=?",
                                  ('00000000-0000-4000-8000-000000000001', worker))
        with patch('coordinator.host.HostClient.recover_state', new=AsyncMock(return_value='running')), \
                patch('coordinator.host.HostClient.connect', new=AsyncMock(side_effect=RuntimeError('Fixture disconnected'))):
            await self.service.workers_once()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        with patch('coordinator.host.HostClient.recover_state', new=AsyncMock(return_value='running')), \
                patch('coordinator.host.HostClient.connect', new=AsyncMock()), \
                patch.object(self.service.workers, 'run', new=AsyncMock()):
            await self.service.workers_once()
            await self.service.worker_tasks[worker]
        self.assertEqual(self.store.wait_get(wait['id'])['outcome'], 'seen')

    async def test_closed_topic_emits_signal_to_other_chats_at_completion(self):
        from coordinator import topics
        wait = self.wait(self.job('Closure observer'), kind='signal', signal='topic.closed')
        closing = self.other_topic()
        request = self.store.service_request('topic.close', {'topic': closing, '_topic': TOPIC})
        row = dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request,)).fetchone())
        progress = {'phase': 'folder', 'choice': 'keep', 'outcome': 'deleted',
                    'snapshot': {'name': 'Sample closed chat', 'cwd': str(self.root), 'jobs': []}}
        with self.store.db:
            self.store.put('topic_lifecycle:' + closing, {'state': 'deleted', 'request': request})
        await topics._close(self.service, row, {'topic': closing}, progress)
        self.assertEqual(self.store.wait_get(wait['id'])['outcome'], 'seen')
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?', (request,)).fetchone()[0], 'done')

    async def test_public_waits_use_the_same_bounded_github_helper(self):
        from coordinator import github
        task, wait = self.arm_pr()
        self.responses[12] = pull_request(checks='SUCCESS')
        with patch.object(self.store, 'code_requests', None), patch.object(github, 'run_github', self.github):
            await self.poll()
        self.assertEqual(self.github.await_count, 1)
        self.assert_opened(task, wait, 'green', 'PR #12 checks passed')

    async def test_merge_of_replaced_pr_opens_without_settling_the_current_pr(self):
        task, wait = self.arm_pr(until='merged')
        self.assertEqual(self.store.task_get(task['id'])['pr'], PR)
        current = 'https://github.com/example/widget/pull/13'
        self.store.task_update(task['id'], pr=current)
        self.responses[12] = pull_request(state='MERGED')
        await self.poll()
        self.assert_opened(task, wait, 'merged', 'PR #12 merged')
        self.assertEqual(self.store.task_get(task['id'])['pr'], current)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_stamps').fetchone()[0], 0)

    async def test_shared_runner_timeout_kills_and_reaps_only_its_child(self):
        from coordinator import github
        self.github_patch.stop()
        process = SimpleNamespace(returncode=None, communicate=AsyncMock(side_effect=asyncio.TimeoutError()),
                                  kill=lambda: None, wait=AsyncMock(return_value=0))
        with patch('coordinator.github.shutil.which', return_value='/sample/bin/gh'), \
                patch('coordinator.github.asyncio.create_subprocess_exec', new=AsyncMock(return_value=process)), \
                patch.object(process, 'kill') as kill:
            with self.assertRaisesRegex(github.GitHubError, '^github_check_failed$'):
                await github.run_github(['gh', 'api', 'graphql'], 30)
        kill.assert_called_once_with()
        process.wait.assert_awaited_once_with()

    async def test_trigger_validation_refuses_without_mutation(self):
        task = self.job()
        invalid = [dict(kind='pr', pr='https://example.invalid/pull/12'),
                   dict(kind='pr', pr=PR, until='anything'), dict(kind='torii_update', commit='main'),
                   dict(kind='signal', signal='bad signal'), dict(kind='signal'),
                   dict(kind='pr', pr=PR, next={'kind': 'owner'}),
                   dict(kind='time', at='2030-01-01T00:00:00Z', signal='sample.deploy')]
        for params in invalid:
            with self.subTest(params=params):
                result = self.call('tasks.wait', task=task['id'], ask='Wait', **params)
                self.assertFalse(result.ok)
                self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM waits').fetchone()[0], 0)

    def review_wait(self, *args, **params):
        with patch('coordinator.waits.time.time', return_value=self.now):
            return self.wait(*args, **params)

    async def review_reply(self, argv, timeout):
        query = next(arg[6:] for arg in argv if arg.startswith('query='))
        numbers = [int(number) for number in re.findall(r'pullRequest\(number:\s*(\d+)\)', query)]
        return {'data': {'repository': {'pr%d' % number: deepcopy(self.responses.get(number)) for number in numbers}},
                'errors': [{'type': 'NOT_FOUND', 'path': ['repository', 'pr9999']}]}

    async def test_review_b1_partial_not_found_fires_good_pr_and_cancels_only_missing_wait(self):
        good, valid = self.arm_pr(until='merged')
        topic = self.other_topic()
        missing = self.job('Missing PR', topic=topic)
        with patch('coordinator.waits.time.time', return_value=self.now):
            bad = self.ok('tasks.wait', context_topic=topic, task=missing['id'], kind='pr',
                          ask='Check the missing PR', pr='https://github.com/example/widget/pull/9999').data
        self.responses = {12: pull_request(state='MERGED')}
        self.github.side_effect = self.review_reply
        await self.poll()
        self.assertEqual(self.store.task_get(good['id'])['status'], 'done')
        self.assertEqual(self.store.wait_get(valid['id'])['state'], 'fired')
        self.assertEqual(self.store.wait_get(bad['id'])['next_check'], self.now + 900)
        await self.poll(self.now + 900)
        await self.poll(self.now + 1800)
        self.assertEqual(self.store.wait_get(bad['id'])['state'], 'cancelled')
        self.assertEqual(self.store.task_get(missing['id'])['status'], 'open')
        self.assertIn('PR #9999 was not found', self.callbacks(topic)[0]['text'])
        self.assertEqual(self.github.await_count, 3)

    async def test_review_b1_malformed_entry_only_backs_off_its_wait(self):
        good, valid = self.arm_pr(until='merged')
        unused, bad = self.arm_pr(number=13)
        self.responses = {12: pull_request(state='MERGED'), 13: {'number': 13}}
        self.github.side_effect = self.review_reply
        await self.poll()
        self.assertEqual(self.store.task_get(good['id'])['status'], 'done')
        self.assertEqual(self.store.wait_get(valid['id'])['state'], 'fired')
        self.assertEqual(self.store.wait_get(bad['id'])['next_check'], self.now + 900)

    async def test_review_b1_missing_repository_ends_with_not_found_callback(self):
        task, wait = self.arm_pr(until='merged')
        self.github.side_effect = None
        self.github.return_value = {'data': {'repository': None},
                                    'errors': [{'type': 'NOT_FOUND', 'path': ['repository']}]}
        for step in (0, 900, 1800):
            await self.poll(self.now + step)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'cancelled')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertIn('PR #12 was not found', self.callbacks()[0]['text'])

    async def test_review_b1_not_found_error_without_alias_ends_with_callback(self):
        unused, wait = self.arm_pr(until='merged')
        self.github.side_effect = None
        self.github.return_value = {'data': {'repository': {}},
                                    'errors': [{'type': 'NOT_FOUND', 'path': ['repository', 'pr12']}]}
        for step in (0, 900, 1800):
            await self.poll(self.now + step)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'cancelled')
        self.assertIn('PR #12 was not found', self.callbacks()[0]['text'])

    async def test_review_b1_rate_limit_never_becomes_not_found(self):
        task, wait = self.arm_pr(until='merged')
        self.github.side_effect = None
        self.github.return_value = {'data': {'repository': None},
                                    'errors': [{'type': 'RATE_LIMITED', 'path': ['repository']}]}
        for step in (0, 900, 1800, 2700):
            await self.poll(self.now + step)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.wait_get(wait['id'])['next_check'], self.now + 3600)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        self.assertEqual(self.callbacks(), [])

    async def test_review_b1_nested_field_error_never_becomes_not_found(self):
        task, wait = self.arm_pr(until='merged')
        self.github.side_effect = None
        self.github.return_value = {'data': {'repository': {'pr12': None}}, 'errors': [
            {'type': 'INTERNAL', 'path': ['repository', 'pr12', 'commits', 'nodes']}]}
        for step in (0, 900, 1800):
            await self.poll(self.now + step)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        self.assertEqual(self.callbacks(), [])

    async def test_review_b1_renamed_repository_returns_callback_without_accepting_merge(self):
        task, wait = self.arm_pr(until='merged')
        self.responses[12] = pull_request(state='MERGED')
        self.responses[12]['url'] = 'https://github.com/example/renamed/pull/12'
        await self.poll()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'cancelled')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertIn('PR #12 moved to https://github.com/example/renamed/pull/12', self.callbacks()[0]['text'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_stamps').fetchone()[0], 0)

    async def test_review_b1_nonzero_graphql_with_partial_data_keeps_valid_entries(self):
        from coordinator import github
        self.github_patch.stop()
        good, valid = self.arm_pr(until='merged')
        unused, bad = self.arm_pr(number=9999)
        response = {'data': {'repository': {'pr12': pull_request(state='MERGED'), 'pr9999': None}},
                    'errors': [{'type': 'NOT_FOUND', 'path': ['repository', 'pr9999']}]}
        process = SimpleNamespace(returncode=1, communicate=AsyncMock(return_value=(json.dumps(response).encode(), b'')))
        with patch('coordinator.github.shutil.which', return_value='/sample/bin/gh'), \
                patch('coordinator.github.asyncio.create_subprocess_exec', new=AsyncMock(return_value=process)):
            await self.poll()
        self.assertEqual(self.store.task_get(good['id'])['status'], 'done')
        self.assertEqual(self.store.wait_get(valid['id'])['state'], 'fired')
        self.assertEqual(self.store.wait_get(bad['id'])['next_check'], self.now + 900)

    async def test_review_b2_default_pr_keys_keep_each_chats_ask(self):
        first = self.job('Implement')
        topic = self.other_topic()
        second = self.job('Review', topic=topic)
        a = self.review_wait(first, kind='pr', ask='Merge then announce', pr=PR, until='checks')
        with patch('coordinator.waits.time.time', return_value=self.now):
            b = self.ok('tasks.wait', context_topic=topic, task=second['id'], kind='pr',
                        ask='Re-run the review', pr=PR, until='checks').data
        self.assertNotEqual(a['id'], b['id'])
        self.responses[12] = pull_request(checks='FAILURE')
        await self.poll()
        self.assertIn('Merge then announce.', self.callbacks(TOPIC)[0]['text'])
        self.assertIn('Re-run the review.', self.callbacks(topic)[0]['text'])
        self.assertEqual(self.github.await_count, 1)

    async def test_review_b2_merged_and_live_finish_can_arm_the_same_pr(self):
        merged, first = self.arm_pr(until='merged')
        live = self.job('Deploy')
        self.store.task_update(live['id'], finish='live')
        second = self.review_wait(live, kind='pr', ask='Close when live', pr=PR, until='merged', next={'kind': 'torii_update'})
        self.assertNotEqual(first['id'], second['id'])
        self.responses[12] = pull_request(state='MERGED')
        await self.poll()
        self.assertEqual(self.store.task_get(merged['id'])['status'], 'done')
        self.assertEqual(self.store.task_get(live['id'])['status'], 'waiting')
        self.assertEqual([(item['kind'], item['ask']) for item in self.store.waits_list()], [('torii_update', 'Close when live')])
        self.assertEqual(self.github.await_count, 1)

    async def test_review_b2_update_and_signal_keys_include_waiter_and_ask(self):
        for kind, params in (('torii_update', {'commit': SHA}), ('signal', {'signal': 'sample.deploy'})):
            with self.subTest(kind=kind):
                first, second = self.job('First'), self.job('Second')
                a = self.review_wait(first, kind=kind, ask='First ask', **params)
                b = self.review_wait(second, kind=kind, ask='Second ask', **params)
                c = self.review_wait(first, kind=kind, ask='Another ask', **params)
                self.assertEqual(len({a['id'], b['id'], c['id']}), 3)
                self.assertEqual(self.review_wait(first, kind=kind, ask='First ask', **params)['id'], a['id'])

    async def test_review_b3_pending_clock_starts_on_observation_and_resets_for_new_head(self):
        task, wait = self.arm_pr(until='merged')
        self.responses[12] = pull_request(checks='SUCCESS')
        await self.poll()
        self.responses[12] = pull_request(checks='PENDING')
        await self.poll(self.now + 4 * 3600)
        item = self.store.wait_get(wait['id'])
        self.assertEqual(item['state'], 'armed')
        self.assertEqual((item['detail']['head_sha'], item['detail']['pending_since']), (RUNNING, self.now + 4 * 3600))
        self.responses[12]['commits']['nodes'][0]['commit']['oid'] = SHA
        await self.poll(self.now + 7 * 3600)
        item = self.store.wait_get(wait['id'])
        self.assertEqual(item['state'], 'armed')
        self.assertEqual((item['detail']['head_sha'], item['detail']['pending_since']), (SHA, self.now + 7 * 3600))
        await self.poll(self.now + 10 * 3600)
        self.assert_opened(task, wait, 'checks_stuck', 'PR #12 checks have been pending for 3 hours')

    async def test_review_b3_no_checks_never_reports_checks_stuck(self):
        unused, wait = self.arm_pr(until='merged')
        self.responses[12]['commits']['nodes'][0]['commit']['statusCheckRollup'] = None
        await self.poll()
        await self.poll(self.now + 4 * 3600)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.callbacks(), [])

    async def test_review_note1_shared_github_uses_account_home_fallback_and_respects_override(self):
        from coordinator import github
        self.github_patch.stop()
        for configured in (None, '/sample/gh-config'):
            with self.subTest(configured=configured):
                env = {'HOME': '/sample/service-home'}
                if configured:
                    env['GH_CONFIG_DIR'] = configured
                process = SimpleNamespace(returncode=0, communicate=AsyncMock(return_value=(b'{}', b'')))
                with patch.dict(os.environ, env, clear=True), \
                        patch.object(pwd, 'getpwuid', return_value=SimpleNamespace(pw_dir='/sample/account-home')), \
                        patch('coordinator.github.shutil.which', return_value='/sample/bin/gh'), \
                        patch('coordinator.github.asyncio.create_subprocess_exec', new=AsyncMock(return_value=process)) as spawn:
                    await github.run_github(['gh', 'api', 'graphql'], 30)
                actual = spawn.await_args.kwargs.get('env', {})
                self.assertEqual(actual.get('GH_CONFIG_DIR'), configured or '/sample/account-home/.config/gh')
                self.assertEqual(actual.get('HOME'), '/sample/service-home')

    async def test_review_note2_update_hook_failure_records_problem_and_keeps_essential_loop_alive(self):
        self.service.updater.commit = RUNNING
        with patch.object(self.store, 'waits_list', side_effect=RuntimeError('Fixture wait lookup failed')):
            await self.service.waits_started()
            self.store.put(updates.KEY, {'id': 'fixture-hook', 'phase': 'updated', 'reported': True})
            await self.service.updater.once()
        self.assertGreaterEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE area='waits' AND code='update-hook-failed'").fetchone()[0], 2)

    async def test_review_note3_new_update_wait_checks_running_commit_once_when_armed(self):
        self.service.updater.commit = RUNNING
        await self.service.waits_started()
        landed = self.review_wait(self.job('Signal'), kind='signal', signal='update.landed')
        task = self.job('Already running')
        self.store.task_update(task['id'], finish='live')
        wait = self.review_wait(task, kind='torii_update', commit=SHA)
        with patch('coordinator.updates.git', return_value='') as git:
            await self.service.controls_once()
            await self.service.controls_once()
            await self.service.updater.once()
        git.assert_called_once_with(self.service.updater.root, 'merge-base', '--is-ancestor', SHA, RUNNING)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'done')
        self.assertEqual(self.store.wait_get(wait['id'])['outcome'], 'landed')
        self.assertEqual(self.store.wait_get(landed['id'])['state'], 'armed')

    async def test_review_text_callback_separates_ask_and_next_step(self):
        self.arm_pr()
        self.responses[12] = pull_request(checks='FAILURE')
        await self.poll()
        self.assertIn('CI failed (check lint). Continue the change. Decide the next step.', self.callbacks()[0]['text'])

    async def test_review_text_followup_names_commit(self):
        task = self.job('Deploy')
        self.store.task_update(task['id'], finish='live')
        self.arm_pr(task, until='merged', next={'kind': 'torii_update', 'commit': RUNNING})
        self.responses[12] = pull_request(state='MERGED')
        await self.poll()
        self.assertIn('Waiting for the declared follow-up containing %s.' % RUNNING[:12], self.callbacks()[0]['text'])

    async def test_review_text_signal_singular(self):
        self.review_wait(self.job(), kind='signal', signal='sample.deploy')
        result = self.ok('pending.signal', name='sample.deploy')
        self.assertEqual(result.text, 'Signal sample.deploy received. 1 wait fired.')

    async def test_review_docs_signal_describes_source_boundary(self):
        text = (Path(__file__).resolve().parents[1] / 'docs/control-api.md').read_text()
        self.assertIn('local CLI only, refused over MCP and from worker sources', ' '.join(text.split()))
        self.assertNotIn('workers cannot call it', text)


if __name__ == '__main__':
    unittest.main()
