"""Bounded identity checks for the last surviving process-group members."""

import asyncio
from collections import Counter
from contextlib import ExitStack
import errno
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import process_ownership as ownership


class DeleteDirectoryTests(unittest.TestCase):
    def test_directory_repairs_permissions_without_rewriting_a_writable_directory(self):
        from coordinator import worker_storage
        self.assertTrue(callable(getattr(worker_storage, '_directory', None)),
                        'shared no-follow directory permission handling is missing')
        with tempfile.TemporaryDirectory(prefix='directory-permissions-') as temporary:
            root = Path(temporary)
            parent = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            real_fchmod = os.fchmod
            try:
                for mode in (0o700, 0o500, 0):
                    with self.subTest(mode=oct(mode)):
                        directory = root / ('directory-' + str(mode))
                        directory.mkdir(mode=0o700)
                        directory.chmod(mode)
                        calls = []

                        def counted_fchmod(descriptor, permissions):
                            calls.append(permissions)
                            return real_fchmod(descriptor, permissions)

                        with patch.object(worker_storage.os, 'fchmod', counted_fchmod):
                            descriptor = worker_storage._directory(directory.name, parent)
                            try:
                                self.assertEqual(stat.S_IMODE(os.fstat(descriptor).st_mode), 0o700)
                            finally:
                                os.close(descriptor)
                        if mode == 0o700:
                            self.assertEqual(calls, [], 'A writable directory was needlessly fchmoded')
            finally:
                os.close(parent)


class HostDeleteParentTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_parent_swap_never_deletes_outside_journal(self):
        from coordinator import worker_storage
        self.assertTrue(callable(getattr(worker_storage, 'delete', None)),
                        'shared anchored host deletion is missing')
        with tempfile.TemporaryDirectory(prefix='parent-swap-repro-') as temporary:
            root = Path(temporary)
            hosts = root / 'hosts'
            worker = hosts / 'worker-1'
            worker.mkdir(parents=True)
            saved = hosts / 'worker-1-original'
            outside = root / 'outside'
            outside.mkdir()
            original = worker / 'events.jsonl'
            original.write_text('owned original journal')
            protected = outside / 'events.jsonl'
            protected.write_text('outside journal must stay')
            real_detach = worker_storage._detach
            injected = []

            def swapped(path, *args, **kwargs):
                if path == original and not injected:
                    worker.rename(saved)
                    worker.symlink_to(outside, target_is_directory=True)
                    injected.append(True)
                return real_detach(path, *args, **kwargs)

            counts = Counter()
            opened = []
            real_open = os.open

            def tracked_open(*args, **kwargs):
                descriptor = real_open(*args, **kwargs)
                opened.append(descriptor)
                return descriptor

            with patch.object(worker_storage, '_detach', swapped), \
                    patch.object(worker_storage.os, 'open', tracked_open):
                removed = await worker_storage.delete(original, hosts / '.deleting', counts)
            for descriptor in opened:
                with self.assertRaises(OSError) as closed:
                    os.fstat(descriptor)
                self.assertEqual(closed.exception.errno, errno.EBADF, 'Worker delete leaked a directory descriptor')
            print('Worker parent swap evidence: ' + json.dumps({
                'injected': bool(injected), 'removed': removed, 'outside_journal_exists': protected.exists(),
                'owned_journal_exists': (saved / 'events.jsonl').exists(), 'counts': dict(counts),
                'closed_descriptors': len(opened)}, sort_keys=True))
            self.assertEqual(injected, [True])
            self.assertTrue(protected.is_file(), 'Swapping a worker parent to a symlink deleted the outside journal')
            self.assertEqual(protected.read_text(), 'outside journal must stay')
            self.assertFalse((saved / 'events.jsonl').exists(), 'The held worker directory was not cleaned')

    async def test_quarantine_parent_swap_never_moves_a_journal_outside(self):
        from coordinator import worker_storage
        self.assertTrue(callable(getattr(worker_storage, 'delete', None)),
                        'shared anchored host deletion is missing')
        with tempfile.TemporaryDirectory(prefix='quarantine-swap-repro-') as temporary:
            root = Path(temporary)
            hosts = root / 'hosts'
            worker = hosts / 'worker-1'
            worker.mkdir(parents=True)
            original = worker / 'events.jsonl'
            original.write_text('owned original journal')
            quarantine = hosts / '.deleting'
            quarantine.mkdir()
            saved = hosts / '.deleting-original'
            outside = root / 'outside'
            outside.mkdir()
            (outside / 'sentinel').write_text('outside sentinel must stay')
            real_detach = worker_storage._detach
            injected = []

            def swapped(path, *args, **kwargs):
                if path == original and not injected:
                    quarantine.rename(saved)
                    quarantine.symlink_to(outside, target_is_directory=True)
                    injected.append(True)
                return real_detach(path, *args, **kwargs)

            error = None
            opened = []
            real_open = os.open

            def tracked_open(*args, **kwargs):
                descriptor = real_open(*args, **kwargs)
                opened.append(descriptor)
                return descriptor

            with patch.object(worker_storage, '_detach', swapped), \
                    patch.object(worker_storage.os, 'open', tracked_open):
                try:
                    await worker_storage.delete(original, quarantine, Counter())
                except OSError as failure:
                    error = type(failure).__name__
            for descriptor in opened:
                with self.assertRaises(OSError) as closed:
                    os.fstat(descriptor)
                self.assertEqual(closed.exception.errno, errno.EBADF, 'Quarantine delete leaked a directory descriptor')
            entries = sorted(path.name for path in outside.iterdir())
            print('Quarantine parent swap evidence: ' + json.dumps({
                'injected': bool(injected), 'error': error, 'outside_entries': entries,
                'owned_journal_exists': original.exists(), 'closed_descriptors': len(opened)}, sort_keys=True))
            self.assertEqual(injected, [True])
            self.assertEqual(entries, ['sentinel'], 'Swapping the quarantine parent to a symlink moved a journal outside')
            self.assertEqual((outside / 'sentinel').read_text(), 'outside sentinel must stay')
            self.assertFalse(any(saved.iterdir()), 'The held quarantine directory was not cleaned')


class ProcEntries:
    def __init__(self, names):
        self.entries = [SimpleNamespace(name=str(name)) for name in names]

    def __enter__(self):
        return iter(self.entries)

    def __exit__(self, *args):
        return False


class DarwinGroup:
    def __init__(self, snapshots):
        self.snapshots = iter(snapshots)
        self.current = {}
        self.flavors = []
        self.unique_reads = {}

    def proc_listpids(self, flavor, pgid, buf, size):
        self.flavors.append((flavor, pgid))
        self.current = next(self.snapshots)
        self.unique_reads.clear()
        for index, pid in enumerate(self.current):
            if index >= len(buf):
                return size
            buf[index] = pid
        return len(self.current) * ownership.C.sizeof(ownership.C.c_int)

    def proc_pidinfo(self, pid, flavor, argument, pointer, size):
        item = self.current[pid]
        if item.get('missing') or (item.get('require_zombie_arg') and argument != 1):
            ownership.C.set_errno(errno.ESRCH)
            return 0
        value = pointer._obj
        if flavor == 13:
            value.pid = pid
            value.pgid = item.get('pgid', 30)
            value.status = item.get('status', 2)
        else:
            count = self.unique_reads.get(pid, 0)
            self.unique_reads[pid] = count + 1
            value.unique = item.get('unique', 1000 + pid) + (count if item.get('race') else 0)
            value.version = 7
        return size


class GroupMembersTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertTrue(callable(getattr(ownership, 'group_members', None)),
                        'bounded process-group termination proof is missing')

    def linux(self, entries, metadata, ready=False):
        stack = ExitStack()
        stack.enter_context(patch.object(ownership.sys, 'platform', 'linux'))
        stack.enter_context(patch.object(ownership.os, 'scandir',
                                        side_effect=lambda directory: ProcEntries(entries)))
        stack.enter_context(patch.object(ownership, '_proc_stat', side_effect=metadata))
        stack.enter_context(patch.object(ownership.os, 'pidfd_open', return_value=900, create=True))
        stack.enter_context(patch.object(ownership.os, 'close'))
        stack.enter_context(patch.object(ownership.polling, 'select',
                                        return_value=([900] if ready else [], [], [])))
        self.addCleanup(stack.close)
        return stack

    async def test_live_members_exclude_other_groups(self):
        self.linux([41, 42, 'self'], lambda pid: ('S', 30 if pid == 41 else 31, pid + 100))
        self.assertEqual(await ownership.group_members(30), (((41, ('linux', 141)),), False))

    async def test_zombie_leader_with_running_threads_still_needs_exit(self):
        self.linux([41], lambda pid: ('Z', 30, 141), ready=False)
        self.assertEqual(await ownership.group_members(30), (((41, ('linux', 141)),), False))

    async def test_ready_pidfd_proves_stable_zombie_group_terminated(self):
        self.linux([41, 42], lambda pid: ('Z', 30, pid + 100), ready=True)
        self.assertEqual(await ownership.group_members(30), ((), True))

    async def test_reused_zombie_pid_does_not_prove_group_terminated(self):
        self.linux([41], iter([('Z', 30, 141), ('Z', 30, 141),
                              ('Z', 30, 142), ('Z', 30, 142)]), ready=True)
        with self.assertRaises(OSError) as caught:
            await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.EAGAIN)

    async def test_pid_reused_while_opening_pidfd_is_unknown(self):
        self.linux([41], iter([('Z', 30, 141), ('Z', 30, 142)]), ready=True)
        with self.assertRaisesRegex(OSError, 'identity changed'):
            await ownership.group_members(30)

    async def test_new_live_member_is_returned_after_roster_changes(self):
        self.linux([41, 42], iter([('Z', 30, 141), ('Z', 30, 141), ('S', 31, 142),
                                  ('Z', 30, 141), ('Z', 30, 141), ('S', 30, 142)]), ready=True)
        self.assertEqual(await ownership.group_members(30), (((42, ('linux', 142)),), False))

    async def test_empty_enumeration_does_not_override_group_liveness(self):
        self.linux([], lambda pid: ('S', 30, 141))
        with self.assertRaises(OSError) as caught:
            await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.EAGAIN)

    async def test_live_to_zombie_transition_rescans_for_new_child(self):
        self.linux([41, 42], iter([('S', 30, 141), ('S', 31, 142),
                                  ('Z', 30, 141), ('Z', 30, 141), ('S', 31, 142),
                                  ('Z', 30, 141), ('Z', 30, 141), ('S', 30, 142)]), ready=True)
        self.assertEqual(await ownership.group_members(30), (((42, ('linux', 142)),), False))

    async def test_live_to_stable_terminal_rosters_proves_termination(self):
        self.linux([41], iter([('S', 30, 141), ('Z', 30, 141), ('Z', 30, 141),
                              ('Z', 30, 141), ('Z', 30, 141)]), ready=True)
        self.assertEqual(await ownership.group_members(30), ((), True))

    async def test_vanishing_unread_proc_entry_does_not_hide_the_group(self):
        def metadata(pid):
            if pid == 42:
                raise FileNotFoundError(errno.ENOENT, 'Unrelated process exited')
            return 'Z', 30, 141

        self.linux([41, 42], metadata, ready=True)
        self.assertEqual(await ownership.group_members(30), ((), True))
        with patch.object(ownership.polling, 'select', return_value=([], [], [])):
            self.assertEqual(await ownership.group_members(30), (((41, ('linux', 141)),), False))

    async def test_vanishing_zombie_during_identity_check_cannot_prove_terminal_group(self):
        self.linux([41], iter([('Z', 30, 141), FileNotFoundError()]), ready=True)
        with self.assertRaises(OSError) as caught:
            await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.EAGAIN)

    async def test_only_vanishing_proc_entries_do_not_prove_terminal_group(self):
        self.linux([41], FileNotFoundError(errno.ENOENT, 'Process exited'))
        with self.assertRaises(OSError) as caught:
            await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.EAGAIN)

    async def test_pidfd_unavailable_keeps_zombie_termination_unknown(self):
        self.linux([41], lambda pid: ('Z', 30, 141))
        with patch.object(ownership.os, 'pidfd_open', side_effect=OSError(errno.ENOSYS, 'unsupported')):
            with self.assertRaises(OSError) as caught:
                await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.ENOSYS)

    async def test_entry_and_group_caps_refuse_incomplete_proof(self):
        self.linux([41, 42, 43], lambda pid: ('S', 30, pid + 100))
        with patch.object(ownership, 'PROCESS_SCAN_LIMIT', 2):
            with self.assertRaisesRegex(OSError, 'Process enumeration exceeded'):
                await ownership.group_members(30)
        with patch.object(ownership, 'GROUP_MEMBER_LIMIT', 1):
            with self.assertRaisesRegex(OSError, 'Process group enumeration exceeded'):
                await ownership.group_members(30)

    async def test_scan_yields_to_another_coroutine(self):
        self.linux(range(41, 241), lambda pid: ('S', 30, pid + 100))
        turns = []

        async def concurrent():
            for _ in range(5):
                await asyncio.sleep(0)
                turns.append(True)

        scan = asyncio.create_task(ownership.group_members(30))
        other = asyncio.create_task(concurrent())
        members, ended = await scan
        self.assertEqual(len(members), 200)
        self.assertFalse(ended)
        self.assertEqual(turns, [True] * 5)
        await other

    async def test_proc_stat_handles_parentheses_and_rejects_bad_identity(self):
        values = 'S 1 30 30 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 141'
        with patch.object(Path, 'read_text', return_value='41 (a ) tricky ( name) ' + values):
            self.assertEqual(ownership._proc_stat(41), ('S', 30, 141))
        with patch.object(ownership.sys, 'platform', 'linux'), \
                patch.object(ownership.os, 'scandir', return_value=ProcEntries([41])), \
                patch.object(Path, 'read_text', return_value='42 (wrong) ' + values):
            with self.assertRaisesRegex(OSError, 'Invalid process group metadata'):
                await ownership.group_members(30)

    async def test_permission_failure_is_not_empty_group_proof(self):
        self.linux([41], PermissionError(errno.EACCES, 'denied'))
        with self.assertRaises(PermissionError):
            await ownership.group_members(30)

    async def test_darwin_group_scan_uses_group_filter_and_stable_identity(self):
        rows = {41: {'status': 5}, 42: {'status': 2}}
        fake = DarwinGroup([rows, rows])
        with patch.object(ownership.sys, 'platform', 'darwin'), \
                patch.object(ownership.backend, 'load', return_value=fake):
            self.assertEqual(await ownership.group_members(30),
                             (((42, ('darwin', 1042, 7)),), False))
        self.assertEqual(fake.flavors, [(2, 30), (2, 30)])

    async def test_darwin_terminal_roster_proves_termination_but_identity_race_does_not(self):
        rows = {41: {'status': 5}}
        with patch.object(ownership.sys, 'platform', 'darwin'), \
                patch.object(ownership.backend, 'load', return_value=DarwinGroup([rows, rows])):
            self.assertEqual(await ownership.group_members(30), ((), True))
        with patch.object(ownership.sys, 'platform', 'darwin'), \
                patch.object(ownership.backend, 'load',
                             return_value=DarwinGroup([{41: {'status': 5, 'race': True}}])):
            with self.assertRaisesRegex(OSError, 'identity changed'):
                await ownership.group_members(30)

    async def test_darwin_already_zombie_records_still_prove_termination(self):
        rows = {41: {'status': 5, 'require_zombie_arg': True}}
        with patch.object(ownership.sys, 'platform', 'darwin'), \
                patch.object(ownership.backend, 'load', return_value=DarwinGroup([rows, rows])):
            try:
                result = await ownership.group_members(30)
            except OSError as error:
                self.fail('Darwin group lookup must include existing zombie identities: ' + str(error))
            self.assertEqual(result, ((), True))

    async def test_darwin_member_cap_refuses_truncated_roster(self):
        fake = DarwinGroup([{41: {}, 42: {}}])
        with patch.object(ownership.sys, 'platform', 'darwin'), \
                patch.object(ownership, 'GROUP_MEMBER_LIMIT', 1), \
                patch.object(ownership.backend, 'load', return_value=fake):
            with self.assertRaisesRegex(OSError, 'exceeded its bound'):
                await ownership.group_members(30)

    async def test_vanishing_darwin_member_cannot_prove_terminal_group(self):
        rows = {41: {'status': 5}, 42: {'missing': True}}
        with patch.object(ownership.sys, 'platform', 'darwin'), \
                patch.object(ownership.backend, 'load', return_value=DarwinGroup([rows])):
            with self.assertRaises(OSError) as caught:
                await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.EAGAIN)

    async def test_invalid_and_unsupported_groups_are_unknown(self):
        for pgid in (0, 1, -1, True, '30'):
            with self.subTest(pgid=pgid), self.assertRaisesRegex(OSError, 'Invalid'):
                await ownership.group_members(pgid)
        with patch.object(ownership.sys, 'platform', 'unsupported'):
            with self.assertRaises(OSError) as caught:
                await ownership.group_members(30)
        self.assertEqual(caught.exception.errno, errno.ENOSYS)

    async def test_real_process_group_is_reported_while_its_process_runs(self):
        if sys.platform != 'darwin' and not sys.platform.startswith('linux'):
            with self.assertRaises(OSError):
                await ownership.group_members(30)
            return
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            members, ended = await ownership.group_members(process.pid)
            self.assertFalse(ended)
            self.assertEqual([pid for pid, identity in members], [process.pid])
            self.assertEqual(members[0][1][0], sys.platform)
        finally:
            process.terminate()
            process.wait(timeout=5)
