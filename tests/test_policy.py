import os
from pathlib import Path
import pwd
import tempfile
from types import SimpleNamespace
import unittest
import runpy
from unittest.mock import patch

with patch('coordinator.host_os.SYSTEM', 'darwin'):
    COORDINATOR_POLICY = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'coordinator/policy.py'),
                                         run_name='coordinator._capture')['COORDINATOR_POLICY']

ROOT = Path(__file__).resolve().parents[1]


class InstallDefaultsTests(unittest.TestCase):
    def open_store(self, directory):
        from coordinator.store import Store
        store = Store(directory)
        self.addCleanup(store.close)
        return store

    def service(self, store, model=None):
        from coordinator.service import Service
        return Service(store, None, SimpleNamespace(binaries={}), store.directory,
                       **({'coordinator_model': model} if model is not None else {}))

    def test_fresh_install_uses_neutral_policy_model_and_effort_after_startup_and_reopen(self):
        from coordinator.parent_state import effective_agent
        from coordinator.policy import usage_policy
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'state'
            store = self.open_store(directory)
            policy = usage_policy(directory)
            for current in (store, self.open_store(directory)):
                self.assertEqual(self.service(current).coordinator_model, 'sonnet')
                self.assertEqual(effective_agent(current, provider='claude')['model'], 'sonnet')
                for provider in ('claude', 'codex'):
                    self.assertEqual(effective_agent(current, provider=provider)['effort'], 'medium')
                self.assertEqual(usage_policy(directory), policy)

    def test_fresh_policy_has_no_task_to_provider_routing(self):
        from coordinator.policy import usage_policy
        with tempfile.TemporaryDirectory() as temp:
            store = self.open_store(Path(temp))
            policy = usage_policy(store.directory)
            self.assertIn('Use connected providers', policy)
            self.assertIn('policy.set', policy)
            self.assertNotIn('for frontend', policy)
            self.assertNotIn('for development', policy)

    def test_fresh_parent_effort_is_medium_for_each_provider(self):
        from coordinator.parent_state import effective_agent
        with tempfile.TemporaryDirectory() as temp:
            store = self.open_store(Path(temp))
            for provider in ('claude', 'codex'):
                with self.subTest(provider=provider):
                    self.assertEqual(effective_agent(store, provider=provider)['effort'], 'medium')

class CoordinatorPolicyTests(unittest.TestCase):
    def test_review_followups_preserve_explicit_coordinator_rules(self):
        for rule in ('Each message starts with [topic=ID name="NAME" message=N kind=KIND]',
                     'Delegate anything beyond a few commands, with folder, instructions, expected outcome',
                     "worker_result messages arrive in the job's channel. Verify them.",
                     "If a worker result is only an interim status while the worker's submitted work is still running, steer the worker to continue.",
                     'resume waiting work without asking the owner'):
            with self.subTest(rule=rule):
                self.assertIn(rule, COORDINATOR_POLICY)
        self.assertIn('Never read the vault, worker environments, or worker transcripts', COORDINATOR_POLICY)
        self.assertNotIn('Only Claude workers receive them', COORDINATOR_POLICY)

    def test_policy_keeps_task_rules_and_account_tools_explain_owner_controls(self):
        from coordinator.mcp import tools_list
        self.assertNotIn('account.redeem', COORDINATOR_POLICY)
        descriptions = {tool['name']: tool['description'] for tool in tools_list()}
        self.assertIn('only when the owner picks it', descriptions['account_use'])
        self.assertIn('originating topic', descriptions['account_use'])
        self.assertIn('only when the owner asks', descriptions['accounts_claude_auto'])
        for rule in ('Questions and brainstorming need no job', 'Create a job when work begins with tasks.create',
                     'One worktree per job',
                     'telegram.send is the only owner channel',
                     'Markdown, one image or file per message',
                     'Show account emails, not keys.',
                     'Spawn only connected providers',
                     'Use /goal when the owner asks for a goal; set it with workers.goal'):
            self.assertIn(rule, COORDINATOR_POLICY)

    def test_job_notes_use_one_line_appends_instead_of_rewrites(self):
        self.assertIn('Keep job status and stage current; add notes with note=, one line per new '
                      'job/PR number, commit, run ID, version or proof path.', COORDINATOR_POLICY)
        self.assertNotIn('Keep job status, notes and stage current.', COORDINATOR_POLICY)
        self.assertNotIn('Job/PR numbers, commits, run IDs, versions, proof paths stay in job notes.',
                         COORDINATOR_POLICY)

    def test_reply_first_replaces_the_old_question_policy(self):
        self.assertNotIn('Answer questions when asked', COORDINATOR_POLICY)

    def test_reply_first_uses_the_exact_owner_message_reminder(self):
        from coordinator.policy import REPLY_FIRST
        self.assertEqual(REPLY_FIRST,
                         "Owner message: if it starts a worker or a long task, confirm that in one line first.\n")

    def test_owner_shipping_rules_and_enforced_account_reminders_are_not_built_in(self):
        for removed in (
                'Ship each finished piece on its own.',
                'Split work into independent jobs that run in parallel, each with its own branch and PR.',
                "The job's coordinator reviews its PR (one fresh review only if risky) and merges it as soon as CI passes on the current integration branch.",
                'Never hold a change for unrelated work or bundle unrelated changes.',
                'Risky or one-way changes ship switched off; turning them on is its own small step.',
                'A new check becomes a CI test, not a manual gate, and a rule that no longer prevents a real failure gets deleted.',
                'The coordinator that owns a piece merges it and hands back its PR, branch, tested commit and proof.',
                'Launch on available Claude else active ChatGPT unless ChatGPT is selected.',
                'Keep healthy parents. Apply eligible selections after work settles.',
                'Claude prefers an available owner pick, else soonest eligible all-model weekly reset.',
                'Claude switches automatically at 98% or a local per-account reserve.',
                'ChatGPT accounts never switch automatically for the main chat. Owner sets Codex worker auto-switch.',
                'Use account.use only for owner picks; topic routes fresh usage.',
                'Clear pick via accounts.claude_auto only on owner request.'):
            with self.subTest(rule=removed):
                self.assertNotIn(removed, COORDINATOR_POLICY)

    def test_policy_omits_the_tool_directory_and_direct_tool_mandate(self):
        for removed in ('Torii tools:',
                        'workers.spawn, workers.steer, workers.stop, workers.goal, workers.list, service.restart, secret.ask,',
                        'secret.list, secret.rotate, secret.revoke; read-only settings, topics, workers, accounts, policy, projects.',
                        'Use direct Torii tools.'):
            self.assertNotIn(removed, COORDINATOR_POLICY)

    def test_policy_documents_the_message_tag_and_topic_bound_tasks(self):
        from coordinator.session import message_tag
        tag = message_tag({'id': 'ID', 'name': 'NAME'}, {'id': 0, 'kind': 'KIND'}).replace('=0 ', '=N ').strip()
        self.assertIn(tag, COORDINATOR_POLICY)
        self.assertIn('Topic IDs identify channels', COORDINATOR_POLICY)
        self.assertIn('tasks.create message=N', COORDINATOR_POLICY)
        self.assertIn('cross_topic=true', COORDINATOR_POLICY)
        self.assertIn('worker_result messages arrive in the job\'s channel', COORDINATOR_POLICY)
        self.assertIn('On secret_filled, resume waiting work without asking the owner; spawn only if no worker resumed.',
                      COORDINATOR_POLICY)

    def test_owner_reports_describe_completion_and_keep_identifiers_in_notes(self):
        from coordinator.workers import WORKER_POLICY
        self.assertIn('Report proven feature completion and owner steps',
                      COORDINATOR_POLICY)
        self.assertIn('add notes with note=, one line per new job/PR number, commit, run ID, version or proof path.',
                      COORDINATOR_POLICY)
        self.assertIn('Ask only key decisions. No reply-word menus or unprompted rollback offers',
                      COORDINATOR_POLICY)
        self.assertIn('Report changes,\nchecks actually run, artifacts', WORKER_POLICY)
        self.assertIn('Ending your turn ends your work, so do not stop to post a status update; keep working and wait for what you started until the task is done or truly blocked, then give one final report.',
                      WORKER_POLICY)

    def test_policies_keep_credentials_out_of_topics_and_output(self):
        from coordinator.workers import WORKER_POLICY
        self.assertIn('Never access or print the bot token', COORDINATOR_POLICY)
        self.assertIn('Ask credentials by name only via secret.ask', COORDINATOR_POLICY)
        self.assertIn("Either provider's job worker gets secrets; parent gets metadata only", COORDINATOR_POLICY)
        self.assertIn('Never read the vault, worker environments, or worker transcripts', COORDINATOR_POLICY)
        self.assertIn('Never print, echo, log, write, commit, or report their values', WORKER_POLICY)

    def test_job_files_stay_in_job_folders_for_coordinators_and_workers(self):
        from coordinator.workers import WORKER_POLICY
        paragraph = ("Put a job's working files only in that job's scratch or results path, when Torii gives it those paths, "
                     "or in the worker's own `TMPDIR`, when Torii provides one. "
                     'Closing the job, or ending the worker, deletes them after the work is saved. '
                     "Never create new top-level folders under Torii's state folder or the home folder for a job. "
                     'Delete large outputs (downloads, media, test copies, dependency installs) '
                     'once delivered or verified; keep only what the owner needs. '
                     'Keep reports, proofs and logs under results when Torii gives the job that path; nothing else there long-term. '
                     "Before keeping something large, say why in the job's notes. "
                     'Do not re-run a check, test suite or review that passed on the exact same commit and inputs; '
                     'reuse and cite its saved result, and re-run only when the code, the base it merges with, '
                     'or the check itself changed.\n')
        for policy in (COORDINATOR_POLICY, WORKER_POLICY):
            with self.subTest(policy='coordinator' if policy is COORDINATOR_POLICY else 'worker'):
                self.assertEqual(policy.count(paragraph), 1)

    def test_policy_length_is_independent_of_home_path_length(self):
        trash = str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.Trash')
        for home in ('/Users/owner', '/Users/owner' + '-long-name' * 20):
            other_trash = home + '/.Trash'
            policy = COORDINATOR_POLICY.replace(trash, other_trash)
            self.assertLess(len(policy.replace(other_trash, '/Users/owner/.Trash')), 3250)

    def test_removed_guidance_stays_out_and_the_embedded_layers_do_not_repeat_it(self):
        for removed in ('Do not invent progress', 'A delivery receipt means', 'Do not duplicate a saved worker',
                        'Inspect partial effects', 'For a failure, state the known cause',
                        'private provider logs', 'Service restart interrupts', 'On a restarted event',
                        'Report progress'):
            self.assertNotIn(removed, COORDINATOR_POLICY)
        trash = str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.Trash')
        self.assertLess(len(COORDINATOR_POLICY.replace(trash, '/Users/owner/.Trash')), 3250)
        claude = (ROOT / 'CLAUDE.md').read_text()
        self.assertEqual([line for line in claude.splitlines() if line.strip()],
                         ['# Torii', 'Read and follow `AGENTS.md` in this directory.',
                          'Reply in normal Markdown; Torii converts it for Telegram, '
                          'and owner formatting arrives as Markdown.'])
        agents = (ROOT / 'AGENTS.md').read_text()
        for sentence in ('Questions and brainstorming', 'secret.ask', 'Python 3.9', '/usr/bin/python3'):
            self.assertNotIn(sentence, agents)


if __name__ == '__main__':
    unittest.main()
