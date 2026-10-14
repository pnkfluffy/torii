import os
from pathlib import Path
import pwd
import unittest
import runpy
from unittest.mock import patch

with patch('coordinator.host_os.SYSTEM', 'darwin'):
    COORDINATOR_POLICY = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'coordinator/policy.py'),
                                         run_name='coordinator._capture')['COORDINATOR_POLICY']

ROOT = Path(__file__).resolve().parents[1]


class CoordinatorPolicyTests(unittest.TestCase):
    def test_review_followups_preserve_explicit_coordinator_rules(self):
        for rule in ('Each message starts with [topic=ID name="NAME" message=N kind=KIND]',
                     'read-only settings, topics, workers, accounts, policy',
                     'Claude prefers an available owner pick', 'local per-account reserve',
                     'Claude switches automatically at 98%', 'Delegate with folder, instructions, expected outcome',
                     'Ship each finished piece on its own.\n'
                     'Split work into independent jobs that run in parallel, each with its own branch and PR.\n'
                     "The job's coordinator reviews its PR (one fresh review only if risky) and merges it as soon as CI passes on the current integration branch.\n"
                     'Never hold a change for unrelated work or bundle unrelated changes.\n'
                     'Risky or one-way changes ship switched off; turning them on is its own small step.\n'
                     'A new check becomes a CI test, not a manual gate, and a rule that no longer prevents a real failure gets deleted.\n'
                     'The coordinator that owns a piece merges it and hands back its PR, branch, tested commit and proof.',
                     "worker_result messages arrive in the job's channel. Verify them.",
                     'resume waiting work without asking the owner'):
            with self.subTest(rule=rule):
                self.assertIn(rule, COORDINATOR_POLICY)
        self.assertIn('Never read the vault, worker environments, or worker transcripts', COORDINATOR_POLICY)
        self.assertNotIn('Only Claude workers receive them', COORDINATOR_POLICY)

    def test_policy_lists_every_coordinator_tool_and_the_task_rules(self):
        from coordinator.mcp import COORDINATOR_OPS
        self.assertNotIn('account.redeem', COORDINATOR_POLICY)
        for name in COORDINATOR_OPS:
            if name == 'account.redeem':
                continue
            self.assertIn(name, COORDINATOR_POLICY)
        for rule in ('Questions and brainstorming need no job', 'Create a job when work begins with tasks.create',
                     'Torii tools: telegram.send, tasks.create',
                     'One worktree per job', 'Keep job status, notes and stage current',
                     'telegram.send is the only owner channel', 'Answer questions when asked, even while jobs run',
                     'Markdown, one image or file per message',
                     'Claude prefers an available owner pick',
                     'Claude switches automatically at 98% or a local per-account reserve',
                     'Launch on available Claude else active ChatGPT unless ChatGPT is selected',
                     'Keep healthy parents. Apply eligible selections after work settles',
                     'ChatGPT accounts never switch automatically for the main chat',
                     'Owner sets Codex worker auto-switch', 'Use account.use only for owner picks',
                     'Spawn only connected providers', 'Use direct Torii tools',
                     'Use /goal when the owner asks for a goal; set it with workers.goal'):
            self.assertIn(rule, COORDINATOR_POLICY)
        from coordinator.policy import usage_policy
        self.assertIn('Run independent pieces in parallel, one worker per piece.',
                      usage_policy(ROOT / 'coordinator'))

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
        self.assertIn('Job/PR numbers, commits, run IDs, versions, proof paths stay in job notes',
                      COORDINATOR_POLICY)
        self.assertIn('Ask only key decisions. No reply-word menus or unprompted rollback offers',
                      COORDINATOR_POLICY)
        self.assertIn('Report changes,\nchecks actually run, artifacts', WORKER_POLICY)

    def test_policies_keep_credentials_out_of_topics_and_output(self):
        from coordinator.workers import WORKER_POLICY
        self.assertIn('Never access or print the bot token', COORDINATOR_POLICY)
        self.assertIn('Ask credentials by name only via secret.ask', COORDINATOR_POLICY)
        self.assertIn("Either provider's job worker gets secrets; parent gets metadata only", COORDINATOR_POLICY)
        self.assertIn('Never read the vault, worker environments, or worker transcripts', COORDINATOR_POLICY)
        self.assertIn('Never print, echo, log, write, commit, or report their values', WORKER_POLICY)

    def test_policy_length_is_independent_of_home_path_length(self):
        trash = str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.Trash')
        for home in ('/Users/owner', '/Users/owner' + '-long-name' * 20):
            other_trash = home + '/.Trash'
            policy = COORDINATOR_POLICY.replace(trash, other_trash)
            self.assertLess(len(policy.replace(other_trash, '/Users/owner/.Trash')), 3200)

    def test_removed_guidance_stays_out_and_the_embedded_layers_do_not_repeat_it(self):
        for removed in ('Do not invent progress', 'A delivery receipt means', 'Do not duplicate a saved worker',
                        'Inspect partial effects', 'For a failure, state the known cause',
                        'private provider logs', 'Service restart interrupts', 'On a restarted event',
                        'Report progress'):
            self.assertNotIn(removed, COORDINATOR_POLICY)
        trash = str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.Trash')
        self.assertLess(len(COORDINATOR_POLICY.replace(trash, '/Users/owner/.Trash')), 3200)
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
