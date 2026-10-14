import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from coordinator import control_api
from coordinator.service import Service
from coordinator.store import Store


class HomeLaunchViewTests(unittest.IsolatedAsyncioTestCase):
    async def test_running_parent_keeps_launch_view_until_a_new_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary))
            try:
                with store.db:
                    for number in (2, 3):
                        store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,1,?,?,?,1)',
                                         ('1:' + str(number), number, 'channel-' + str(number), temporary))
                    store.put('coordinator_home_topic', '1:2')
                launched = []

                async def factory(store, runner, root, model, instructions, topic):
                    parent = SimpleNamespace(closed=False, rejected=False, instructions=instructions,
                                             wait_closed=AsyncMock())
                    launched.append(parent)
                    return parent

                service = Service(store, None, None, Path(temporary), session_factory=factory)
                parent = await service.start_session('1:2')
                original = parent.instructions
                channels = json.loads(original.split('Registered channels: ')[1].splitlines()[0])
                self.assertEqual([channel['id'] for channel in channels], ['1:2', '1:3'])
                with store.db:
                    self.assertTrue(control_api.call(store, 'topic.home', {'topic': '1:3'}).ok)
                self.assertIs(await service.start_session('1:2'), parent)
                self.assertEqual(parent.instructions, original)
                self.assertEqual(len(launched), 1)
                parent.closed = True
                replacement = await service.start_session('1:2')
                channels = json.loads(replacement.instructions.split('Registered channels: ')[1].splitlines()[0])
                self.assertEqual([channel['id'] for channel in channels], ['1:2'])
                self.assertEqual(len(launched), 2)
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
