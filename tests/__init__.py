import tempfile
from pathlib import Path
from unittest.mock import patch

from coordinator import extension
from coordinator import accounts
from tests.support import install_test_host_cleanup

extension.use(extension.Native())

_test_local_overrides = tempfile.TemporaryDirectory()
_test_local_overrides_patch = patch.object(
    accounts, 'LOCAL_OVERRIDES_PATH', Path(_test_local_overrides.name) / 'local-overrides.json')
_test_local_overrides_patch.start()

install_test_host_cleanup()
