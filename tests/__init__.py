import os
import shlex
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch


def _security_guard(event, args):
    if event not in ('subprocess.Popen', 'os.exec', 'os.posix_spawn', 'os.system'):
        return
    def security(value):
        return isinstance(value, (str, bytes, os.PathLike)) and os.path.basename(os.fsdecode(value)) == 'security'
    def shell_command(value):
        if not isinstance(value, (str, bytes)):
            return False
        try:
            return any(security(part) for part in shlex.split(os.fsdecode(value)))
        except ValueError:
            return False
    if event == 'os.system':
        blocked = shell_command(args[0])
    else:
        blocked = security(args[0])
        argv = args[1]
        if (event == 'subprocess.Popen' and isinstance(argv, (list, tuple)) and len(argv) >= 3 and
                os.path.basename(os.fsdecode(argv[0])) in ('sh', 'bash', 'zsh') and argv[1] == '-c'):
            blocked = blocked or shell_command(argv[2])
    if blocked:
        raise AssertionError('Unmocked security binary is forbidden in tests')


sys.addaudithook(_security_guard)

from coordinator import extension
from coordinator import accounts
from coordinator import store
from tests.support import install_inprocess_scans, install_test_host_cleanup

extension.use(extension.Native())

_test_local_overrides = tempfile.TemporaryDirectory()
_test_local_overrides_patch = patch.object(
    accounts, 'LOCAL_OVERRIDES_PATH', Path(_test_local_overrides.name) / 'local-overrides.json')
_test_local_overrides_patch.start()
_test_secret_homes_patch = patch.object(store, '_secret_homes', return_value=[])
_test_secret_homes_patch.start()

install_test_host_cleanup()
install_inprocess_scans()
