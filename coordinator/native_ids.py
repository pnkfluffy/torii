"""Validate native session ids before using them in provider locks."""

import re
import uuid


def valid(provider, session_id):
    if not isinstance(session_id, str):
        return False
    if provider == 'opencode':
        return re.fullmatch(r'ses_[0-9A-Za-z]{26}', session_id) is not None
    if provider not in ('claude', 'codex', 'muse'):
        return False
    try:
        return str(uuid.UUID(session_id)) == session_id
    except ValueError:
        return False
