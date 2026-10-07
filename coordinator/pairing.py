"""Group pairing links and safe Telegram display names."""

import hashlib
import hmac
import time


def valid_code(store, code):
    pairing = store.get('pairing', {}) or {}
    return (pairing.get('expires', 0) > time.time() and hmac.compare_digest(
        pairing.get('hash', ''), hashlib.sha256(code.encode()).hexdigest()))


def safe_display(text):
    return ''.join(character for character in str(text) if character.isprintable())


def describe_sender(message):
    sender = message.get('from', message)
    name = ' '.join(safe_display(sender.get(key, '')) for key in ('first_name', 'last_name')).strip() or 'Telegram user'
    username = safe_display(sender.get('username', ''))
    return name + (' (@' + username + ')' if username else '')


def link(bot, code):
    return 'https://t.me/' + bot + '?startgroup=' + code + '&admin=manage_topics+delete_messages+pin_messages'
