"""Replace injected secret values in text that Torii captures from a worker."""

import json
import uuid


IDENTITIES = ('session_id', 'thread_id', 'threadId', 'id', 'uuid')


class Scrubber:
    """Each value, and its two JSON string forms, becomes `[secret NAME]`. Longer forms go first."""

    def __init__(self, secrets):
        forms = {}
        for name, value in secrets.items():
            for form in (value, json.dumps(value)[1:-1], json.dumps(value, ensure_ascii=False)[1:-1]):
                forms[form] = '[secret ' + name + ']'
        self.forms = sorted(forms.items(), key=lambda item: -len(item[0]))

    @classmethod
    def of(cls, forms):
        scrubber = cls({})
        scrubber.forms = [tuple(form) for form in forms]
        return scrubber

    def extend(self, forms):
        self.forms = sorted(set(self.forms) | {tuple(form) for form in forms}, key=lambda item: -len(item[0]))

    def __call__(self, text):
        if not self.forms or not isinstance(text, str):
            return text
        for form, label in self.forms:
            text = text.replace(form, label)
        return text

    def data(self, value):
        """Scrub a JSON-compatible value through a JSON round trip."""
        if not self.forms or value is None:
            return value
        encoded = self(json.dumps(value, default=str))
        try:
            return json.loads(encoded)
        except ValueError:
            return encoded

    def event(self, raw):
        """Scrub one output line. JSON keys and the UUIDs that name sessions, threads, and messages keep their form."""
        if not self.forms:
            return raw
        try:
            value = json.loads(raw)
        except ValueError:
            return self(raw)
        return json.dumps(self._walk(value), ensure_ascii=False)

    def _walk(self, value, key=None):
        if isinstance(value, dict):
            return {name: self._walk(item, name) for name, item in value.items()}
        if isinstance(value, list):
            return [self._walk(item) for item in value]
        if isinstance(value, str) and not (key in IDENTITIES and _canonical_uuid(value)):
            return self(value)
        return value


def _canonical_uuid(value):
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False
