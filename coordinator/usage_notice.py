import time


def clear_unknown_limit_notice(store, provider, account_label):
    notices = store.get('account_limit_notices', {}) or {}
    previous = notices.get(provider, [])
    remaining = [entry for entry in previous if entry[0] != account_label or entry[2] is not None]
    if remaining != previous:
        store.put('account_limit_notices', {**notices, provider: remaining})


def queue_limit_notice(store, provider, account_label, windows, message,
                       send=True, reset_tolerance=0, clear_recovered=False):
    """Queue hard-stop notices only at home, once per account and window."""
    warnings = store.get('account_limit_notices', {}) or {}
    valid = [entry for entry in warnings.get(provider, []) if entry[2] is None or entry[2] > time.time()]
    seen = [entry for entry in valid if entry[0] != account_label]
    previous = [entry for entry in valid if entry[0] == account_label]

    def same_reset(left, right):
        return (left is None and right is None or
                isinstance(left, (int, float)) and isinstance(right, (int, float)) and
                abs(left - right) <= reset_tolerance)

    kept = [entry for entry in previous if (entry[1] not in windows and not clear_recovered or
            entry[1] in windows and same_reset(entry[2], windows[entry[1]]))]
    prior = valid if account_label is None else kept
    fresh = [[account_label, name, reset] for name, reset in windows.items() if not any(
        entry[1] == name and same_reset(entry[2], reset) for entry in prior)]
    home_topic = store.get('coordinator_home_topic')
    topic = store.topic(home_topic) if home_topic else None
    queued = bool(send and fresh and topic and topic['enabled'])
    updated = seen + kept + (fresh if queued else [])
    if updated != warnings.get(provider, []) or queued:
        with store.db:
            if queued:
                store.enqueue_report(home_topic, message)
            if updated != warnings.get(provider, []):
                store.put('account_limit_notices', {**warnings, provider: updated})
    return queued
