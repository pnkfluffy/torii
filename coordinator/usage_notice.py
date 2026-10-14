from datetime import datetime, timedelta, timezone
import math
import time


def clear_low_capacity_warning(store, provider):
    """Clear one provider's warning history when another account becomes usable."""
    warnings = store.get('low_capacity_warnings', {}) or {}
    if provider in warnings:
        with store.db:
            store.put('low_capacity_warnings', {key: value for key, value in warnings.items()
                                                if key != provider})


def queue_low_capacity_notice(store, provider, account_label, windows, home_topic, message,
                              send=True, reset_tolerance=0, clear_recovered=False):
    """Queue one notice for fresh windows, and forget windows that recovered."""
    warnings = store.get('low_capacity_warnings', {}) or {}
    valid = [entry for entry in warnings.get(provider, []) if entry[2] is None or entry[2] > time.time()]
    seen = [entry for entry in valid if entry[0] != account_label]
    previous = [entry for entry in valid if entry[0] == account_label]

    def same_reset(left, right):
        return (left is None and right is None or
                isinstance(left, (int, float)) and isinstance(right, (int, float)) and
                abs(left - right) <= reset_tolerance)

    kept = [entry for entry in previous if (entry[1] not in windows and not clear_recovered or
            entry[1] in windows and same_reset(entry[2], windows[entry[1]]))]
    fresh = [[account_label, name, reset] for name, reset in windows.items() if not any(
        entry[1] == name and same_reset(entry[2], reset) for entry in kept)]
    topic = store.topic(home_topic) if home_topic else None
    queued = bool(send and fresh and topic and topic['enabled'])
    updated = seen + kept + (fresh if queued else [])
    if updated != warnings.get(provider, []) or queued:
        with store.db:
            if queued:
                store.enqueue_report(home_topic, message)
            if updated != warnings.get(provider, []):
                store.put('low_capacity_warnings', {**warnings, provider: updated})
    return queued


def warn_low_capacity(store, provider, account_label, window, used_pct, resets_at,
                      next_available_at, home_topic, capacity_pct=95):
    """Queue a warning for one usable account within 20 points of capacity_pct.

    Times are Unix seconds. Call clear_low_capacity_warning when two accounts are usable.
    """
    if not isinstance(used_pct, (int, float)) or isinstance(used_pct, bool) or not math.isfinite(used_pct):
        return False
    if used_pct < capacity_pct - 20:
        queue_low_capacity_notice(store, provider, account_label, {}, home_topic, '', send=False,
                                  clear_recovered=True)
        return False
    if (not home_topic or used_pct >= capacity_pct or
            resets_at is not None and (not isinstance(resets_at, (int, float)) or
                                       not math.isfinite(resets_at) or resets_at <= time.time())):
        return False
    jst = timezone(timedelta(hours=9), 'JST')
    def when(value):
        return datetime.fromtimestamp(value, jst).strftime('%H:%M JST') if value else 'unknown'
    message = (f'Heads-up: {provider} is down to its last account ({account_label}) '
               f'at {used_pct:g}% of its {window} limit. It resets at {when(resets_at)}; '
               f'the next account returns at {when(next_available_at)}.')
    return queue_low_capacity_notice(store, provider, account_label, {window: resets_at}, home_topic,
                                    message, reset_tolerance=30 * 60)
