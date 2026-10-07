"""Validate the Telegram structures consumed by admission before any intake effects."""


def message_problem(message):
    if not isinstance(message, dict):
        return 'message-object'
    for field in ('from', 'chat'):
        if (field == 'from' and field not in message and isinstance(message.get('forum_topic_edited'), dict)
                and isinstance(message.get('sender_chat'), dict)):
            continue
        if not isinstance(message.get(field), dict):
            return 'message-' + field + '-object'
    for field in ('text', 'caption'):
        if field == 'caption' and message.get(field) is None:
            continue
        if field in message and not isinstance(message[field], str):
            return 'message-' + field + '-string'
    for field in ('reply_to_message', 'forum_topic_created', 'forum_topic_edited',
                  'document', 'audio', 'video', 'voice', 'video_note', 'animation',
                  'left_chat_member', 'chat_owner_left', 'chat_owner_changed'):
        if field in message and not isinstance(message[field], dict):
            return 'message-' + field + '-object'
    reply = message.get('reply_to_message', {})
    if 'message_id' in reply and type(reply['message_id']) is not int:
        return 'reply-message-id'
    for field in ('photo', 'entities', 'caption_entities', 'new_chat_members'):
        if field not in message:
            continue
        items = message[field]
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            return 'message-' + field + '-objects'
        if field == 'photo' and any(type(item.get(size, 0)) is not int
                                    for item in items for size in ('width', 'height')):
            return 'message-photo-dimensions'
        if field == 'new_chat_members':
            if any(type(item.get('id')) is not int for item in items):
                return 'message-member-id'
            continue
        if field != 'photo' and any(not isinstance(item.get('type'), str) for item in items):
            return 'message-' + field + '-types'
    return None


def update_problem(update):
    if 'my_chat_member' in update:
        member = update['my_chat_member']
        if not isinstance(member, dict):
            return 'membership-object'
        for field in ('chat', 'from'):
            value = member.get(field)
            if not isinstance(value, dict) or type(value.get('id')) is not int:
                return 'membership-' + field
        if member['chat'].get('type') not in ('group', 'supergroup', 'private'):
            return 'membership-chat-type'
        for field in ('old_chat_member', 'new_chat_member'):
            value = member.get(field)
            if (not isinstance(value, dict) or not isinstance(value.get('status'), str)
                    or not isinstance(value.get('user'), dict) or type(value['user'].get('id')) is not int):
                return 'membership-' + field
        return None
    if 'callback_query' in update:
        query = update['callback_query']
        if not isinstance(query, dict) or not isinstance(query.get('from'), dict):
            return 'callback-object'
        message = query.get('message')
        if not isinstance(message, dict) or not isinstance(message.get('chat'), dict):
            return 'callback-message-object'
        return None
    if 'message' in update:
        return message_problem(update['message'])
    if 'message_reaction' in update:
        reaction = update['message_reaction']
        if not isinstance(reaction, dict) or not isinstance(reaction.get('chat'), dict):
            return 'reaction-object'
        for field in ('user', 'actor_chat'):
            if field in reaction and not isinstance(reaction[field], dict):
                return 'reaction-' + field + '-object'
        for field in ('old_reaction', 'new_reaction'):
            items = reaction.get(field)
            if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
                return 'reaction-' + field + '-objects'
        return None
    return 'unknown-update-type'
