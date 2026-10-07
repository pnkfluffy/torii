"""Telegram transport. Errors never include credential-bearing URLs."""

import asyncio
import http.client
import json
import logging
import mimetypes
import os
import re
from pathlib import Path
import socket
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from .formatting import markdown_to_html

logger = logging.getLogger(__name__)

MAX_PHOTO_BYTES = 10 * 1024 * 1024
MAX_DOCUMENT_BYTES = 50 * 1024 * 1024
PHOTO_SUFFIXES = {'.jpg', '.jpeg', '.png', '.webp'}
LONG_POLL_SECONDS = 20
REQUEST_SECONDS = 40
CONNECT_SECONDS = 5
TOPIC_ERRORS = {'thread_not_found': ('message thread not found', 'thread_not_found'),
                'topics_off': ('the chat is not a forum', 'channel_forum_missing')}
CALL_SECONDS = {'setMessageReaction': 5, 'deleteMessage': 10}


def _create_connection(address, timeout=REQUEST_SECONDS, source_address=None):
    host, port = address
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses.sort(key=lambda entry: entry[0] != socket.AF_INET)
    last_error = None
    for family, socktype, proto, _, sockaddr in addresses:
        connection = None
        try:
            connection = socket.socket(family, socktype, proto)
            connection.settimeout(min(CONNECT_SECONDS, timeout) if timeout is not None else CONNECT_SECONDS)
            if source_address:
                connection.bind(source_address)
            connection.connect(sockaddr)
            connection.settimeout(timeout)
            return connection
        except OSError as error:
            last_error = error
            if connection is not None:
                connection.close()
    if last_error is not None:
        raise last_error
    raise OSError('no address available')


class TelegramHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_connection = _create_connection


class TelegramHTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, request):
        return self.do_open(TelegramHTTPSConnection, request, context=self._context)


opener = urllib.request.build_opener(TelegramHTTPSHandler())


def photo_path(path, size):
    return path.suffix.lower() in PHOTO_SUFFIXES and size <= MAX_PHOTO_BYTES


def multipart_body(parameters, path, method, protected=None):
    boundary = 'torii-' + uuid.uuid4().hex
    chunks = []
    for name, value in parameters.items():
        encoded = json.dumps(value, separators=(',', ':')) if isinstance(value, (dict, list)) else str(value)
        chunks.extend((('--' + boundary + '\r\n').encode(),
                       ('Content-Disposition: form-data; name="' + name + '"\r\n\r\n').encode(),
                       encoded.encode(), b'\r\n'))
    field = 'photo' if method == 'sendPhoto' else 'document'
    filename = urllib.parse.quote(path.name, safe='')
    mime = mimetypes.guess_type(path.name)[0] or 'application/octet-stream'
    limit = MAX_PHOTO_BYTES if method == 'sendPhoto' else MAX_DOCUMENT_BYTES
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as source:
        metadata = os.fstat(source.fileno())
        if protected is not None and protected.protected(path, metadata):
            raise TelegramError('protected-image-path')
        if not stat.S_ISREG(metadata.st_mode):
            raise TelegramError('image-is-not-a-file')
        file_bytes = source.read(limit + 1)
    if len(file_bytes) > limit:
        raise TelegramError('image-too-large')
    chunks.extend((('--' + boundary + '\r\n').encode(),
                   ('Content-Disposition: form-data; name="' + field + '"; filename="' + filename + '"\r\n').encode(),
                   ('Content-Type: ' + mime + '\r\n\r\n').encode(), file_bytes, b'\r\n',
                   ('--' + boundary + '--\r\n').encode()))
    return b''.join(chunks), 'multipart/form-data; boundary=' + boundary


def _reason(detail):
    """A fixed code for the one API description Torii acts on. The body itself is never kept."""
    description = detail.get("description") if isinstance(detail, dict) else None
    description = description.lower() if isinstance(description, str) else ''
    if "message to delete not found" in description:
        return "message_not_found"
    for code, fragments in TOPIC_ERRORS.items():
        if any(fragment in description for fragment in fragments):
            return code
    return "message_not_modified" if "message is not modified" in description else None


class TelegramError(Exception):
    def __init__(self, code, retry_after=None, reason=None):
        self.code = code
        self.retry_after = retry_after
        self.reason = reason
        super().__init__(f"Telegram request failed ({code})")


class ImageUnavailable(Exception):
    pass


class Telegram:
    def __init__(self, token_file: Path, store=None):
        self._store = store
        self._token_file = token_file.resolve()
        self._token = token_file.read_text().strip()
        if not self._token:
            raise ValueError("Token file is empty")

    def _call(self, method, data, timeout=REQUEST_SECONDS):
        return self._request(method, json.dumps(data).encode(), 'application/json', timeout)

    def _request(self, method, data, content_type, timeout=REQUEST_SECONDS):
        started = time.monotonic()
        request = urllib.request.Request(
            "https://api.telegram.org/bot" + self._token + "/" + method,
            data=data, headers={"Content-Type": content_type})
        try:
            with opener.open(request, timeout=timeout) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            try:
                detail = json.load(error)
                retry = detail.get("parameters", {}).get("retry_after")
            except (ValueError, OSError, AttributeError):
                detail, retry = None, None
            raise TelegramError(error.code, retry, _reason(detail)) from None
        except (OSError, ValueError) as error:
            timed_out = isinstance(error, socket.timeout) or isinstance(getattr(error, 'reason', None), socket.timeout)
            raise TelegramError("network-or-invalid-response", reason='timeout' if timed_out else None) from None
        finally:
            seconds = time.monotonic() - started
            if method != 'getUpdates' and seconds > 5:
                logger.info('telegram call slow method=%s seconds=%.1f', method, seconds)
        if not result.get("ok"):
            raise TelegramError(result.get("error_code", "api"), result.get("parameters", {}).get("retry_after"),
                                _reason(result))
        return result["result"]

    def _call_multipart(self, method, parameters, image, protected=None):
        from .attachment_paths import attachment_paths

        protected = protected or attachment_paths(self._store, self._token_file)
        try:
            path = Path(image).resolve(strict=True)
            if protected.protected(path):
                raise TelegramError('protected-image-path')
            body, content_type = multipart_body(parameters, path, method, protected)
        except FileNotFoundError:
            raise ImageUnavailable() from None
        except OSError:
            raise TelegramError('image-read-failed') from None
        return self._request(method, body, content_type)

    async def call(self, method, **data):
        return await asyncio.to_thread(self._call, method, data, CALL_SECONDS.get(method, REQUEST_SECONDS))

    async def call_multipart(self, method, parameters, image):
        from .attachment_paths import attachment_paths

        protected = attachment_paths(self._store, self._token_file)
        return await asyncio.to_thread(self._call_multipart, method, parameters, image, protected)

    async def updates(self, offset):
        return await asyncio.to_thread(self._call, "getUpdates", {
            "offset": offset, "timeout": LONG_POLL_SECONDS, "allowed_updates": ["message", "callback_query", "message_reaction", "my_chat_member"]},
            LONG_POLL_SECONDS + 10)

    async def create_topic(self, chat, name):
        topic = await self.call('createForumTopic', chat_id=chat, name=name)
        return topic['message_thread_id']

    async def get_chat(self, chat):
        return await self.call('getChat', chat_id=chat)

    async def get_chat_member(self, chat, user):
        return await self.call('getChatMember', chat_id=chat, user_id=user)

    async def leave_chat(self, chat):
        return await self.call('leaveChat', chat_id=chat)

    async def set_default_admin_rights(self):
        return await self.call('setMyDefaultAdministratorRights', rights={
            'is_anonymous': False, 'can_manage_chat': True, 'can_delete_messages': True,
            'can_manage_video_chats': False, 'can_restrict_members': False,
            'can_promote_members': False, 'can_change_info': False, 'can_invite_users': False,
            'can_pin_messages': True, 'can_manage_topics': True})

    async def send(self, row):
        rendered = markdown_to_html(row['text'])
        if row.get('edit_message'):
            parameters = {"chat_id": row["chat"], "message_id": row["edit_message"], "text": rendered,
                          "parse_mode": "HTML",
                          "reply_markup": json.loads(row['reply_markup']) if row.get('reply_markup')
                          else {"inline_keyboard": []}}
            try:
                await self.call("editMessageText", **parameters)
            except TelegramError as error:
                if error.code != 400:
                    raise
                if error.reason != 'message_not_modified':
                    parameters = dict(parameters)
                    parameters.pop('parse_mode')
                    parameters['text'] = row['text']
                    try:
                        await self.call("editMessageText", **parameters)
                    except TelegramError as retry:
                        if retry.reason != 'message_not_modified':
                            raise
            return {"message_id": row["edit_message"]}
        parameters = {"chat_id": row["chat"]}
        if row["thread"]:
            parameters["message_thread_id"] = row["thread"]
        if row.get('image'):
            parameters['caption'] = rendered
        else:
            parameters['text'] = rendered
        parameters['parse_mode'] = 'HTML'
        if row.get('reply_markup'):
            parameters['reply_markup'] = json.loads(row['reply_markup'])
        if row.get("reply_to"):
            parameters["reply_parameters"] = {"message_id": row["reply_to"], "allow_sending_without_reply": True}
        if row.get('image'):
            path = Path(row['image'])
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                raise ImageUnavailable() from None
            except OSError:
                raise TelegramError('image-read-failed') from None
            method = 'sendPhoto' if photo_path(path, size) else 'sendDocument'
            try:
                return await self.call_multipart(method, parameters, row['image'])
            except TelegramError as error:
                if error.code != 400:
                    raise
                parameters = dict(parameters)
                parameters.pop('parse_mode')
                parameters['caption'] = row['text']
                return await self.call_multipart(method, parameters, row['image'])
        try:
            return await self.call("sendMessage", **parameters)
        except TelegramError as error:
            if error.code != 400:
                raise
            parameters.pop('parse_mode')
            parameters['text'] = row['text']
            return await self.call("sendMessage", **parameters)

    def _download_file(self, file_id, max_bytes):
        info = self._call("getFile", {"file_id": file_id})
        path = info.get("file_path")
        size = info.get("file_size")
        if (not isinstance(path, str) or not re.fullmatch(r"[A-Za-z0-9_./-]+", path)
                or path.startswith("/") or ".." in path.split("/")
                or (size is not None and (type(size) is not int or size > max_bytes))):
            raise TelegramError("invalid-or-oversized-file")

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, fp, code, msg, headers, newurl):
                return None

        request = urllib.request.Request("https://api.telegram.org/file/bot" + self._token + "/" + path)
        try:
            download_opener = urllib.request.build_opener(NoRedirect, TelegramHTTPSHandler())
            with download_opener.open(request, timeout=40) as response:
                data = response.read(max_bytes + 1)
        except urllib.error.HTTPError as error:
            raise TelegramError(error.code) from None
        except (OSError, ValueError) as error:
            timed_out = isinstance(error, socket.timeout) or isinstance(getattr(error, 'reason', None), socket.timeout)
            raise TelegramError("file-download-timeout" if timed_out else "file-download-failed") from None
        if not data or len(data) > max_bytes:
            raise TelegramError("invalid-or-oversized-file")
        return data

    async def download_file(self, file_id, max_bytes):
        return await asyncio.to_thread(self._download_file, file_id, max_bytes)
