import io
import json
import socket
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import AsyncMock, call, Mock, patch

from coordinator import telegram as transport
from coordinator.telegram import Telegram, TelegramError, multipart_body


class ConnectionTests(unittest.TestCase):
    def test_ipv4_is_tried_before_ipv6(self):
        ipv6 = (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::1', 443, 0, 0))
        ipv4 = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 443))
        first = Mock()
        first.connect.side_effect = socket.timeout('timed out')
        second = Mock()
        with patch('coordinator.telegram.socket.getaddrinfo', return_value=[ipv6, ipv4]), \
                patch('coordinator.telegram.socket.socket', side_effect=[first, second]) as sockets:
            connected = transport._create_connection(('api.telegram.org', 443), timeout=40)
        self.assertIs(connected, second)
        self.assertEqual(sockets.call_args_list, [call(socket.AF_INET, socket.SOCK_STREAM, 6),
                                                   call(socket.AF_INET6, socket.SOCK_STREAM, 6)])
        first.connect.assert_called_once_with(ipv4[4])
        first.close.assert_called_once_with()
        second.connect.assert_called_once_with(ipv6[4])

    def test_connect_timeout_is_bounded_and_call_timeout_is_restored(self):
        address = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 443))
        for timeout, connect_timeout in ((40, 5), (3, 3)):
            connection = Mock()
            with patch('coordinator.telegram.socket.getaddrinfo', return_value=[address]), \
                    patch('coordinator.telegram.socket.socket', return_value=connection):
                self.assertIs(transport._create_connection(('api.telegram.org', 443), timeout), connection)
            self.assertEqual(connection.settimeout.call_args_list,
                             [call(connect_timeout), call(timeout)])

    def test_https_connection_uses_bounded_connect(self):
        connection = transport.TelegramHTTPSConnection('api.telegram.org', timeout=40)
        self.assertIs(connection._create_connection, transport._create_connection)

    def test_all_connect_failures_raise_the_last_error(self):
        addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 443)),
                     (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::1', 443, 0, 0))]
        first = Mock()
        first.connect.side_effect = OSError('first')
        second = Mock()
        second.connect.side_effect = OSError('last')
        with patch('coordinator.telegram.socket.getaddrinfo', return_value=addresses), \
                patch('coordinator.telegram.socket.socket', side_effect=[first, second]):
            with self.assertRaisesRegex(OSError, 'last'):
                transport._create_connection(('api.telegram.org', 443), timeout=40)
        first.close.assert_called_once_with()
        second.close.assert_called_once_with()


class TelegramTests(unittest.IsolatedAsyncioTestCase):
    def test_topic_rename_errors_have_safe_reason_codes(self):
        cases = [('Bad Request: not enough rights to manage topics', 'topic_manage_required'),
                 ('Forbidden: CHAT_ADMIN_REQUIRED', 'topic_manage_required'),
                 ('Bad Request: TOPIC_NAME_INVALID', 'topic_name_invalid'),
                 ('Bad Request: TOPIC_NOT_MODIFIED', 'topic_not_modified'),
                 ('Bad Request: topic is not modified', 'topic_not_modified')]
        for description, expected in cases:
            with self.subTest(description=description):
                self.assertEqual(transport._reason({'description': description}, 'editForumTopic'), expected)

    def test_administrator_permission_reason_is_scoped_to_edit_forum_topic(self):
        detail = {'ok': False, 'error_code': 400, 'description': 'Bad Request: CHAT_ADMIN_REQUIRED'}
        for method, expected in (('editForumTopic', 'topic_manage_required'), ('sendMessage', None)):
            for http_error in (False, True):
                with self.subTest(method=method, http_error=http_error):
                    api = object.__new__(Telegram)
                    api._token = 'fake-test-token'
                    response = io.BytesIO(json.dumps(detail).encode())
                    effect = urllib.error.HTTPError('https://example.invalid', 400, '', {}, response) if http_error else None
                    with patch.object(transport.opener, 'open', side_effect=effect, return_value=response):
                        with self.assertRaises(TelegramError) as caught:
                            api._call(method, {})
                    self.assertEqual(caught.exception.reason, expected)

    async def test_general_omits_thread_and_create_topic(self):
        api = object.__new__(Telegram)
        api.call = AsyncMock(return_value={'message_id': 12, 'message_thread_id': 8})
        await api.send({'chat': 42, 'thread': 0, 'text': 'done'})
        self.assertNotIn('message_thread_id', api.call.call_args.kwargs)
        self.assertEqual(await api.create_topic(42, 'Project'), 8)
        api.call.assert_awaited_with('createForumTopic', chat_id=42, name='Project')

    def test_group_topic_error_codes(self):
        self.assertEqual(transport._reason({'description': 'Bad Request: message thread not found'}),
                         'thread_not_found')
        for text in ('Bad Request: the chat is not a forum', 'CHANNEL_FORUM_MISSING'):
            self.assertEqual(transport._reason({'description': text}), 'topics_off')

    def test_topic_gone_error_codes_preserve_other_failures(self):
        cases = [('Bad Request: TOPIC_ID_INVALID', 'topic_gone'),
                 ('Bad Request: TOPIC_DELETED', 'topic_gone'),
                 ('Bad Request: message thread not found', 'thread_not_found'),
                 ('Bad Request: invalid forum topic identifier specified', None),
                 ('Bad Request: not enough rights to delete the topic', None),
                 ('Bad Request: not enough rights to close or open the topic', None),
                 ('Bad Request: CHAT_ADMIN_REQUIRED', None),
                 ('Bad Request: CHAT_WRITE_FORBIDDEN', None),
                 ('Bad Request: the chat is not a forum', 'topics_off'),
                 ('Bad Request: the chat is not a supergroup forum', None),
                 ('Bad Request: CHANNEL_FORUM_MISSING', 'topics_off')]
        for description, reason in cases:
            with self.subTest(description=description):
                self.assertEqual(transport._reason({'description': description}), reason)

    def test_multipart_body_contains_file_caption_and_thread(self):
        with tempfile.TemporaryDirectory() as temp:
            image = Path(temp) / 'sample.png'
            image.write_bytes(b'\x00\xffimage-bytes')
            body, content_type = multipart_body(
                {'chat_id': -42, 'message_thread_id': 7, 'caption': 'A caption',
                 'reply_markup': {'inline_keyboard': []},
                 'reply_parameters': {'message_id': 9, 'allow_sending_without_reply': True}},
                image, 'sendPhoto')
            self.assertIn(b'\x00\xffimage-bytes', body)
            self.assertIn(b'A caption', body)
            self.assertIn(b'name="message_thread_id"\r\n\r\n7', body)
            self.assertIn(b'name="photo"; filename="sample.png"', body)
            self.assertIn(b'"message_id":9', body)
            self.assertIn(b'"inline_keyboard":[]', body)
            self.assertTrue(content_type.startswith('multipart/form-data; boundary='))

    async def test_network_error_does_not_expose_token(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "token"
            path.write_text("secret-test-token")
            api = Telegram(path)
            with patch('coordinator.telegram.opener.open',
                       side_effect=urllib.error.URLError('url with secret-test-token')):
                with self.assertRaises(TelegramError) as caught:
                    await api.call("getMe")
                self.assertNotIn("secret-test-token", str(caught.exception))

    async def test_topic_and_reply_are_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "token"
            path.write_text("test-token")
            api = Telegram(path)
            with patch.object(api, "_call", return_value={"message_id": 12}) as call:
                await api.send({"chat": -42, "thread": 7, "text": "done", "reply_to": 9})
                self.assertEqual(call.call_args.args[1]["message_thread_id"], 7)
                self.assertEqual(call.call_args.args[1]["reply_parameters"]["message_id"], 9)
                self.assertEqual(call.call_args.args[1]['parse_mode'], 'HTML')

    async def test_html_400_falls_back_once_to_rendered_text(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            with patch.object(api, 'call', side_effect=[TelegramError(400), {'message_id': 12}]) as call:
                await api.send({'chat': -42, 'thread': 7, 'text': '**bold**'})
                self.assertEqual(call.call_count, 2)
                self.assertEqual(call.call_args_list[0].kwargs['text'], '<b>bold</b>')
                self.assertEqual(call.call_args_list[0].kwargs['parse_mode'], 'HTML')
                self.assertEqual(call.call_args_list[1].kwargs['text'], 'bold')
                self.assertNotIn('parse_mode', call.call_args_list[1].kwargs)

    async def test_two_400_responses_stop_after_plain_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            with patch.object(api, 'call', side_effect=TelegramError(400)) as call:
                with self.assertRaises(TelegramError):
                    await api.send({'chat': -42, 'thread': 7, 'text': '**bold**'})
                self.assertEqual(call.call_count, 2)

    async def test_plain_fallback_fits_when_markdown_exceeds_the_text_limit(self):
        api = object.__new__(Telegram)
        for edit in (None, 12):
            with self.subTest(edit=edit):
                api.call = AsyncMock(side_effect=[TelegramError(400), {'message_id': 12}])
                await api.send({'chat': -42, 'thread': 7, 'text': '**' + '😀' * 4096 + '**',
                                'edit_message': edit})
                fallback = api.call.call_args.kwargs
                self.assertEqual(fallback['text'], '😀' * 4096)
                self.assertEqual(len(fallback['text']), 4096)
                self.assertNotIn('parse_mode', fallback)

    async def test_plain_retry_keeps_link_urls_for_messages_edits_and_captions(self):
        api = object.__new__(Telegram)
        url = 'https://example.test/?a=1&b=2'
        with tempfile.TemporaryDirectory() as temp:
            image = Path(temp) / 'image.png'
            image.write_bytes(b'image')
            for extra in ({}, {'edit_message': 12}, {'image': str(image)}):
                with self.subTest(extra=extra):
                    api.call = AsyncMock(side_effect=[TelegramError(400), {'message_id': 12}])
                    api.call_multipart = AsyncMock(side_effect=[TelegramError(400), {'message_id': 12}])
                    await api.send({'chat': -42, 'thread': 7, 'text': '[review & notes](' + url + ')', **extra})
                    fallback = api.call_multipart.call_args.args[1] if 'image' in extra else api.call.call_args.kwargs
                    self.assertEqual(fallback['caption' if 'image' in extra else 'text'],
                                     'review & notes (' + url + ')')
                    self.assertNotIn('parse_mode', fallback)

    async def test_plain_retry_keeps_long_link_urls_without_exceeding_capacity(self):
        api = object.__new__(Telegram)
        url = 'https://example.test/' + 'a' * 5000
        with tempfile.TemporaryDirectory() as temp:
            image = Path(temp) / 'image.png'
            image.write_bytes(b'image')
            for extra, count in (({}, 4096), ({'edit_message': 12}, 4096), ({'image': str(image)}, 1024)):
                with self.subTest(extra=extra):
                    api.call = AsyncMock(side_effect=[TelegramError(400), {'message_id': 12}])
                    api.call_multipart = AsyncMock(side_effect=[TelegramError(400), {'message_id': 12}])
                    await api.send({'chat': -42, 'thread': 7, 'text': '[' + '😀' * count + '](' + url + ')', **extra})
                    fallback = api.call_multipart.call_args.args[1] if 'image' in extra else api.call.call_args.kwargs
                    self.assertEqual(fallback['caption' if 'image' in extra else 'text'], '😀' * count)
                    self.assertEqual(fallback.get('caption_entities' if 'image' in extra else 'entities'),
                                     [{'type': 'text_link', 'offset': 0, 'length': count * 2, 'url': url}])
                    self.assertNotIn('parse_mode', fallback)

    async def test_plain_retry_link_offsets_use_utf16_without_expanding_prior_urls(self):
        api = object.__new__(Telegram)
        url = 'https://example.test/' + 'a' * 5000
        api.call = AsyncMock(side_effect=[TelegramError(400), {'message_id': 12}])
        await api.send({'chat': -42, 'thread': 7,
                        'text': '**😀** [first](' + url + ') & [next](https://next.test)'})
        fallback = api.call.call_args.kwargs
        self.assertEqual(fallback['text'], '😀 first & next')
        self.assertEqual(fallback['entities'], [
            {'type': 'text_link', 'offset': 3, 'length': 5, 'url': url},
            {'type': 'text_link', 'offset': 11, 'length': 4, 'url': 'https://next.test'}])

    async def test_edit_card_escapes_text_and_uses_html(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            with patch.object(api, 'call', return_value={'message_id': 12}) as call:
                await api.send({'chat': -42, 'thread': 7, 'text': 'A < B & C', 'edit_message': 12})
                self.assertEqual(call.call_args.args[0], 'editMessageText')
                self.assertEqual(call.call_args.kwargs['text'], 'A &lt; B &amp; C')
                self.assertEqual(call.call_args.kwargs['parse_mode'], 'HTML')

    async def test_unchanged_card_edit_counts_as_delivered_in_place(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            unchanged = TelegramError(400, reason='message_not_modified')
            with patch.object(api, 'call', side_effect=unchanged) as call:
                result = await api.send({'chat': -42, 'thread': 7, 'text': 'Same', 'edit_message': 12})
            self.assertEqual(result, {'message_id': 12})
            self.assertEqual(call.call_count, 1)

    async def test_inline_keyboard_transport_and_callback_subscription(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            markup = {'inline_keyboard': [[{'text': 'New project', 'callback_data': 'torii:abc:0'}]]}
            with patch.object(api, '_call', return_value=[]) as call:
                await api.send({'chat': -42, 'thread': 7, 'text': 'Setup', 'reply_markup': json.dumps(markup)})
                self.assertEqual(call.call_args.args[1]['reply_markup'], markup)
                await api.updates(10)
                self.assertIn('callback_query', call.call_args.args[1]['allowed_updates'])

    async def test_long_poll_socket_timeout_is_ten_seconds_past_the_poll(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            with patch('coordinator.telegram.opener.open', side_effect=urllib.error.URLError('down')) as urlopen:
                with self.assertRaises(TelegramError):
                    await api.updates(10)
                self.assertEqual(urlopen.call_args.kwargs['timeout'], 30)
                self.assertEqual(json.loads(urlopen.call_args.args[0].data)['timeout'], 20)
                with self.assertRaises(TelegramError):
                    await api.call('getMe')
                self.assertEqual(urlopen.call_args.kwargs['timeout'], 40)
                with self.assertRaises(TelegramError):
                    await api.call('deleteMessage', chat_id=-42, message_id=5)
                self.assertEqual(urlopen.call_args.kwargs['timeout'], 10)
                with self.assertRaises(TelegramError):
                    await api.call('setMessageReaction', chat_id=-42, message_id=5,
                                   reaction=[{'type': 'emoji', 'emoji': '👀'}])
                self.assertEqual(urlopen.call_args.kwargs['timeout'], 5)
            for error, reason in ((socket.timeout('timed out'), 'timeout'),
                                  (urllib.error.URLError(socket.timeout('timed out')), 'timeout'),
                                  (urllib.error.URLError('down'), None)):
                with patch('coordinator.telegram.opener.open', side_effect=error):
                    with self.assertRaises(TelegramError) as raised:
                        await api.call('setMessageReaction', chat_id=-42, message_id=5,
                                       reaction=[{'type': 'emoji', 'emoji': '👀'}])
                self.assertEqual((raised.exception.code, raised.exception.reason),
                                 ('network-or-invalid-response', reason))

    async def test_file_fetch_failures_keep_the_status_or_timeout_code(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('secret-test-token')
            api = Telegram(path)
            url = 'https://api.telegram.org/file/botsecret-test-token/photos/a.jpg'
            cases = [(urllib.error.HTTPError(url, 503, url, {}, io.BytesIO()), 503),
                     (urllib.error.URLError(socket.timeout('timed out')), 'file-download-timeout'),
                     (socket.timeout('timed out'), 'file-download-timeout'),
                     (urllib.error.URLError('refused ' + url), 'file-download-failed')]
            for error, code in cases:
                opener = type('Opener', (), {'open': lambda self, request, timeout, error=error: (_ for _ in ()).throw(error)})()
                with patch.object(api, '_call', return_value={'file_path': 'photos/a.jpg'}), \
                        patch('urllib.request.build_opener', return_value=opener) as builder:
                    with self.assertRaises(TelegramError) as caught:
                        await api.download_file('file', 100)
                self.assertEqual(caught.exception.code, code)
                self.assertNotIn('secret-test-token', str(caught.exception))
                self.assertTrue(any(isinstance(handler, transport.TelegramHTTPSHandler)
                                    for handler in builder.call_args.args))

    async def test_file_path_is_encoded_once_without_encoding_the_token(self):
        api = object.__new__(Telegram)
        api._token = '123:test-token'
        cases = [
            ('documents/file_42. Q3,',
             'https://api.telegram.org/file/bot123:test-token/documents/file_42.%20Q3%2C'),
            ("documents/file_42. Q3,'#?%20é",
             'https://api.telegram.org/file/bot123:test-token/documents/file_42.%20Q3%2C%27%23%3F%2520%C3%A9'),
        ]
        for file_path, expected_url in cases:
            with self.subTest(file_path=file_path):
                download_opener = Mock()
                download_opener.open.return_value = io.BytesIO(b'document')
                with patch.object(api, '_call', return_value={'file_path': file_path, 'file_size': 8}), \
                        patch('urllib.request.build_opener', return_value=download_opener):
                    self.assertEqual(await api.download_file('file', 100), b'document')
                download_opener.open.assert_called_once()
                self.assertEqual(download_opener.open.call_args.args[0].full_url, expected_url)

    async def test_invalid_file_paths_are_rejected_without_a_download_request(self):
        api = object.__new__(Telegram)
        paths = [None, '', 42, b'documents/file', {}, [], '/documents/file', '//documents/file',
                 '..', '../file', 'documents/../file', 'documents/file/..', 'documents\\file']
        paths.extend('documents/file' + chr(code) for code in list(range(32)) + list(range(127, 160)))
        for file_path in paths:
            with self.subTest(file_path=file_path), \
                    patch.object(api, '_call', return_value={'file_path': file_path}), \
                    patch('urllib.request.build_opener') as build_opener:
                with self.assertRaises(TelegramError) as caught:
                    await api.download_file('file', 100)
                self.assertEqual(caught.exception.code, 'invalid-file-path')
                build_opener.assert_not_called()

    async def test_bad_file_metadata_is_rejected_without_a_download_request(self):
        api = object.__new__(Telegram)
        metadata = [None, [], 'file', 42, {}, {'file_path': 'documents/file', 'file_size': True},
                    {'file_path': 'documents/file', 'file_size': '8'},
                    {'file_path': 'documents/file', 'file_size': -1}]
        for info in metadata:
            with self.subTest(info=info), patch.object(api, '_call', return_value=info), \
                    patch('urllib.request.build_opener') as build_opener:
                with self.assertRaises(TelegramError) as caught:
                    await api.download_file('file', 100)
                self.assertEqual(caught.exception.code, 'invalid-file-path')
                build_opener.assert_not_called()

    async def test_file_size_metadata_over_the_limit_is_rejected_without_a_download_request(self):
        api = object.__new__(Telegram)
        for info in ({'file_path': 'documents/file', 'file_size': 101}, {'file_size': 101}):
            with self.subTest(info=info), patch.object(api, '_call', return_value=info), \
                    patch('urllib.request.build_opener') as build_opener:
                with self.assertRaises(TelegramError) as caught:
                    await api.download_file('file', 100)
                self.assertEqual(caught.exception.code, 'file-too-large')
                build_opener.assert_not_called()

    async def test_oversized_and_empty_downloads_have_distinct_codes(self):
        api = object.__new__(Telegram)
        api._token = 'test-token'
        for body, code in ((b'four', 'file-too-large'), (b'', 'empty-file-download')):
            with self.subTest(code=code):
                download_opener = Mock()
                download_opener.open.return_value = io.BytesIO(body)
                with patch.object(api, '_call', return_value={'file_path': 'documents/file', 'file_size': 3}), \
                        patch('urllib.request.build_opener', return_value=download_opener):
                    with self.assertRaises(TelegramError) as caught:
                        await api.download_file('file', 3)
                self.assertEqual(caught.exception.code, code)

    def test_slow_call_logs_method_only_and_skips_get_updates(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('secret-test-token')
            api = Telegram(path)
            response = b'{"ok":true,"result":{}}'
            with patch('coordinator.telegram.opener.open', side_effect=[io.BytesIO(response),
                                                                         io.BytesIO(response)]), \
                    patch('coordinator.telegram.time') as clock, \
                    patch.object(transport.logger, 'info') as log:
                clock.monotonic.side_effect = [0, 5.1, 10, 30]
                api._call('getMe', {})
                api._call('getUpdates', {})
            log.assert_called_once_with('telegram call slow method=%s seconds=%.1f', 'getMe', 5.1)

    async def test_image_delivery_selects_photo_or_document(self):
        with tempfile.TemporaryDirectory() as temp:
            token = Path(temp) / 'token'
            token.write_text('test-token')
            api = Telegram(token)
            photo = Path(temp) / 'photo.webp'
            document = Path(temp) / 'report.pdf'
            large_photo = Path(temp) / 'large.png'
            photo.write_bytes(b'photo')
            document.write_bytes(b'document')
            with large_photo.open('wb') as output:
                output.truncate(10 * 1024 * 1024 + 1)
            with patch.object(api, 'call_multipart', return_value={'message_id': 12}) as call:
                for image, method in ((photo, 'sendPhoto'), (document, 'sendDocument'),
                                      (large_photo, 'sendDocument')):
                    await api.send({'chat': -42, 'thread': 7, 'text': 'Attached',
                                    'image': str(image), 'reply_to': 9})
                    self.assertEqual(call.call_args.args[0], method)
                    self.assertEqual(call.call_args.args[1]['caption'], 'Attached')
                    self.assertEqual(call.call_args.args[1]['message_thread_id'], 7)
                    self.assertEqual(call.call_args.args[1]['reply_parameters']['message_id'], 9)
                    self.assertEqual(call.call_args.args[1]['parse_mode'], 'HTML')

    async def test_as_document_forces_document_for_small_photo(self):
        with tempfile.TemporaryDirectory() as temp:
            token = Path(temp) / 'token'
            token.write_text('test-token')
            api = Telegram(token)
            photo = Path(temp) / 'original.jpg'
            photo.write_bytes(b'jpeg')
            with patch.object(api, 'call_multipart', return_value={'message_id': 12}) as call:
                await api.send({'chat': -42, 'thread': 7, 'text': 'Attached', 'image': str(photo),
                                'as_document': 1})
                self.assertEqual(call.call_args.args[0], 'sendDocument')
                self.assertEqual(call.call_args.args[2], str(photo))
                await api.send({'chat': -42, 'thread': 7, 'text': 'Attached', 'image': str(photo),
                                'as_document': 0})
                self.assertEqual(call.call_args.args[0], 'sendPhoto')

    async def test_caption_uses_html_and_400_falls_back_once(self):
        with tempfile.TemporaryDirectory() as temp:
            token = Path(temp) / 'token'
            token.write_text('test-token')
            photo = Path(temp) / 'photo.png'
            photo.write_bytes(b'photo')
            api = Telegram(token)
            with patch.object(api, 'call_multipart', side_effect=[TelegramError(400),
                                                                   {'message_id': 12}]) as call:
                await api.send({'chat': -42, 'thread': 7, 'text': '**Photo** & more',
                                'image': str(photo)})
                self.assertEqual(call.call_count, 2)
                self.assertEqual(call.call_args_list[0].args[1]['caption'], '<b>Photo</b> &amp; more')
                self.assertEqual(call.call_args_list[0].args[1]['parse_mode'], 'HTML')
                self.assertEqual(call.call_args_list[1].args[1]['caption'], 'Photo & more')
                self.assertNotIn('parse_mode', call.call_args_list[1].args[1])
