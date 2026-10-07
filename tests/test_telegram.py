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

    async def test_html_400_falls_back_once_to_original_markdown(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'token'
            path.write_text('test-token')
            api = Telegram(path)
            with patch.object(api, 'call', side_effect=[TelegramError(400), {'message_id': 12}]) as call:
                await api.send({'chat': -42, 'thread': 7, 'text': '**bold**'})
                self.assertEqual(call.call_count, 2)
                self.assertEqual(call.call_args_list[0].kwargs['text'], '<b>bold</b>')
                self.assertEqual(call.call_args_list[0].kwargs['parse_mode'], 'HTML')
                self.assertEqual(call.call_args_list[1].kwargs['text'], '**bold**')
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
                self.assertEqual(call.call_args_list[1].args[1]['caption'], '**Photo** & more')
                self.assertNotIn('parse_mode', call.call_args_list[1].args[1])
