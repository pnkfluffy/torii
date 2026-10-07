import re
import unittest

from coordinator.formatting import entities_to_markdown, markdown_to_html


class EntityTests(unittest.TestCase):
    def test_utf16_offsets_nested_and_adjacent_entities(self):
        text = '😀big mix link'
        entities = [
            {'type': 'bold', 'offset': 2, 'length': 7},
            {'type': 'italic', 'offset': 6, 'length': 3},
            {'type': 'text_link', 'offset': 10, 'length': 4, 'url': 'https://example.com/a'},
        ]
        self.assertEqual(entities_to_markdown(text, entities),
                         '😀**big *mix*** [link](https://example.com/a)')

    def test_supported_entities_and_plain_url(self):
        cases = [('underline', '__x__'), ('strikethrough', '~~x~~'),
                 ('code', '`x`'), ('spoiler', '||x||'), ('blockquote', '> x')]
        for kind, expected in cases:
            with self.subTest(kind=kind):
                self.assertEqual(entities_to_markdown('x', [{'type': kind, 'offset': 0,
                                                              'length': 1}]), expected)
        self.assertEqual(entities_to_markdown('x', [{'type': 'pre', 'offset': 0,
                                                    'length': 1, 'language': 'python'}]),
                         '```python\nx\n```')
        self.assertEqual(entities_to_markdown('https://x.test',
                                               [{'type': 'url', 'offset': 0, 'length': 14}]),
                         'https://x.test')


class MarkdownTests(unittest.TestCase):
    def test_inline_styles_links_and_escaping(self):
        source = '**bold** __bold__ *italic* _italic_ ~~strike~~ [a & b](https://x.test/?a=1&b=2)'
        self.assertEqual(markdown_to_html(source),
                         '<b>bold</b> <b>bold</b> <i>italic</i> <i>italic</i> <s>strike</s> '
                         '<a href="https://x.test/?a=1&amp;b=2">a &amp; b</a>')
        self.assertEqual(markdown_to_html('A < B & C > D'), 'A &lt; B &amp; C &gt; D')

    def test_code_is_literal_and_unclosed_markup_is_text(self):
        self.assertEqual(markdown_to_html('`**x** & <`'),
                         '<code>**x** &amp; &lt;</code>')
        self.assertEqual(markdown_to_html('```python\n**x** & <\n```'),
                         '<pre><code class="language-python">**x** &amp; &lt;</code></pre>')
        self.assertEqual(markdown_to_html('unclosed **bold'), 'unclosed **bold')

    def test_lines_quotes_bullets_headings_and_tables(self):
        self.assertEqual(markdown_to_html('# Head\n- one\n* two\n> A\n> *B*'),
                         '<b>Head</b>\n• one\n• two\n<blockquote>A\n<i>B</i></blockquote>')
        self.assertEqual(markdown_to_html('| A | B |\n| --- | --- |\n| x | y |'),
                         '<pre>| A | B |\n| --- | --- |\n| x | y |</pre>')

    def test_underscores_inside_words_are_literal_and_tags_balance(self):
        source = 'snake_case foo__bar__baz **bold *italic***'
        rendered = markdown_to_html(source)
        self.assertEqual(rendered, 'snake_case foo__bar__baz <b>bold <i>italic</i></b>')
        stack = []
        for match in re.finditer(r'<(/?)([a-z-]+)(?: [^>]*)?>', rendered):
            closing, tag = match.group(1, 2)
            if closing:
                self.assertEqual(stack.pop(), tag)
            else:
                stack.append(tag)
        self.assertEqual(stack, [])
