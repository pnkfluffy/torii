"""Convert Telegram entities and coordinator Markdown at the transport boundary."""

import html
import re


FENCE = re.compile(r'^[ \t]*(`{3,}|~{3,})(?:[ \t]*([A-Za-z0-9_+.-]+))?[ \t]*$')
HEADING = re.compile(r'^#{1,6}[ \t]+(.+?)\s*#*\s*$')
TABLE_RULE = re.compile(r'^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$')


def _utf16_index(text):
    indexes = [0]
    for position, char in enumerate(text):
        indexes.extend([position + 1] * (2 if ord(char) > 0xffff else 1))
    return indexes


def entities_to_markdown(text, entities):
    """Render the supported Telegram entities using UTF-16 offsets."""
    if not entities:
        return text
    indexes = _utf16_index(text)
    nodes = []
    supported = {'bold', 'italic', 'underline', 'strikethrough', 'code', 'pre',
                 'text_link', 'blockquote', 'expandable_blockquote', 'spoiler'}
    for entity in entities:
        if not isinstance(entity, dict) or entity.get('type') not in supported:
            continue
        offset, length = entity.get('offset'), entity.get('length')
        if (type(offset) is not int or type(length) is not int or offset < 0 or length <= 0
                or offset + length >= len(indexes)):
            continue
        start, end = indexes[offset], indexes[offset + length]
        if start < end:
            nodes.append({'start': start, 'end': end, 'entity': entity, 'children': []})
    nodes.sort(key=lambda node: (node['start'], -node['end']))
    roots = []
    stack = []
    for node in nodes:
        while stack and node['start'] >= stack[-1]['end']:
            stack.pop()
        if stack and node['end'] > stack[-1]['end']:
            continue
        (stack[-1]['children'] if stack else roots).append(node)
        stack.append(node)

    def render(start, end, children):
        output = []
        cursor = start
        for node in children:
            output.append(text[cursor:node['start']])
            kind = node['entity']['type']
            if kind in ('code', 'pre'):
                content = text[node['start']:node['end']]
            else:
                content = render(node['start'], node['end'], node['children'])
            if kind == 'bold':
                content = '**' + content + '**'
            elif kind == 'italic':
                content = '*' + content + '*'
            elif kind == 'underline':
                content = '__' + content + '__'
            elif kind == 'strikethrough':
                content = '~~' + content + '~~'
            elif kind == 'code':
                ticks = '`' * (max([len(run) for run in re.findall(r'`+', content)] or [0]) + 1)
                content = ticks + content + ticks
            elif kind == 'pre':
                fence = '`' * max(3, max([len(run) for run in re.findall(r'`+', content)] or [0]) + 1)
                language = node['entity'].get('language', '')
                language = language if isinstance(language, str) and re.fullmatch(r'[A-Za-z0-9_+.-]+', language) else ''
                content = fence + language + '\n' + content + '\n' + fence
            elif kind == 'text_link':
                url = node['entity'].get('url', '')
                url = url.replace('\\', '\\\\').replace(')', '\\)') if isinstance(url, str) else ''
                content = '[' + content + '](' + url + ')' if url else content
            elif kind in ('blockquote', 'expandable_blockquote'):
                content = '\n'.join('> ' + line for line in content.split('\n'))
            elif kind == 'spoiler':
                content = '||' + content + '||'
            output.append(content)
            cursor = node['end']
        output.append(text[cursor:end])
        return ''.join(output)

    return render(0, len(text), roots)


def _closing(text, marker, start):
    position = start
    while True:
        position = text.find(marker, position)
        if position < 0:
            return -1
        if position > 0 and text[position - 1] == '\\':
            position += len(marker)
            continue
        if marker == '**' and text.startswith('***', position):
            position += 1
            continue
        if marker in ('_', '__') and position + len(marker) < len(text):
            if text[position + len(marker)].isalnum():
                position += len(marker)
                continue
        return position


def _inline(text):
    if not re.search(r'[\\`[*_~|]', text):
        return html.escape(text, quote=False)
    output = []
    index = 0
    while index < len(text):
        if text[index] == '\\' and index + 1 < len(text):
            output.append(html.escape(text[index + 1], quote=False))
            index += 2
            continue
        if text[index] == '`':
            width = len(text[index:]) - len(text[index:].lstrip('`'))
            marker = '`' * width
            close = text.find(marker, index + width)
            if close >= 0:
                output.append('<code>' + html.escape(text[index + width:close]) + '</code>')
                index = close + width
                continue
        if text[index] == '[':
            match = re.match(r'\[([^\]\n]+)\]\(((?:\\.|[^)\n])+)\)', text[index:])
            if match:
                label, url = match.groups()
                output.append('<a href="' + html.escape(url.replace('\\)', ')'), quote=True) + '">'
                              + _inline(label) + '</a>')
                index += match.end()
                continue
        matched = False
        for marker, tag in (('**', 'b'), ('__', 'b'), ('~~', 's'), ('||', 'tg-spoiler'),
                            ('*', 'i'), ('_', 'i')):
            if not text.startswith(marker, index):
                continue
            if marker in ('_', '__') and index and (text[index - 1].isalnum() or
                                                     text[index - 1] == '_'):
                continue
            close = _closing(text, marker, index + len(marker))
            if close <= index + len(marker):
                continue
            output.append('<' + tag + '>' + _inline(text[index + len(marker):close]) + '</' + tag + '>')
            index = close + len(marker)
            matched = True
            break
        if matched:
            continue
        output.append(html.escape(text[index], quote=False))
        index += 1
    return ''.join(output)


def markdown_to_html(markdown):
    """Render a small Markdown subset with balanced Telegram HTML tags."""
    lines = markdown.split('\n')
    output = []
    index = 0
    while index < len(lines):
        fence = FENCE.fullmatch(lines[index])
        if fence:
            marker, language = fence.groups()
            index += 1
            content = []
            while index < len(lines):
                closing = FENCE.fullmatch(lines[index])
                if closing and closing.group(1)[0] == marker[0] and len(closing.group(1)) >= len(marker):
                    index += 1
                    break
                content.append(lines[index])
                index += 1
            attribute = ' class="language-' + html.escape(language, quote=True) + '"' if language else ''
            output.append('<pre><code' + attribute + '>' + html.escape('\n'.join(content)) + '</code></pre>')
            continue
        if (index + 1 < len(lines) and '|' in lines[index]
                and TABLE_RULE.fullmatch(lines[index + 1])):
            content = [lines[index], lines[index + 1]]
            index += 2
            while index < len(lines) and '|' in lines[index] and lines[index].strip():
                content.append(lines[index])
                index += 1
            output.append('<pre>' + html.escape('\n'.join(content)) + '</pre>')
            continue
        if lines[index].startswith('> '):
            content = []
            while index < len(lines) and lines[index].startswith('> '):
                content.append(_inline(lines[index][2:]))
                index += 1
            output.append('<blockquote>' + '\n'.join(content) + '</blockquote>')
            continue
        heading = HEADING.fullmatch(lines[index])
        if heading:
            output.append('<b>' + _inline(heading.group(1)) + '</b>')
        elif lines[index].startswith(('- ', '* ')):
            output.append('• ' + _inline(lines[index][2:]))
        else:
            output.append(_inline(lines[index]))
        index += 1
    return '\n'.join(output)
