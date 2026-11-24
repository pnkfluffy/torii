#!/usr/bin/env python3
"""PreToolUse guard: refuse tool calls that would print a secret file or a secret variable.

Reads the hook event JSON on stdin (Claude Code and Codex use the same shape:
``{"tool_name": ..., "tool_input": {...}, "cwd": ...}``). On a hit it writes one
line to stderr and exits 2, which blocks the call and shows the reason to the
agent. Everything else exits 0 and prints nothing. A guard bug fails open and
says so on stderr.

Scope:
  protected files   legacy provider account homes, ~/.config/torii-secrets/**, ~/.config/*-router/**,
                    ~/.config/*-ci/* (top level) and any *token* file below it,
                    *.token, *.pem, *.key, *.p12, .env*, the coordinator vault
                    (envelope/master.key, envelope/vault.enc), bot-token,
                    gh hosts.yml, .netrc, .npmrc, .aws/credentials, .pgpass,
                    .claude*/.credentials.json, .claude.json, .codex/auth.json,
                    .wrangler/config/*, any path with a /secrets/ segment, and any
                    file whose name has a token/secret/credential word.
  blocked           cat head tail less more bat strings xxd od hexdump nl tac base64
                    sed awk cut tr sort uniq diff cmp vim vi nano emacs open pbcopy
                    python/node/perl/ruby/php one-liners naming a protected file,
                    grep without -c/-q/-l/-L, jq without a key-names filter,
                    plutil -p / plutil -convert on a LaunchAgents plist (unless piped
                    into jq keys), printenv, bare env, env piped, set -x, sh -x,
                    curl -v/--trace*, echo/printf of $NAME where NAME looks like a
                    secret, launchctl getenv/print of a secret-looking name,
                    security find-*-password -w / dump-keychain / export,
                    $(cat FILE) anywhere except a plain NAME=$(cat FILE) capture.
  allowed           stat test ls file du wc shasum chmod chown cp mv install rm touch
                    mkdir ln readlink realpath gitleaks find (without -exec reader),
                    grep -c/-q/-l/-L, jq 'keys' / 'length' / 'type' / 'has(..)',
                    NAME=$(cat FILE) with no echo/printf/tee in the same command.
The Read tool is refused for protected paths; the Grep tool is refused on a
protected path unless its output mode is count or files_with_matches.
Write calls and Codex patches go to the optional `docs` module of this checkout, which
refuses a new plan-like file in a repository. Shell calls go to the optional `wait_guard`
module, which refuses shell wait loops.
"""
import glob
import json
import os
import re
import shlex
import sys

HOMES = [os.path.expanduser('~')]
_env_home = os.environ.get('HOME')
if _env_home and _env_home not in HOMES:
    HOMES.append(_env_home)

PROTECTED_DIR_SUFFIXES = ('/.config/torii-secrets', '/.muse-accounts')
PROTECTED_CONFIG_DIR = re.compile(r'/\.config/[^/]+-router(?:/|$)')
PROTECTED_CI_DIR = re.compile(r'/\.config/[^/]+-ci(?:/(.*))?$')
PROTECTED_FILE_SUFFIXES = (
    '/.config/telegram-agent-coordinator/bot-token',
    '/.config/gh/hosts.yml',
    '/.netrc', '/.npmrc', '/.aws/credentials', '/.pgpass',
    '/.codex/auth.json', '/.credentials.json', '/.claude.json',
    '/envelope/master.key', '/envelope/vault.enc',
)
PROTECTED_PATH_PARTS = ('/secrets/', '/envelope/', '/.wrangler/config/')
PROTECTED_EXTENSIONS = ('.token', '.pem', '.key', '.p12', '.pfx', '.keystore', '.jks')
CODE_EXTENSIONS = ('.py', '.mjs', '.cjs', '.js', '.ts', '.tsx', '.md', '.sh', '.go', '.rs', '.rb', '.toml', '.yml', '.yaml', '.txt', '.html', '.css')
TOKEN_BASENAME = re.compile(r'(^|[-_.])(tokens?|secrets?|credentials?)([-_.]|$)', re.I)
ENV_BASENAME = re.compile(r'^\.env($|[.-])')
SECRET_WORDS = r'TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|APIKEY|PRIVATE_KEY|ACCESS_KEY|CREDENTIAL'
PATH_IN_TEXT = re.compile(r'(?:~|\$\{?HOME\}?|/)[^\s\'"()<>|;&`,]*')
SUBSHELL = re.compile(r'\$\(([^()]*)\)|`([^`]*)`')

READERS = {
    'cat', 'head', 'tail', 'less', 'more', 'bat', 'strings', 'xxd', 'od', 'hexdump', 'nl', 'tac', 'rev',
    'base64', 'sed', 'awk', 'gawk', 'cut', 'tr', 'sort', 'uniq', 'diff', 'cmp', 'colordiff', 'vim', 'vi', 'nano',
    'emacs', 'open', 'pbcopy', 'tee', 'paste', 'fold', 'fmt', 'pr', 'expand', 'unexpand', 'iconv', 'openssl',
    'python', 'python3', 'node', 'perl', 'ruby', 'php', 'deno', 'bun', 'yq', 'curl', 'wget', 'scp', 'rsync', 'ssh',
    'source', '.', 'sqlite3', 'tar', 'zip', 'gzip', 'bzip2', 'xz', 'zstd', 'mail', 'sendmail', 'nc', 'telnet',
    'plutil', 'defaults', 'security', 'gh', 'op', 'jq',
}
SAFE = {
    'stat', 'test', '[', 'ls', 'file', 'du', 'wc', 'shasum', 'sha256sum', 'md5', 'md5sum', 'chmod', 'chown',
    'cp', 'mv', 'install', 'rm', 'touch', 'mkdir', 'ln', 'readlink', 'realpath', 'gitleaks', 'find', 'basename',
    'dirname', 'true', 'false', 'echo', 'printf', 'cd', 'pushd', 'popd', 'export', 'unset', 'exit', 'return',
    'mktemp', 'date', 'id', 'pwd', 'which', 'command', 'type', 'launchctl', 'git', 'sleep', 'xargs', 'env',
    'sudo', 'nohup', 'time', 'nice', 'ionice', 'caffeinate', 'trash', 'chflags', 'xattr', 'umask', 'set', 'local',
}
WRAPPERS = {'sudo', 'nohup', 'time', 'nice', 'ionice', 'caffeinate', 'command', 'exec', 'xargs', 'env', 'busybox'}
GREP_LIKE = {'grep', 'egrep', 'fgrep', 'zgrep', 'rg', 'ag', 'ack'}
COPIERS = {'cp', 'mv', 'install', 'rsync', 'scp', 'ditto'}


def expand(token, cwd):
    value = token.strip('\'"')
    for prefix in ('${HOME}', '$HOME'):
        if value.startswith(prefix):
            value = HOMES[0] + value[len(prefix):]
            break
    if value == '~' or value.startswith('~/'):
        value = HOMES[0] + value[1:]
    if value.startswith('~'):
        return value
    if not value.startswith('/') and cwd and ('/' in value or value.startswith('.')):
        value = os.path.join(cwd, value)
    return value


def protected_path(path):
    """True when ``path`` (already expanded) names a protected file or directory."""
    candidates = {path}
    try:
        candidates.add(os.path.realpath(path))
    except (OSError, ValueError):
        pass
    for candidate in candidates:
        normalized = candidate.rstrip('/')
        basename = os.path.basename(normalized)
        for root in PROTECTED_DIR_SUFFIXES:
            if normalized.endswith(root) or root + '/' in normalized:
                return True
        if PROTECTED_CONFIG_DIR.search(normalized):
            return True
        ci = PROTECTED_CI_DIR.search(normalized)
        if ci and (ci.group(1) is None or '/' not in ci.group(1) or TOKEN_BASENAME.search(basename)):
            return True
        if re.search(r'/[^/]+-watch/control$', normalized):
            return True
        if normalized.endswith(PROTECTED_FILE_SUFFIXES):
            return True
        if any(part in normalized + '/' for part in PROTECTED_PATH_PARTS):
            return True
        if normalized.lower().endswith(PROTECTED_EXTENSIONS):
            return True
        if ENV_BASENAME.match(basename):
            return True
        if TOKEN_BASENAME.search(basename) and not basename.lower().endswith(CODE_EXTENSIONS):
            return True
    return False


def path_candidates(text, cwd):
    """Every path-looking piece of a command: shell words and paths embedded in code."""
    found = []
    cwd_protected = bool(cwd) and protected_path(cwd)
    try:
        words = shlex.split(text, posix=True)
    except ValueError:
        words = text.replace('"', ' ').replace("'", ' ').split()
    for word in words:
        if ('/' in word or word.startswith('~') or ENV_BASENAME.match(word) or word.lower().endswith(PROTECTED_EXTENSIONS)
                or (cwd_protected and not word.startswith('-') and '=' not in word)):
            found.append(word)
    seen = set(found)
    for match in PATH_IN_TEXT.findall(text):
        if len(match) > 1 and match not in seen:
            found.append(match)
            seen.add(match)
    expanded = []
    for item in found:
        value = expand(item, cwd)
        if any(ch in value for ch in '*?['):
            matches = glob.glob(os.path.expanduser(value)) or [value.split('*')[0].rstrip('/') or value]
            expanded.extend(matches)
        else:
            expanded.append(value)
    return expanded


def protected_hits(text, cwd):
    return [path for path in path_candidates(text, cwd) if protected_path(path)]


def split_commands(command):
    """Split a shell line into simple commands on ; && || | newline, keeping $( ) and quotes."""
    parts, current, depth, i, quote = [], '', 0, 0, None
    while i < len(command):
        ch = command[i]
        if quote:
            current += ch
            if ch == quote and command[i - 1] != '\\':
                quote = None
        elif ch in ('"', "'"):
            quote = ch
            current += ch
        elif command.startswith('$(', i):
            depth += 1
            current += '$('
            i += 2
            continue
        elif ch == ')' and depth:
            depth -= 1
            current += ch
        elif depth == 0 and (ch in ';\n|' or command.startswith('&&', i)):
            parts.append(current)
            current = ''
            if command.startswith('&&', i) or command.startswith('||', i):
                i += 1
        else:
            current += ch
        i += 1
    parts.append(current)
    return [part.strip() for part in parts if part.strip()]


def tokens_of(text):
    try:
        return shlex.split(text, posix=True)
    except ValueError:
        return text.replace('"', ' ').replace("'", ' ').split()


def program_of(tokens):
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*=', token):
            index += 1
            continue
        name = os.path.basename(token)
        if name in WRAPPERS:
            index += 1
            while index < len(tokens) and tokens[index].startswith('-'):
                index += 1
            continue
        return name, tokens[index + 1:]
    return '', []


def grep_prints_content(args):
    for arg in args:
        if arg == '--':
            break
        if arg.startswith('--'):
            if arg in ('--count', '--quiet', '--silent', '--files-with-matches', '--files-without-match'):
                return False
            continue
        if arg.startswith('-') and len(arg) > 1 and any(flag in arg[1:] for flag in 'cqlL'):
            return False
    return True


JQ_SAFE = re.compile(r'^\s*(?:keys(?:_unsorted)?(?:\[\])?|length|type|has\("[^"\n]*"\))\s*$')


def jq_prints_content(args):
    for arg in args:
        if arg.startswith('-'):
            continue
        return not JQ_SAFE.match(arg)
    return True


def assignment_capture(simple):
    """NAME=$(...) or NAME=`...` alone on the command, nothing else printing."""
    return re.match(r'^\s*(export\s+|local\s+)?[A-Za-z_][A-Za-z0-9_]*=(\$\([^()]*\)|`[^`]*`)\s*$', simple) is not None


def reader_verdict(program, args, text, cwd):
    """Reason string when ``program`` applied to a protected path in ``text`` prints its content."""
    if program == 'gh' and args[:2] == ['pr', 'create']:
        filtered = []
        skip = False
        for arg in args:
            if skip:
                skip = False
                continue
            if arg in ('--body', '--title', '-b', '-t'):
                skip = True
            elif not arg.startswith(('--body=', '--title=')):
                filtered.append(arg)
        text = ' '.join(shlex.quote(arg) for arg in filtered)
    if program in GREP_LIKE:
        positional = [arg for arg in args if not arg.startswith('-')]
        text = ' '.join(shlex.quote(arg) for arg in positional[1:])
    if program == 'cat' and re.search(r'(?<!<)>{1,2}\s*[^\s]+', text):
        text = re.sub(r'(?<!<)>{1,2}\s*[^\s]+', '', text)
    hits = protected_hits(text, cwd)
    if not hits:
        return None
    if program in GREP_LIKE:
        return 'grep would print lines of %s (use -c, -q or -l)' % hits[0] if grep_prints_content(args) else None
    if program == 'jq':
        return 'jq would print values of %s (use keys)' % hits[0] if jq_prints_content(args) else None
    if program == 'find':
        for flag in ('-exec', '-execdir', '-ok', '-okdir'):
            if flag in args:
                inner = program_of(args[args.index(flag) + 1:])[0]
                if inner in READERS or inner in GREP_LIKE:
                    return 'find %s %s would print %s' % (flag, inner, hits[0])
        return None
    if program in COPIERS:
        targets = [arg for arg in args if not arg.startswith('-')]
        destination = expand(targets[-1], cwd) if targets else ''
        to_trash = any(destination.startswith(home + trash) for home in HOMES
                       for trash in ('/.Trash/', '/.local/share/Trash/files/'))
        if len(targets) >= 2 and not to_trash and protected_hits(' '.join(shlex.quote(t) for t in targets[:-1]), cwd) and not protected_hits(shlex.quote(targets[-1]), cwd):
            return '%s would copy %s to an unprotected place' % (program, hits[0])
        return None
    if program in SAFE:
        return None
    return '%s would print %s' % (program or 'command', hits[0])


def strip_heredocs(command):
    lines = command.splitlines(keepends=True)
    result = []
    delimiters = []
    for line in lines:
        if delimiters:
            if line.strip() == delimiters[0]:
                delimiters.pop(0)
            continue
        result.append(line)
        delimiters.extend(match.group(2) for match in re.finditer(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z_0-9]*)\1", line))
    return ''.join(result)


def check_bash(command, cwd):
    if len(command) > 65536:
        return None
    command = strip_heredocs(command)
    if re.search(r'(^|[;&|\s])set\s+-[a-zA-Z]*x', command) or re.search(r'(^|[;&|\s])(ba|z|da)?sh\s+-[a-zA-Z]*x\b', command):
        return 'shell tracing (set -x / sh -x) would print expanded secrets'
    if re.search(r'(^|[;&|\s(])(echo|printf)\b[^;&|\n]*\$\{?[A-Za-z_0-9]*(' + SECRET_WORDS + r')', command, re.I):
        return 'echo/printf of a secret-looking variable'
    if re.search(r'(^|[;&|\s(])(curl|wget)\b[^;&|\n]*\s(-[a-zA-Z]*v[a-zA-Z]*|--verbose|--trace(-ascii)?(\s|=|$)|--debug)', command):
        return 'curl -v/--trace prints request headers including Authorization'
    if re.search(r'(^|[;&|\s(])security\s+(find-(generic|internet)-password\b[^;&|\n]*\s-[a-zA-Z]*[wg]|dump-keychain|export)\b', command):
        return 'Keychain dump would print a stored secret'
    if re.search(r'(^|[;&|\s(])launchctl\s+(print|getenv)\b[^\n]*(' + SECRET_WORDS + r')', command, re.I):
        return 'launchctl print/getenv of a secret-looking variable'
    if re.search(r'(^|[;&|\s(])gh\s+auth\s+token\b', command) or re.search(r'(^|[;&|\s(])op\s+(read|item\s+get)\b', command):
        return 'gh auth token / op read prints a credential'
    if (re.search(r'(^|[;&|\s(])(plutil\s+(-p|-convert)|defaults\s+(read|export))\b[^;&|\n]*(LaunchAgents|LaunchDaemons)', command)
            and not re.search(r'\|\s*jq\b[^;&\n]*(keys|paths)', command)):
        return 'plutil/defaults output of a launchd plist can print environment secrets; pipe into jq keys'
    in_protected_dir = False
    for simple in split_commands(command):
        tokens = tokens_of(simple)
        if not tokens:
            continue
        program, args = program_of(tokens)
        first = os.path.basename(tokens[0])
        if program == 'printenv' or (first == 'env' and not program):
            return 'environment command prints secrets'
        if program in ('bash', 'sh', 'zsh', 'dash'):
            for index, arg in enumerate(args):
                if arg.startswith('-') and 'c' in arg[1:] and index + 1 < len(args):
                    reason = check_bash(args[index + 1], cwd)
                    if reason:
                        return reason
                    break
        if program in ('cd', 'pushd') and args and protected_hits(args[0], cwd):
            in_protected_dir = True
        for group in SUBSHELL.findall(simple):
            inner_text = group[0] or group[1]
            inner_tokens = tokens_of(inner_text)
            if not inner_tokens:
                continue
            inner_program, inner_args = program_of(inner_tokens)
            reason = reader_verdict(inner_program, inner_args, inner_text, cwd)
            if reason and not (assignment_capture(simple) and not re.search(r'\b(echo|printf|tee)\b', simple)):
                return reason + ' (only NAME=$(cat FILE) captures are allowed)'
        outer_text = SUBSHELL.sub(' ', simple)
        reason = reader_verdict(program, args, outer_text, cwd)
        if reason:
            return reason
        if in_protected_dir and (program in READERS or program in GREP_LIKE) and not protected_hits(outer_text, cwd):
            if program in GREP_LIKE and not grep_prints_content(args):
                continue
            if program == 'jq' and not jq_prints_content(args):
                continue
            return '%s inside a protected directory (after cd) would print a secret file' % program
    return None


def check_read(tool_input):
    path = tool_input.get('file_path') or tool_input.get('path') or ''
    if path and protected_path(expand(path, None)):
        return 'Read of a protected secret file: %s' % path
    return None


def check_grep(tool_input):
    path = tool_input.get('path') or ''
    mode = tool_input.get('output_mode') or 'files_with_matches'
    if path and protected_path(expand(path, None)) and mode == 'content':
        return 'Grep content mode on a protected secret path: %s (use count or files_with_matches)' % path
    return None


def bash_command(event):
    """The shell command text of a Claude Bash or Codex shell call, or None for other tools."""
    if (event.get('tool_name') or '') not in ('Bash', 'exec_command', 'unified_exec', 'shell', 'local_shell', 'shell_command'):
        return None
    tool_input = event.get('tool_input') or {}
    command = tool_input.get('command') or tool_input.get('cmd') or ''
    if isinstance(command, list):
        command = ' '.join(shlex.quote(str(part)) for part in command)
    return str(command)


def decide(event):
    tool = event.get('tool_name') or ''
    tool_input = event.get('tool_input') or {}
    cwd = event.get('cwd') or tool_input.get('cwd') or os.getcwd()
    command = bash_command(event)
    if command is not None:
        return check_bash(command, cwd)
    if tool == 'Read':
        return check_read(tool_input)
    if tool == 'Grep':
        return check_grep(tool_input)
    return None


def extension_denial(event):
    """The refusal from this checkout's optional modules: `docs` for a new plan-like file in a
    repository, `wait_guard` for a shell wait loop."""
    name, check = (('docs', 'write_denial') if event.get('tool_name') in ('Write', 'apply_patch')
                   else ('wait_guard', 'denial') if bash_command(event) is not None else (None, None))
    if name is None:
        return None
    root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    import importlib
    try:
        module = importlib.import_module('coordinator.' + name)
    except ModuleNotFoundError as error:
        if error.name != 'coordinator.' + name:
            raise
        return None
    return getattr(module, check)(event)


def main():
    try:
        event = json.load(sys.stdin)
    except (ValueError, OSError):
        return 0
    try:
        reason = decide(event)
        denial = None if reason else extension_denial(event)
    except Exception as error:
        sys.stderr.write('secret-read-guard: internal error %s\n' % type(error).__name__)
        return 0
    if denial:
        sys.stderr.write(denial + '\n')
        return 2
    if reason:
        sys.stderr.write('secret-read-guard: blocked. %s. Never print a token value: read settings with jq keys, '
                         'check files with stat or ls, or capture into a variable with NAME=$(cat FILE).\n' % reason)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
