ObjC.import('Foundation');
ObjC.import('AppKit');

function frame(value) {
    $.NSFileHandle.fileHandleWithStandardOutput.writeData($(value + '\n').dataUsingEncoding($.NSUTF8StringEncoding));
}

function behavior(board) {
    return board.respondsToSelector('accessBehavior') ? Number(board.accessBehavior) : 2;
}

function watch() {
    const board = $.NSPasteboard.generalPasteboard;
    const access = behavior(board);
    if (![0, 1, 2, 3].includes(access)) {
        frame('ERROR');
        return;
    }
    if (access === 3) {
        frame('DENY');
        return;
    }
    let count = Number(board.changeCount);
    let pending = false;
    frame('READY');
    const until = Date.now() + 300000;
    while (Date.now() < until) {
        $.NSThread.sleepForTimeInterval(0.5);
        const currentAccess = behavior(board);
        if (currentAccess === 3 || currentAccess !== access) {
            frame('PERMISSION');
            return;
        }
        const changed = Number(board.changeCount);
        if (changed !== count) {
            count = changed;
            pending = true;
        }
        if (pending && typeof ObjC.unwrap(board.availableTypeFromArray($([$.NSPasteboardTypeString]))) === 'string') {
            pending = false;
            frame('READING');
            const attempts = access === 2 ? 3 : 1;
            for (let attempt = 0; attempt < attempts; attempt++) {
                if (behavior(board) !== access) {
                    frame('PERMISSION');
                    return;
                }
                if (Number(board.changeCount) !== count) break;
                if (access === 2) frame('ASK');
                const value = board.stringForType($.NSPasteboardTypeString);
                if (value && Number(value.length) > 256) break;
                const text = ObjC.unwrap(value);
                if (typeof text === 'string') {
                    const candidate = text.trim();
                    if (/^[0-9]{1,20}:[A-Za-z0-9_-]{20,128}$/.test(candidate)) {
                        frame('TOKEN ' + candidate);
                        return;
                    }
                    break;
                }
                if (attempt + 1 === attempts) {
                    frame('NIL');
                    return;
                }
                $.NSThread.sleepForTimeInterval(0.5);
            }
        }
        frame('IDLE');
    }
    frame('TIMEOUT');
}

function run() {
    try {
        watch();
    } catch (error) {
        frame('ERROR');
    }
}
