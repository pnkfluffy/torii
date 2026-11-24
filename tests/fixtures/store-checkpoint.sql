BEGIN TRANSACTION;
CREATE TABLE attachments (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    message INTEGER NOT NULL, file_id TEXT NOT NULL, mime TEXT NOT NULL,
                    size INTEGER, group_id TEXT, path TEXT, created REAL NOT NULL,
                    name TEXT, error TEXT, kind TEXT, duration INTEGER, UNIQUE(topic,message));
INSERT INTO "attachments" VALUES(1,'-1001073001:2',90,'fixture-file','text/plain',12,NULL,NULL,1.79205706682537293e+09,'fixture.txt',NULL,'document',NULL);
CREATE TABLE cli_calls (
                    id INTEGER PRIMARY KEY, created REAL NOT NULL, op TEXT NOT NULL,
                    from_name TEXT, topic TEXT, state TEXT NOT NULL, process TEXT);
CREATE TABLE envelope_effects (
                    id INTEGER PRIMARY KEY, envelope INTEGER REFERENCES envelopes(id),
                    kind TEXT NOT NULL CHECK(kind IN ('delete','reply')), chat INTEGER NOT NULL,
                    message INTEGER, text TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt REAL NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done','failed')),
                    created REAL NOT NULL, thread INTEGER);
INSERT INTO "envelope_effects" VALUES(1,1,'reply',-1001073001,NULL,'Fixture effect',0,0.0,'pending',1.79205706683938622e+09,2);
CREATE TABLE envelope_events (
                    id INTEGER PRIMARY KEY, envelope INTEGER REFERENCES envelopes(id), name TEXT,
                    event TEXT NOT NULL CHECK(event IN ('ask','arm','fill','reject','use','cancel','expire','revoke','delete_failed')),
                    reason TEXT, worker INTEGER REFERENCES workers(id), source TEXT, created REAL NOT NULL);
INSERT INTO "envelope_events" VALUES(1,1,'FIXTURE_INPUT','ask',NULL,NULL,'cli',1.79205706683925819e+09);
CREATE TABLE envelopes (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL, reason TEXT NOT NULL, consumer TEXT NOT NULL,
                    task INTEGER REFERENCES tasks(id), topic TEXT NOT NULL REFERENCES topics(id),
                    token_hash TEXT, chat INTEGER, card_outbox INTEGER,
                    state TEXT NOT NULL CHECK(state IN ('open','armed','filled','superseded','cancelled','expired','revoked')),
                    created REAL NOT NULL, expires REAL NOT NULL, armed_at REAL, filled_at REAL,
                    length INTEGER, fingerprint TEXT, updated REAL NOT NULL, arm_thread INTEGER);
INSERT INTO "envelopes" VALUES(1,'FIXTURE_INPUT','Offline shape coverage','fixture',NULL,'-1001073001:2','fixture-envelope-token-hash',NULL,2,'open',1.79205706683920884e+09,1.79205766683920884e+09,NULL,NULL,NULL,NULL,1.79205706683920884e+09,NULL);
CREATE TABLE host_log_hosts (
                    name TEXT PRIMARY KEY, worker INTEGER NOT NULL REFERENCES workers(id));
CREATE TABLE "messages" (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL CHECK(kind IN ('owner','forwarded','worker_result','restarted','callback','secret_filled','event')),
                    telegram_message INTEGER, text TEXT NOT NULL,
                    source_worker INTEGER REFERENCES workers(id), images TEXT,
                    source_envelope INTEGER REFERENCES envelopes(id),
                    delivered TEXT NOT NULL DEFAULT 'pending'
                        CHECK(delivered IN ('pending','sent','received','uncertain')),
                    receipt TEXT, reaction_state TEXT NOT NULL DEFAULT 'sent',
                    reaction_desired TEXT, reaction_sent TEXT, turn_started REAL,
                    turn_outbox INTEGER, turn_eyes INTEGER NOT NULL DEFAULT 0,
                    turn_tools INTEGER NOT NULL DEFAULT 0, reacted_to INTEGER,
                    reaction_retry REAL NOT NULL DEFAULT 0,
                    reaction_attempts INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL, from_name TEXT,
                    reaction_first_failed REAL, batch_id INTEGER);
INSERT INTO "messages" VALUES(1,'-1001073001:2','owner',90,'Fixture retained attachment',NULL,'[{"file_id": "fixture-file", "mime": "text/plain", "size": 12, "group_id": null, "name": "fixture.txt", "kind": "document"}]',NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706682540798e+09,1.79205706682540798e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(2,'-1001073001:2','owner',100,'Fixture owner',NULL,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683093404e+09,1.79205706683093404e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(3,'-1001073001:2','restarted',102,'Fixture restarted',NULL,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683101582e+09,1.79205706683101582e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(4,'-1001073001:2','callback',103,'Fixture callback',NULL,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683106684e+09,1.79205706683106684e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(5,'-1001073001:2','worker_result',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 5, "task": 5, "topic": "-1001073001:2", "provider": "codex", "status": "done", "cwd": "/tmp/torii-checkpoint/path-02"}',5,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683749198e+09,1.79205706683749198e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(6,'-1001073001:2','worker_result',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 6, "task": 6, "topic": "-1001073001:2", "provider": "codex", "status": "needs_input", "cwd": "/tmp/torii-checkpoint/path-02"}',6,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683832812e+09,1.79205706683832812e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(7,'-1001073001:2','worker_result',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 7, "task": 7, "topic": "-1001073001:2", "provider": "codex", "status": "interrupted", "cwd": "/tmp/torii-checkpoint/path-02"}',7,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683908891e+09,1.79205706683908891e+09,NULL,NULL,NULL);
INSERT INTO "messages" VALUES(8,'-1001073001:2','secret_filled',NULL,'Fixture envelope notification without any secret value',NULL,NULL,1,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1.79205706683934116e+09,1.79205706683934116e+09,NULL,NULL,NULL);
CREATE TABLE native_session_hosts (
        native INTEGER NOT NULL REFERENCES native_sessions(id), host TEXT NOT NULL,
        PRIMARY KEY(native,host));
CREATE TABLE native_sessions (
        id INTEGER PRIMARY KEY, provider TEXT NOT NULL, session TEXT NOT NULL,
        home TEXT NOT NULL, root TEXT NOT NULL, cwd TEXT NOT NULL, host TEXT,
        worker INTEGER REFERENCES workers(id), parent TEXT, generation INTEGER NOT NULL DEFAULT 0,
        planned INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
        CHECK ((worker IS NULL) != (parent IS NULL)),
        UNIQUE(provider,session,home,worker,parent));
CREATE TABLE outbox (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL, text TEXT NOT NULL, reply_to INTEGER,
                    delivered INTEGER NOT NULL DEFAULT 0, telegram_message INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    reply_markup TEXT, image TEXT, edit_message INTEGER, retired INTEGER NOT NULL DEFAULT 0, reaction_desired TEXT, reaction_sent TEXT, reaction_state TEXT NOT NULL DEFAULT 'sent', reaction_retry REAL NOT NULL DEFAULT 0, reaction_attempts INTEGER NOT NULL DEFAULT 0, reaction_first_failed REAL, as_document INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL DEFAULT 0);
INSERT INTO "outbox" VALUES(1,'-1001073001:2','status','Link this channel to a project.
Choose New project or Existing project below.

New projects go in /tmp/torii-checkpoint/path-01
You can also type New project or Existing project. /setup returns here.
/projects lists folders. /help lists commands.
No Claude or ChatGPT account yet. Open /accounts to sign one in. Project setup works before sign-in.',10,0,NULL,0,0.0,'{"inline_keyboard": [[{"text": "New project", "callback_data": "torii:fixture-control-nonce:0"}], [{"text": "Existing project", "callback_data": "torii:fixture-control-nonce:1"}], [{"text": "Accounts", "callback_data": "torii:fixture-control-nonce:2"}, {"text": "Projects", "callback_data": "torii:fixture-control-nonce:3"}]]}',NULL,NULL,0,NULL,NULL,'sent',0.0,0,NULL,0,0.0);
INSERT INTO "outbox" VALUES(2,'-1001073001:2','envelope','Envelope: FIXTURE_INPUT
Why: Offline shape coverage
Used by: fixture
Job: any job
Tap Fill privately within 10 minutes and send the value there in one message. Never paste it in this channel.',NULL,0,NULL,0,0.0,'{"inline_keyboard": [[{"text": "Fill privately", "url": "https://t.me/fixture_bot?start=fixture-envelope-link"}], [{"text": "Cancel", "callback_data": "envelope:1:cancel"}]]}',NULL,NULL,0,NULL,NULL,'sent',0.0,0,NULL,0,0.0);
INSERT INTO "outbox" VALUES(3,'-1001073001:2','report','Fixture pending report',100,0,NULL,0,0.0,NULL,NULL,NULL,0,NULL,NULL,'sent',0.0,0,NULL,0,0.0);
INSERT INTO "outbox" VALUES(4,'-1001073001:2','report','Fixture delivered report',NULL,1,500,0,0.0,NULL,NULL,NULL,0,NULL,NULL,'sent',0.0,0,NULL,0,0.0);
CREATE TABLE parent_reply_owners (
                message INTEGER PRIMARY KEY REFERENCES messages(id), host TEXT NOT NULL, turn TEXT NOT NULL,
                FOREIGN KEY(host,turn) REFERENCES parent_reply_turns(host,turn));
CREATE TABLE parent_reply_turns (
                host TEXT NOT NULL, turn TEXT NOT NULL, provider TEXT NOT NULL,
                topic TEXT NOT NULL REFERENCES topics(id) ON UPDATE CASCADE, outbox_start INTEGER NOT NULL,
                messages TEXT NOT NULL, checked INTEGER NOT NULL DEFAULT 0 CHECK(checked IN (0,1)),
                outbox INTEGER REFERENCES outbox(id), PRIMARY KEY(host,turn));
CREATE TABLE problems (
                    id INTEGER PRIMARY KEY, created REAL NOT NULL, area TEXT NOT NULL, code TEXT NOT NULL,
                    detail TEXT, topic TEXT, task INTEGER, worker INTEGER, message INTEGER, attachment INTEGER);
INSERT INTO "problems" VALUES(1,1.7920570668395698e+09,'service','started','previous exit: unknown, no record',NULL,NULL,NULL,NULL,NULL);
CREATE TABLE service_requests (
                    id INTEGER PRIMARY KEY, op TEXT NOT NULL, params TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', result TEXT,
                    created REAL NOT NULL, updated REAL NOT NULL);
INSERT INTO "service_requests" VALUES(1,'workers.steer','{"worker": 1, "prompt": "Fixture steer"}','queued',NULL,1.79205706683952403e+09,1.79205706683952403e+09);
CREATE TABLE session_cleanup_homes (
        provider TEXT NOT NULL, root TEXT NOT NULL, attempts INTEGER NOT NULL,
        PRIMARY KEY(provider,root));
CREATE TABLE session_deletions (
        id INTEGER PRIMARY KEY, native INTEGER NOT NULL REFERENCES native_sessions(id),
        provider TEXT NOT NULL, session TEXT NOT NULL, home TEXT NOT NULL, root TEXT NOT NULL,
        path TEXT NOT NULL, device INTEGER, inode INTEGER, bytes INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'pending', retry_at REAL NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
        UNIQUE(root,path,provider,session));
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO "settings" VALUES('owner','1073001');
INSERT INTO "settings" VALUES('owner_name','"Fixture owner"');
INSERT INTO "settings" VALUES('group','-1001073001');
INSERT INTO "settings" VALUES('group_name','"Fixture group"');
INSERT INTO "settings" VALUES('mode','"group"');
INSERT INTO "settings" VALUES('pairing','null');
INSERT INTO "settings" VALUES('pair_delete','{"chat": -1001073001, "message": 10}');
INSERT INTO "settings" VALUES('project_setup:-1001073001:2','{"stage": "choose"}');
INSERT INTO "settings" VALUES('control_ui:-1001073001:2','{"token": "fixture-control-nonce", "actions": [{"command": "/setup new"}, {"command": "/setup existing"}, {"view": "accounts:0"}, {"command": "/projects"}], "pending": null, "outbox": 1, "where": null}');
INSERT INTO "settings" VALUES('coordinator_home_topic','"-1001073001:2"');
INSERT INTO "settings" VALUES('control_topic','"-1001073001:2"');
INSERT INTO "settings" VALUES('execution','"agents"');
INSERT INTO "settings" VALUES('accounts','{}');
INSERT INTO "settings" VALUES('codex_accounts','{}');
INSERT INTO "settings" VALUES('pair_only','false');
INSERT INTO "settings" VALUES('forced_pair_only','false');
INSERT INTO "settings" VALUES('bot_id','1073002');
INSERT INTO "settings" VALUES('bot_username','"fixture_bot"');
INSERT INTO "settings" VALUES('group_check_requested','false');
INSERT INTO "settings" VALUES('group_is_forum','true');
INSERT INTO "settings" VALUES('group_type','"supergroup"');
INSERT INTO "settings" VALUES('rights_notice:-1001073001','null');
INSERT INTO "settings" VALUES('offset','3');
INSERT INTO "settings" VALUES('service_run','{"state": "exited", "cause": "fixture-offline-exit", "ended": 1792057066.8402371, "checked": 1792057066.840029}');
INSERT INTO "settings" VALUES('envelope_prompt_exit','{"envelope": 1, "acknowledged": true}');
CREATE TABLE task_iterations (
            id INTEGER PRIMARY KEY, task INTEGER NOT NULL REFERENCES tasks(id),
            k INTEGER NOT NULL CHECK(k>=0), opened REAL NOT NULL, closed REAL,
            opened_by TEXT NOT NULL CHECK(opened_by IN ('create','owner','wait','coordinator','resume','migration')),
            origin INTEGER REFERENCES messages(id),
            outcome TEXT CHECK(outcome IN ('done','waiting','interrupted','dropped')), reason TEXT, cause TEXT CHECK(cause IN ('crash','unfinished','quota','account','offline','config')), attempts INTEGER NOT NULL DEFAULT 0, resume_at REAL, progress_head TEXT, nudged INTEGER NOT NULL DEFAULT 0, last_worker INTEGER REFERENCES workers(id), head TEXT, saved TEXT, rebuild_seconds REAL, bytes_freed INTEGER, space_outbox_after INTEGER NOT NULL DEFAULT 0, wait INTEGER REFERENCES waits(id),
            UNIQUE(task,k));
INSERT INTO "task_iterations" VALUES(1,1,0,1.79205706683111715e+09,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
INSERT INTO "task_iterations" VALUES(2,2,0,1.79205706683459091e+09,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
INSERT INTO "task_iterations" VALUES(3,3,0,1.79205706683534479e+09,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
INSERT INTO "task_iterations" VALUES(4,4,0,1.7920570668360691e+09,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
INSERT INTO "task_iterations" VALUES(5,5,0,1.79205706683677911e+09,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
INSERT INTO "task_iterations" VALUES(6,6,0,1792057066.83761,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
INSERT INTO "task_iterations" VALUES(7,7,0,1.79205706683841514e+09,NULL,'migration',NULL,NULL,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,NULL);
CREATE TABLE task_space_saves (
        task INTEGER NOT NULL REFERENCES tasks(id), k INTEGER NOT NULL,
        phase TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(task,k));
CREATE TABLE task_stamps (
                    id INTEGER PRIMARY KEY, task INTEGER NOT NULL REFERENCES tasks(id),
                    stage TEXT NOT NULL CHECK(stage IN ('ready','reviewed','ci','merged','live')),
                    created REAL NOT NULL, iteration INTEGER NOT NULL DEFAULT 0 CHECK(iteration>=0));
CREATE TABLE "tasks" (
                id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                number INTEGER NOT NULL, title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','interrupted','waiting','done','dropped')),
                worktree TEXT, notes TEXT, created REAL NOT NULL, updated REAL NOT NULL,
                secrets TEXT, origin INTEGER REFERENCES messages(id), pr TEXT,
                iteration INTEGER NOT NULL DEFAULT 0 CHECK(iteration>=0),
                finish TEXT CHECK(finish IN ('merged','live','answered','delivered')), reason TEXT,
                copy TEXT NOT NULL DEFAULT 'present' CHECK(copy IN ('present','saving','held','cleared')),
                copy_reason TEXT, parent INTEGER REFERENCES tasks(id), UNIQUE(topic,number));
INSERT INTO "tasks" VALUES(1,'-1001073001:2',1,'Fixture queued','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1.79205706683111715e+09,1.79205706683111715e+09,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
INSERT INTO "tasks" VALUES(2,'-1001073001:2',2,'Fixture waiting_for_secret','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1.79205706683459091e+09,1.79205706683459091e+09,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
INSERT INTO "tasks" VALUES(3,'-1001073001:2',3,'Fixture waiting_for_quota','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1.79205706683534479e+09,1.79205706683534479e+09,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
INSERT INTO "tasks" VALUES(4,'-1001073001:2',4,'Fixture running','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1.7920570668360691e+09,1.7920570668360691e+09,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
INSERT INTO "tasks" VALUES(5,'-1001073001:2',5,'Fixture done','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1.79205706683677911e+09,1.79205706683677911e+09,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
INSERT INTO "tasks" VALUES(6,'-1001073001:2',6,'Fixture needs_input','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.83761,1792057066.83761,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
INSERT INTO "tasks" VALUES(7,'-1001073001:2',7,'Fixture interrupted','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1.79205706683841514e+09,1.79205706683841514e+09,NULL,NULL,NULL,0,NULL,NULL,'present',NULL,NULL);
CREATE TABLE topics (
                    id TEXT PRIMARY KEY, chat INTEGER NOT NULL, thread INTEGER NOT NULL,
                    name TEXT NOT NULL, telegram_title TEXT, cwd TEXT NOT NULL, provider TEXT,
                    session TEXT, enabled INTEGER NOT NULL DEFAULT 0,
                    waiting_job INTEGER, source_pid INTEGER);
INSERT INTO "topics" VALUES('-1001073001:2',-1001073001,2,'Fixture project',NULL,'/tmp/torii-checkpoint/path-02',NULL,NULL,1,NULL,NULL);
INSERT INTO "topics" VALUES('-1001073001:0',-1001073001,0,'General',NULL,'',NULL,NULL,0,NULL,NULL);
CREATE TABLE updates (id INTEGER PRIMARY KEY);
INSERT INTO "updates" VALUES(1);
INSERT INTO "updates" VALUES(2);
CREATE TABLE wait_jobs (
        wait INTEGER NOT NULL REFERENCES waits(id), task INTEGER NOT NULL REFERENCES tasks(id),
        blocking INTEGER NOT NULL DEFAULT 1 CHECK(blocking IN (0,1)),
        then_provider TEXT, then_prompt TEXT, then_model TEXT, then_effort TEXT, then_work TEXT,
        created REAL NOT NULL, PRIMARY KEY(wait,task));
CREATE TABLE waits (
        id INTEGER PRIMARY KEY, kind TEXT NOT NULL
            CHECK(kind IN ('owner','time','job','pr','torii_update','signal')), key TEXT NOT NULL,
        topic TEXT NOT NULL REFERENCES topics(id), ask TEXT NOT NULL,
        detail TEXT NOT NULL DEFAULT '{}', ask_outbox INTEGER REFERENCES outbox(id),
        state TEXT NOT NULL DEFAULT 'armed' CHECK(state IN ('armed','fired','cancelled')),
        outcome TEXT, fired_by TEXT, reason TEXT, next_check REAL,
        checks INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, updated REAL NOT NULL);
CREATE TABLE worker_temp_state (
                    worker INTEGER PRIMARY KEY REFERENCES workers(id), path TEXT,
                    finished INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0,
                    epoch INTEGER NOT NULL DEFAULT 0, staged TEXT NOT NULL DEFAULT '[]',
                    retry_at REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0);
INSERT INTO "worker_temp_state" VALUES(6,NULL,1,1,1,'[]',0.0,0);
CREATE TABLE workers (
                    id INTEGER PRIMARY KEY, task INTEGER REFERENCES tasks(id),
                    topic TEXT NOT NULL REFERENCES topics(id), provider TEXT NOT NULL,
                    prompt TEXT NOT NULL, session TEXT, cwd TEXT, workspace TEXT,
                    fresh INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'queued',
                    pid INTEGER, result TEXT, goal TEXT, model TEXT, effort TEXT NOT NULL DEFAULT 'medium', account_alias TEXT,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL, host TEXT, work TEXT NOT NULL DEFAULT 'research', iteration INTEGER NOT NULL DEFAULT 0 CHECK(iteration>=0), resume_of INTEGER REFERENCES workers(id));
INSERT INTO "workers" VALUES(1,1,'-1001073001:2','codex','Fixture prompt for queued',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'queued',NULL,NULL,NULL,NULL,'medium',NULL,0,1.79205706683450102e+09,1.79205706683450102e+09,NULL,'research',0,NULL);
INSERT INTO "workers" VALUES(2,2,'-1001073001:2','codex','Fixture prompt for waiting_for_secret',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'waiting_for_secret',NULL,NULL,NULL,NULL,'medium',NULL,0,1.79205706683526802e+09,1792057066.83531,NULL,'research',0,NULL);
INSERT INTO "workers" VALUES(3,3,'-1001073001:2','codex','Fixture prompt for waiting_for_quota',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'waiting_for_quota',NULL,NULL,NULL,NULL,'medium',NULL,0,1.79205706683600306e+09,1.79205706683604097e+09,NULL,'research',0,NULL);
INSERT INTO "workers" VALUES(4,4,'-1001073001:2','codex','Fixture prompt for running',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'running',NULL,NULL,NULL,NULL,'medium',NULL,0,1.79205706683670902e+09,1.79205706683675122e+09,NULL,'research',0,NULL);
INSERT INTO "workers" VALUES(5,5,'-1001073001:2','codex','Fixture prompt for done','00000000-0000-4000-8000-000000001073','/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',0,'done',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 5, "task": 5, "topic": "-1001073001:2", "provider": "codex", "status": "done", "cwd": "/tmp/torii-checkpoint/path-02"}',NULL,NULL,'medium',NULL,0,1.7920570668374021e+09,1.79205706683749198e+09,NULL,'research',0,NULL);
INSERT INTO "workers" VALUES(6,6,'-1001073001:2','codex','Fixture prompt for needs_input','00000000-0000-4000-8000-000000001073','/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',0,'done',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 6, "task": 6, "topic": "-1001073001:2", "provider": "codex", "status": "needs_input", "cwd": "/tmp/torii-checkpoint/path-02"}',NULL,NULL,'medium',NULL,0,1.79205706683827781e+09,1.79545947875244259e+09,NULL,'research',0,NULL);
INSERT INTO "workers" VALUES(7,7,'-1001073001:2','codex','Fixture prompt for interrupted','00000000-0000-4000-8000-000000001073','/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',0,'interrupted',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 7, "task": 7, "topic": "-1001073001:2", "provider": "codex", "status": "interrupted", "cwd": "/tmp/torii-checkpoint/path-02"}',NULL,NULL,'medium',NULL,0,1.792057066839046e+09,1.79205706683908891e+09,NULL,'research',0,NULL);
CREATE INDEX workers_tasks ON workers(task,id);
CREATE INDEX workers_states ON workers(status,id);
CREATE INDEX attachment_groups ON attachments(topic,group_id);
CREATE INDEX outbox_messages ON outbox(telegram_message,topic) WHERE delivered=1;
CREATE INDEX outbox_pending ON outbox(id) WHERE delivered=0;
CREATE INDEX outbox_topic_pending ON outbox(topic,id) WHERE delivered=0;
CREATE UNIQUE INDEX envelopes_live_name ON envelopes(name) WHERE state IN ('open','armed');
CREATE UNIQUE INDEX envelopes_filled_name ON envelopes(name) WHERE state='filled';
CREATE UNIQUE INDEX envelopes_armed_chat ON envelopes(chat) WHERE state='armed';
CREATE UNIQUE INDEX envelopes_token ON envelopes(token_hash) WHERE token_hash IS NOT NULL;
CREATE INDEX problems_created ON problems(created);
CREATE INDEX worker_temp_pending ON worker_temp_state(pending,retry_at,worker);
CREATE INDEX host_log_hosts_worker ON host_log_hosts(worker);
CREATE INDEX host_log_receipts ON service_requests(json_extract(params,'$.worker'),op)
                    WHERE op IN ('workers.steer','workers.goal');
CREATE INDEX settings_host_refs ON settings(key) WHERE substr(key,-5)='_host';
CREATE INDEX task_stamps_task ON task_stamps(task,id);
CREATE INDEX task_stamps_created ON task_stamps(created);
CREATE TRIGGER worker_temp_insert AFTER INSERT ON workers
                    WHEN NEW.status IN ('done','interrupted') BEGIN
                    INSERT INTO worker_temp_state(worker,finished,pending,epoch) VALUES (NEW.id,1,1,1)
                    ON CONFLICT(worker) DO UPDATE SET finished=1,pending=1,retry_at=0,attempts=0,
                        epoch=worker_temp_state.epoch+1;
                    END;
CREATE TRIGGER worker_temp_finish AFTER UPDATE OF status ON workers
                    WHEN NEW.status IN ('done','interrupted') BEGIN
                    INSERT INTO worker_temp_state(worker,finished,pending,epoch) VALUES (NEW.id,1,1,1)
                    ON CONFLICT(worker) DO UPDATE SET finished=1,pending=1,retry_at=0,attempts=0,
                        epoch=worker_temp_state.epoch+1;
                    END;
CREATE UNIQUE INDEX workers_resume_of ON workers(resume_of) WHERE resume_of IS NOT NULL;
CREATE INDEX native_sessions_identity
        ON native_sessions(provider,session);
CREATE INDEX native_sessions_parent
        ON native_sessions(parent,generation);
CREATE INDEX session_deletions_pending
        ON session_deletions(state,retry_at,id);
CREATE TRIGGER task_space_round AFTER INSERT ON task_iterations
        WHEN (SELECT value FROM settings WHERE key='close_clears_space')='true'
        BEGIN UPDATE task_iterations SET space_outbox_after=(SELECT COALESCE(MAX(id),0) FROM outbox)
        WHERE task=NEW.task AND k=NEW.k; END;
CREATE TRIGGER task_space_close AFTER UPDATE OF status ON tasks
        WHEN NEW.status IN ('done','dropped','waiting','interrupted')
        AND (SELECT value FROM settings WHERE key='close_clears_space')='true'
        BEGIN UPDATE tasks SET copy='saving',copy_reason=NULL WHERE id=NEW.id; END;
CREATE UNIQUE INDEX waits_armed_key ON waits(key) WHERE state='armed';
CREATE INDEX waits_due ON waits(next_check)
        WHERE state='armed' AND next_check IS NOT NULL;
CREATE INDEX wait_jobs_task ON wait_jobs(task,wait);
CREATE UNIQUE INDEX messages_telegram ON messages(topic,telegram_message) WHERE telegram_message IS NOT NULL;
CREATE INDEX messages_workers ON messages(source_worker);
CREATE UNIQUE INDEX messages_envelopes ON messages(source_envelope) WHERE source_envelope IS NOT NULL;
CREATE INDEX parent_reply_outbox ON parent_reply_turns(outbox);
CREATE INDEX parent_reply_owner_turn ON parent_reply_owners(host,turn);
CREATE INDEX messages_batches ON messages(batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX workers_task_status ON workers(task,status);
CREATE INDEX service_requests_states ON service_requests(state,id);
COMMIT;
