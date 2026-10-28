BEGIN TRANSACTION;
CREATE TABLE attachments (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    message INTEGER NOT NULL, file_id TEXT NOT NULL, mime TEXT NOT NULL,
                    size INTEGER, group_id TEXT, path TEXT, created REAL NOT NULL,
                    name TEXT, error TEXT, kind TEXT, UNIQUE(topic,message));
INSERT INTO "attachments" VALUES(1,'-1001073001:2',90,'fixture-file','text/plain',12,NULL,NULL,1792057066.8253729,'fixture.txt',NULL,'document');
CREATE TABLE envelope_effects (
                    id INTEGER PRIMARY KEY, envelope INTEGER REFERENCES envelopes(id),
                    kind TEXT NOT NULL CHECK(kind IN ('delete','reply')), chat INTEGER NOT NULL,
                    message INTEGER, text TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt REAL NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done','failed')),
                    created REAL NOT NULL, thread INTEGER);
INSERT INTO "envelope_effects" VALUES(1,1,'reply',-1001073001,NULL,'Fixture effect',0,0.0,'pending',1792057066.8393862,2);
CREATE TABLE envelope_events (
                    id INTEGER PRIMARY KEY, envelope INTEGER REFERENCES envelopes(id), name TEXT,
                    event TEXT NOT NULL CHECK(event IN ('ask','arm','fill','reject','use','cancel','expire','revoke','delete_failed')),
                    reason TEXT, worker INTEGER REFERENCES workers(id), source TEXT, created REAL NOT NULL);
INSERT INTO "envelope_events" VALUES(1,1,'FIXTURE_INPUT','ask',NULL,NULL,'cli',1792057066.8392582);
CREATE TABLE envelopes (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL, reason TEXT NOT NULL, consumer TEXT NOT NULL,
                    task INTEGER REFERENCES tasks(id), topic TEXT NOT NULL REFERENCES topics(id),
                    token_hash TEXT, chat INTEGER, card_outbox INTEGER,
                    state TEXT NOT NULL CHECK(state IN ('open','armed','filled','superseded','cancelled','expired','revoked')),
                    created REAL NOT NULL, expires REAL NOT NULL, armed_at REAL, filled_at REAL,
                    length INTEGER, fingerprint TEXT, updated REAL NOT NULL, arm_thread INTEGER);
INSERT INTO "envelopes" VALUES(1,'FIXTURE_INPUT','Offline shape coverage','fixture',NULL,'-1001073001:2','fixture-envelope-token-hash',NULL,2,'open',1792057066.8392088,1792057666.8392088,NULL,NULL,NULL,NULL,1792057066.8392088,NULL);
CREATE TABLE messages (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL CHECK(kind IN ('owner','worker_result','restarted','callback','secret_filled')),
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
                    created REAL NOT NULL, updated REAL NOT NULL);
INSERT INTO "messages" VALUES(1,'-1001073001:2','owner',90,'Fixture retained attachment',NULL,'[{"file_id": "fixture-file", "mime": "text/plain", "size": 12, "group_id": null, "name": "fixture.txt", "kind": "document"}]',NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.825408,1792057066.825408);
INSERT INTO "messages" VALUES(2,'-1001073001:2','owner',100,'Fixture owner',NULL,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.8309341,1792057066.8309341);
INSERT INTO "messages" VALUES(3,'-1001073001:2','restarted',102,'Fixture restarted',NULL,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.8310158,1792057066.8310158);
INSERT INTO "messages" VALUES(4,'-1001073001:2','callback',103,'Fixture callback',NULL,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.8310669,1792057066.8310669);
INSERT INTO "messages" VALUES(5,'-1001073001:2','worker_result',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 5, "task": 5, "topic": "-1001073001:2", "provider": "codex", "status": "done", "cwd": "/tmp/torii-checkpoint/path-02"}',5,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.837492,1792057066.837492);
INSERT INTO "messages" VALUES(6,'-1001073001:2','worker_result',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 6, "task": 6, "topic": "-1001073001:2", "provider": "codex", "status": "needs_input", "cwd": "/tmp/torii-checkpoint/path-02"}',6,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.8383281,1792057066.8383281);
INSERT INTO "messages" VALUES(7,'-1001073001:2','worker_result',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 7, "task": 7, "topic": "-1001073001:2", "provider": "codex", "status": "interrupted", "cwd": "/tmp/torii-checkpoint/path-02"}',7,NULL,NULL,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.8390889,1792057066.8390889);
INSERT INTO "messages" VALUES(8,'-1001073001:2','secret_filled',NULL,'Fixture envelope notification without any secret value',NULL,NULL,1,'pending',NULL,'sent',NULL,NULL,NULL,NULL,0,0,NULL,0.0,0,1792057066.8393412,1792057066.8393412);
CREATE TABLE outbox (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL, text TEXT NOT NULL, reply_to INTEGER,
                    delivered INTEGER NOT NULL DEFAULT 0, telegram_message INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    reply_markup TEXT, image TEXT, edit_message INTEGER, retired INTEGER NOT NULL DEFAULT 0, reaction_desired TEXT, reaction_sent TEXT, reaction_state TEXT NOT NULL DEFAULT 'sent', reaction_retry REAL NOT NULL DEFAULT 0, reaction_attempts INTEGER NOT NULL DEFAULT 0);
INSERT INTO "outbox" VALUES(1,'-1001073001:2','status','Link this channel to a project.
Choose New project or Existing project below.

New projects go in /tmp/torii-checkpoint/path-01
You can also type New project or Existing project. /setup returns here.
/projects lists folders. /help lists commands.
No Claude or ChatGPT account yet. Open /accounts to sign one in. Project setup works before sign-in.',10,0,NULL,0,0.0,'{"inline_keyboard": [[{"text": "New project", "callback_data": "torii:fixture-control-nonce:0"}], [{"text": "Existing project", "callback_data": "torii:fixture-control-nonce:1"}], [{"text": "Accounts", "callback_data": "torii:fixture-control-nonce:2"}, {"text": "Projects", "callback_data": "torii:fixture-control-nonce:3"}]]}',NULL,NULL,0,NULL,NULL,'sent',0.0,0);
INSERT INTO "outbox" VALUES(2,'-1001073001:2','envelope','Envelope: FIXTURE_INPUT
Why: Offline shape coverage
Used by: fixture
Job: any job
Tap Fill privately within 10 minutes and send the value there in one message. Never paste it in this channel.',NULL,0,NULL,0,0.0,'{"inline_keyboard": [[{"text": "Fill privately", "url": "https://t.me/fixture_bot?start=fixture-envelope-link"}], [{"text": "Cancel", "callback_data": "envelope:1:cancel"}]]}',NULL,NULL,0,NULL,NULL,'sent',0.0,0);
INSERT INTO "outbox" VALUES(3,'-1001073001:2','report','Fixture pending report',100,0,NULL,0,0.0,NULL,NULL,NULL,0,NULL,NULL,'sent',0.0,0);
INSERT INTO "outbox" VALUES(4,'-1001073001:2','report','Fixture delivered report',NULL,1,500,0,0.0,NULL,NULL,NULL,0,NULL,NULL,'sent',0.0,0);
CREATE TABLE problems (
                    id INTEGER PRIMARY KEY, created REAL NOT NULL, area TEXT NOT NULL, code TEXT NOT NULL,
                    detail TEXT, topic TEXT, task INTEGER, worker INTEGER, message INTEGER, attachment INTEGER);
INSERT INTO "problems" VALUES(1,1792057066.8395698,'service','started','previous exit: unknown, no record',NULL,NULL,NULL,NULL,NULL);
CREATE TABLE service_requests (
                    id INTEGER PRIMARY KEY, op TEXT NOT NULL, params TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', result TEXT,
                    created REAL NOT NULL, updated REAL NOT NULL);
INSERT INTO "service_requests" VALUES(1,'workers.steer','{"worker": 1, "prompt": "Fixture steer"}','queued',NULL,1792057066.839524,1792057066.839524);
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
CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    number INTEGER NOT NULL, title TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','done','dropped')),
                    worktree TEXT, notes TEXT, created REAL NOT NULL, updated REAL NOT NULL, secrets TEXT, origin INTEGER REFERENCES messages(id),
                    UNIQUE(topic,number));
INSERT INTO "tasks" VALUES(1,'-1001073001:2',1,'Fixture queued','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.8311172,1792057066.8311172,NULL,NULL);
INSERT INTO "tasks" VALUES(2,'-1001073001:2',2,'Fixture waiting_for_secret','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.8345909,1792057066.8345909,NULL,NULL);
INSERT INTO "tasks" VALUES(3,'-1001073001:2',3,'Fixture waiting_for_quota','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.8353448,1792057066.8353448,NULL,NULL);
INSERT INTO "tasks" VALUES(4,'-1001073001:2',4,'Fixture running','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.8360691,1792057066.8360691,NULL,NULL);
INSERT INTO "tasks" VALUES(5,'-1001073001:2',5,'Fixture done','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.8367791,1792057066.8367791,NULL,NULL);
INSERT INTO "tasks" VALUES(6,'-1001073001:2',6,'Fixture needs_input','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.83761,1792057066.83761,NULL,NULL);
INSERT INTO "tasks" VALUES(7,'-1001073001:2',7,'Fixture interrupted','open','/tmp/torii-checkpoint/path-02','Fixture retained notes',1792057066.8384152,1792057066.8384152,NULL,NULL);
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
CREATE TABLE workers (
                    id INTEGER PRIMARY KEY, task INTEGER REFERENCES tasks(id),
                    topic TEXT NOT NULL REFERENCES topics(id), provider TEXT NOT NULL,
                    prompt TEXT NOT NULL, session TEXT, cwd TEXT, workspace TEXT,
                    fresh INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'queued',
                    pid INTEGER, result TEXT, goal TEXT, model TEXT, effort TEXT NOT NULL DEFAULT 'medium', account_alias TEXT,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL, host TEXT, work TEXT NOT NULL DEFAULT 'research');
INSERT INTO "workers" VALUES(1,1,'-1001073001:2','codex','Fixture prompt for queued',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'queued',NULL,NULL,NULL,NULL,'medium',NULL,0,1792057066.834501,1792057066.834501,NULL,'research');
INSERT INTO "workers" VALUES(2,2,'-1001073001:2','codex','Fixture prompt for waiting_for_secret',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'waiting_for_secret',NULL,NULL,NULL,NULL,'medium',NULL,0,1792057066.835268,1792057066.83531,NULL,'research');
INSERT INTO "workers" VALUES(3,3,'-1001073001:2','codex','Fixture prompt for waiting_for_quota',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'waiting_for_quota',NULL,NULL,NULL,NULL,'medium',NULL,0,1792057066.8360031,1792057066.836041,NULL,'research');
INSERT INTO "workers" VALUES(4,4,'-1001073001:2','codex','Fixture prompt for running',NULL,'/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',1,'running',NULL,NULL,NULL,NULL,'medium',NULL,0,1792057066.836709,1792057066.8367512,NULL,'research');
INSERT INTO "workers" VALUES(5,5,'-1001073001:2','codex','Fixture prompt for done','00000000-0000-4000-8000-000000001073','/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',0,'done',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 5, "task": 5, "topic": "-1001073001:2", "provider": "codex", "status": "done", "cwd": "/tmp/torii-checkpoint/path-02"}',NULL,NULL,'medium',NULL,0,1792057066.8374021,1792057066.837492,NULL,'research');
INSERT INTO "workers" VALUES(6,6,'-1001073001:2','codex','Fixture prompt for needs_input','00000000-0000-4000-8000-000000001073','/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',0,'needs_input',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 6, "task": 6, "topic": "-1001073001:2", "provider": "codex", "status": "needs_input", "cwd": "/tmp/torii-checkpoint/path-02"}',NULL,NULL,'medium',NULL,0,1792057066.8382778,1792057066.8383281,NULL,'research');
INSERT INTO "workers" VALUES(7,7,'-1001073001:2','codex','Fixture prompt for interrupted','00000000-0000-4000-8000-000000001073','/tmp/torii-checkpoint/path-02','{"cwd": "/tmp/torii-checkpoint/path-02", "isolated": true}',0,'interrupted',NULL,'{"success": true, "text": "Fixture result", "session_id": "00000000-0000-4000-8000-000000001073", "worker": 7, "task": 7, "topic": "-1001073001:2", "provider": "codex", "status": "interrupted", "cwd": "/tmp/torii-checkpoint/path-02"}',NULL,NULL,'medium',NULL,0,1792057066.839046,1792057066.8390889,NULL,'research');
CREATE UNIQUE INDEX messages_telegram ON messages(topic,telegram_message) WHERE telegram_message IS NOT NULL;
CREATE INDEX messages_workers ON messages(source_worker);
CREATE UNIQUE INDEX messages_envelopes ON messages(source_envelope) WHERE source_envelope IS NOT NULL;
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
COMMIT;
