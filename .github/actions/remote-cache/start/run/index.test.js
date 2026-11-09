'use strict';
// Tests of the shim against a fake sidecar.sh: node --test <this directory>
const test = require('node:test');
const assert = require('node:assert');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const shim = path.join(__dirname, 'index.js');

// A fake sidecar.sh: logs each command; start writes the state file (stop in
// 14s at most), stop removes it, as the real one does; FAKE_START_EXIT is
// the start's exit code, FAKE_STOP_EXIT the stop's.
const fakeScript = `#!/usr/bin/env bash
echo "$1 name=$SIDECAR_NAME dir=$SIDECAR_DIR" >>"$FAKE_LOG"
case "$1" in
start)
  mkdir -p "$SIDECAR_DIR/$SIDECAR_NAME"
  printf 'service=x\\nstop_seconds=14\\n' >"$SIDECAR_DIR/$SIDECAR_NAME/state"
  exit "\${FAKE_START_EXIT:-0}" ;;
stop)
  [ -f "$SIDECAR_DIR/$SIDECAR_NAME/state" ] || echo "remote-cache-sidecar: nothing to stop"
  rm -f "$SIDECAR_DIR/$SIDECAR_NAME/state"
  exit "\${FAKE_STOP_EXIT:-0}" ;;
esac
`;

function setup(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shim-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const script = path.join(dir, 'sidecar.sh');
  fs.writeFileSync(script, fakeScript, { mode: 0o755 });
  const env = {
    PATH: process.env.PATH,
    SIDECAR_SCRIPT: script,
    SIDECAR_DIR: path.join(dir, 'state'),
    SIDECAR_NAME: 't',
    FAKE_LOG: path.join(dir, 'log'),
    GITHUB_STATE: path.join(dir, 'github-state'),
  };
  fs.writeFileSync(env.GITHUB_STATE, '');
  return { dir, env, log: () => (fs.existsSync(env.FAKE_LOG) ? fs.readFileSync(env.FAKE_LOG, 'utf8') : '') };
}

// The step the runner runs: main, or post with the state main saved as
// STATE_<key>.
function main(env, extra = {}) {
  return spawnSync('node', [shim], { env: { ...env, ...extra }, encoding: 'utf8' });
}
function post(env, extra = {}) {
  const saved = {};
  for (const line of fs.readFileSync(env.GITHUB_STATE, 'utf8').split('\n')) {
    const i = line.indexOf('=');
    if (i > 0) saved['STATE_' + line.slice(0, i)] = line.slice(i + 1);
  }
  return spawnSync('node', [shim], { env: { ...env, ...saved, ...extra }, encoding: 'utf8' });
}

test('main runs sidecar.sh start, and saves what the post step needs', (t) => {
  const { env, log } = setup(t);
  const r = main(env);
  assert.strictEqual(r.status, 0, r.stdout + r.stderr);
  assert.match(log(), /^start name=t /);
  const saved = fs.readFileSync(env.GITHUB_STATE, 'utf8');
  assert.match(saved, /^isPost=true$/m);
  assert.match(saved, /^name=t$/m);
});

test('a start that fails fails the step, and the post step still stops what it left', (t) => {
  const { env, log } = setup(t);
  const r = main(env, { FAKE_START_EXIT: '3' });
  assert.notStrictEqual(r.status, 0);
  const p = post(env);
  assert.strictEqual(p.status, 0, p.stdout + p.stderr);
  assert.deepStrictEqual(log().trim().split('\n').map((l) => l.split(' ')[0]), ['start', 'stop']);
});

test('the post step stops the sidecar after a job that failed, with no stop step', (t) => {
  const { env, log } = setup(t);
  assert.strictEqual(main(env).status, 0);
  // (the job's later step fails; the runner then runs post-if: always())
  assert.strictEqual(post(env).status, 0);
  assert.deepStrictEqual(log().trim().split('\n').map((l) => l.split(' ')[0]), ['start', 'stop']);
  assert.ok(!fs.existsSync(path.join(env.SIDECAR_DIR, 't', 'state')));
});

test('after an explicit stop the post step still runs stop, which finds nothing to stop', (t) => {
  const { env, log } = setup(t);
  assert.strictEqual(main(env).status, 0);
  // remote-cache/stop ran sidecar.sh stop: the state file is gone.
  fs.rmSync(path.join(env.SIDECAR_DIR, 't', 'state'));
  const p = post(env);
  assert.strictEqual(p.status, 0);
  assert.match(p.stdout, /nothing to stop/);
  assert.deepStrictEqual(log().trim().split('\n').map((l) => l.split(' ')[0]), ['start', 'stop']);
});

// zerobull-consulting/dagger#174: the shim used to look for the state itself
// and skip the stop if it found none, so a script whose state is elsewhere
// (the script is the caller's commit's, the shim is pinned) was never stopped.
test('the post step stops a sidecar whose state is not where the shim looks', (t) => {
  const { env, log } = setup(t);
  assert.strictEqual(main(env).status, 0);
  fs.rmSync(path.join(env.SIDECAR_DIR, 't', 'state'));
  const p = post(env);
  assert.strictEqual(p.status, 0);
  assert.ok(/^stop /m.test(log()), log());
});

// zerobull-consulting/dagger#174: a bound that passed killed only bash, and
// left the docker, timeout and pull processes it had spawned running.
test('a run that passes its bound is killed with the processes it spawned', (t) => {
  const { dir, env } = setup(t);
  const pidFile = path.join(dir, 'child.pid');
  fs.writeFileSync(env.SIDECAR_SCRIPT, '#!/usr/bin/env bash\nsleep 300 &\necho $! >"$CHILD_PID"\nwait\n', { mode: 0o755 });
  const code = require(shim).run('stop', { ...env, CHILD_PID: pidFile }, 1000);
  assert.strictEqual(code, 1);
  const pid = Number(fs.readFileSync(pidFile, 'utf8'));
  // A killed process may stay a zombie until its parent reaps it.
  let state = '';
  try {
    state = /^State:\s*(\S)/m.exec(fs.readFileSync(`/proc/${pid}/status`, 'utf8'))[1];
  } catch (_) {
    state = 'gone';
  }
  if (state !== 'gone' && state !== 'Z') process.kill(pid, 'SIGKILL');
  assert.ok(state === 'gone' || state === 'Z', `the spawned sleep ${pid} is still alive (state ${state})`);
});

test('stopping twice is safe: a second post step after a first one finds nothing to stop', (t) => {
  const { env, log } = setup(t);
  main(env);
  assert.strictEqual(post(env).status, 0);
  assert.strictEqual(post(env).status, 0);
  assert.deepStrictEqual(log().trim().split('\n').map((l) => l.split(' ')[0]), ['start', 'stop', 'stop']);
});

test('a stop that fails fails the post step', (t) => {
  const { env } = setup(t);
  main(env);
  assert.strictEqual(post(env, { FAKE_STOP_EXIT: '1' }).status, 1);
});

test('the post step finds the state under the name and directory main used', (t) => {
  const { env, log } = setup(t);
  main(env, { SIDECAR_NAME: 'other' });
  // The post step's own environment differs (a later step's, say): the saved state wins.
  const p = post({ ...env, SIDECAR_NAME: 'wrong', SIDECAR_DIR: path.join(env.SIDECAR_DIR, 'x') });
  assert.strictEqual(p.status, 0);
  assert.match(log(), /^stop name=other /m);
});

test('a SIDECAR_SCRIPT that is not a sidecar.sh is refused, and nothing runs', (t) => {
  const { dir, env } = setup(t);
  fs.writeFileSync(path.join(dir, 'other.sh'), '#!/bin/sh\ntouch "$FAKE_LOG"\n', { mode: 0o755 });
  for (const bad of ['', 'sidecar.sh', path.join(dir, 'other.sh'), path.join(dir, 'missing', 'sidecar.sh')]) {
    const r = main(env, { SIDECAR_SCRIPT: bad });
    assert.strictEqual(r.status, 1, `'${bad}'`);
    assert.match(r.stdout, /isn't the path of a sidecar.sh/);
  }
  assert.ok(!fs.existsSync(env.FAKE_LOG));
});

test('the stop is bounded by its own bound plus slack', () => {
  const { stopTimeoutMs } = require('./index.js');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shim-'));
  try {
    const file = path.join(dir, 'state');
    fs.writeFileSync(file, 'service=x\nstop_seconds=700\n');
    assert.strictEqual(stopTimeoutMs(file), 700 * 1000 + 5 * 60 * 1000);
    // No state: the default drain's bound, still bounded.
    assert.strictEqual(stopTimeoutMs(path.join(dir, 'none')), (10 * 60 + 25) * 1000 + 5 * 60 * 1000);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
