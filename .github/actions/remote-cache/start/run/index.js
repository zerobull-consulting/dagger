'use strict';
// The shim behind remote-cache/start: `main` runs `sidecar.sh start`, and
// `post` (post-if: always(), so after a failed or cancelled job too) runs
// `sidecar.sh stop`. It holds no logic of its own and has no dependencies:
// the Docker steps are sidecar.sh's alone, the script the demo and the
// stop action run too.
//
// It runs the script named by SIDECAR_SCRIPT, which start/action.yml sets to
// the sidecar.sh of its own checkout. That makes the shim generic, so the
// composite can pin it by commit: a composite action can't reference a
// sibling at its own commit (a local `uses: ./...` resolves against the
// caller's workspace), and a pin to the shim never pins the script.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');


// The start's own bound: two 15-minute pulls at once, then waits of at most
// SIDECAR_START_TIMEOUT (120s) each, and some docker calls.
const startMs = 30 * 60 * 1000;
// With no state file to say better: the default drain (10m), its linger and
// slack.
const defaultStopSeconds = 10 * 60 + 15 + 10;
// Added to the stop's own bound (stop_seconds in the state file) for the
// engine's stop (90s) and the cleanup.
const stopSlackMs = 5 * 60 * 1000;

function sidecarDir(env) {
  if (env.SIDECAR_DIR) return env.SIDECAR_DIR;
  if (env.RUNNER_TEMP) return path.join(env.RUNNER_TEMP, 'remote-cache-sidecar');
  return path.join(env.TMPDIR || os.tmpdir(), 'remote-cache-sidecar');
}

function stopSeconds(file) {
  try {
    const m = /^stop_seconds=(\d{1,9})$/m.exec(fs.readFileSync(file, 'utf8'));
    if (m) return Number(m[1]);
  } catch (_) {
    // No state: nothing to bound by.
  }
  return defaultStopSeconds;
}

// The script to run: an absolute path to a sidecar.sh.
function scriptOf(env) {
  const script = env.SIDECAR_SCRIPT || '';
  if (!path.isAbsolute(script) || path.basename(script) !== 'sidecar.sh' || !fs.existsSync(script)) {
    throw new Error(`SIDECAR_SCRIPT '${script}' isn't the path of a sidecar.sh`);
  }
  return script;
}

// The stop's bound: its own (stop_seconds in the state file) and some slack.
function stopTimeoutMs(file) {
  return stopSeconds(file) * 1000 + stopSlackMs;
}

function run(command, env, timeoutMs) {
  const r = spawnSync('bash', [scriptOf(env), command], {
    env,
    stdio: 'inherit',
    timeout: timeoutMs,
    killSignal: 'SIGKILL',
  });
  if (r.error) {
    console.log(`::error::remote-cache-sidecar: ${command} did not finish: ${r.error.message}`);
    return 1;
  }
  return r.status === null ? 1 : r.status;
}

// Values the main step saves for the post step (STATE_<name> there).
function saveState(env, key, value) {
  if (!env.GITHUB_STATE) return;
  fs.appendFileSync(env.GITHUB_STATE, `${key}=${value}\n`);
}

function main(env) {
  const name = env.SIDECAR_NAME || 'remote-cache';
  // Saved first: the post step must stop what a start that failed halfway
  // left.
  saveState(env, 'isPost', 'true');
  saveState(env, 'name', name);
  saveState(env, 'dir', sidecarDir(env));
  return run('start', env, startMs);
}

function post(env) {
  const name = env.STATE_name || env.SIDECAR_NAME || 'remote-cache';
  const dir = env.STATE_dir || sidecarDir(env);
  // Always `sidecar.sh stop`, which knows where its state is and says
  // "nothing to stop" itself: this shim holds a copy of the state's place and
  // format only to bound the stop, and a drift between the two (the script
  // comes from the caller's commit, the shim is pinned) shortens or lengthens
  // that bound, but never leaves a sidecar running.
  const file = path.join(dir, name, 'state');
  try {
    scriptOf(env);
  } catch (e) {
    // No script to run. With no state either, no start of this action's ever
    // got going: one line, and no second error on top of the main step's.
    // With state, a sidecar is running and can't be stopped: say so.
    if (!fs.existsSync(file)) {
      console.log(`remote-cache-sidecar: nothing to stop for ${name}: ${e.message}`);
      return 0;
    }
    console.log(`::error::remote-cache-sidecar: ${name} is still running (state at ${file}) and can't be stopped: ${e.message}`);
    return 1;
  }
  console.log(`remote-cache-sidecar: stopping ${name} at the end of the job (if it isn't stopped already)`);
  const childEnv = { ...env, SIDECAR_NAME: name, SIDECAR_DIR: dir };
  return run('stop', childEnv, stopTimeoutMs(file));
}

module.exports = { main, post, run, stopSeconds, stopTimeoutMs, sidecarDir };

if (require.main === module) {
  // The runner hands the values main saved to the post step as STATE_<key>.
  const isPost = process.env.STATE_isPost === 'true';
  try {
    process.exitCode = isPost ? post(process.env) : main(process.env);
  } catch (e) {
    console.log(`::error::remote-cache-sidecar: ${e.message}`);
    process.exitCode = 1;
  }
}
