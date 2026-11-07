'use strict';
// Runs after remote-cache/start's post step (post steps run in reverse order
// of their main steps), so by now everything the start made must be gone.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const names = (process.env.INPUT_NAMES || '').split(/\s+/).filter(Boolean);
const temp = process.env.RUNNER_TEMP || '';
const problems = [];

// The names docker lists, or null when docker itself failed: that says
// nothing about the sidecar, and is reported as such.
function docker(...args) {
  const r = spawnSync('docker', args, { encoding: 'utf8', timeout: 60000 });
  if (r.status !== 0) {
    problems.push(`could not ask docker (docker ${args.join(' ')}): ${(r.stderr || r.error || '').toString().trim()}`);
    return null;
  }
  return r.stdout.trim();
}

if (!temp) problems.push('RUNNER_TEMP is not set');
if (!names.length) problems.push('no names given');

for (const name of names) {
  for (const [what, args] of [
    ['containers', ['ps', '-a', '--filter', `label=remote-cache-sidecar=${name}`, '--format', '{{.Names}}']],
    ['volumes', ['volume', 'ls', '--filter', `label=remote-cache-sidecar=${name}`, '--format', '{{.Name}}']],
    ['networks', ['network', 'ls', '--filter', `label=remote-cache-sidecar=${name}`, '--format', '{{.Name}}']],
  ]) {
    const left = docker(...args);
    if (left) problems.push(`${name}: ${what} left: ${left}`);
  }
  if (!temp) continue;
  const dir = path.join(temp, 'remote-cache-sidecar', name);
  if (fs.existsSync(path.join(dir, 'state'))) problems.push(`${name}: its state file is still there: not stopped`);
  // Every stop, explicit or the post step's, keeps the service's log.
  const log = path.join(dir, 'logs', 'service.log');
  if (!fs.existsSync(log)) problems.push(`${name}: no service log at ${log}: it was not stopped`);
}

if (problems.length) {
  for (const p of problems) console.log(`::error::verify-stopped: ${p}`);
  process.exitCode = 1;
} else {
  console.log(`verify-stopped: ${names.join(', ')} stopped (post-stopped: ${byPost.join(', ') || 'none'})`);
}
