'use strict';
// Runs after remote-cache/start's post step (post steps run in reverse order
// of their main steps), so by now everything the start made must be gone.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const names = (process.env.INPUT_NAMES || '').split(/\s+/).filter(Boolean);
const byPost = (process.env['INPUT_STOPPED-BY-POST'] || '').split(/\s+/).filter(Boolean);
const temp = process.env.RUNNER_TEMP;
const problems = [];

function docker(...args) {
  const r = spawnSync('docker', args, { encoding: 'utf8', timeout: 60000 });
  return r.status === 0 ? r.stdout.trim() : `docker ${args.join(' ')} failed: ${r.stderr}`;
}

for (const name of names) {
  for (const [what, args] of [
    ['containers', ['ps', '-a', '--filter', `label=remote-cache-sidecar=${name}`, '--format', '{{.Names}}']],
    ['volumes', ['volume', 'ls', '--filter', `label=remote-cache-sidecar=${name}`, '--format', '{{.Name}}']],
    ['networks', ['network', 'ls', '--filter', `label=remote-cache-sidecar=${name}`, '--format', '{{.Name}}']],
  ]) {
    const left = docker(...args);
    if (left) problems.push(`${name}: ${what} left: ${left}`);
  }
  const dir = path.join(temp, 'remote-cache-sidecar', name);
  if (fs.existsSync(path.join(dir, 'state'))) problems.push(`${name}: its state file is still there: not stopped`);
  if (byPost.includes(name)) {
    const log = path.join(dir, 'logs', 'service.log');
    if (!fs.existsSync(log)) problems.push(`${name}: no service log at ${log}: the post step did not stop it`);
  }
}

if (problems.length) {
  for (const p of problems) console.log(`::error::verify-stopped: ${p}`);
  process.exitCode = 1;
} else {
  console.log(`verify-stopped: ${names.join(', ')} stopped (post-stopped: ${byPost.join(', ') || 'none'})`);
}
