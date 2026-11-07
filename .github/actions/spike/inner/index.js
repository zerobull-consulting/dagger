const fs = require('node:fs');
const post = process.env.STATE_isPost === 'true';
console.log(post ? 'SPIKE POST RAN' : 'SPIKE MAIN RAN', 'env SPIKE_X=' + process.env.SPIKE_X, 'cwd=' + process.cwd(), 'dir=' + __dirname);
if (!post) {
  fs.appendFileSync(process.env.GITHUB_STATE, 'isPost=true\n');
  fs.appendFileSync(process.env.GITHUB_OUTPUT, 'hello=world\n');
  fs.appendFileSync(process.env.GITHUB_PATH, '/tmp/spike-path\n');
}
