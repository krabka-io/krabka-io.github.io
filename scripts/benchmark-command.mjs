import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';

// Benchmark clients can log indefinitely. Keep their full output on disk and
// retain only a bounded tail in exceptions, without execFile's buffer ceiling.
export async function runLoggedCommand(executable, args, { output, timeout, signal }) {
  const stdout = await fs.open(`${output}.stdout`, 'w+');
  let stderr;
  try {
    stderr = await fs.open(`${output}.stderr`, 'w+');
    const child = spawn(executable, args, { timeout, signal, killSignal: 'SIGKILL',
      stdio: ['ignore', stdout.fd, stderr.fd] });
    const result = await new Promise(resolve => {
      let error;
      child.once('error', value => { error = value; });
      child.once('close', (code, killedBy) => resolve({ code, killedBy, error }));
    });
    if (result.error || result.code !== 0) {
      const tails = await Promise.all([stdout, stderr].map(async file => {
        const size = (await file.stat()).size;
        const buffer = Buffer.alloc(Math.min(size, 3000));
        const { bytesRead } = await file.read(buffer, 0, buffer.length, size - buffer.length);
        return buffer.subarray(0, bytesRead).toString('utf8');
      }));
      throw new Error(`${executable} ${args.slice(0, 4).join(' ')}: ${result.error?.message
        ?? `exit ${result.code}, signal ${result.killedBy ?? 'none'}`}\n${tails.join('\n')}`, { cause: result.error });
    }
    return '';
  } finally {
    await stdout.close();
    await stderr?.close();
  }
}
