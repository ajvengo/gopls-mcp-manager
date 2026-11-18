// Run each package's native Go fuzz targets separately.
const [fuzzTime = '3s', ...extra] = process.argv.slice(2);
if (extra.length !== 0 || !/^[1-9][0-9]*(?:x|ms|s|m)$/.test(fuzzTime)) {
  throw new Error('Expected a positive fuzz budget, such as 3s, 2m, or 1000x');
}

function goOutput(...args: string[]): string {
  const result = Bun.spawnSync(['go', ...args], { stderr: 'inherit' });
  if (result.exitCode !== 0) throw new Error(`go ${args.join(' ')} exited with ${result.exitCode}`);
  return result.stdout.toString().trim();
}

let count = 0;
for (const pkg of goOutput('list', './...').split('\n')) {
  const names = goOutput('test', '-list=^Fuzz', pkg);
  for (const name of names.split('\n').filter(line => /^Fuzz[\p{L}\p{N}_]*$/u.test(line))) {
    console.log(`Fuzzing ${pkg} ${name} for ${fuzzTime}`);
    const result = Bun.spawnSync(['go', 'test', '-run=^$', `-fuzz=^${name}$`,
      `-fuzztime=${fuzzTime}`, '-parallel=2', pkg], { stdout: 'inherit', stderr: 'inherit' });
    if (result.exitCode !== 0) process.exit(result.exitCode || 1);
    count++;
  }
}
if (count === 0) throw new Error('No Go fuzz targets found');
console.log(`Passed ${count} Go fuzz targets`);
