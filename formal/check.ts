import { createHash } from 'node:crypto';
import { mkdir, rename } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

// Run with: bun --install=force run formal/check.ts [all|models|mutations|proofs]
// Tools and evidence stay outside the checkout. Asset IDs and hashes pin the
// downloaded bytes; cached archives are verified on every run.
const sourceDir = dirname(import.meta.path);
const mode = process.argv[2] ?? 'all';
if (!['all', 'models', 'mutations', 'proofs'].includes(mode)) {
  throw new Error('Expected all, models, mutations, or proofs');
}
const toolDir = resolve(process.env.GOPLS_FORMAL_TOOLS ??
  join(homedir(), '.cache', 'gopls-mcp-formal'));
const evidenceDir = resolve(process.env.GOPLS_FORMAL_OUT ??
  join(homedir(), '.codex', 'artifacts', 'gopls-audit', 'formal',
    new Date().toISOString().replaceAll(':', '-')));
if (evidenceDir === sourceDir || evidenceDir.startsWith(`${sourceDir}/`)) {
  throw new Error('Evidence directory must be outside formal/');
}
await mkdir(toolDir, { recursive: true });
await mkdir(evidenceDir, { recursive: true });
const sha256 = (bytes: Uint8Array | string): string =>
  createHash('sha256').update(bytes).digest('hex');
const jarHash = '7e53aa636dac8a3cb0533f343115c0548e03fc0ce5edc7739becf58d3d0fb84f';
const jar = join(toolDir, `tla2tools-${jarHash}.jar`);
const tlapsCommit = 'bf665f4d162c1ae5010ca878578cd50a810ec205';
const tlapsAssets: Record<string, { id: number; hash: string }> = {
  'darwin-arm64': {
    id: 753937802,
    hash: '05945a4e2c72d96a60b180fdb89403b36f9ef7a16d2f247c49667cafffb20318',
  },
  'linux-x64': {
    id: 753940334,
    hash: '1fcb037a300929d4bed8539511f65073fc19762abd9cea5260fe91d0f2d617df',
  },
};
type TLCConfig = { model: string; config: string };
type TLCOutcome = 'pass' | 'safety' | 'liveness';
type TLCCase = TLCConfig & { name: string; expected: TLCOutcome };

const lifecycle = {
  model: 'SupervisorLifecycle.tla',
  config: "CONSTANTS\n    Trees = {home}\n    Clients = {bridgeA, bridgeB}\n    Generations = {first, second}\n    Capacity = 1\n    OperationCapacity = 2\n    IdleTicks = 2\n    MaxBoot = 2\n    CooperativeExit = TRUE\n    Mutation = \"none\"\nSPECIFICATION Spec\nINVARIANTS TypeOK Safety\nCHECK_DEADLOCK FALSE\n",
};
const operation = {
  model: 'OperationAccounting.tla',
  config: "CONSTANTS\n    Clients = {bridgeA, bridgeB}\n    Connections = {old, retry}\n    MaxRequest = 2\n    Credits = 2\n    Mutation = \"none\"\nSPECIFICATION Spec\nINVARIANTS TypeOK Safety\nCHECK_DEADLOCK FALSE\n",
};
const retirement = {
  model: 'AbandonedRetirement.tla',
  config: "CONSTANTS\n    Generations = {first, second}\n    Clients = {bridgeA, bridgeB}\n    Capacity = 1\n    OperationCapacity = 2\n    IdleTicks = 2\n    CooperativeExit = TRUE\n    Mutation = \"none\"\nSPECIFICATION Spec\nINVARIANTS TypeOK Safety\nCHECK_DEADLOCK FALSE\n",
};
const handoff = {
  model: 'RestartHandoff.tla',
  config: "CONSTANTS\n    Frontends = {stdio, http}\n    Mutation = \"none\"\nSPECIFICATION Spec\nINVARIANTS TypeOK Safety\nCHECK_DEADLOCK FALSE\n",
};
const payload = {
  model: 'PayloadAccounting.tla',
  config: "CONSTANTS\n    Calls = {first, second, third}\n    Capacity = 3\n    Mutation = \"none\"\nSPECIFICATION Spec\nINVARIANTS TypeOK Safety\nCHECK_DEADLOCK FALSE\n",
};

const pureIdle = livenessCase('LifecyclePureIdle', lifecycle, 'PureIdleReclaims');
const retirementLiveness = livenessCase('RetirementLiveness', retirement, 'AbandonedReclaims');
const handoffLiveness = livenessCase('RestartHandoffLiveness', handoff, 'Settles');
const modelCases = [
  tlcCase('LifecycleSafety', lifecycle),
  livenessCase('LifecycleLiveness', lifecycle, 'QuiescenceReclaims', 'ResponseFairSpec'),
  pureIdle,
  tlcCase('LifecycleMultiTree', lifecycle, [
    ['Trees = {home}', 'Trees = {home, worktree}'],
    ['Generations = {first, second}', 'Generations = {first}'],
    ['Capacity = 1', 'Capacity = 2'], ['MaxBoot = 2', 'MaxBoot = 1'],
  ]),
  tlcCase('LifecycleThreeClients', lifecycle, [
    ['Clients = {bridgeA, bridgeB}', 'Clients = {bridgeA, bridgeB, http}'],
    ['MaxBoot = 2', 'MaxBoot = 1'],
  ]),
  tlcCase('OperationSafety', operation),
  tlcCase('OperationThreeClients', operation, [['Clients = {bridgeA, bridgeB}', 'Clients = {bridgeA, bridgeB, http}']]),
  tlcCase('RetirementSafety', retirement),
  retirementLiveness,
  tlcCase('RetirementThreeClients', retirement, [['Clients = {bridgeA, bridgeB}', 'Clients = {bridgeA, bridgeB, bridgeC}']]),
  tlcCase('RestartHandoffSafety', handoff),
  handoffLiveness,
  tlcCase('PayloadSafety', payload),
];
const modelSources: Record<string, string> = Object.fromEntries(
  modelCases.map(({ name, config }) => [`${name}.cfg`, config]),
);
for await (const name of new Bun.Glob('*.tla').scan(sourceDir)) {
  modelSources[name] = await Bun.file(join(sourceDir, name)).text();
}
const sourceHashes: Record<string, string> = {};
for (const [name, text] of Object.entries(modelSources)) {
  sourceHashes[name] = sha256(text);
  await Bun.write(join(evidenceDir, name), text);
}

async function pinnedDownload(path: string, url: string, hash: string): Promise<void> {
  if (!await Bun.file(path).exists()) {
    const headers: Record<string, string> = { Accept: 'application/octet-stream' };
    if (process.env.GITHUB_TOKEN && new URL(url).origin === 'https://api.github.com') {
      headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
    }
    const response = await fetch(url, { headers });
    if (!response.ok) throw new Error(`Download failed: ${response.status} ${url}`);
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (sha256(bytes) !== hash) throw new Error(`SHA-256 mismatch: ${url}`);
    const temporary = `${path}.${process.pid}.tmp`;
    await Bun.write(temporary, bytes);
    await rename(temporary, path);
  }
  if (sha256(new Uint8Array(await Bun.file(path).arrayBuffer())) !== hash) {
    throw new Error(`SHA-256 mismatch: ${path}`);
  }
}

interface RunResult {
  name: string;
  exitCode: number;
  timedOut: boolean;
  elapsedMS: number;
  generated: number | null;
  distinct: number | null;
  depth: number | null;
  proved: number | null;
  expected: string;
  output: string;
}
const results: Omit<RunResult, 'output'>[] = [];
async function run(name: string, command: string[], expected: string): Promise<RunResult> {
  const started = performance.now();
  const log = join(evidenceDir, `${name}.log`);
  const stderr = join(evidenceDir, `${name}.stderr`);
  const child = Bun.spawn(command, {
    cwd: evidenceDir,
    stdout: Bun.file(log), stderr: Bun.file(stderr),
  });
  let timedOut = false;
  let forceTimer: ReturnType<typeof setTimeout> | undefined;
  const timer = setTimeout(() => {
    timedOut = true;
    child.kill('SIGTERM');
    forceTimer = setTimeout(() => child.kill('SIGKILL'), 2_000);
  }, 180_000);
  const exitCode = await child.exited;
  clearTimeout(timer);
  if (forceTimer) clearTimeout(forceTimer);
  if (Bun.file(log).size + Bun.file(stderr).size > 16 * 1024 * 1024) {
    throw new Error(`Unexpectedly large checker output: ${name}`);
  }
  const output = await Bun.file(log).text() + await Bun.file(stderr).text();
  const counts = output.match(/([\d,]+) states generated, ([\d,]+) distinct states found/);
  const number = (value: string | undefined): number | null =>
    value === undefined ? null : Number(value.replaceAll(',', ''));
  const result: RunResult = {
    name, exitCode, timedOut, elapsedMS: Math.round(performance.now() - started),
    generated: number(counts?.[1]), distinct: number(counts?.[2]),
    depth: number(output.match(/complete state graph search is (\d+)/)?.[1]),
    proved: number(output.match(/All (\d+) obligations proved/ )?.[1]),
    expected, output,
  };
  const { output: _output, ...summary } = result;
  results.push(summary);
  await Bun.write(join(evidenceDir, 'results.json'), JSON.stringify({
    tools: { tlcSHA256: jarHash, tlapsCommit },
    environment: { platform: process.platform, arch: process.arch,
      bun: Bun.version, revision: process.env.GOPLS_FORMAL_REVISION ?? null },
    sourceHashes, results,
  }, null, 2) + '\n');
  console.log(`${name}: exit=${exitCode}${timedOut ? ' TIMEOUT' : ''}` +
    (result.distinct === null ? '' : ` states=${result.distinct}`) +
    (result.proved === null ? '' : ` obligations=${result.proved}`));
  if (timedOut) throw new Error(`Checker timed out; not a passing result: ${name}`);
  return result;
}

async function tlc({ name, model, expected }: TLCCase): Promise<void> {
  const result = await run(name, ['java', '-Xmx512m', '-cp', jar, 'tlc2.TLC',
    '-workers', '1', '-fp', '0', '-seed', '1', '-noGenerateSpecTE',
    '-metadir', join(evidenceDir, `${name}-states`), '-config', `${name}.cfg`, model], expected);
  const correct = expected === 'pass'
    ? result.exitCode === 0 && result.output.includes('Model checking completed. No error has been found.')
    : expected === 'safety'
      ? result.exitCode === 12 && result.output.includes('Invariant Safety is violated')
      : result.exitCode === 13 && /Temporal propert(?:y|ies).*violated/.test(result.output);
  if (!correct) throw new Error(`Unexpected ${expected} result in ${name}; inspect ${evidenceDir}`);
}

function tlcCase(name: string, base: TLCConfig, replacements: [string, string][] = [],
                 expected: TLCOutcome = 'pass'): TLCCase {
  let { config } = base;
  for (const [oldValue, newValue] of replacements) {
    if (config.split(oldValue).length !== 2) throw new Error(`${name}: nonunique config field: ${oldValue}`);
    config = config.replace(oldValue, newValue);
  }
  return { name, model: base.model, config, expected };
}

function livenessCase(name: string, base: TLCConfig, property: string, spec = 'FairSpec'): TLCCase {
  return tlcCase(name, base, [
    ['SPECIFICATION Spec', `SPECIFICATION ${spec}`],
    ['CHECK_DEADLOCK', `PROPERTY ${property}\nCHECK_DEADLOCK`],
  ]);
}

function mutationCases(base: TLCConfig, prefix: string, mutations: string[],
                       expected: 'safety' | 'liveness' = 'safety'): TLCCase[] {
  return mutations.map(mutation => tlcCase(`mutation-${prefix}${mutation}`, base,
    [['Mutation = "none"', `Mutation = "${mutation}"`]], expected));
}

if (mode !== 'proofs') {
  await pinnedDownload(jar,
    'https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar', jarHash);
}
if (mode === 'all' || mode === 'models') {
  for (const check of modelCases) await tlc(check);
}
if (mode === 'all' || mode === 'mutations') {
  for (const check of [
    ...mutationCases(lifecycle, '', ['busy-drain', 'early-slot', 'stale-token', 'unowned-signal', 'early-idle']),
    ...mutationCases(operation, '', ['cancel-releases', 'disconnect-releases', 'wrong-completion', 'id-reuse', 'wrong-credit']),
    ...mutationCases(pureIdle, '', ['never-drain'], 'liveness'),
    tlcCase('noncooperative-exit', pureIdle, [['CooperativeExit = TRUE', 'CooperativeExit = FALSE']], 'liveness'),
    ...mutationCases(retirement, 'retirement-', ['active-retire', 'early-abandon', 'retire-releases', 'early-slot']),
    ...mutationCases(retirementLiveness, '', ['never-retire'], 'liveness'),
    tlcCase('noncooperative-retirement', retirementLiveness, [['CooperativeExit = TRUE', 'CooperativeExit = FALSE']], 'liveness'),
    ...mutationCases(handoff, 'handoff-', ['commit-refunds', 'commit-starts', 'commit-opens-http',
      'commit-resumes', 'failure-opens-admission', 'early-admission']),
    ...mutationCases(handoffLiveness, 'handoff-', ['never-settles'], 'liveness'),
    ...mutationCases(payload, 'payload-', ['overbook', 'refund-cancel', 'refund-dequeue', 'refund-writer']),
  ]) {
    await Bun.write(join(evidenceDir, `${check.name}.cfg`), check.config);
    await tlc(check);
  }
}
if (mode === 'all' || mode === 'proofs') {
  const asset = tlapsAssets[`${process.platform}-${process.arch}`];
  if (!asset) throw new Error('No pinned TLAPS binary for this platform; proof run is incomplete');
  const archive = join(toolDir, `tlapm-${asset.hash}.tar.gz`);
  await pinnedDownload(archive,
    `https://api.github.com/repos/tlaplus/tlapm/releases/assets/${asset.id}`, asset.hash);
  const installation = join(toolDir, `tlaps-${asset.hash}`);
  await mkdir(installation, { recursive: true });
  const extract = await run('extract-tlaps', ['tar', '-xzf', archive, '-C', installation], 'pass');
  if (extract.exitCode !== 0) throw new Error('TLAPS extraction failed');
  const prover = join(installation, 'tlapm', 'bin', 'tlapm');
  const version = await run('tlaps-version', [prover, '--version'], 'pass');
  if (version.exitCode !== 0 || version.output.trim() !== tlapsCommit.slice(0, 7)) {
    throw new Error(`Unexpected TLAPS revision; expected ${tlapsCommit}`);
  }
  for (const model of ['AdmissionCore', 'ReservationCore', 'LifecycleCore']) {
    const result = await run(`proof-${model}`, [prover, '--strict', '--nofp', '--threads', '2',
      '--cache-dir', join(evidenceDir, `${model}-proof-cache`), `${model}.tla`], 'proved');
    if (result.exitCode !== 0 || result.proved === null) {
      throw new Error(`Proof incomplete: ${model}`);
    }
  }
  for (const [name, model, oldGuard, replacement] of [
    ['AdmissionCore', 'AdmissionCore', '                /\\ g \\notin draining\n', ''],
    ['ReservationCore', 'ReservationCore', '    /\\ r \\notin physical\n', ''],
    ['core-overlap', 'LifecycleCore',
      '    /\\ ~\\E p \\in reserved : p[1] = r[1] \\/ p[2] = r[2]\n', ''],
    ['core-unreserved-start', 'LifecycleCore',
      'Start(r) ==\n    /\\ r \\in reserved \\ exited\n', 'Start(r) ==\n'],
    ['core-publish-fenced', 'LifecycleCore',
      '    /\\ r \\in physical \\ draining\n', '    /\\ r \\in physical\n'],
    ['core-busy-drain', 'LifecycleCore',
      '    /\\ ~\\E o \\in Operations : <<o,r>> \\in protected\n', ''],
    ['core-unconfirmed-release', 'LifecycleCore',
      '    /\\ r \\in exited \\cap removed\n', '    /\\ r \\in reserved\n'],
    ['core-uncommitted-release', 'LifecycleCore',
      '    /\\ r \\in exited \\cap removed\n', '    /\\ r \\in exited\n'],
    ['core-wrong-completion', 'LifecycleCore',
      "    /\\ held' = held \\ {<<o,r>>}\n", "    /\\ held' = {}\n"],
    ['core-uncertain-release', 'LifecycleCore',
      '                   held, protected, abandoned, retired>>\nObserveExit(r)',
      "                   protected, abandoned, retired>>\n    /\\ held' = {}\nObserveExit(r)"],
  ] as const) {
    const path = join(evidenceDir, `${model}.tla`);
    const original = await Bun.file(path).text();
    if (original.split(oldGuard).length !== 2) throw new Error(`Nonunique proof guard: ${name}`);
    await Bun.write(path, original.replace(oldGuard, replacement));
    const result = await run(`mutation-proof-${name}`, [prover, '--strict', '--nofp',
      '--threads', '2', '--cache-dir', join(evidenceDir, `${name}-mutant-cache`),
      `${model}.tla`], 'proof-rejected');
    await Bun.write(path, original);
    if (result.exitCode !== 10 || !/obligations failed/.test(result.output)) {
      throw new Error(`Proof mutation did not fail as expected: ${name}`);
    }
  }
}
console.log(`Evidence: ${evidenceDir}`);
