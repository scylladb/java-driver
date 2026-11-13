// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync, mkdtempSync, mkdirSync, writeFileSync, rmSync} = require('node:fs');
const {join} = require('node:path');
const {tmpdir} = require('node:os');
const {spawnSync} = require('node:child_process');
const {test} = require('node:test');
const yaml = require('js-yaml');
const workflow = name => yaml.load(readFileSync(join(__dirname, '../workflows/', name), 'utf8'));
const ci = workflow('tests.yml');
const release = workflow('release.yml');

// Evaluate the actual, deliberately small boolean/matrix expressions in the workflows.
function expression(source, inputs = {}, needs = {}, event = 'pull_request', context = {}) {
  const github = {event_name: event, ...context};
  const js = source.replace(/^\$\{\{\s*|\s*\}\}$/g, '')
    .replace(/\b(inputs|needs|github)((?:\.[\w-]+)+)/g,
      (_, root, path) => root + path.split('.').slice(1).map(k => `?.[${JSON.stringify(k)}]`).join(''));
  return Function('inputs', 'needs', 'github', 'fromJSON', 'cancelled', `return (${js})`)(
    inputs, needs, github, JSON.parse, () => false);
}
function matrix(matrix) {
  let rows = [{}];
  for (const [axis, values] of Object.entries(matrix)) {
    if (['include', 'exclude'].includes(axis)) continue;
    rows = rows.flatMap(row => values.map(value => ({...row, [axis]: value})));
  }
  return rows.filter(row => !matrix.exclude?.some(ex =>
    Object.entries(ex).every(([key, value]) => row[key] === value)));
}
function python(step, env, cwd) {
  return spawnSync('bash', ['-e', '-o', 'pipefail', '-c', step.run], {
    cwd, env: {...process.env, ...env}, encoding: 'utf8'
  });
}

test('routing covers every scylla-4.x PR and candidate, without push tests or path skips', () => {
  assert.deepEqual(Object.keys(ci.on).sort(), ['merge_group', 'pull_request', 'workflow_call', 'workflow_dispatch']);
  for (const event of ['pull_request', 'merge_group']) {
    assert.deepEqual(ci.on[event].branches, ['scylla-4.x']);
    assert.equal(ci.on[event]['paths-ignore'], undefined);
    assert.equal(ci.on[event].paths, undefined);
  }
  assert.equal(ci.on.workflow_dispatch.inputs['full-suite'].default, true);
  assert.equal(ci.on.workflow_call.inputs['full-suite'].default, true);
  assert.match(ci.concurrency.group, /pull_request.number \|\| github.sha/);
  assert.match(ci.concurrency['cancel-in-progress'], /event_name == 'pull_request'/);
  for (const name of ['release-credential-safety.yml', 'release-notes-guard.yml']) {
    assert.deepEqual(Object.keys(workflow(name).on), ['workflow_call']);
  }
  assert.deepEqual(Object.keys(workflow('coverage-after-merge.yml').on), ['push']);
  assert.match(workflow('docs-pages.yml').on.push.branches.join(','), /scylla-4.x/);
});

test('profiles preserve all full lanes and reduce queue integration to two complete JDK 11 jobs', () => {
  for (const job of ['build', 'verify', 'unit-tests']) {
    const axis = ci.jobs[job].strategy.matrix['java-version'];
    assert.deepEqual(expression(axis, {'full-suite': false}), [11]);
    assert.deepEqual(expression(axis, {'full-suite': true}), [11, 17]);
  }
  assert.equal(matrix(ci.jobs['cassandra-integration-tests'].strategy.matrix).length, 6);
  const scylla = ci.jobs['scylla-integration-tests'];
  const full = expression(scylla.strategy.matrix.include, {'full-suite': true});
  assert.equal(full.length, 9);
  assert.deepEqual([...new Set(full.map(x => x['test-group']))].sort(), ['isolated', 'parallelizable', 'serial']);
  assert.deepEqual(expression(scylla.strategy.matrix.include, {'full-suite': false}), [
    {'scylla-version': 'LTS-PRIOR', 'java-version': 11, 'test-group': 'all'},
    {'scylla-version': 'LATEST', 'java-version': 11, 'test-group': 'all'}
  ]);
  assert.equal(scylla['timeout-minutes'], 90);
  assert.equal(expression(scylla.if, {}, {}, 'pull_request'), undefined);
  assert.equal(expression(scylla.if, {}, {}, 'merge_group'), true);
  assert.equal(expression(scylla.if, {}, {}, 'workflow_dispatch'), true);
  const reserve = scylla.steps.find(s => s.name === 'Reserve shard-aware test source ports');
  assert.match(reserve.if, /test-group == 'all'/);
  assert.match(scylla.steps.find(s => s.id === 'test-skip-args').run, /all\).*value=/);
  for (const job of ['unit-tests', 'cassandra-integration-tests', 'scylla-integration-tests']) {
    const step = ci.jobs[job].steps.find(s => s.env?.COVERAGE);
    assert.equal(step.env.COVERAGE, '${{ matrix.java-version == env.COVERAGE_JAVA_VERSION }}');
  }
  assert.equal(expression(ci.env.COVERAGE_JAVA_VERSION, {'full-suite': false}), '11');
  assert.equal(expression(ci.env.COVERAGE_JAVA_VERSION, {'full-suite': true}), '17');
});

test('CI gate rejects missing, failed, cancelled and unexpectedly skipped required jobs', () => {
  const gate = ci.jobs.gate;
  assert.equal(gate.name, 'CI gate');
  assert.equal(gate.if, 'always()');
  for (const [event, full, required] of [
    ['pull_request', false, ['build', 'verify', 'unit-tests', 'release-credential-safety', 'release-notes-guard', 'coverage-report']],
    ['merge_group', false, gate.needs.filter(x => x !== 'cassandra-integration-tests')],
    ['workflow_dispatch', false, gate.needs.filter(x => x !== 'cassandra-integration-tests')],
    ['workflow_dispatch', true, gate.needs]
  ]) {
    const results = Object.fromEntries(gate.needs.map(job => [job, {result: required.includes(job) ? 'success' : 'skipped'}]));
    const run = data => python(gate.steps[0], {RESULTS: JSON.stringify(data), EVENT: event, FULL_SUITE: String(full)});
    assert.equal(run(results).status, 0);
    for (const job of required) {
      for (const state of ['failure', 'cancelled', 'skipped', 'missing']) {
        const data = structuredClone(results);
        if (state === 'missing') delete data[job]; else data[job].result = state;
        assert.equal(run(data).status, 1, `${event}/${full}/${job}/${state}`);
      }
    }
  }
});

test('release pins both suites and admits skipped CI only with the matching explicit skip', () => {
  const internal = release.jobs['full-internal-ci'];
  assert.equal(internal.needs, 'validate');
  assert.equal(internal.uses, './.github/workflows/tests.yml');
  assert.equal(internal.with['test-ref'], '${{ needs.validate.outputs.ref }}');
  assert.equal(internal.with['driver-version'], '${{ needs.validate.outputs.version }}');
  assert.equal(internal.with['full-suite'], true);
  assert.deepEqual(Object.keys(internal.secrets), ['CODECOV_TOKEN']);
  assert.equal(release.on.workflow_dispatch.inputs['skip-full-ci'].default, false);
  for (const fullSkip of [false, true]) for (const matrixSkip of [false, true]) {
    for (const state of ['success', 'failure', 'cancelled', 'skipped']) {
      const needs = {validate: {result: 'success'}, 'full-internal-ci': {result: state}, 'pre-release-integration': {result: 'success'}};
      const inputs = {'skip-full-ci': fullSkip, 'skip-integration-matrix': matrixSkip};
      assert.equal(expression(release.jobs.release.if, inputs, needs), state === 'success' || (state === 'skipped' && fullSkip));
      needs['full-internal-ci'].result = 'success';
      needs['pre-release-integration'].result = state;
      assert.equal(expression(release.jobs.release.if, inputs, needs), state === 'success' || (state === 'skipped' && matrixSkip));
    }
  }
  for (const job of Object.values(ci.jobs)) for (const step of job.steps ?? []) {
    if (step.uses?.startsWith('actions/checkout@')) {
      assert.equal(step.with.ref, '${{ inputs.test-ref || github.sha }}');
    }
    if (step.uses?.startsWith('actions/setup-java@')) {
      assert.ok(job.steps.some(s => s.env?.DRIVER_VERSION === '${{ inputs.driver-version }}'));
    }
  }
});

test('historical releases use launch tooling and attribute coverage to the tested source', () => {
  const inputs = {'test-ref': '769492785613ff57e3cf3643217a078cc644f800'};
  const context = {sha: 'launch-sha', event: {}};
  for (const name of ['release-credential-safety', 'release-notes-guard']) {
    assert.equal(expression(ci.jobs[name].with['test-ref'], inputs, {}, 'workflow_dispatch', context),
      'launch-sha');
  }
  const upload = ci.jobs['coverage-report'].steps.find(s => s.name === 'Upload coverage to Codecov');
  assert.equal(expression(upload.with.override_commit, inputs, {}, 'workflow_dispatch', context),
    inputs['test-ref']);
  assert.equal(expression(upload.with.override_commit, {}, {}, 'pull_request',
    {sha: 'merge-sha', event: {pull_request: {head: {sha: 'pr-head'}}}}), 'pr-head');
  assert.equal(expression(upload.with.override_commit, {}, {}, 'merge_group', context), 'launch-sha');
});

test('coverage validates exact lanes, every module, JDK consistency and test outcomes', () => {
  const step = ci.jobs['coverage-report'].steps.find(s => s.id === 'lanes');
  const modules = ['core', 'query-builder', 'mapper-runtime', 'mapper-processor', 'metrics/micrometer', 'metrics/microprofile', 'test-infra', 'integration-tests'];
  for (const [profile, jdk] of [['unit', '11'], ['reduced', '11'], ['full', '17']]) {
    const dir = mkdtempSync(join(tmpdir(), 'java-coverage-'));
    try {
      for (const module of modules) {
        mkdirSync(join(dir, module), {recursive: true});
        writeFileSync(join(dir, module, 'pom.xml'), 'fixture');
      }
      const laneNames = [`coverage-exec-unit-${jdk}`];
      if (profile === 'reduced') laneNames.push('coverage-exec-scylla-LTS-PRIOR-11-all', 'coverage-exec-scylla-LATEST-11-all');
      if (profile === 'full') for (const db of ['cassandra-4-LATEST', 'scylla-LATEST']) for (const group of ['parallelizable', 'serial', 'isolated']) laneNames.push(`coverage-exec-${db}-17-${group}`);
      for (const name of laneNames) {
        const lane = join(dir, 'coverage-exec', name);
        mkdirSync(lane, {recursive: true});
        writeFileSync(join(lane, 'jdk.txt'), jdk);
        for (const module of name.startsWith('coverage-exec-unit-') ? modules.slice(0, -1) : ['integration-tests']) {
          writeFileSync(join(lane, module.replace('/', '-') + '.exec'), 'recorded data');
        }
      }
      const results = {'unit-tests': {result: 'success'}, 'scylla-integration-tests': {result: 'success'}, 'cassandra-integration-tests': {result: 'success'}};
      const output = join(dir, 'output');
      const run = () => python(step, {COVERAGE_JAVA_VERSION: jdk, FULL_SUITE: String(profile === 'full'), PROFILE: profile, RESULTS: JSON.stringify(results), GITHUB_OUTPUT: output}, dir);
      assert.equal(run().status, 0);
      assert.match(readFileSync(output, 'utf8'), /passed=true/);
      results['unit-tests'].result = 'failure';
      writeFileSync(output, '');
      assert.equal(run().status, 0);
      assert.equal(readFileSync(output, 'utf8'), 'passed=false\n');
      results['unit-tests'].result = 'success';
      const lane = join(dir, 'coverage-exec', laneNames[0]);
      for (const module of modules) rmSync(join(dir, module, 'target'), {recursive: true, force: true});
      rmSync(join(lane, 'query-builder.exec'));
      writeFileSync(output, '');
      results['unit-tests'].result = 'failure';
      assert.notEqual(run().status, 0);
      assert.equal(readFileSync(output, 'utf8'), '');
      assert.equal(readFileSync(join(dir, 'core/target', `jacoco-${laneNames[0]}.exec`), 'utf8'),
        'recorded data');
      writeFileSync(join(lane, 'query-builder.exec'), 'recorded data');
      results['unit-tests'].result = 'success';
      writeFileSync(join(lane, 'jdk.txt'), jdk === '11' ? '17' : '11');
      assert.notEqual(run().status, 0);
      writeFileSync(join(lane, 'jdk.txt'), jdk);
      rmSync(join(lane, 'core.exec'));
      assert.notEqual(run().status, 0);
      rmSync(lane, {recursive: true});
      assert.notEqual(run().status, 0);
    } finally { rmSync(dir, {recursive: true, force: true}); }
  }
  const publish = ci.jobs['coverage-report'].steps.filter(s => /Prepare verified|Upload verified|Upload coverage to Codecov/.test(s.name));
  assert.equal(publish.length, 3);
  for (const step of publish) assert.match(step.if, /lanes.outputs.passed == 'true'/);
  const aggregate = ci.jobs['coverage-report'].steps.find(s => s.name === 'Aggregate coverage');
  assert.equal(aggregate.if, "${{ !cancelled() && steps.lanes.outcome != 'skipped' }}");
});
