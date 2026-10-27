// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const yaml = require('js-yaml');
const guard = require('../scripts/release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'java-driver'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter((step) => step.with?.script).map((step) => step.with.script);
const collectScript = steps.find((step) => step.id === 'guard').with.script;
const confirmScript = steps.find((step) => step.id === 'target').with.script;
const verifyScript = steps.at(-1).with.script;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {
    number: 1313,
    head: {sha: 'a'.repeat(40)},
    base: {sha: 'b'.repeat(40)},
    changed_files: files.length
  };
  const github = {
    rest: {
      pulls: {
        get: async (params) => {
          assert.deepEqual(params, {...repo, pull_number: 1313});
          calls.push('get');
          gets++;
          if (options.failGetAt === gets) throw new Error('PR lookup unavailable');
          return {data: {...initial, ...options.revisions?.[gets - 1]}};
        },
        listFiles: async (params) => {
          assert.deepEqual(params, {...repo, pull_number: 1313, per_page: 100, page: params.page});
          calls.push(`page:${params.page}`);
          return {data: options.pages[params.page - 1]};
        }
      },
      issues: {
        listLabelsOnIssue: () => {},
        removeLabel: async (params) => {
          assert.deepEqual(params, {...repo, issue_number: 1313, name: label});
          calls.push('remove');
          if (options.cleanupFailure) throw new Error('Removal unavailable');
          // GitHub looks up the logical label without case sensitivity while
          // preserving its stored spelling in returned metadata.
          const actual = [...labels].find((name) => name.toLowerCase() === label);
          if (actual === undefined) throw {status: 404};
          labels.delete(actual);
        }
      }
    },
    paginate: async (method, params) => {
      assert.equal(params.per_page, 100);
      if (method === github.rest.pulls.listFiles) {
        assert.deepEqual(params, {...repo, pull_number: 1313, per_page: 100});
        calls.push('files');
        if (options.fileFailure) throw new Error('File listing unavailable');
        if (Object.hasOwn(options, 'rawFiles')) return options.rawFiles;
        if (options.pages) {
          const records = [];
          for (let page = 1; page <= options.pages.length; page++) {
            records.push(...(await method({...params, page})).data);
          }
          return records;
        }
        return options.listings?.[listings++] ?? files;
      }
      assert.equal(method, github.rest.issues.listLabelsOnIssue);
      assert.deepEqual(params, {...repo, issue_number: 1313, per_page: 100});
      calls.push('labels');
      if (options.labelFailure) throw new Error('Labels unavailable');
      return Object.hasOwn(options, 'labelMetadata')
        ? options.labelMetadata
        : [...labels].map((name) => ({name}));
    }
  };
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = () => guard) {
  const env = {
    PR_NUMBER: '1313',
    CONFIRMED_PR: 'true',
    TARGET_OUTCOME: 'success',
    CHECKOUT_OUTCOME: 'success',
    INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success',
    LABELER_OUTCOME: 'success',
    UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash,
    ...overrides
  };
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', verifyScript);
  let error;
  try {
    await run(
      f.github,
      {repo},
      {info: (text) => logs.push(text), error: (text) => logs.push(text)},
      load,
      {env}
    );
  } catch (caught) {
    error = caught;
  }
  return {error, logs};
}

test('Java support union preserves runtime and Maven/build visibility', async () => {
  const files = [
    'docs/guide.md',
    'manual/core/index.md',
    'faq/index.md',
    'upgrade_guide/from_3x/index.md',
    'changelog/README.md',
    'ci/test.sh',
    '.github/workflows/release.yml',
    '.github/package.json',
    '.github/.hidden',
    'core/src/test/java/Test.java',
    'integration-tests/src/test/resources/fixture.cql',
    'README.md',
    'README-dev.md',
    'CONTRIBUTING.md',
    'codecov.yml',
    '.gitignore',
    '.editorconfig'
  ].map((name) => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 1313)).omit, true);
  for (const name of [
    'core/src/main/java/Driver.java',
    'mapper-runtime/src/main/java/Mapper.java',
    'pom.xml',
    'core/pom.xml',
    'core/src/test/resources/pom.xml',
    'docs/pom.xml',
    'Makefile',
    '.mvn/config',
    'distribution/src/assembly/binary-tarball.xml',
    'scripts/release/github_release.py',
    'LICENSE',
    'unknown.txt'
  ]) {
    assert.equal(
      (await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 1313)).omit,
      false,
      name
    );
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [
    ['README.md', true],
    ['core/src/main/java/Driver.java', false],
    ['unknown', false]
  ]) {
    const current = await guard.snapshot(
      fixture({
        files: [
          file('docs/releasing.md', {
            status: 'renamed',
            previous_filename: previous
          })
        ]
      }).github,
      repo,
      1313
    );
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('support-to-runtime renames remain visible without skipping the stock action', async () => {
  const current = await guard.snapshot(
    fixture({
      files: [
        file('core/src/main/java/Driver.java', {
          status: 'renamed',
          previous_filename: 'core/src/test/java/DriverTest.java'
        })
      ]
    }).github,
    repo,
    1313
  );
  assert.equal(current.omit, false);
  assert.equal(current.unsafeRename, false);
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of [
    '{}',
    'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]'
  ]) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('1313'), 1313);
  for (const value of [
    '',
    '0',
    '-1',
    '01',
    '1.0',
    '1e2',
    ' 1313',
    '1313\n',
    '1313,21',
    '${{ arbitrary }}',
    '9007199254740992',
    undefined,
    1313
  ]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination collects complete metadata across multiple pages', async () => {
  const files = Array.from({length: 105}, (_, i) =>
    file(`core/src/test/resources/fixture-${i}.cql`)
  );
  const f = fixture({files, pages: [files.slice(0, 100), files.slice(100)]});
  const snapshot = await guard.snapshot(f.github, repo, 1313);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'page:1', 'page:2', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {rawFiles: null},
    {rawFiles: {}},
    {rawFiles: [null]},
    ...[{head: {sha: ''}}, {head: {sha: 42}}, {base: {sha: 'invalid'}}].map(
      (invalid) => ({revisions: [invalid, invalid]})
    ),
    ...[0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1].map((changed_files) => ({
      revisions: [{changed_files}]
    })),
    {
      revisions: [{changed_files: 3001}, {changed_files: 3001}],
      files: Array.from({length: 3000}, (_, i) => file(`docs/${i}.md`))
    },
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases)
    await assert.rejects(guard.snapshot(fixture(options).github, repo, 1313));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [
    {head: {sha: 'c'.repeat(40)}},
    {base: {sha: 'c'.repeat(40)}},
    {changed_files: 2}
  ]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 1313), /revisions changed/);
  }
});

test('verification detects revision and file-record changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 1313);
  for (const options of [
    {revisions: [{head: {sha: 'c'.repeat(40)}}, {head: {sha: 'c'.repeat(40)}}]},
    {revisions: [{base: {sha: 'c'.repeat(40)}}, {base: {sha: 'c'.repeat(40)}}]},
    {files: [file('docs/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(
      guard.verify(fixture(options).github, repo, 1313, original.hash),
      /snapshot changed/
    );
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'c'.repeat(40)}}]});
  await assert.rejects(
    guard.verify(f.github, repo, 1313, original.hash),
    /revisions changed during label/
  );
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(
    guard.snapshot(fixture({failGetAt: 1}).github, repo, 1313),
    /lookup unavailable/
  );
  await assert.rejects(
    guard.snapshot(fixture({failGetAt: 2}).github, repo, 1313),
    /lookup unavailable/
  );
  await assert.rejects(
    guard.snapshot(fixture({fileFailure: true}).github, repo, 1313),
    /listing unavailable/
  );
  const original = await guard.snapshot(fixture().github, repo, 1313);
  await assert.rejects(
    guard.verify(fixture({labelFailure: true}).github, repo, 1313, original.hash),
    /Labels unavailable/
  );
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [
    [[file('README.md')], []],
    [[file('core/src/main/java/Config.java')], [label]]
  ]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 1313);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter((call) => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 1313);
  for (const options of [
    {files: [file('docs/changed.md')]},
    {failGetAt: 1},
    {fileFailure: true},
    {labelFailure: true}
  ]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter((call) => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [
    [[file('README.md')], [label]],
    [[file('core/src/main/java/Config.java')], []]
  ]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 1313);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(
      logs[0],
      /Verified head=[a-f0-9]{40} base=[a-f0-9]{40} count=1 snapshot=.*actual label checked/
    );
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('logical marker identity handles case and preserves unrelated labels', async () => {
  const marker = 'Omit-from-release-notes';
  const other = 'OMIT-FROM-RELEASE-NOTES-extra';
  const prefix = 'prefix-omit-from-release-notes';
  for (const [name, omit] of [
    ['README.md', true],
    ['core/src/main/java/Driver.java', false]
  ]) {
    const f = fixture({files: [file(name)], labels: [marker, other, prefix, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 1313);
    const {error} = await adapter(f, original.hash);
    if (omit) {
      assert.ifError(error);
      assert.equal(f.labels.has(marker), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has(marker), false);
      assert.equal(f.calls.filter((call) => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has(other), true);
    assert.equal(f.labels.has(prefix), true);
    assert.equal(f.labels.has('unrelated'), true);
  }
});

test('invalid actual-label metadata triggers cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 1313);
  for (const item of [null, {}, {name: 42}, {name: ''}]) {
    const f = fixture({labels: [label, 'unrelated'], labelMetadata: [item]});
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter((call) => call === 'remove').length, 1);
  }
});

test('unsafe-rename skip removes the marker and verifies visibility', async () => {
  const f = fixture({
    files: [
      file('README.md', {status: 'renamed', previous_filename: 'core/src/main/java/Driver.java'})
    ],
    labels: [label, 'unrelated']
  });
  const original = await guard.snapshot(f.github, repo, 1313);
  const {error, logs} = await adapter(f, original.hash, {
    UNSAFE_RENAME: 'true',
    LABELER_OUTCOME: 'skipped'
  });
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter((call) => call === 'remove').length, 1);
});

test('failed setup or classification cleans up without loading helpers', async () => {
  for (const key of [
    'TARGET_OUTCOME',
    'CHECKOUT_OUTCOME',
    'INSTALL_OUTCOME',
    'GUARD_OUTCOME',
    'LABELER_OUTCOME'
  ]) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => {
        throw new Error('Must not require helper');
      });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => {
    throw new Error('Helper missing');
  });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup failures preserve the original error without retries', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

async function confirm(f, context = {repo, eventName: 'workflow_dispatch'}) {
  const outputs = {};
  const run = new AsyncFunction('github', 'context', 'core', 'process', confirmScript);
  let error;
  try {
    await run(
      f.github,
      context,
      {
        setOutput: (key, value) => {
          outputs[key] = value;
        }
      },
      {env: {PR_NUMBER: '1313'}}
    );
  } catch (caught) {
    error = caught;
  }
  return {outputs, error};
}

test('target confirmation precedes setup for manual and trusted PR events', async () => {
  const order = ['target', 'checkout', 'install', 'guard', 'labeler'].map((id) =>
    steps.findIndex((step) => step.id === id)
  );
  assert.ok(
    order.every((position, index) => position >= 0 && (index === 0 || position > order[index - 1]))
  );
  const manual = fixture();
  const first = await confirm(manual);
  assert.ifError(first.error);
  assert.deepEqual(first.outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(manual.calls, ['get']);
  const event = fixture({failGetAt: 1});
  const second = await confirm(event, {
    repo,
    eventName: 'pull_request_target',
    payload: {pull_request: {number: 1313}}
  });
  assert.ifError(second.error);
  assert.deepEqual(second.outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(event.calls, []);
});

test('unconfirmed targets never enable label writes', async () => {
  for (const options of [{failGetAt: 1}, {revisions: [{number: 1314}]}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ok(target.error);
    assert.deepEqual(target.outputs, {});
    const result = await adapter(f, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'}, () => {
      throw new Error('Must not load helper for unconfirmed target');
    });
    assert.match(result.error.message, /not confirmed as a pull request/);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
    assert.ok(!f.calls.includes('remove'));
  }
  const issue = fixture({labels: [label, 'unrelated']});
  issue.github.rest.pulls.get = async () => {
    throw {status: 404};
  };
  const target = await confirm(issue);
  assert.equal(target.error.status, 404);
  const result = await adapter(issue, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'});
  assert.match(result.error.message, /labels were not changed/);
  assert.deepEqual([...issue.labels], [label, 'unrelated']);
  assert.ok(!issue.calls.includes('remove'));
});

test('confirmed manual PR retains helper-independent cleanup after later failures', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    const f = fixture({labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ifError(target.error);
    const result = await adapter(
      f,
      '',
      {CONFIRMED_PR: target.outputs['confirmed-pr'], [key]: 'failure'},
      () => {
        throw new Error('Must not need helper for predecessor failure');
      }
    );
    assert.match(result.error.message, /predecessor failed/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.deepEqual(f.calls, ['get', 'remove']);
  }
});

test('workflow adapters preserve input and verified snapshot wiring', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript);
  const outputs = {};
  const f = fixture();
  await run(
    f.github,
    {repo},
    {
      setOutput: (key, value) => {
        outputs[key] = value;
      },
      info: () => {}
    },
    () => guard,
    {env: {PR_NUMBER: '1313'}}
  );
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  assert.equal(steps.find((step) => step.id === 'checkout').with.ref, '${{ github.sha }}');
  assert.equal(steps.find((step) => step.id === 'checkout').with['persist-credentials'], false);
  assert.equal(
    steps.find((step) => step.id === 'install').run,
    'npm ci --ignore-scripts --prefix .github'
  );
  const action = steps.find((step) => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with.dot, true);
  assert.deepEqual(Object.keys(workflow.on.workflow_dispatch.inputs), ['pull-request']);
  assert.equal(
    workflow.jobs.classify.env.PR_NUMBER,
    '${{ github.event.pull_request.number || inputs.pull-request }}'
  );
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('merged-PR backfills load event tooling rather than the old PR base or head', () => {
  const expression = steps
    .find((step) => step.id === 'checkout')
    .with.ref.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const checkoutRef = new Function('github', `return ${expression}`);
  for (const event of [{pull_request: {head: {sha: 'untrusted'}, base: {sha: 'old-base'}}}, {}]) {
    assert.equal(checkoutRef({sha: 'trusted-default-event', event}), 'trusted-default-event');
  }
});

test('dispatch rejects untrusted refs without setup or label writes', async () => {
  assert.equal(steps[0].name, 'Require default-branch manual dispatch');
  const run = new AsyncFunction('context', steps[0].with.script);
  const payload = {repository: {default_branch: 'scylla-4.x'}};
  await run({eventName: 'workflow_dispatch', ref: 'refs/heads/scylla-4.x', payload});
  await run({eventName: 'pull_request_target', ref: 'refs/heads/release-branch', payload});
  for (const ref of ['refs/heads/feature', 'refs/tags/v1.2.3']) {
    await assert.rejects(
      run({eventName: 'workflow_dispatch', ref, payload}),
      /repository default branch/
    );
  }
  const f = fixture({labels: [label, 'unrelated']});
  const {error} = await adapter(
    f,
    '',
    {
      CONFIRMED_PR: '',
      TARGET_OUTCOME: 'skipped',
      CHECKOUT_OUTCOME: 'skipped',
      INSTALL_OUTCOME: 'skipped',
      GUARD_OUTCOME: 'skipped',
      LABELER_OUTCOME: 'skipped'
    },
    () => {
      throw new Error('Must not load helper');
    }
  );
  assert.match(error.message, /not confirmed as a pull request/);
  assert.deepEqual([...f.labels], [label, 'unrelated']);
  assert.deepEqual(f.calls, []);
});

test('trusted PR confirmation survives later setup failure and an unavailable lookup', async () => {
  const f = fixture({failGetAt: 1, labels: [label, 'unrelated']});
  const target = await confirm(f, {
    repo,
    eventName: 'pull_request_target',
    payload: {pull_request: {number: 1313}}
  });
  assert.ifError(target.error);
  const result = await adapter(
    f,
    '',
    {CONFIRMED_PR: target.outputs['confirmed-pr'], CHECKOUT_OUTCOME: 'failure'},
    () => {
      throw Error('Helper unavailable');
    }
  );
  assert.match(result.error.message, /predecessor failed/);
  assert.deepEqual(f.calls, ['remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
});

test('non-array label metadata is rejected and attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 1313);
  for (const labelMetadata of [null, {}]) {
    const f = fixture({labels: [label], labelMetadata});
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], []);
  }
});

test('actual Jira condition filters only logical marker add/remove events', () => {
  const jira = yaml.load(readFileSync(join(__dirname, '../workflows/call_jira_sync.yml'), 'utf8'));
  const condition = jira.jobs['jira-sync'].if.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const evaluate = (action, name) => {
    const values = {'github.event.action': action, 'github.event.label.name': name};
    // GitHub expression string comparisons are case-insensitive. Evaluate the
    // actual bounded condition rather than copying it into a separate predicate.
    const expression = condition.replace(
      /(github\.[\w.]+)\s*(==|!=)\s*'([^']*)'/g,
      (_, key, op, value) =>
        JSON.stringify(values[key]?.toLowerCase()) +
        (op === '==' ? '===' : '!==') +
        JSON.stringify(value.toLowerCase())
    );
    assert.ok(!expression.includes('github.'));
    return new Function(`return (${expression})`)();
  };
  for (const action of jira.on.pull_request_target.types) {
    for (const name of [label, 'Omit-From-Release-Notes', 'other', label + '-extra', undefined]) {
      const expected = !(
        ['labeled', 'unlabeled'].includes(action) &&
        [label, 'Omit-From-Release-Notes'].includes(name)
      );
      assert.equal(evaluate(action, name), expected, `${action}/${name}`);
    }
  }
  assert.equal(jira.jobs['jira-sync'].with.caller_action, '${{ github.event.action }}');
  assert.deepEqual(jira.jobs['jira-sync'].secrets, {
    caller_jira_auth: '${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}'
  });
});
