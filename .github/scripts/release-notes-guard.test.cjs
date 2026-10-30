// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const yaml = require('js-yaml');
const guard = require('./release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'scylla-cdc-source-connector'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const confirmScript = steps.find(step => step.id === 'target').with.script;
const collectScript = steps.find(step => step.id === 'guard').with.script;
const verifyScript = steps.at(-1).with.script;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {number: 20, head: {sha: 'head'}, base: {sha: 'base'}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 20});
      calls.push('get');
      gets++;
      if (options.failGetAt === gets) throw Object.assign(new Error('PR lookup unavailable'),
        {status: options.lookupStatus});
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 20, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      // GitHub looks up the logical label without case sensitivity while
      // preserving its stored spelling in returned metadata.
      const actual = [...labels].find(name => name.toLowerCase() === label);
      if (actual === undefined) throw {status: 404};
      labels.delete(actual);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 20, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 20, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return options.labelMetadata ?? [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = () => guard) {
  const env = {PR_NUMBER: '20', TARGET_OUTCOME: 'success', CONFIRMED_PR_NUMBER: '20',
    CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', verifyScript);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

test('canonical policy matches mixed support files and keeps runtime or unknown paths visible', async () => {
  const files = ['.github/workflows/pr-verify.yml', '.github/.hidden', 'README.md',
    'README-QUICKSTART.md', 'QUICKSTART-ELASTICSEARCH-INTEGRATION.md', 'cp-quickstart/README.md',
    'src/test/java/ConnectorTest.java', 'src/test/resources/logback.xml',
    'images/scylla-cdc-source-connector-control-center1.png', '.gitignore', 'renovate.json',
    'scripts/release/test_guard.py', 'scripts/release/test_workflow.py',
    'scripts/release/test_github_release.py'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 20)).omit, true);
  for (const name of ['.github/workflows/release.yml', 'pom.xml', 'Makefile', 'LICENSE',
    'src/main/java/Runtime.java', 'src/main/resources/config.properties', 'scripts/release/guard.py',
    'images/connector.png', 'images/scylla.png', 'images/new.png', 'examples/run.sh',
    'cp-quickstart/run.sh', 'unknown.txt', 'docs/code.java', 'docs/guide.md']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 20);
    assert.equal(current.omit, false, name);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['README.md', true], ['src/main/java/Runtime.java', false],
    ['.github/workflows/release.yml', false], ['images/connector.png', false],
    ['images/scylla.png', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('README-QUICKSTART.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 20);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('support-to-runtime renames remain visible without skipping the stock action', async () => {
  const current = await guard.snapshot(fixture({files: [file('src/main/java/Runtime.java', {
    status: 'renamed', previous_filename: 'src/test/java/RuntimeTest.java'})]}).github, repo, 20);
  assert.equal(current.omit, false);
  assert.equal(current.unsafeRename, false);
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('20'), 20);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 20', '20\n',
    '20,21', '${{ arbitrary }}', '9007199254740992', undefined, 20]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`src/test/java/Fixture${i}.java`))});
  const snapshot = await guard.snapshot(f.github, repo, 20);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 20));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'new'}}, {base: {sha: 'new'}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 20), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [
    {revisions: [{head: {sha: 'new'}}, {head: {sha: 'new'}}]},
    {revisions: [{base: {sha: 'new'}}, {base: {sha: 'new'}}]},
    {files: [file('docs/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 20, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'new'}}]});
  await assert.rejects(guard.verify(f.github, repo, 20, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 20), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 20);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 20, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('README.md')], []], [[file('doc.go')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [{files: [file('docs/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('README.md')], [label]], [[file('doc.go')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=head base=base count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('actual logical marker identity preserves support omissions and rejects runtime omissions regardless of casing', async () => {
  const marker = 'Omit-from-release-notes';
  const other = 'OMIT-FROM-RELEASE-NOTES-extra';
  for (const [name, omit] of [['README.md', true], ['src/main/java/Runtime.java', false]]) {
    const f = fixture({files: [file(name)], labels: [marker, other, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    if (omit) {
      assert.ifError(error);
      assert.equal(f.labels.has(marker), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has(marker), false);
      assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has(other), true);
    assert.equal(f.labels.has('unrelated'), true);
  }
});

test('invalid actual-label metadata fails verification and attempts canonical cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const item of [null, {}, {name: 42}, {name: ''}]) {
    const f = fixture({labels: [label, 'unrelated'], labelMetadata: [item]});
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('README.md', {status: 'renamed', previous_filename: 'src/main/java/Runtime.java'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 20);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('failed or skipped setup, guard and labeler clean up without requiring the helper', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
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

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    () => guard, {env: {PR_NUMBER: '20'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  assert.equal(steps.find(step => step.id === 'checkout').with.ref, '${{ github.sha }}');
  assert.equal(steps.find(step => step.id === 'checkout').with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('merged-PR backfills load event tooling rather than the old PR base or head', () => {
  const expression = steps.find(step => step.id === 'checkout').with.ref.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
  const checkoutRef = new Function('github', `return ${expression}`);
  for (const event of [{pull_request: {head: {sha: 'untrusted'}, base: {sha: 'old-base'}}}, {}]) {
    assert.equal(checkoutRef({sha: 'trusted-default-event', event}), 'trusted-default-event');
  }
});

test('dispatch precondition rejects non-default refs before checkout and leaves target unconfirmed', async () => {
  assert.equal(steps[0].name, 'Require default-branch manual dispatch');
  const run = new AsyncFunction('context', steps[0].with.script);
  const payload = {repository: {default_branch: 'master'}};
  await run({eventName: 'workflow_dispatch', ref: 'refs/heads/master', payload});
  await run({eventName: 'pull_request_target', ref: 'refs/heads/release-branch', payload});
  for (const ref of ['refs/heads/feature', 'refs/tags/v1.2.3']) {
    await assert.rejects(run({eventName: 'workflow_dispatch', ref, payload}), /repository default branch/);
  }
  const f = fixture({labels: [label, 'unrelated']});
  const {error} = await adapter(f, '', {TARGET_OUTCOME: 'skipped', CONFIRMED_PR_NUMBER: '',
    CHECKOUT_OUTCOME: 'skipped', INSTALL_OUTCOME: 'skipped', GUARD_OUTCOME: 'skipped',
    LABELER_OUTCOME: 'skipped'}, () => {throw new Error('Must not load helper');});
  assert.match(error.message, /not confirmed as the requested pull request/);
  assert.deepEqual([...f.labels], [label, 'unrelated']);
  assert.deepEqual(f.calls, []);
});

test('dispatch and event PR numbers retain the local input and serialization contract', () => {
  assert.equal(workflow.on.workflow_dispatch.inputs['pull-request'].type, 'string');
  assert.equal(workflow.on.workflow_dispatch.inputs['pull-request'].required, true);
  assert.equal(workflow.concurrency.group,
    'release-notes-${{ github.event.pull_request.number || inputs.pull-request }}');
  assert.equal(workflow.concurrency['cancel-in-progress'], false);
  assert.equal(workflow.jobs.classify.env.PR_NUMBER,
    '${{ github.event.pull_request.number || inputs.pull-request }}');
  assert.deepEqual(workflow.on.pull_request_target.types,
    ['opened', 'synchronize', 'reopened', 'edited', 'closed']);
});

async function confirm(f, context = {repo, eventName: 'workflow_dispatch'}, raw = '20') {
  const outputs = {};
  const run = new AsyncFunction('github', 'context', 'core', 'process', confirmScript);
  let error;
  try {
    await run(f.github, context, {setOutput: (name, value) => {outputs[name] = value;}},
      {env: {PR_NUMBER: raw}});
  } catch (caught) { error = caught; }
  return {outputs, error};
}

async function failedSetup(f, confirmation, overrides = {}) {
  return adapter(f, '', {TARGET_OUTCOME: confirmation.error ? 'failure' : 'success',
    CONFIRMED_PR_NUMBER: confirmation.outputs['confirmed-pr-number'] ?? '',
    CHECKOUT_OUTCOME: 'failure', ...overrides},
  () => {throw new Error('Helper must remain unavailable during failed setup');});
}

test('exact PR confirmation runs before checkout and install and binds finalizer inputs', async () => {
  const target = steps.findIndex(step => step.id === 'target');
  assert.ok(target > 0);
  assert.ok(target < steps.findIndex(step => step.id === 'checkout'));
  assert.ok(target < steps.findIndex(step => step.id === 'install'));
  assert.equal(steps.at(-1).env.TARGET_OUTCOME, '${{ steps.target.outcome }}');
  assert.equal(steps.at(-1).env.CONFIRMED_PR_NUMBER,
    '${{ steps.target.outputs.confirmed-pr-number }}');
  const f = fixture();
  const confirmed = await confirm(f);
  assert.ifError(confirmed.error);
  assert.deepEqual(confirmed.outputs, {'confirmed-pr-number': '20'});
  assert.deepEqual(f.calls, ['get']);
});

test('manual issues, unavailable lookups and mismatched identities make zero label writes', async () => {
  for (const options of [{failGetAt: 1, lookupStatus: 404}, {failGetAt: 1},
    {revisions: [{number: 21}]}, {revisions: [{number: undefined}]},
    {revisions: [{number: '20'}]}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const result = await confirm(f);
    assert.ok(result.error);
    assert.deepEqual(result.outputs, {});
    const final = await failedSetup(f, result);
    assert.match(final.error.message, /not confirmed as the requested pull request/);
    assert.deepEqual(f.calls, ['get']);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
});

test('invalid confirmation inputs and mismatched trusted events remain unconfirmed without API writes', async () => {
  for (const raw of ['', '0', '-1', '01', '1.0', '1e2', ' 20', '20\n',
    '20,21', '9007199254740992']) {
    const f = fixture({labels: [label, 'unrelated']});
    const result = await confirm(f, {repo, eventName: 'workflow_dispatch'}, raw);
    assert.ok(result.error);
    assert.deepEqual(result.outputs, {});
    assert.ok((await failedSetup(f, result, {PR_NUMBER: raw})).error);
    assert.deepEqual(f.calls, []);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
  for (const context of [{repo, eventName: 'pull_request_target', payload: {}},
    {repo, eventName: 'pull_request_target', payload: {pull_request: {number: 21}}},
    {repo, eventName: 'issues', payload: {pull_request: {number: 20}}}]) {
    const f = fixture({labels: [label, 'unrelated']});
    const result = await confirm(f, context);
    assert.ok(result.error);
    assert.deepEqual(result.outputs, {});
    assert.ok((await failedSetup(f, result)).error);
    assert.deepEqual(f.calls, []);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
});

test('missing, failed, skipped or mismatched persisted confirmation cannot authorize finalizer writes', async () => {
  const confirmed = {outputs: {'confirmed-pr-number': '20'}};
  for (const overrides of [{CONFIRMED_PR_NUMBER: ''}, {CONFIRMED_PR_NUMBER: 'true'},
    {CONFIRMED_PR_NUMBER: '21'}, {PR_NUMBER: '21'}, {TARGET_OUTCOME: ''},
    {TARGET_OUTCOME: 'failure'}, {TARGET_OUTCOME: 'skipped'}, {TARGET_OUTCOME: 'cancelled'}]) {
    const f = fixture({labels: [label, 'unrelated']});
    const result = await failedSetup(f, confirmed, overrides);
    assert.match(result.error.message, /not confirmed as the requested pull request/);
    assert.deepEqual(f.calls, []);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
});

test('confirmed manual PRs retain cleanup after later setup, helper and read failures', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    const f = fixture({failGetAt: 2, labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ifError(target.error);
    const result = await adapter(f, '', {CONFIRMED_PR_NUMBER: target.outputs['confirmed-pr-number'],
      [key]: 'failure'}, () => {throw new Error('Helper must not be needed');});
    assert.match(result.error.message, /predecessor failed/);
    assert.deepEqual(f.calls, ['get', 'remove']);
    assert.deepEqual([...f.labels], ['unrelated']);
  }
  for (const [options, load, message] of [
    [{}, () => {throw new Error('Helper unavailable');}, /Helper unavailable/],
    [{failGetAt: 2}, () => guard, /lookup unavailable/],
    [{fileFailure: true}, () => guard, /listing unavailable/]
  ]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ifError(target.error);
    const result = await adapter(f, '', {CONFIRMED_PR_NUMBER: target.outputs['confirmed-pr-number']}, load);
    assert.match(result.error.message, message);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    assert.deepEqual([...f.labels], ['unrelated']);
  }
});

test('trusted matching PR events retain cleanup without another required identity lookup', async () => {
  for (const options of [{labels: [label, 'unrelated']}, {labels: ['unrelated']},
    {labels: [label, 'unrelated'], cleanupFailure: true}]) {
    const f = fixture({...options, failGetAt: 1});
    const target = await confirm(f, {repo, eventName: 'pull_request_target',
      payload: {pull_request: {number: 20}}});
    assert.ifError(target.error);
    assert.deepEqual(target.outputs, {'confirmed-pr-number': '20'});
    assert.deepEqual(f.calls, []);
    const result = await failedSetup(f, target);
    assert.match(result.error.message, /predecessor failed/);
    assert.deepEqual(f.calls, ['remove']);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.labels.has(label), Boolean(options.cleanupFailure));
    if (options.cleanupFailure) assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  }
});
