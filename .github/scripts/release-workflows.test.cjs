// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync, readdirSync} = require('node:fs');
const {join} = require('node:path');
const {spawnSync} = require('node:child_process');
const yaml = require('js-yaml');
const {minimatch} = require('minimatch');

const root = join(__dirname, '../..');
const load = path => yaml.load(readFileSync(join(root, path), 'utf8'));
const workflows = ['release', 'release-dry-run'].map(name =>
  load(`.github/workflows/${name}.yml`));
const validation = load('.github/actions/release-validate/action.yml');
const preparation = load('.github/actions/release-prepare/action.yml');
const action = (job, name) => job.steps.find(step =>
  step.uses === `./.github/actions/${name}`);
const expanded = job => (job.steps || []).flatMap(step => step.uses?.startsWith('./')
  ? load(`${step.uses}/action.yml`).runs.steps : [step]);

test('release entry points expose only their applicable inputs and steps', () => {
  const [release, dryRun] = workflows;
  assert.deepEqual(Object.keys(release.on), ['workflow_dispatch']);
  assert.deepEqual(Object.keys(dryRun.on), ['workflow_dispatch']);
  assert.deepEqual(Object.keys(release.on.workflow_dispatch.inputs),
    ['skip-tests', 'target-tag', 'release-version']);
  assert.deepEqual(Object.keys(dryRun.on.workflow_dispatch.inputs),
    ['target-tag', 'release-version']);

  const drySteps = Object.values(dryRun.jobs).flatMap(expanded);
  const drySource = JSON.stringify(drySteps);
  for (const irrelevant of ['check-blockers.sh', 'github-release', 'git push',
    'action-send-mail', 'gh issue create', 'EMAIL_USERNAME', 'EMAIL_PASSWORD']) {
    assert.ok(!drySource.includes(irrelevant), irrelevant);
  }
  assert.ok(drySteps.some(step => step.run?.endsWith(' release-dry-run')));
  assert.ok(!expanded(release.jobs.release).some(step =>
    step.run?.endsWith(' release-dry-run')));
  assert.ok(expanded(release.jobs.release).some(step => step.run?.endsWith(' release')));
  for (const workflow of workflows) {
    assert.ok(!JSON.stringify(workflow).includes('inputs.dry-run'));
  }
});

test('dry-run project tests are mandatory in unsigned validation', () => {
  const [release, dryRun] = workflows;
  assert.equal(action(release.jobs.validate, 'release-validate').with['skip-tests'],
    '${{ inputs.skip-tests }}');
  assert.ok(!Object.hasOwn(action(dryRun.jobs.validate, 'release-validate').with,
    'skip-tests'));
  assert.equal(validation.inputs['skip-tests'].default, 'false');
  const build = validation.runs.steps.find(step =>
    step.name === 'Validate target without release credentials');
  assert.equal(build.env.RELEASE_SKIP_TESTS, '${{ inputs.skip-tests }}');
  assert.match(build.run, /-Prelease -Dgpg.skip=true verify/);
  assert.ok(!build.if);
});

test('both entry points share validation and preparation across the approval boundary', () => {
  for (const workflow of workflows) {
    const {validate, release} = workflow.jobs;
    assert.equal(validate.permissions.contents, 'read');
    assert.ok(!validate.environment);
    assert.ok(!JSON.stringify(validate).includes('secrets.'));
    assert.equal(validate.outputs['target-sha'], '${{ steps.target.outputs.target-sha }}');
    const validateCall = action(validate, 'release-validate');
    assert.equal(validateCall.id, 'target');
    assert.equal(validateCall.with['target-tag'], '${{ inputs.target-tag }}');
    assert.equal(validateCall.with['release-version'], '${{ inputs.release-version }}');
    assert.equal(release.needs, 'validate');
    assert.equal(release.environment, 'release');
    assert.deepEqual(release.concurrency,
      {group: 'connector-release', 'cancel-in-progress': false});
    const prepare = action(release, 'release-prepare');
    assert.equal(prepare.with['target-sha'], '${{ needs.validate.outputs.target-sha }}');
    assert.equal(prepare.with['target-tag'], '${{ inputs.target-tag }}');
    assert.equal(prepare.with['release-version'], '${{ inputs.release-version }}');
    assert.equal(prepare.with['signing-private-key'], '${{ secrets.GPG_PRIVATE_KEY }}');
    assert.equal(prepare.with['signing-passphrase'], '${{ secrets.GPG_PASSPHRASE }}');
    assert.equal(prepare.with['sonatype-token-username'], '${{ secrets.SONATYPE_TOKEN_USERNAME }}');
    assert.equal(prepare.with['sonatype-token-password'], '${{ secrets.SONATYPE_TOKEN_PASSWORD }}');
    assert.equal(validate.steps[0].with.ref, '${{ github.sha }}');
    assert.equal(release.steps[0].with.ref, '${{ github.sha }}');
    for (const job of [validate, release]) {
      const uploads = job.steps.filter(step => step.uses?.startsWith('actions/upload-artifact@'));
      assert.equal(uploads.length, 2);
      assert.deepEqual(uploads.map(step => step.with.path),
        ['${{ runner.temp }}/release-logs/stdout.log', '${{ runner.temp }}/release-logs/stderr.log']);
      assert.ok(uploads.every(step => step.if === 'always()'));
    }
  }
  for (const job of Object.values(workflows[1].jobs)) {
    assert.deepEqual(job.permissions, {contents: 'read'});
    if (job.steps) assert.equal(job.steps[0].with['persist-credentials'], false);
  }
  assert.equal(validation.outputs['target-sha'].value, '${{ steps.target.outputs.sha }}');
});

test('shared actions use pinned dependencies and valid bash without shell input interpolation', () => {
  for (const composite of [validation, preparation]) {
    assert.equal(composite.runs.using, 'composite');
    for (const step of composite.runs.steps) {
      if (step.uses) assert.match(step.uses, /@[0-9a-f]{40}$/);
      if (step.run) {
        assert.equal(step.shell, 'bash');
        assert.ok(!step.run.includes('${{'), step.name);
        const parsed = spawnSync('bash', ['-n'], {input: step.run, encoding: 'utf8'});
        assert.equal(parsed.status, 0, `${step.name}: ${parsed.stderr}`);
      }
    }
  }
  const steps = preparation.runs.steps;
  const position = name => steps.findIndex(step => step.name === name);
  assert.ok(position('Checkout release target') <
    position('Check release credential safety before importing credentials'));
  assert.ok(position('Check release credential safety before importing credentials') <
    position('Reject missing or placeholder Sonatype tokens'));
  assert.ok(position('Reject missing or placeholder Sonatype tokens') <
    position('Prepare release'));
  const prepare = steps.find(step => step.name === 'Prepare release');
  assert.equal(prepare.env.RELEASE_GPG_PRIVATE_KEY, '${{ inputs.signing-private-key }}');
  assert.equal(prepare.env.MAVEN_GPG_PASSPHRASE, '${{ inputs.signing-passphrase }}');
});

test('both release entry points require shared tooling checks before approval', () => {
  for (const workflow of workflows) {
    const tooling = workflow.jobs['release-tooling'];
    assert.equal(tooling.uses, './.github/workflows/release-credential-safety.yml');
    assert.deepEqual(tooling.permissions, {contents: 'read'});
    assert.ok(!tooling.if, 'skip-tests must not bypass release tooling checks');
    assert.ok(!tooling.secrets, 'tooling checks must not inherit release secrets');
    assert.equal(workflow.jobs.validate.needs, 'release-tooling');
    assert.equal(workflow.jobs.release.needs, 'validate');
  }
  assert.ok(!JSON.stringify(validation).includes('python3 -m unittest'),
    'the validation runner must not repeat the parallel helper suite');
});

test('parallel tooling groups cover every Python test module exactly once', () => {
  const policy = load('.github/workflows/release-credential-safety.yml');
  assert.ok(Object.hasOwn(policy.on, 'workflow_call'));
  assert.deepEqual(policy.permissions, {contents: 'read'});
  const python = policy.jobs['python-tests'];
  assert.equal(python.strategy['fail-fast'], false);
  const groups = python.strategy.matrix.include;
  const assigned = groups.flatMap(group => group.modules.split(/\s+/));
  const discovered = readdirSync(join(root, 'scripts/release'))
    .filter(name => /^test_.*\.py$/.test(name)).map(name => name.slice(0, -3));
  assert.deepEqual(assigned.sort(), discovered.sort(), 'no missing or duplicated modules');
  for (const heavy of ['test_capture', 'test_signing']) {
    assert.ok(groups.some(group => group.modules === heavy), `${heavy} needs its own runner`);
  }
  const runner = python.steps.find(step => step.name === 'Test release helpers');
  assert.equal(runner.env.PYTHONPATH, 'scripts/release');
  assert.equal(runner.env.TEST_MODULES, '${{ matrix.modules }}');
  assert.ok(runner.run.includes('python3 -m unittest -v "${modules[@]}"'));
  for (const job of Object.values(policy.jobs)) {
    assert.ok(!job.needs, 'tooling jobs must be independent');
    assert.ok(!job.environment);
    assert.ok(!JSON.stringify(job).includes('secrets.'));
    assert.equal(job.steps[0].with['persist-credentials'], false);
  }
});

test('automatic tooling checks include their inputs and skip ordinary connector changes', () => {
  const policy = load('.github/workflows/release-credential-safety.yml');
  assert.deepEqual(policy.on.push.branches, ['master']);
  assert.deepEqual(policy.on.pull_request.paths, policy.on.push.paths);
  for (const event of ['pull_request', 'push']) {
    const paths = policy.on[event].paths;
    const triggers = file => paths.some(pattern => minimatch(file, pattern, {dot: true}));
    for (const file of [
      'Makefile', '.mvn/maven.config', '.mvn/jvm.config', 'pom.xml',
      'scripts/release/guard.py', 'scripts/release/signing.py',
      'scripts/release/publisher.asc', 'scripts/release/test_capture.py',
      '.github/actions/release-validate/action.yml', '.github/actions/release-prepare/action.yml',
      '.github/workflows/release.yml', '.github/workflows/release-dry-run.yml',
      '.github/workflows/release-credential-safety.yml',
      '.github/workflows/label-release-notes.yml', '.github/workflows/call_jira_sync.yml',
      '.github/scripts/release-notes-guard.cjs', '.github/scripts/release-workflows.test.cjs',
      '.github/package.json', '.github/package-lock.json', '.github/labeler.yml', '.github/release.yml'
    ]) assert.ok(triggers(file), `${event} must cover ${file}`);
    for (const file of ['README.md', 'src/main/java/Connector.java',
      'src/test/java/ConnectorTest.java', '.github/workflows/pr-verify.yml']) {
      assert.ok(!triggers(file), `${event} should skip ${file}`);
    }
  }
});
