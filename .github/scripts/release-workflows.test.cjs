// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {spawnSync} = require('node:child_process');
const yaml = require('js-yaml');

const root = join(__dirname, '../..');
const load = path => yaml.load(readFileSync(join(root, path), 'utf8'));
const workflows = ['release', 'release-dry-run'].map(name =>
  load(`.github/workflows/${name}.yml`));
const validation = load('.github/actions/release-validate/action.yml');
const preparation = load('.github/actions/release-prepare/action.yml');
const action = (job, name) => job.steps.find(step =>
  step.uses === `./.github/actions/${name}`);
const expanded = job => job.steps.flatMap(step => step.uses?.startsWith('./')
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
    assert.equal(job.steps[0].with['persist-credentials'], false);
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
