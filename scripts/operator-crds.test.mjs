import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { fromJSONSchema } from 'astro/zod';
import { fieldDefault, fieldExample, fieldId, flattenSchema, requiredProperties, schemaExample, schemaFacts, schemaTree, schemaType } from '../src/utils/operator-crds.mjs';

function validationSchema(schema) {
  if (typeof schema === 'boolean') return schema;
  const { default: unused, ...result } = schema;
  if (schema['x-kubernetes-int-or-string']) result.type = ['integer', 'string'];
  if (schema.properties) result.properties = Object.fromEntries(Object.entries(schema.properties).map(([name, child]) => [name, validationSchema(child)]));
  if (schema.items) result.items = Array.isArray(schema.items) ? schema.items.map(validationSchema) : validationSchema(schema.items);
  if (schema.additionalProperties && typeof schema.additionalProperties === 'object') result.additionalProperties = validationSchema(schema.additionalProperties);
  for (const keyword of ['allOf', 'anyOf', 'oneOf']) {
    if (schema[keyword]) result[keyword] = schema[keyword].map(validationSchema);
  }
  return result;
}

// Zod's JSON Schema converter does not enforce required-only composition branches.
function assertRequiredKeys(schema, value, label) {
  if (value === null || typeof value !== 'object') return;
  for (const name of requiredProperties(schema)) assert.ok(Object.hasOwn(value, name), `${label}: missing required ${name}`);
  for (const [name, child] of Object.entries(schema.properties ?? {})) {
    if (Object.hasOwn(value, name)) assertRequiredKeys(child, value[name], `${label}.${name}`);
  }
  if (Array.isArray(value) && schema.items) {
    value.forEach((item, index) => assertRequiredKeys(Array.isArray(schema.items) ? schema.items[index] : schema.items, item, `${label}[${index}]`));
  }
  if (schema.additionalProperties && typeof schema.additionalProperties === 'object') {
    for (const [name, child] of Object.entries(value)) {
      if (!Object.hasOwn(schema.properties ?? {}, name)) assertRequiredKeys(schema.additionalProperties, child, `${label}.${name}`);
    }
  }
}

function assertExample(schema, value, label) {
  assert.notEqual(value, undefined, `${label}: missing example`);
  assert.doesNotThrow(() => JSON.stringify(value), `${label}: example must serialize`);
  assertRequiredKeys(schema, value, label);
  const result = fromJSONSchema(validationSchema(schema), { defaultTarget: 'openapi-3.0' }).safeParse(value);
  assert.ok(result.success, `${label}: ${result.success ? '' : JSON.stringify(result.error.issues)}`);
}

test('schema walk keeps array, map and union paths with parent requirements', () => {
  const schema = {
    type: 'object', required: ['spec'], properties: {
      spec: { type: 'object', allOf: [{ required: ['name'] }], properties: {
        name: { type: 'string' }, enabled: { type: 'boolean', default: false },
        values: { type: 'array', items: { type: 'object', required: ['id'], properties: { id: { type: 'integer' } } } },
        labels: { type: 'object', additionalProperties: { type: 'string' } },
        choice: { oneOf: [
          { type: 'object', required: ['a'], properties: { a: { type: 'string' } } },
          { type: 'object', required: ['b'], properties: { b: { type: 'number' } } },
        ] },
      } },
    },
  };
  const fields = flattenSchema(schema);
  assert.equal(fields.find((field) => field.path === 'spec').requirement, 'Required in parent');
  assert.equal(fields.find((field) => field.path === 'spec.name').requirement, 'Required in parent');
  assert.equal(fields.find((field) => field.path === 'spec.values').requirement, 'Optional in parent');
  assert.equal(fields.find((field) => field.path === 'spec.values[].id').requirement, 'Required in parent');
  assert.equal(fields.find((field) => field.path === 'spec.labels{key}').requirement, 'Map value');
  assert.match(fields.find((field) => field.path === 'spec.choice.a').requirement, /^Required in spec.choice oneOf option 1$/);
  assert.match(fields.find((field) => field.path === 'spec.choice.b').context, /oneOf option 2/);
  assert.equal(new Set(fields.map((field) => field.id)).size, fields.length);
  assert.notEqual(fieldId('spec.a-b'), fieldId('spec.a.b'));
  assert.deepEqual(schemaFacts(schema.properties.spec.properties.enabled), []);
  assert.deepEqual(schemaFacts({ required: ['name'], minimum: 1 }), [['Minimum', '1']]);
  const tree = schemaTree(fields);
  const spec = tree.find((field) => field.path === 'spec');
  assert.equal(spec.children.find((field) => field.path === 'spec.values').children[0].children[0].name, 'id');
  assert.equal(spec.children.find((field) => field.path === 'spec.labels').children[0].name, '{key}');
  assert.equal(spec.children.find((field) => field.path === 'spec.choice').children[1].children[0].name, 'b');
  assert.equal(tree.find((field) => field.path === '$').children.length, 0);
});

test('examples respect constraints and defaults retain false, zero and empty arrays', () => {
  const schema = { type: 'object', required: ['enabled', 'count', 'items', 'renewers', 'authorization'], properties: {
    enabled: { type: 'boolean', default: false }, count: { type: 'integer', default: 0, minimum: 0, maximum: 2 },
    items: { type: 'array', default: [], minItems: 0, maxItems: 2, items: { type: 'integer', minimum: 3 } },
    renewers: { type: 'array', minItems: 1, items: { type: 'string', pattern: '^User:.+$' } },
    authorization: { type: 'object', properties: { type: { type: 'string', enum: ['simple'] } }, oneOf: [{ required: ['type'] }] },
    endpoint: { type: 'string', description: 'Service endpoint. Defaults to the cluster service when absent.' },
  } };
  const fields = flattenSchema(schema);
  for (const [path, value] of [['enabled', false], ['count', 0], ['items', []]]) {
    assert.deepEqual(fieldDefault(fields.find((field) => field.path === path), 'Example'), { label: 'Schema default', value: JSON.stringify(value, null, 2) });
  }
  for (const field of fields) assertExample(field.exampleSchema, fieldExample(field, 'Example'), field.path);
  assert.equal(fieldDefault(fields.find((field) => field.path === 'endpoint'), 'Example').value, 'Defaults to the cluster service when absent.');
  assert.deepEqual(fieldDefault(fields[0], 'Example'), { label: 'No resource default', value: 'Set apiVersion, kind, metadata, and the required spec fields.' });
  const option = fields.find((field) => field.context === 'authorization oneOf option 1');
  assert.deepEqual(fieldExample(option, 'Example'), { type: 'simple' });
  assert.throws(() => assertExample(option.exampleSchema, {}, 'authorization'), /missing required type/);
  assert.throws(() => assertExample({ oneOf: [{ required: ['type'] }] }, {}, 'required-only option'), /missing required type/);
  assert.throws(() => assertExample(schema, {}, 'resource'));
  assert.throws(() => assertExample(schema.properties.renewers, ['alice'], 'renewers'));
});

test('union requirements stay conditional unless every option requires the field', () => {
  assert.deepEqual([...requiredProperties({ oneOf: [{ required: ['type', 'a'] }, { required: ['type', 'b'] }] })], ['type']);
  assert.deepEqual([...requiredProperties({ anyOf: [{ required: ['a'] }, { required: ['b'] }] })], []);
  assert.equal(schemaType({ anyOf: [{ type: 'integer' }, { type: 'string' }], nullable: true }), 'integer | string | null');
  assert.equal(schemaType({ anyOf: [{}, { type: 'string' }] }), 'any');
  assert.equal(schemaType({ allOf: [{ type: 'integer' }, { type: 'number' }] }), 'integer & number');
  assert.equal(schemaType({ nullable: true }), 'any');
  assert.equal(schemaType({ 'x-kubernetes-int-or-string': true }), 'integer | string');
  const conditional = flattenSchema({ properties: { a: { type: 'string' }, b: { type: 'integer' } }, oneOf: [{ required: ['a'] }, { required: ['b'] }] });
  assert.equal(conditional.find((field) => field.path === 'a').requirement, 'Required in $ oneOf option 1; optional otherwise');
  assert.equal(conditional.find((field) => field.path === 'b').requirement, 'Required in $ oneOf option 2; optional otherwise');
});

const snapshotFile = new URL('../src/content/docs/operator/crds.json', import.meta.url);
test('all synced CRD fields and resource examples satisfy their schemas', { skip: !fs.existsSync(snapshotFile) }, () => {
  const snapshot = JSON.parse(fs.readFileSync(snapshotFile, 'utf8'));
  const values = JSON.parse(fs.readFileSync(new URL('../src/data/operator-field-values.json', import.meta.url), 'utf8'));
  assert.equal(snapshot.resources.length, 8);
  for (const resource of snapshot.resources) {
    const kind = resource.spec.names.kind;
    for (const version of resource.spec.versions) {
      const schema = version.schema.openAPIV3Schema;
      const fields = flattenSchema(schema);
      const paths = new Set(fields.map((field) => field.path));
      for (const path of Object.keys(values[kind] ?? {})) assert.ok(paths.has(path), `${kind}: stale example/default override ${path}`);
      assert.ok(fields.some((field) => field.path === 'spec'), kind);
      assert.ok(fields.some((field) => field.path === 'status.conditions[].type'), kind);
      assert.equal(new Set(fields.map((field) => field.id)).size, fields.length);
      for (const field of fields) {
        assertExample(field.exampleSchema, fieldExample(field, kind, values), `${kind} ${field.path} ${field.context}`);
        const defaultValue = fieldDefault(field, kind, values);
        assert.ok(defaultValue.label && defaultValue.value, `${kind} ${field.path}: missing default explanation`);
        if (Object.hasOwn(field.schema, 'default')) assert.equal(defaultValue.value, JSON.stringify(field.schema.default, null, 2));
      }
      assertExample(schema, {
        apiVersion: `${resource.spec.group}/${version.name}`, kind, metadata: { name: 'example', namespace: 'default' },
        spec: schemaExample(schema.properties.spec, 'spec', kind, values),
      }, `${kind} complete resource`);
    }
  }
  const user = snapshot.resources.find((resource) => resource.spec.names.kind === 'KafkaUser');
  const fields = flattenSchema(user.spec.versions[0].schema.openAPIV3Schema);
  assert.equal(fields.find((field) => field.path === 'spec.authorization.type').requirement, 'Required in parent');
  const authorization = fields.find((field) => field.path === 'spec.authorization' && field.context);
  assert.equal(fieldExample(authorization, 'KafkaUser', values).type, 'simple');
  assert.throws(() => assertExample(authorization.exampleSchema, {}, 'KafkaUser authorization'), /missing required type/);
  const pool = snapshot.resources.find((resource) => resource.spec.names.kind === 'KafkaNodePool');
  assert.equal(flattenSchema(pool.spec.versions[0].schema.openAPIV3Schema).find((field) => field.path === 'spec.resources.limits{key}').type, 'integer | string');
});
