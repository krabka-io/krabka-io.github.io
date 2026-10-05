const compositions = ['allOf', 'anyOf', 'oneOf'];

// A requirement applies to the immediate parent, even when that parent is optional.
export function requiredProperties(schema) {
  const required = new Set(schema.required ?? []);
  for (const option of schema.allOf ?? []) {
    for (const name of requiredProperties(option)) required.add(name);
  }
  for (const keyword of ['anyOf', 'oneOf']) {
    const options = (schema[keyword] ?? []).map(requiredProperties);
    if (options.length) {
      for (const name of options[0]) {
        if (options.every((option) => option.has(name))) required.add(name);
      }
    }
  }
  return required;
}

export function schemaType(schema) {
  const types = new Set(Array.isArray(schema.type) ? schema.type : schema.type ? [schema.type] : []);
  if (schema['x-kubernetes-int-or-string']) {
    types.add('integer');
    types.add('string');
  }
  if (!types.size && (schema.properties || schema.additionalProperties !== undefined)) types.add('object');
  if (!types.size && schema.items) types.add('array');
  const constraints = new Set(types.size ? [[...types].join(' | ')] : []);
  for (const option of schema.allOf ?? []) {
    const type = schemaType(option);
    if (type !== 'any') constraints.add(type);
  }
  for (const keyword of ['anyOf', 'oneOf']) {
    const options = (schema[keyword] ?? []).map(schemaType);
    if (options.length && !options.includes('any')) {
      constraints.add([...new Set(options)].map((type) => type.includes(' & ') ? `(${type})` : type).join(' | '));
    }
  }
  let type = [...constraints].map((part) => constraints.size > 1 && part.includes(' | ') ? `(${part})` : part).join(' & ') || 'any';
  if (schema.nullable && type !== 'any') type = `${type.includes(' & ') ? `(${type})` : type} | null`;
  return type;
}

export function fieldId(path, context = '') {
  // Escape punctuation without collapsing distinct paths such as a-b and a.b.
  return `field-${Array.from(context ? `${path}@${context}` : path, (char) => /[a-z0-9_]/i.test(char) ? char : `-${char.codePointAt(0).toString(16)}-`).join('')}`;
}

export function flattenSchema(schema) {
  const fields = [];
  function visit(node, path, required, context = '', inheritedRequired = new Set(), role = '', exampleSchema = node, parentId = null, name = path) {
    if (!node || typeof node !== 'object') return;
    const localRequired = requiredProperties(node);
    const allRequired = new Set([...inheritedRequired, ...localRequired]);
    fields.push({
      path, name, parentId, context, schema: node, exampleSchema, type: schemaType(node), id: fieldId(path, context),
      section: path === 'spec' || path.startsWith('spec.') || path.startsWith('spec[') ? 'spec' : path === 'status' || path.startsWith('status.') || path.startsWith('status[') ? 'status' : 'resource',
      requirement: role || `${required ? 'Required' : 'Optional'} in ${context || 'parent'}`,
    });
    for (const [name, child] of Object.entries(node.properties ?? {})) {
      const conditional = [];
      for (const keyword of ['anyOf', 'oneOf']) {
        (node[keyword] ?? []).forEach((option, index) => {
          if (requiredProperties(option).has(name)) conditional.push(`${path} ${keyword} option ${index + 1}`);
        });
      }
      const requirement = !allRequired.has(name) && conditional.length ? `Required in ${context ? `${context}; ` : ''}${conditional.join('; ')}; optional otherwise` : '';
      visit(child, path === '$' ? name : `${path}.${name}`, allRequired.has(name), context, new Set(), requirement, child, fieldId(path, context), name);
    }
    const items = Array.isArray(node.items) ? node.items : node.items ? [node.items] : [];
    items.forEach((child, index) => visit(child, `${path}[${Array.isArray(node.items) ? index : ''}]`, false, context, new Set(), 'Array item', child, fieldId(path, context), Array.isArray(node.items) ? `[${index}]` : '[]'));
    if (node.additionalProperties && typeof node.additionalProperties === 'object') {
      visit(node.additionalProperties, `${path}{key}`, false, context, new Set(), 'Map value', node.additionalProperties, fieldId(path, context), '{key}');
    }
    for (const keyword of compositions) {
      (node[keyword] ?? []).forEach((child, index) => {
        const label = `${path} ${keyword} option ${index + 1}`;
        visit(child, path, required, context ? `${context}; ${label}` : label, allRequired, 'Schema option', {
          ...node, [keyword]: [child], required: [...allRequired],
        }, fieldId(path, context), `${keyword} option ${index + 1}`);
      });
    }
  }
  visit(schema, '$', true, '', new Set(), 'Resource schema', schema, null, 'Resource schema');
  return fields;
}

export function schemaTree(fields) {
  const nodes = new Map(fields.map((field) => [field.id, { ...field, children: [] }]));
  const roots = [];
  for (const node of nodes.values()) {
    const parent = nodes.get(node.parentId);
    if (parent?.section === node.section) parent.children.push(node);
    else roots.push(node);
  }
  return roots;
}

const displayed = new Set(['description', 'properties', 'items', 'type', 'title', 'nullable', 'format', 'default', 'required']);
const labels = {
  enum: 'Allowed values',
  minimum: 'Minimum', maximum: 'Maximum', exclusiveMinimum: 'Exclusive minimum', exclusiveMaximum: 'Exclusive maximum',
  minLength: 'Minimum length', maxLength: 'Maximum length', pattern: 'Pattern',
  minItems: 'Minimum items', maxItems: 'Maximum items', uniqueItems: 'Unique items',
  minProperties: 'Minimum properties', maxProperties: 'Maximum properties', multipleOf: 'Multiple of',
  additionalProperties: 'Additional properties', allOf: 'All options must match',
  oneOf: 'Exactly one option must match', anyOf: 'At least one option must match',
};

export function schemaFacts(schema) {
  return Object.entries(schema)
    .filter(([key]) => !displayed.has(key))
    .map(([key, value]) => [labels[key] ?? key, JSON.stringify(value, null, 2)]);
}

// Keep examples small: required children, or one illustrative child for an optional object.
export function schemaExample(schema, path, kind, values = {}) {
  const override = values[kind]?.[path];
  if (override && Object.hasOwn(override, 'example')) return override.example;
  if (Object.hasOwn(schema, 'example')) return schema.example;
  if (schema.examples?.length) return schema.examples[0];
  if (Object.hasOwn(schema, 'const')) return schema.const;
  if (schema.enum?.length) return schema.enum.find((value) => value !== null) ?? null;
  for (const option of [...(schema.allOf ?? []), ...(schema.oneOf?.slice(0, 1) ?? []), ...(schema.anyOf?.slice(0, 1) ?? [])]) {
    schema = {
      ...schema, ...option,
      properties: { ...schema.properties, ...option.properties },
      required: [...new Set([...(schema.required ?? []), ...(option.required ?? [])])],
    };
  }
  if (schema.enum?.length) return schema.enum.find((value) => value !== null) ?? null;
  const type = (Array.isArray(schema.type) ? schema.type.find((value) => value !== 'null') : schema.type)
    ?? (schema.properties || schema.additionalProperties !== undefined ? 'object' : schema.items ? 'array' : 'string');
  if (type === 'object') {
    const properties = schema.properties ?? {};
    const names = [...requiredProperties(schema)];
    if (!names.length && Object.keys(properties).length) names.push(Object.keys(properties)[0]);
    const result = Object.fromEntries(names.map((name) => [name, schemaExample(properties[name] ?? {}, path === '$' ? name : `${path}.${name}`, kind, values)]));
    if (!names.length && schema.additionalProperties && typeof schema.additionalProperties === 'object') {
      result.example = schemaExample(schema.additionalProperties, `${path}{key}`, kind, values);
    }
    return result;
  }
  if (type === 'array') {
    const items = Array.isArray(schema.items) ? schema.items : [schema.items ?? {}];
    const length = Math.min(schema.maxItems ?? Infinity, Math.max(schema.minItems ?? 1, items.length));
    return Array.from({ length }, (_, index) => schemaExample(items[index % items.length], `${path}[${Array.isArray(schema.items) ? index : ''}]`, kind, values));
  }
  if (Object.hasOwn(schema, 'default') && schema.default !== null && schema.default !== '') return schema.default;
  if (schema['x-kubernetes-int-or-string']) return '256Mi';
  if (type === 'boolean') return true;
  if (type === 'integer' || type === 'number') {
    let value = Math.max(1, schema.minimum ?? -Infinity);
    if (typeof schema.exclusiveMinimum === 'number') value = Math.max(value, schema.exclusiveMinimum + 1);
    else if (schema.exclusiveMinimum && value === schema.minimum) value++;
    if (schema.multipleOf) value = Math.ceil(value / schema.multipleOf) * schema.multipleOf;
    value = Math.min(value, schema.maximum ?? Infinity);
    if (typeof schema.exclusiveMaximum === 'number') value = Math.min(value, schema.exclusiveMaximum - 1);
    else if (schema.exclusiveMaximum && value === schema.maximum) value--;
    return type === 'integer' ? Math.ceil(value) : value;
  }
  if (schema.format === 'date-time' || /\.(lastTransitionTime|notAfter)$/.test(path)) return '2026-10-05T12:00:00Z';
  if (schema.format === 'uri') return 'https://example.com';
  let value = path.includes('conditions[].') ? ({ type: 'Ready', status: 'True', reason: 'Reconciled', message: 'The resource is ready.' }[path.split('.').at(-1)] ?? 'example')
    : path.endsWith('topologyKey') ? 'kubernetes.io/hostname'
    : path.endsWith('.effect') ? 'NoSchedule'
    : path.endsWith('.operator') ? (path.includes('tolerations') ? 'Exists' : 'In')
    : /\.matchFields\[\]\.key$/.test(path) ? 'metadata.name'
    : /\.matchExpressions\[\]\.key$/.test(path) ? (path.includes('nodeAffinity') ? 'topology.kubernetes.io/zone' : 'app')
    : /\.match(?:Expressions|Fields)\[\]\.values\[\]$/.test(path) ? (path.includes('nodeAffinity') ? 'us-west-2a' : 'kafka')
    : /\.namespaces\[\]$/.test(path) ? 'default'
    : /\.(?:mis)?matchLabelKeys\[\]$/.test(path) ? 'app'
    : /\.(?:matchLabels|labels)\{key\}$/.test(path) ? 'kafka'
    : /\.annotations\{key\}$/.test(path) ? 'example-value'
    : path.endsWith('.namespace') ? 'default'
    : path.endsWith('.apiVersion') || path === 'apiVersion' ? 'v1'
    : path.endsWith('.kind') || path === 'kind' ? kind
    : path.endsWith('.name') ? 'example'
    : schema.pattern === '^User:.+$' ? 'User:alice' : 'example';
  if (schema.minLength) value = value.padEnd(schema.minLength, 'x');
  if (schema.maxLength) value = value.slice(0, schema.maxLength);
  return value;
}

export const fieldExample = (field, kind, values = {}) => schemaExample(field.exampleSchema ?? field.schema, field.path, kind, values);

export function fieldDefault(field, kind, values = {}) {
  if (Object.hasOwn(field.schema, 'default')) return { label: 'Schema default', value: JSON.stringify(field.schema.default, null, 2) };
  const note = values[kind]?.[field.path]?.defaultNote;
  if (note) return { label: field.section === 'status' ? 'Operator behavior' : 'Runtime / omission', value: note };
  if (field.path === '$') return { label: 'No resource default', value: 'Set apiVersion, kind, metadata, and the required spec fields.' };
  if (field.section === 'status') return { label: 'No schema default', value: 'Reported by the operator; no user-configurable default.' };
  if (['Array item', 'Map value', 'Schema option'].includes(field.requirement)) return { label: 'No independent default', value: 'The parent field determines this value.' };
  const documented = (field.schema.description ?? '').split(/\n\s*\n|(?<=[.!?])\s+(?=[A-Z])/)
    .filter((sentence) => /\bdefaults?\b|when (?:absent|omitted|unset|not set)/i.test(sentence)).join(' ').replace(/\s+/g, ' ').trim();
  if (documented && !field.requirement.startsWith('Required')) return { label: 'Documented behavior', value: documented };
  return {
    label: 'No schema default',
    value: field.requirement.startsWith('Required') ? 'Required when the parent object is present.' : 'No default is declared in the CRD. Omit this field unless needed.',
  };
}

export const kindSlug = (resource) => resource.spec.names.kind.toLowerCase();
