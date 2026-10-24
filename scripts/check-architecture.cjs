const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');
const root = path.resolve(__dirname, '..');
const violations = [];

function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory()
      ? walk(file)
      : file.endsWith('.ts') && !file.endsWith('.test.ts')
        ? [file]
        : [];
  });
}
function owner(file) {
  const relative = path.relative(root, file).replaceAll(path.sep, '/');
  const match = relative.match(
    /^apps\/([^/]+)\/src\/modules\/([^/]+)\/(domain|application|infrastructure|presentation|composition)(?:\/|\.)/,
  );
  return { relative, service: relative.split('/')[1], module: match?.[2], layer: match?.[3] };
}
function report(file, message) {
  violations.push(`${path.relative(root, file)}: ${message}`);
}
let checked = 0;
for (const service of fs.readdirSync(path.join(root, 'apps'))) {
  const directory = path.join(root, 'apps', service, 'src');
  if (!fs.existsSync(directory)) {
    violations.push(`Missing service ${service}`);
    continue;
  }
  for (const file of walk(directory)) {
    checked++;
    const source = ts.createSourceFile(file, fs.readFileSync(file, 'utf8'), ts.ScriptTarget.Latest, true);
    const from = owner(file);
    for (const node of source.statements) {
      if (!ts.isImportDeclaration(node) && !ts.isExportDeclaration(node)) continue;
      const specifier = node.moduleSpecifier;
      if (!specifier || !ts.isStringLiteral(specifier)) continue;
      const target = specifier.text;
      const named =
        ts.isImportDeclaration(node) &&
        node.importClause?.namedBindings &&
        ts.isNamedImports(node.importClause.namedBindings)
          ? node.importClause.namedBindings.elements.map(
              (element) => (element.propertyName ?? element.name).text,
            )
          : [];
      if (target.startsWith('.')) {
        const resolved = path.resolve(path.dirname(file), target);
        const to = owner(resolved);
        if (to.relative.startsWith('apps/') && to.service !== from.service)
          report(file, `cross-service import ${target}`);
        const moduleMatch = to.relative.match(/^apps\/[^/]+\/src\/modules\/([^/]+)(.*)$/);
        if (from.layer === 'domain' && moduleMatch && moduleMatch[1] !== from.module)
          report(file, `domain imports another business module: ${target}`);
        if (
          from.module &&
          moduleMatch &&
          moduleMatch[1] !== from.module &&
          !['', '/index', '/index.ts'].includes(moduleMatch[2]) &&
          !(from.layer === 'composition' && to.layer === 'composition')
        ) {
          report(file, `private module import ${target}`);
        }
        const forbidden = {
          domain: ['application', 'infrastructure', 'presentation', 'composition'],
          application: ['infrastructure', 'presentation', 'composition'],
          presentation: ['infrastructure', 'composition'],
          infrastructure: ['presentation', 'composition'],
        };
        if (from.layer && forbidden[from.layer]?.includes(to.layer))
          report(file, `${from.layer} imports ${to.layer}: ${target}`);
        if (from.relative.includes('/bootstrap/') && moduleMatch && to.layer !== 'composition')
          report(file, `bootstrap bypasses composition: ${target}`);
      } else {
        if (
          target.startsWith('@ticketing/') &&
          !['@ticketing/core', '@ticketing/contracts', '@ticketing/observability'].includes(target)
        )
          report(file, `cross-service package ${target}`);
        if (['domain', 'application'].includes(from.layer) && target === '@ticketing/observability')
          report(file, 'telemetry in business layer');
        if (
          from.layer === 'domain' &&
          !target.startsWith('node:') &&
          target !== '@ticketing/contracts' &&
          target !== '@ticketing/core'
        )
          report(file, `domain external dependency ${target}`);
        if (
          ['domain', 'application', 'presentation'].includes(from.layer) &&
          /^(pg|redis|amqplib|kafkajs)(\/|$)/.test(target)
        )
          report(file, `driver outside infrastructure: ${target}`);
        if (
          ['domain', 'application'].includes(from.layer) &&
          target === '@ticketing/core' &&
          (!named.length || named.some((name) => !['AppError', 'TransactionPort'].includes(name)))
        )
          report(file, `platform leaked into ${from.layer}: ${named.join(',')}`);
        if (
          from.layer === 'application' &&
          target.startsWith('@nestjs/') &&
          (target !== '@nestjs/common' || named.some((name) => !['Inject', 'Injectable'].includes(name)))
        )
          report(file, `Nest API leaked into application`);
        if (
          from.layer === 'presentation' &&
          target === '@ticketing/core' &&
          named.some((name) => ['Database', 'RedisConnection', 'SqlRowStream'].includes(name))
        )
          report(file, `presentation imports storage`);
      }
    }
  }
}
if (violations.length) {
  console.error(violations.join('\n'));
  process.exitCode = 1;
} else console.log(`Architecture OK: ${checked} source files; module layers and service ownership checked.`);
