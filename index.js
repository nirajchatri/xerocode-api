import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const appRoot = path.dirname(fileURLToPath(import.meta.url));
const packageJsonPath = path.join(appRoot, 'package.json');
const expressPath = path.join(appRoot, 'node_modules', 'express', 'package.json');

if (!fs.existsSync(packageJsonPath)) {
  console.error(`Missing package.json in ${appRoot}. Deploy the full API project before starting the server.`);
  process.exit(1);
}

if (!fs.existsSync(expressPath)) {
  console.error(
    [
      `Dependencies are not installed in ${appRoot}.`,
      `Run: cd ${appRoot} && npm install --omit=dev`,
      'Then verify: test -d node_modules/express',
    ].join('\n')
  );
  process.exit(1);
}

const requiredPaths = [
  'server/index.js',
  'api/index.js',
  'api/agentStudioWorkflow.js',
  'api/lib/workflowRunOrder.js',
];
const missingPaths = requiredPaths.filter((relativePath) => !fs.existsSync(path.join(appRoot, relativePath)));
if (missingPaths.length > 0) {
  console.error(
    [
      `API deploy layout is incomplete in ${appRoot}.`,
      'Keep the api/ and server/ folders from the repo; do not flatten api/*.js into the project root.',
      `Missing: ${missingPaths.join(', ')}`,
    ].join('\n')
  );
  process.exit(1);
}

await import('./server/index.js');
