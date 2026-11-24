import type { BackendManifest } from '../../agent/backend-manifest.js'

/**
 * The remote Web backend: attaches the TUI to an already-running DSH Host over
 * its loopback Web protocol. It owns no local Agent and imports no DSH package,
 * and it declares exactly the surfaces its session capabilities serve.
 */
export const manifest: BackendManifest = {
  id: 'remote',
  label: { kind: 'literal', text: 'Remote Web' },
  shortLabel: 'Remote Web',
  product: 'dsh-web',
  inTree: true,
  backendExport: 'remoteBackend',
  vendorPackages: [],
  capabilities: ['transcript', 'permissions', 'questions', 'pendingQueue', 'models', 'effort', 'rename', 'fork', 'subagents', 'tasks'],
  grants: [],
}
