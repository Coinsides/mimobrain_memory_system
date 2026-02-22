# Vault URI v0.1

A **Vault URI** is a stable logical identifier for content in the memory system.
It MUST remain stable across machines and filesystem moves.

## Format

```
vault://<vault_id>/<kind>/<path>
```

- `vault_id`: logical vault namespace (e.g. `default`)
- `kind` (enum):
  - `raw` | `mu` | `assets` | `manifests` | `logs` | `derived`
- `path`: slash-separated path inside the kind (no leading slash)

### Examples
- `vault://default/raw/2026/02/21/foo.md`
- `vault://default/mu/2026/02/mu_01J....mimo`
- `vault://default/assets/sha256_.../blob`

## Rules
- Consumers MUST NOT treat vault URIs as absolute filesystem paths.
- Mapping URIs to local replicas is handled by **manifest** (multi-replica support).
