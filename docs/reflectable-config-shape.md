# Reflectable config shape (scalo contract, cross-language SSoT)

This is the single source of truth for the schema + capability-catalog shape that
BOTH scalo (Rust) and scalo-py (Python) emit. Consumers reflect on contracts
produced by either language through ONE code path, so the two MUST emit identical
JSON. Tracks scalo-rs#6 and scalo-py#3.

## The idea, plainly

An app already knows its own config surface. We want it to hand that knowledge to
the control plane so the engine and UI can build the right CRUD form for each
endpoint without anyone hand-writing a form per source type. Two things get
emitted:

1. A CONFIG SCHEMA -- the typed shape of the whole `Config`, derived
   automatically (schemars in Rust, pydantic in Python). This gives field names,
   types, required-ness, defaults, and doc-string descriptions for free.

2. A CAPABILITY CATALOG -- the part a schema cannot know. Service names like
   "cloudtrail" are runtime DATA (a string in a list), not types, and their knobs
   are read ad-hoc deep in the fetch code. The schema can only say
   `services: [{name: string, config: object}]`. The catalog says "cloudtrail
   exists, it is a child of the aws source, and it takes these knobs". It is
   hand-authored per app, grounded in what the code actually reads.

Both are carried INLINE in the deployment contract AND written as standalone
files, so one fetch of the container contract gives everything, and the files are
diffable in the repo.

## The four files

Emitted into `docs/` in each app repo (safer than the repo root, and where
scalo's other emitted artefacts already live -- `metrics-manifest.json`,
`deployment-contract.json`). Filenames follow scalo's existing kebab-case
`<domain>-<kind>` artefact convention so they never collide with app source
(NOT the generic `config.json` / `capabilities.json`). `docs/**` is CI
paths-ignored, so committing regenerated artefacts is CI-free; the drift test
still runs on any code change.

| File | Content |
|------|---------|
| `config-schema.json` | JSON Schema (draft 2020-12) of `Config`, JSON encoding |
| `config-schema.yaml` | Same schema, YAML encoding (human-friendly, parity with the app's own YAML config) |
| `capability-catalog.json`  | The capability catalog, JSON encoding |
| `capability-catalog.yaml`  | Same catalog, YAML encoding |

`config-schema.*` comes from `config_schema` on the contract;
`capability-catalog.*` from `capabilities` on the contract.

## Catalog types

One combined catalog: a flat list of `Capability`, each discriminated by `kind`
(not separate sources/services/transports documents). A source's services nest
under it via `children`.

```
Capability {
  kind: string             # "source" | "service" | "transport" | "sink" | "dlq" | ...
  name: string             # "aws", "cloudtrail", "kafka", ...
  description: string
  maturity: string | null  # "alpha" | "beta" | "stable"; null/omitted = unspecified
  fields: FieldSpec[]      # connection/service config fields; omitted when empty
  children: Capability[]   # nested capabilities (e.g. a source's services); omitted when empty
}

FieldSpec {
  name: string
  type: FieldType          # serde/JSON tag is "type"
  required: bool
  default: any | null      # omitted when null
  description: string
  secret: bool             # true => also x-dfe-secret in the schema; UI masks, engine routes via secrets seam
  enum_values: string[]    # allowed values for type=enum; omitted when empty
  example: any | null      # omitted when null
}

FieldType (lower_snake_case string):
  string | int | float | bool | secret | enum | duration | list | map | object
```

Serialisation rules (identical in both languages):
- `type` is the JSON key for a `FieldSpec`'s field type (Rust `#[serde(rename =
  "type")]`; Python field alias `type`).
- Empty `fields`, `children`, `enum_values` are OMITTED (not `[]`).
- `null` `maturity`, `default`, `example` are OMITTED.
- Field order = declaration order. Rust uses schemars `preserve_order` +
  serde_json preserve-order; Python relies on pydantic/dict insertion order. So
  running twice yields byte-identical output.

## Secret marker

A secret field is flagged in TWO places:
- catalog `FieldSpec.secret = true`
- JSON Schema: the field's subschema carries `"x-dfe-secret": true` (plus
  `"writeOnly": true`).

Rust: `SensitiveString` implements `JsonSchema` to emit
`{"type": "string", "x-dfe-secret": true, "writeOnly": true}`. Python: the
secret field type sets `json_schema_extra={"x-dfe-secret": True, "writeOnly":
True}`. The engine keys off `x-dfe-secret` to route the value through the secrets
seam and the UI masks the input.

## ENV mapping for nested config (decision: C)

Multi-connection config (a source type with many `connections`) lives in the
MOUNTED YAML config, not in ENV. Each connection carries a scalar
`credential_secret` REF of the form `provider:path:key`, where a vault path
opens with its KV mount and the KV v2 `data` segment is optional (e.g.
`vault:secret/aws/prod:credentials` reads `aws/prod` on the `secret` mount).
External Secrets materialises the real
secret into the referenced backend; the app resolves it at fetch time via
`scalo::secrets::resolve`. Writing a REF is a TRUSTED role: `file:` resolves to
the contents of any path the process can read, so whoever authors the config can
hand any local file to the consumer that config names. ENV vars stay for
singleton/scalar legacy config only
-- there is NO array-index ENV encoding and NO JSON-in-ENV blob. This keeps K8s
manifests sane at 50+ accounts and matches the per-connection secret-ref
contract. The schema/catalog therefore describe `credential_secret` as a normal
(non-secret) string REF field; the actual secret value never appears in config.

## Worked example -- fetcher AWS source

Config (abridged): `sources.aws = { enabled, region, credential_secret,
interval_secs, topic, filter, services: [{name, config}], connections: [{id,
region?, credential_secret?, ...}] }`.

Schema fragment (`config-schema.json`), showing the secret marker on a nested
per-connection field:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "sources": {
      "properties": {
        "aws": {
          "properties": {
            "connections": {
              "type": "array",
              "items": {
                "properties": {
                  "id": { "type": "string" },
                  "credential_secret": { "type": ["string", "null"] },
                  "secret_access_key": { "type": "string", "x-dfe-secret": true, "writeOnly": true }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

Catalog entry (`capability-catalog.json`), aws source with its cloudtrail service child:

```json
{
  "kind": "source",
  "name": "aws",
  "description": "AWS audit sources (CloudTrail, GuardDuty, Security Hub, Config).",
  "maturity": "stable",
  "fields": [
    { "name": "id", "type": "string", "required": true,
      "description": "Stable, unique connection id (cursor key + metric/log label)." },
    { "name": "region", "type": "string", "required": false, "default": "us-east-1",
      "description": "AWS region." },
    { "name": "credential_secret", "type": "string", "required": false,
      "description": "Secret ref for credentials, 'provider:path:key'." },
    { "name": "access_key_id", "type": "string", "required": false,
      "description": "Access key ID (prefer credential_secret in production)." },
    { "name": "secret_access_key", "type": "secret", "required": false, "secret": true,
      "description": "Secret access key (prefer credential_secret in production)." }
  ],
  "children": [
    {
      "kind": "service",
      "name": "cloudtrail",
      "description": "AWS CloudTrail management + data events via LookupEvents.",
      "maturity": "stable"
    },
    {
      "kind": "service",
      "name": "cloudwatch_logs",
      "description": "CloudWatch Logs events for a named log group.",
      "maturity": "stable",
      "fields": [
        { "name": "log_group_name", "type": "string", "required": true,
          "description": "CloudWatch log group to pull events from." }
      ]
    }
  ]
}
```

The service knobs (`log_group_name`, `namespaces`, ...) are exactly what schemars
cannot derive -- they are read from `service.config` deep in the fetch code, so
they are authored here by reading that code.

## Contract carriage

`DeploymentContract` grows two optional, defaulted, back-compat fields:

```
config_schema: object | null      # the JSON Schema; null/absent = not provided
capabilities:  Capability[]       # the catalog; [] = not provided
```

`schema_version` bumps (2 -> 3) so consumers can detect the new capability. Old
consumers ignore the new fields; new consumers read them.

## Determinism + drift

Emission is deterministic (stable field order, no timestamps). Each app commits
the four files and a `#[test]` (Rust) / test (Python) regenerates them and asserts
byte-equality with the committed copies -- drift fails the normal test job. The
schema must also round-trip the app's own `Config::default()` (the emitted schema
validates the struct it claims to describe).
