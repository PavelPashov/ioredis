# AGENTS.md

Guidance for AI coding agents and automated reviewers working in this repository. For people, see [README.md](README.md) and [CONTRIBUTING.md](.github/CONTRIBUTING.md).

## Overview

ioredis is a Redis client for Node.js, written in TypeScript. It supports standalone, Sentinel, and Cluster topologies. See the [README](README.md) for features and usage.

```text
lib/              Source. lib/index.ts is the public export surface.
lib/cluster/      Cluster client, slot cache, MOVED/ASK handling, sharded Pub/Sub.
lib/connectors/   TCP/TLS and Sentinel connectors.
lib/utils/RedisCommander.ts   Generated command typings (see Repo conventions).
bin/              Generator config for RedisCommander.ts.
built/            Compiled CommonJS output; the published artifact.
test/unit/        Unit tests; network is mocked, no Redis needed.
test/functional/  Tests against standalone Redis on 127.0.0.1:6379.
test/cluster/     Tests against the cluster on ports 3000-3005.
test/typing/      tsd declaration tests.
test/helpers/     Global Mocha hooks and shared test helpers.
test/scenario/    Redis Enterprise fault-injection scenarios, run in external CI.
```

## Commands

```bash
npm install
npm run build             # Wipe built/ and compile TypeScript; also the typecheck
npm run lint              # ESLint over lib/
npx prettier --write <files>   # Format the files you changed
npx prettier --check <files>   # Check them. The repo-wide `npm run format`/`format-check` hit hundreds of unformatted files
node bin/index.js         # Regenerate lib/utils/RedisCommander.ts

npm run docker:setup      # Start standalone Redis on 6379 and the cluster on 3000-3005
npm run docker:teardown   # Stop them and remove their volumes

npm run verify            # Everything CI runs: lint, unit, functional, tsd, cluster. Needs the test servers.
npm test                  # test:js (helpers, unit, functional) then test:tsd
npm run test:cluster      # Cluster tests

# One test file, or one test by name. Keep test/helpers/*.ts in the run.
TS_NODE_TRANSPILE_ONLY=true NODE_ENV=test npx mocha --no-experimental-strip-types "test/helpers/*.ts" "test/unit/foo.ts" --grep "partial test title"

# One cluster test file (test:cluster does not load test/helpers)
TS_NODE_TRANSPILE_ONLY=true NODE_ENV=test npx mocha --no-experimental-strip-types "test/cluster/foo.ts"
```

## Repo conventions

- **Generated files.** Do not hand-edit `lib/utils/RedisCommander.ts`, `built/`, or `node_modules/`. To change command typings, edit `bin/` (`overrides.js`, `argumentTypes.js`, `returnTypes.js`, `sortArguments.js`, `typeMaps.js`, `template.ts`) and run `node bin/index.js`. The `implement-command` skill has the full procedure.
- **Defaults.** Client defaults live in `DEFAULT_REDIS_OPTIONS` (`lib/redis/RedisOptions.ts`) and `DEFAULT_CLUSTER_OPTIONS` (`lib/cluster/ClusterOptions.ts`).
- **Public API.** Anything exported from `lib/index.ts` is public and ends up in the published declarations.
- **RESP2 and RESP3.** Command tests run once per entry in `RESP_CONFIGS` (`test/helpers/respConfigs.ts`): RESP2/legacy, RESP3/legacy and RESP3/resp3. A reply change must hold in all three.
- **Autopipelining.** Commands that must not be batched (auth, subscribe, multi, select and similar) are listed in `notAllowedAutoPipelineCommands` in `lib/autoPipelining.ts`.
- **Test hooks.** `test/helpers/global.ts` restores sinon stubs and runs `FLUSHALL`, `SCRIPT FLUSH` and `CLIENT KILL` on 6379 after every test. Leaving it out of a Mocha run lets stubs leak into later tests, which then fail with timeouts.
- **Cluster state.** Cluster tests share one cluster. A test that moves slots must put them back, using `SlotAdmin.restoreSlot` from `test/cluster/helpers/slots.ts`, so the suite can run again.

## Boundaries

### Always

- Add or update tests for every behavior change.
- Run the targeted tests while you work and `npm run verify` before you hand off a code, test or build change. The `code-change-verification` skill has the details. Report failures you did not fix.
- Keep the change to what the task asks for.

### Ask first

- Adding, removing or upgrading a dependency.
- Changing the public API, an option name, or a default value.
- Editing release or CI configuration: `.github/workflows/`, `.releaserc.json`, `package.json` scripts.
- Committing a `RedisCommander.ts` regeneration that changes signatures outside the task.

### Never

- Never add `Signed-off-by`, `Co-Authored-By` or "Generated with" footers.
- Never weaken, skip or delete a test to make a change pass. If a test is wrong or obsolete, say so in the PR description and let a human decide.
- Never commit, push, create or merge PRs, or post comments, reviews or issues without a specific instruction from the user for that action. Draft the text and let a human post it.

## PR conventions

Full process: [CONTRIBUTING.md](.github/CONTRIBUTING.md).

- PR titles and commits follow [Conventional Commits](https://www.conventionalcommits.org/) (`<type>: <subject>`). PRs are squash-merged and semantic-release derives the version and changelog from the message: `fix` is a patch, `feat` a minor, and a `BREAKING CHANGE:` footer a major release.
- New features, API changes, refactors and broader behavior changes need a linked issue where a maintainer agreed to the approach.
- Describe the change and the tests you ran.

## Review guidelines

Automated reviewers are advisory. Flag these:

### General

- Breaking changes to the public API or to documented behavior.
- Changed default values.
- Existing tests that were changed, weakened, skipped or deleted.
- New or upgraded dependencies.
- Edits to release or CI workflows.
- A PR that needs an issue (see PR conventions) but links none with a maintainer's go-ahead.

### ioredis

- Hand edits to `lib/utils/RedisCommander.ts` without a matching `bin/` change, or committed `built/` output.
- Reply or argument changes that are not tested under every `RESP_CONFIGS` entry, or that give RESP3/legacy a different shape from RESP2/legacy.
- Cluster code that sends multi-key commands without checking that the keys share a slot, or changes to `MOVED`/`ASK` handling and slot refresh without cluster tests.
- Timers, listeners or sockets that are not cleaned up on `disconnect()`/`quit()`, including in subscribers and reconnect paths.
- Changes to the offline queue, reconnect, ready check or `enableOfflineQueue` behavior.
- New commands that must bypass autopipelining but are missing from `notAllowedAutoPipelineCommands`.
- Typing changes without a `test/typing` tsd test.
- Command arguments that reach logs, errors or tracing without the sanitization in `lib/tracing.ts`.
- Cluster tests that change slots or topology without restoring them.
- User-visible behavior changes without a README update.

## Skills

Task procedures live in `.agents/skills/` (`.claude/skills` links to it). Use the skill when the task matches.

| Skill                      | Use when                                                       |
| -------------------------- | -------------------------------------------------------------- |
| `implement-command`        | Adding or updating Redis command support and typings           |
| `code-change-verification` | Choosing and running the checks for a change before handoff    |
| `docs-sync`                | A change affects public APIs, options or documented behavior   |
| `pr-draft-summary`         | Drafting the PR title and description for a change             |
| `runtime-behavior-probe`   | The user asks to probe runtime behavior with temporary scripts |

## Architecture

```text
Public API (`Redis`, `Cluster`, `Pipeline`, `Command`, options)
  → Command facade (`Commander`, generated `RedisCommander` typings, Lua scripts)
    → Command objects and queues (`Command`, offline queue, command queue, pipeline queue)
      → Topology routing (standalone, Sentinel, Cluster slots, subscriber groups)
        → Connection layer (`connectors/`, `redis/event_handler.ts`, retry and ready checks)
          → RESP parsing and reply handling (`DataHandler`, `redis-parser`)
```

- **`lib/Redis.ts`** — Standalone and Sentinel client. Connection lifecycle (`wait → connecting → connect → ready → close → reconnecting → end`), offline queue, reconnection strategy, connector selection.
- **`lib/utils/Commander.ts`** — Shared command facade for `Redis` and `Cluster`: builtin commands, `Buffer` variants, autopipelining dispatch, `sendCommand`, `defineCommand`.
- **`lib/Command.ts`** — One command: argument and reply transformation, promise/callback resolution, key-slot calculation, subscriber/monitor flags.
- **`lib/DataHandler.ts`** — Wraps `redis-parser`, resolves queued commands, routes Pub/Sub and monitor replies.
- **`lib/Pipeline.ts`**, **`lib/transaction.ts`** — `pipeline()` batching and `multi()` MULTI/EXEC.
- **`lib/Script.ts`** — Lua scripts for `defineCommand`: `EVALSHA` with `EVAL` fallback.
- **`lib/ScanStream.ts`** — Readable stream over `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN`.
- **`lib/tracing.ts`** — Command and connection tracing with argument sanitization.
