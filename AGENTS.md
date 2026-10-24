# Repository Guidelines

## Project Structure & Architecture

This pnpm workspace contains four NestJS services in `apps/`: `sales`, `fulfillment`, `analytics`, and `payment-mock`. Each owns its PostgreSQL database and SQL migrations.

Organize `src/modules/<feature>/` into `domain`, `application`, `infrastructure`, `presentation`, and `composition`; process entrypoints belong in `src/bootstrap/`. Keep drivers behind application ports, wire adapters in composition, and use public module exports. Avoid cross-service implementation imports and database access.

Shared contracts, runtime utilities, and telemetry live in `packages/`. Infrastructure configuration is in `infra/` and `compose.yaml`; operational scripts are in `scripts/`. Design sources and previews live in `design/`.

## Build, Test, and Development Commands

Use Node.js **24.18.0**, pnpm **11.15.1**, and Docker Compose.

- `pnpm install`: install workspace dependencies.
- `pnpm dev`: start infrastructure, build, provision databases, migrate, seed, upgrade state, and launch six application processes.
- `pnpm build`: compile all workspace packages.
- `pnpm check`: build, enforce architecture boundaries, and run unit tests.
- `pnpm test`: run checks, integration tests, and Fulfillment/Analytics verification.
- `pnpm infra:up` / `pnpm infra:down`: start or stop containers while retaining volumes.
- `pnpm migrate`: provision databases and apply SQL migrations.
- `pnpm replay all` / `pnpm redrive tickets`: rebuild reports or retry failed ticket deliveries.

Prepare infrastructure and migrations before integration tests; replay requires running Analytics.

## Coding Style & Naming

Use strict TypeScript, two-space indentation, single quotes, and semicolons. Use PascalCase for classes, camelCase for methods, and kebab-case filenames such as `checkout.service.ts`. Follow surrounding formatting; no dedicated formatter or linter is configured.

Write parameterized SQL through `pg`, without an ORM. Add numbered migrations under each service’s `migrations/`; never edit applied migrations because checksums are enforced.

## Testing Guidelines

Tests use `node:test` and `node:assert/strict`. Name service tests `*.test.ts`; shared unit and integration tests use `test/{unit,integration}/*.test.cjs`. No coverage percentage is configured. Cover changed behavior, especially concurrency, rollback, idempotency, and redelivery. Reuse isolated fixtures; preserve demonstration data.

## Commit & Pull Request Guidelines

Git history currently has no commits, so no established message convention exists. Use concise imperative subjects, such as `Fix reservation expiry race`.

Describe the problem, resulting behavior, validation, and migration implications. Link relevant issues when available; include screenshots for design changes.

## Documentation & Configuration

Keep Russian explanations and TSDoc useful for studying finished implementations. Maintain the 13 equally ranked tasks in `README.md` and `docs/tasks/`; update code links and `GUIDE.md` when implementations move.

Use `.env.example` as the configuration reference. Keep secrets in ignored `.env` files.
