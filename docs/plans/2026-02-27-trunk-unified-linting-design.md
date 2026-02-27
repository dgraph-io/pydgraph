# Trunk as Single Linting Orchestrator

**Date:** 2026-02-27 **Status:** Approved

## Problem

The project runs linting through three overlapping systems:

1. **pre-commit framework** (`.pre-commit-config.yaml`) — git hooks for ruff, shellcheck, yamllint,
   mypy, ty, pygrep-hooks, and pre-commit-hooks
2. **Trunk** (`.trunk/trunk.yaml`) — ruff, shellcheck, yamllint, bandit, prettier, markdownlint, and
   others
3. **CI workflows** — two separate jobs that each skip the other's tools

ruff, shellcheck, and yamllint run in both pre-commit and Trunk with different pinned versions.
Trunk's git hook actions (`trunk-fmt-pre-commit`) hijack the `core.hooksPath` git config, which
silently bypasses the pre-commit framework. This caused the mypy CI failure on PR #303 — Trunk's
hook ran instead of pre-commit, so mypy never executed locally.

## Decision

Consolidate on Trunk as the single orchestrator. Remove pre-commit entirely.

## Hook Mapping

### Already covered by Trunk

| pre-commit hook        | Trunk equivalent                              |
| ---------------------- | --------------------------------------------- |
| `ruff`                 | `ruff` (already enabled)                      |
| `shellcheck`           | `shellcheck` (already enabled)                |
| `yamllint`             | `yamllint` (already enabled)                  |
| `check-yaml`           | Covered by `yamllint`                         |
| `check-toml`           | Covered by `taplo` (already enabled)          |
| `check-json`           | Covered by `prettier` (already enabled)       |
| `end-of-file-fixer`    | Covered by `trunk fmt`                        |
| `trailing-whitespace`  | Covered by `trunk fmt`                        |
| `check-merge-conflict` | Covered by `git-diff-check` (already enabled) |

### Need to enable in Trunk

| pre-commit hook           | Trunk action                              |
| ------------------------- | ----------------------------------------- |
| `mypy`                    | Enable native `mypy` linter in trunk.yaml |
| `check-added-large-files` | Enable `pre-commit-hooks` subcommand      |
| `check-docstring-first`   | Enable `pre-commit-hooks` subcommand      |

### Replace with ruff rules

| pre-commit hook                    | ruff rule                       |
| ---------------------------------- | ------------------------------- |
| `python-check-blanket-noqa`        | `PGH004`                        |
| `python-check-blanket-type-ignore` | `PGH003`                        |
| `python-no-eval`                   | `S307` (also covered by bandit) |
| `python-use-type-annotations`      | `UP037`                         |

### Custom linter definition needed

| Tool | Reason                                          |
| ---- | ----------------------------------------------- |
| `ty` | No native Trunk plugin; define as custom linter |

### Dropped

| Hook                     | Reason                                         |
| ------------------------ | ---------------------------------------------- |
| `no-commit-to-branch`    | CI protects main; unnecessary friction locally |
| `requirements-txt-fixer` | Project uses uv; no requirements.txt files     |

## trunk.yaml Changes

1. Re-enable `trunk-fmt-pre-commit` and `trunk-check-pre-push` actions (Trunk owns the hooks now)
2. Enable `mypy@1.18.2` with custom command args pointing to `pyproject.toml`
3. Enable `pre-commit-hooks` subcommands: `check-added-large-files`, `check-docstring-first`
4. Define custom `ty` linter
5. Add ruff rules `PGH003`, `PGH004`, `S307`, `UP037` to pyproject.toml

## mypy Additional Dependencies

The pre-commit config installs 9 type-stub packages (types-requests, grpc-stubs, types-protobuf,
etc.) into mypy's isolated environment. Trunk's native mypy plugin also runs in isolation.

Options to resolve:

- Configure Trunk to run mypy from the project venv (which already has these installed via
  `uv sync --group dev`)
- Use Trunk's `extra_packages` config if supported
- Define mypy as a custom linter that runs from the project venv

## CI Changes

### ci-pydgraph-code-quality.yml

Replace `SKIP=no-commit-to-branch,trunk-check,trunk-fmt make check` with `trunk check --all --ci`.
Remove the pre-commit setup steps.

### ci-pydgraph-trunk.yml

Evaluate whether this separate workflow is still needed. If the code-quality workflow now runs
`trunk check`, this may be redundant. If it's a shared org workflow, keep it.

### Makefile

- `make check`: Change from `pre-commit run --all-files` to `trunk check --all`
- `make setup`: Remove `pre-commit install` step (Trunk manages hooks)
- `deps`: Keep `deps-trunk` since it's now the sole tool dependency

## Files Removed

- `.pre-commit-config.yaml`
- `pre-commit` from dev dependencies in `pyproject.toml`

## Risks

1. **mypy environment** — Trunk's isolated mypy needs access to type stubs. If Trunk can't install
   them, fall back to running mypy from the project venv via custom linter definition.
2. **Trunk availability** — Contributors must install Trunk. The project already requires it
   (`deps-trunk` in Makefile), so this adds no new requirement.
3. **Trunk version drift** — Trunk pins tool versions independently. We control this through
   `trunk.yaml` version pins.

---

## Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task.

**Goal:** Replace pre-commit with Trunk as the sole linting orchestrator across local dev, Makefile,
and CI.

**Architecture:** Enable mypy/pre-commit-hooks/ty in trunk.yaml, add PGH ruff rules, update Makefile
and CI workflows, then delete .pre-commit-config.yaml.

**Tech Stack:** Trunk CLI, ruff, mypy, ty (via uvx), GitHub Actions

---

### Task 1: Add PGH rules to ruff config

**Files:**

- Modify: `pyproject.toml:82` (ruff.lint.select list)

**Step 1: Add `PGH` to the ruff select list**

In `pyproject.toml` under `[tool.ruff.lint]`, add `"PGH"` to the `select` array. This replaces
`python-check-blanket-noqa` (PGH004) and `python-check-blanket-type-ignore` (PGH003). `S307` and
`UP037` are already covered by the existing `"S"` and `"UP"` selectors.

```toml
select = [
  "A",     # flake8-builtins (shadowing builtins)
  ...
  "PGH",   # pygrep-hooks (blanket noqa, blanket type-ignore)
  ...
]
```

**Step 2: Verify ruff passes with the new rules**

Run: `uv run ruff check pydgraph tests --select PGH` Expected: No errors (or fix any that appear)

**Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "chore: add PGH rules to ruff replacing pygrep-hooks"
```

---

### Task 2: Configure trunk.yaml with mypy, ty, and pre-commit-hooks

**Files:**

- Modify: `.trunk/trunk.yaml`

**Step 1: Enable mypy with extra_packages and custom command**

Add `mypy@1.18.2` to `lint.enabled`. Override the default command to use
`--config-file=pyproject.toml` instead of `--ignore-missing-imports`. Add `extra_packages` for the
type stubs that pre-commit used to install:

```yaml
lint:
  enabled:
    - mypy@1.18.2
  definitions:
    - name: mypy
      extra_packages:
        - types-requests>=2.32.0
        - pydantic-settings>=2.12.0
        - mypy-protobuf>=4.0.0
        - grpc-stubs>=1.53.0
        - types-grpcio>=1.0.0
        - types-protobuf>=6.32.0
        - pytest>=8.3.3
        - grpcio-tools>=1.66.2
        - packaging>=24.0
      commands:
        - name: lint
          output: mypy
          run: mypy --config-file=pyproject.toml --show-column-numbers ${target}
          run_from: ${root_or_parent_with_any_config}
          success_codes: [0, 1]
          stdin: false
```

**Step 2: Enable pre-commit-hooks subcommands**

Add to `lint.enabled`:

```yaml
- pre-commit-hooks@6.0.0
```

And enable the specific subcommands in definitions:

```yaml
- name: pre-commit-hooks
  commands:
    - name: check-added-large-files
      enabled: true
    - name: check-docstring-first
      enabled: true
    - name: check-merge-conflict
      enabled: true
```

**Step 3: Define custom ty linter**

Add a custom linter definition for ty:

```yaml
- name: ty
  files: [python]
  commands:
    - name: lint
      output: pass_fail
      read_output_from: stderr
      run: >
        uvx ty check pydgraph tests --exclude 'pydgraph/proto/api_pb2\.py' --exclude
        'pydgraph/proto/api_pb2_grpc\.py' --exclude 'pydgraph/proto/api_pb2\.pyi' --exclude
        'pydgraph/proto/api_pb2_grpc\.pyi'
      success_codes: [0, 1]
```

**Step 4: Re-enable trunk git hook actions**

Change the `actions` section back to:

```yaml
actions:
  enabled:
    - trunk-announce
    - trunk-fmt-pre-commit
    - trunk-check-pre-push
    - trunk-upgrade-available
```

Remove the `disabled` section.

**Step 5: Verify trunk check passes**

Run: `trunk check --all --no-fix` Expected: All linters pass (or only pre-existing issues)

**Step 6: Commit**

```bash
git add .trunk/trunk.yaml
git commit -m "chore: enable mypy, ty, pre-commit-hooks in trunk as sole linting orchestrator"
```

---

### Task 3: Update Makefile

**Files:**

- Modify: `Makefile:57-68`

**Step 1: Update `make setup` to remove pre-commit install**

Replace the pre-commit hook install block:

```makefile
setup: deps ## Setup project (install tools and sync dependencies)
	@$(MAKE) sync
```

Trunk manages its own git hooks automatically when its actions are enabled.

**Step 2: Update `make check`**

Change from `pre-commit run --all-files` to:

```makefile
check: ## Run code quality checks on all files
	trunk check --all --no-fix
```

**Step 3: Verify make check works**

Run: `make check` Expected: All trunk linters pass

**Step 4: Commit**

```bash
git add Makefile
git commit -m "chore: update Makefile to use trunk check instead of pre-commit"
```

---

### Task 4: Update CI code-quality workflow

**Files:**

- Modify: `.github/workflows/ci-pydgraph-code-quality.yml`

**Step 1: Simplify the code-quality workflow**

Replace the current workflow steps. Remove `make setup` (which installed pre-commit hooks) and
`make check` (which ran pre-commit). Instead run trunk directly:

```yaml
jobs:
  check-code-quality:
    name: Code Quality Checks
    runs-on: ubuntu-latest
    steps:
      - name: Checkout pydgraph
        uses: actions/checkout@v5
        with:
          repository: dgraph-io/pydgraph
          ref: ${{ github.ref }}
      - name: Setup python runtime and tooling
        uses: ./.github/actions/setup-python-and-tooling
        with:
          python-version: "3.13"
      - name: Sync python virtualenv
        run: make sync
      - name: Check generated protobufs are current
        run: |
          make protogen
          git diff --exit-code -- .
      - name: Run code quality checks
        run: trunk check --all --ci
```

Note: `make setup` is no longer needed because trunk is installed by the action and `make sync`
handles the venv. The `SKIP=...` env var is gone since there's no pre-commit to skip hooks in.

**Step 2: Commit**

```bash
git add .github/workflows/ci-pydgraph-code-quality.yml
git commit -m "ci: use trunk check directly in code-quality workflow"
```

---

### Task 5: Evaluate and update CI trunk workflow

**Files:**

- Modify: `.github/workflows/ci-pydgraph-trunk.yml`

**Step 1: Decide whether to keep the trunk workflow**

The `ci-pydgraph-trunk.yml` calls a shared org workflow
`dgraph-io/.github/.github/workflows/trunk.yml@main`. This runs Trunk's own CI check (using
`trunk-io/trunk-action`), which provides inline PR annotations.

The code-quality workflow now also runs `trunk check --all --ci`. However, the trunk action provides
GitHub Check annotations (via `permissions: checks: write`) which are useful for inline PR comments
on specific lines.

**Decision:** Keep `ci-pydgraph-trunk.yml` as-is. It's a shared org standard and provides PR
annotation features that plain `trunk check --ci` output doesn't. The code-quality workflow provides
the gate; the trunk workflow provides the annotations.

No changes needed to this file.

---

### Task 6: Remove pre-commit

**Files:**

- Delete: `.pre-commit-config.yaml`
- Modify: `pyproject.toml:37` (remove `pre-commit` from dev dependencies)

**Step 1: Delete `.pre-commit-config.yaml`**

```bash
git rm .pre-commit-config.yaml
```

**Step 2: Remove pre-commit from dev dependencies**

In `pyproject.toml`, remove `"pre-commit>=3.5.0"` from the `[dependency-groups] dev` list.

**Step 3: Remove shellcheck-py from dev dependencies**

`shellcheck-py` was only needed by pre-commit. Trunk manages its own shellcheck binary. Remove
`"shellcheck-py>=0.10.0.1"` from the dev dependency group.

**Step 4: Sync the lockfile**

Run: `uv sync --group dev --extra dev` Expected: Lockfile updates, pre-commit and shellcheck-py
removed

**Step 5: Commit**

```bash
git add .pre-commit-config.yaml pyproject.toml uv.lock
git commit -m "chore: remove pre-commit framework, trunk is now the sole linting orchestrator"
```

---

### Task 7: End-to-end verification

**Step 1: Run `make check` and verify all linters pass**

Run: `make check` Expected: trunk check runs all linters including mypy, ty, ruff, etc.

**Step 2: Test git commit hook**

Make a trivial whitespace change, stage it, and commit. Verify trunk's pre-commit hook runs:

```bash
echo "" >> README.md
git add README.md
git commit -m "test: verify trunk hooks"
```

Expected: Trunk fmt runs and either commits cleanly or fixes whitespace. Then `git reset HEAD~1` to
undo.

**Step 3: Verify no pre-commit artifacts remain**

Run: `grep -r "pre-commit" Makefile .github pyproject.toml` Expected: No references to pre-commit
except possibly in comments or changelog.

**Step 4: Final commit if any fixes were needed**

```bash
git add -A
git commit -m "chore: fix any issues found during trunk migration verification"
```
