# AGENTS.md: Repo hygiene

Scope: this file covers *repo hygiene*: branching, remotes, CI, cleanup. It is not project documentation.

This repo follows the branch/release workflow documented in `CONTRIBUTING.md`,
read and follow it for any git, branch, or release work here (the
single-trunk model, `feature/x`/`fix/x` branch naming, how RC tags work,
etc). Don't improvise a different workflow. The short version: there is one
long-lived branch, `main`; no `dev` or `beta` branch should be used.
`main` is supposed to auto-publish a dev-track build on every push. "Beta"
and "stable" are both just tags, not branches: push a `vX.Y.Z-rc.N` tag to
publish a beta-track build, push a plain `vX.Y.Z` tag to cut the signed
stable release. "Freezing" for stabilization means pausing pushes to
`main`, not moving a branch. This replaced an earlier three-branch
(`dev`/`beta`/`main`) model after `main` was found to have silently rotted
out of sync with `dev`/`beta` across most repos in this ecosystem.

## Remotes
- `origin`: Forgejo (`git.breadway.dev` via Hestia, SSH), authoritative.
- `github`: GitHub mirror. Push both when publishing.

## CI
- `release.yml` triggers on `push: tags: ['v*']` but skips any tag whose
  name contains `-rc`; a plain `vX.Y.Z` cuts the signed stable build.
- `beta-release.yml` publishes a beta-track build from a `vX.Y.Z-rc.N`
  tag (and still on leftover `beta` so an old branch push does not go silent).
- `dev-release.yml` publishes a dev-track build on push to `main` (and
  still on leftover `dev` so an old branch push does not go silent).
- `check.yml` runs clippy + test on `main`, `feature/**`, and `fix/**`.

## Product cut
breadcast is a **bakery product**, not shipped on the BOS ISO, and not part
of the default desktop. Do not add BOS skel/keybinds. `bread` (breadd) is
optional event integration (see `EVENTS.md`), not a bakery `bread_deps`
entry.

## Status
The daemon + GTK picker + Cast Streaming / DLNA pipelines are built and
validated against real hardware. `EVENTS.md` is the bus contract.

## Pins
`bread-theme` pins `git.breadway.dev/Breadway/bread-ecosystem` tag
`v0.7.4` (per-monitor palette). `bread-utils` pins the same repo at
`v0.7.2`. Do not switch those to github.com or `branch = "main"`.

## Don't
- Don't embed credentials in remote URLs; SSH or a credential helper only.
- Don't claim breadcast is on the BOS ISO or the default desktop.

## Persistent memory
Read `~/.knowledge.md` at the start of a task: it points to a shared memory index at `~/.claude/memory/MEMORY.md` (also used by Claude Code). Selectively read the linked entries relevant to what you're working on.
