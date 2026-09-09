# Contributing

`breadcast`: cast your screen to a Chromecast/Google TV or DLNA renderer
(daemon + GTK4 popup).

A bakery product in the bread ecosystem, **not** shipped on the BOS ISO
and not part of the default desktop. This repo follows the same
branch/release workflow as every other ecosystem product.

## Branches

There is one long-lived branch: **`main`**. All day-to-day work lands here.
Every push to `main` is supposed to publish a **dev-track** build (see
Tracks below), a real install you can test before cutting anything more
formal.

New work, features and bug fixes alike, goes on a short-lived branch:

```
feature/<short-name>
fix/<issue-number-or-short-name>
```

Branch off `main`, open a PR/push back into `main` when ready. Short-lived
branches get deleted on merge; they never accumulate the kind of drift a
second long-lived branch does.

## The release cycle

There's no separate `beta` or release branch: "stable" and "beta" are both
just **tags** on `main`, not branches that need to be kept in sync:

1. Work accumulates on `main` via `feature/x` / `fix/x` branches. Each push
   is meant to auto-publish a dev build; install it with
   `bakery track set dev` and `bakery update --all`, then fix anything
   broken with another push.
2. When you want to stabilize before a real release, tag a release
   candidate: `git tag vX.Y.Z-rc.1 && git push origin vX.Y.Z-rc.1` (push to
   both remotes). That tag alone should trigger a beta-track build:
   "freezing" is just pausing pushes to `main` while you test it, not a
   branch operation. Cut `-rc.2`, `-rc.3`, etc. for further fixes.
3. Once an RC has gone without issues, tag the real release:
   `git tag vX.Y.Z && git push origin vX.Y.Z`, that's what triggers the
   signed stable release build.

Do **not** use leftover `dev` / `beta` branches as integration trunks.

## Tracks, from a user's perspective

```
bakery track show              # what you're currently on (defaults to stable)
bakery track set dev           # or beta, or stable
bakery update --all            # pull the latest build on your current track
```

| Track  | What it is | Published from |
|--------|-----------|-----------------|
| `stable` | The last tagged release | a `vX.Y.Z` tag |
| `beta` | Latest release candidate | a `vX.Y.Z-rc.N` tag |
| `dev` | Bleeding edge | `main`, on every push |

Dev versions are auto-computed (`X.Y.Z-dev.<timestamp>+<sha>`) from the
latest published stable tag, so they always sort as newer than what you
have installed: no manual version bumping needed. Beta versions are just
the RC tag itself (already valid semver, already sorts below the real
release it's a candidate for).

## Local development

```sh
cargo build --release --workspace
cargo test --release --workspace
```

## CI

Intended single-trunk wiring (see
[bread-ecosystem's docs/release-channels.md](https://git.breadway.dev/Breadway/bread-ecosystem/src/branch/main/docs/release-channels.md)):

- push to `main` → dev-track build
- `vX.Y.Z-rc.N` tag → beta-track build
- `vX.Y.Z` tag → signed stable release

`release.yml` already fires on a `v*` tag push. `dev-release.yml` and
`beta-release.yml` are leftover from the old three-branch model and still
trigger on the `dev` / `beta` branch names; they have not been rewritten.
Follow the cycle above; do not treat those branches as current, and do not
rewrite the leftover workflows unless you are deliberately migrating CI.

`check.yml` runs clippy + test on `feature/**` and `fix/**` branches.

All CI runs on a self-hosted runner.

## Questions

Open an issue on this repo's Forgejo tracker.
