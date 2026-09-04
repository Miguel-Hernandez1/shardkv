# Working conventions for this repo

Read this before doing anything else in ShardKV.

## Attribution (read this one first)

- Every commit's author AND committer must be Miguel Hernandez, never the
  default identity of whatever environment is running. Check
  `git log --format='%an <%ae>' -1` before pushing, not after.
- Never name any AI tool or assistant anywhere in this repository: not in
  code, comments, commit messages, docs, filenames, branch names,
  `.gitignore`, or any generated file. This file's own name was chosen
  with that in mind; keep it that way.
- No em dashes anywhere in repo text: code, comments, commit messages,
  docs. Use a comma, period, colon, or parentheses instead.
- If you notice the wrong author landed on a pushed commit, fix it with a
  non-destructive rebase (`git commit --amend --author=... `, never touch
  git config) and force-push the correction immediately, don't wait to be
  asked.
- One branch: `main`. Do not create other branches (including ones named
  after this tool) unless explicitly asked to. Commit and push directly
  to `main`.
- Commits should be signed, and this environment cannot sign as the repo
  owner, so the badge will read "Unverified" here. That is accepted as a
  known limitation, not something to work around by branching or by
  holding work back from `main`.

## Correctness and honesty

- Never fabricate metrics, benchmark numbers, test results, or claims
  about what's implemented. If something hasn't been measured, say so;
  don't estimate a number and present it as real.
- Any change that could affect a previously-measured benchmark (new
  default behavior, added overhead on the hot path, etc.) means rerun
  the benchmark, don't reuse old numbers. State the methodology and test
  machine explicitly every time, since results aren't comparable across
  different hardware.
- Keep README, ROADMAP, and docs/architecture.md in sync with the actual
  code after every change. A stale doc describing removed behavior is a
  bug.
- Before a large architectural change, explain the current state, the
  proposed change, the files affected, and the correctness risks, before
  writing code. Then implement it.

## Development process

- Run `go build ./...`, `go vet ./...`, and the full test suite with
  `-race` before considering any change done.
- Prefer real integration tests over unit tests for anything involving
  the HTTP layer, redirects, or multi-node behavior; a lower-level test
  that never exercises the actual server can hide real bugs (this
  happened once this session: a redirect bug only showed up once a real
  HTTP server was tested instead of calling `node.Node` directly).
- Commit in small, real, logically separate units, not one giant diff
  and not padded/fake commits to hit a count. If natural work results in
  fewer commits than some target number, say so instead of manufacturing
  splits.
- Reuse existing code and types where they still fit; don't add
  abstraction, config knobs, or generality the current task doesn't need.

## Project-specific

- Shard-related port arithmetic (`internal/cluster.ShardPortOffset`,
  `BaseRaftPort`) is a load-bearing convention, not incidental: HTTP
  redirects and the internal scan fan-out both depend on it. Any test
  harness that spins up real HTTP servers must follow it too.
- `node.Node.Shutdown` must close the BoltDB log/stable stores, not just
  shut down Raft, or a restart against the same data directory deadlocks
  on bbolt's file lock. Keep this in mind if touching shutdown/restart
  paths anywhere.
