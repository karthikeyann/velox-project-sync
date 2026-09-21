# Velox + Presto Portfolio sync

Keeps the [Velox + Presto Portfolio](https://github.com/users/karthikeyann/projects/11)
project in step with pull requests across Velox, Presto and the RAPIDS forks.

## What each run does

1. **Ingest** new open PRs per the rules in `REPO_RULES` (`sync.py`).
2. **Classify** each PR into a Workstream (`classify.py`), creating a new
   Workstream option when a PR carries an unrecognized commit scope.
3. **Refresh** `Blocked on`, `Status` and `PR Author` from live PR state.
4. **Age out** closed PRs: `Done this week` → `Done this month` → archived,
   recording the URL in `state/cleared.json`.

`state/cleared.json` is what stops a cleared PR from being re-ingested;
archiving the item alone would not.

## Fields

| Field | Source |
|---|---|
| `Workstream` | `classify.py` rules; hand edits are preserved |
| `Blocked on` | Author / Reviewer / CI / Nothing - ready to land, from live review state |
| `Status` | Todo, In Progress, Done, Parked |
| `CI` | Green / Red / Running / None, from the check rollup |
| `Size` | XS-XL by lines changed, recomputed each run since a PR grows |
| `Last commit` | Date of the last commit |
| `PR Author` | PR author login |
| `Priority` | Never written by the automation |

`Last commit` is stored as a date rather than an age so it cannot go stale
between runs. It is also a better activity signal than `updatedAt`: upstream
velox runs `stale[bot]`, whose comments reset `updatedAt` and make dormant
PRs look fresh.

`Parked` mirrors how upstream marks inactive work -- the `stale` label, plus
`[WIP]`, `[DNR]` and `do not merge` title markers.

## Ingest rules

| Repo | Rule |
|---|---|
| `facebookincubator/velox` | `cudf` label or `cudf` keyword (~52/month of ~490) |
| `rapidsai/velox` | everything |
| `rapidsai/velox-testing` | non-draft only |
| `prestodb/presto` | `cudf` keyword; anything else must be added by hand |

## Workstream classification

Ordered rules, first match wins, measured at 94% coverage over 52 real PRs.
A PR matching nothing lands in `Unclassified` and shows up in the
**Needs triage** view. Edit the rules in `classify.py`; they are plain
regexes, deliberately readable.

## Status model

Closed PRs carry a single `Done` status. The "merged in the last N days"
views select on the built-in `Closed` date rather than moving items between
week and month statuses; at 30 days `sync.py` archives the item and records
it in `state/cleared.json`.

Archived items keep their field values and stay queryable by script, but
Projects views cannot display them -- there is no include-archived filter.
They are reachable only through the project's Archived items pane.

## Cost

One paginated GraphQL query returns every board item together with the state
of the pull request behind it, and field updates are batched many per
request. A full run over 153 items takes ~20s. The per-item version took
~8 minutes, which would have exceeded the free Actions tier roughly sixfold
on a 30-minute schedule.

This repository is public, so Actions minutes are free and unlimited.

## Setup

The default `GITHUB_TOKEN` cannot write to user-owned Projects v2, so the
workflow needs a PAT:

1. Create a token at <https://github.com/settings/tokens> with the
   **`project`** and **`repo`** scopes.
2. Add it to this repo as the secret **`PROJECT_TOKEN`**.

## Running locally

```bash
python3 sync.py --dry-run   # report only
python3 sync.py             # apply
```
