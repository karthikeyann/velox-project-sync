#!/usr/bin/env python3
"""One-time backfill of recently closed cudf PRs into the portfolio project.

sync.py only ingests open PRs. This adds cudf PRs closed in the last
`--days` days so the "Done this week" and "Done this month" views start out
populated instead of empty. PRs closed longer ago are skipped: sync.py would
archive and tombstone them on the next run anyway.

Run once, then let sync.py take over:

    python3 backfill.py --dry-run
    python3 backfill.py
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from sync import gh, PROJECT_NUMBER, OWNER, REPO_RULES, CLEAR_AFTER_DAYS


def closed_cudf_prs(days):
    """Return {url: title} for cudf PRs closed within the window."""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    found = {}
    for repo, rule in REPO_RULES.items():
        queries = []
        if rule["label"]:
            queries.append(["--label", rule["label"]])
        for term in rule["search"]:
            queries.append([term])
        if not queries:
            queries.append([])
        for extra in queries:
            raw = gh(["search", "prs", "--repo", repo, "--state", "closed",
                      "--closed", f">{since}", "--limit", "200",
                      "--json", "number,title,url,isDraft", *extra])
            for pr in json.loads(raw):
                if not rule["drafts"] and pr["isDraft"]:
                    continue
                found[pr["url"]] = pr["title"]
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=CLEAR_AFTER_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    items = json.loads(gh(["project", "item-list", PROJECT_NUMBER, "--owner",
                           OWNER, "--format", "json", "-L", "500"]))["items"]
    existing = {i["content"].get("url") for i in items if i.get("content")}

    candidates = {u: t for u, t in closed_cudf_prs(args.days).items()
                  if u not in existing}
    print(f"{len(candidates)} closed cudf PR(s) to backfill "
          f"(last {args.days} days)")
    for url, title in sorted(candidates.items()):
        print(f"  + {title[:66]}")
        if not args.dry_run:
            gh(["project", "item-add", PROJECT_NUMBER, "--owner", OWNER,
                "--url", url])
    if not args.dry_run and candidates:
        print("\nNow run: python3 sync.py   (classifies and dates them)")


if __name__ == "__main__":
    main()
