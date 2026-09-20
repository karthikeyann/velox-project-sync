#!/usr/bin/env python3
"""Remove board items that the current ingest rules would no longer add.

Ingest rules tightened over time; this drops items admitted under a looser
rule. Items are deleted rather than archived so that, if a rule later widens
again, the PR is eligible to come back.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

from sync import gh, graphql, PROJECT_ID, PROJECT_NUMBER, OWNER, REPO_RULES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    items = json.loads(gh(["project", "item-list", PROJECT_NUMBER, "--owner",
                           OWNER, "--format", "json", "-L", "500"]))["items"]
    now = datetime.now(timezone.utc)
    doomed = []

    for item in items:
        content = item.get("content") or {}
        repo = content.get("repository")
        rule = REPO_RULES.get(repo)
        if not rule:
            continue
        window = rule.get("updated_within_days")
        if not window:
            continue
        try:
            detail = json.loads(gh(["pr", "view", str(content["number"]),
                                    "--repo", repo, "--json",
                                    "updatedAt,title"]))
        except RuntimeError as err:
            print(f"  ! skip {repo}#{content['number']}: {err}")
            continue
        updated = datetime.fromisoformat(detail["updatedAt"].replace("Z", "+00:00"))
        age = (now - updated).days
        if age > window:
            doomed.append((item["id"], repo, content["number"],
                           detail["title"], age))

    print(f"{len(doomed)} item(s) outside their repo's ingest window")
    for _, repo, number, title, age in sorted(doomed, key=lambda d: -d[4]):
        print(f"  - {repo}#{number} ({age}d stale)  {title[:50]}")
        
    if args.dry_run:
        return
    for item_id, *_ in doomed:
        graphql("mutation($p:ID!,$i:ID!){deleteProjectV2Item("
                "input:{projectId:$p,itemId:$i}){deletedItemId}}",
                p=PROJECT_ID, i=item_id)
    print(f"removed {len(doomed)} item(s)")


if __name__ == "__main__":
    main()
