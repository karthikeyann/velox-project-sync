#!/usr/bin/env python3
"""Sync Velox and Presto pull requests into the "Velox + Presto Portfolio" project.

Four passes per run: ingest new PRs, classify them, refresh derived fields on
every item, then age closed items through the done pipeline and clear them.

Cleared items are recorded in state/cleared.json. Archiving alone is not
enough -- an archived item would be re-added by the next ingest pass, so the
tombstone file is what makes "cleared stays cleared" hold.
"""

import json
import os
import pathlib
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from classify import classify, UNCLASSIFIED

PROJECT_ID = "PVT_kwHOAGMDEM4BkDSj"
PROJECT_NUMBER = "11"
OWNER = "karthikeyann"

STATE = pathlib.Path(__file__).parent / "state" / "cleared.json"

# Per-repo ingest rules. `search` is appended to the gh search prs query.
REPO_RULES = {
    # Only cudf work; upstream Velox merges ~490 PRs a month otherwise.
    "facebookincubator/velox": {"search": ["cudf"], "label": "cudf", "drafts": True},
    "rapidsai/velox": {"search": [], "label": None, "drafts": True},
    # Non-draft only, by request.
    "rapidsai/velox-testing": {"search": [], "label": None, "drafts": False},
    # Manual additions are preserved; only cudf PRs are auto-ingested.
    "prestodb/presto": {"search": ["cudf"], "label": None, "drafts": True},
}

DONE_WEEK = "Done this week"
DONE_MONTH = "Done this month"
CLEAR_AFTER_DAYS = 30
MONTH_AFTER_DAYS = 7


def gh(args, retries=4):
    """Run a gh command, retrying transient API failures."""
    for attempt in range(retries):
        proc = subprocess.run(["gh", *args], capture_output=True, text=True)
        if proc.returncode == 0:
            return proc.stdout
        if attempt == retries - 1:
            raise RuntimeError(f"gh {' '.join(args)} failed: {proc.stderr.strip()}")
        time.sleep(3 * (attempt + 1))


def graphql(query, **variables):
    args = ["api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        args += ["-f", f"{key}={value}"]
    return json.loads(gh(args))


def load_fields():
    """Return {field name: {id, options: {option name: id}}}."""
    data = json.loads(gh(["project", "field-list", PROJECT_NUMBER,
                          "--owner", OWNER, "--format", "json", "-L", "50"]))
    fields = {}
    for field in data["fields"]:
        fields[field["name"]] = {
            "id": field["id"],
            "options": {o["name"]: o["id"] for o in field.get("options", [])},
        }
    return fields


def load_items():
    data = json.loads(gh(["project", "item-list", PROJECT_NUMBER,
                          "--owner", OWNER, "--format", "json", "-L", "500"]))
    return data["items"]


def set_select(item_id, field, option_name):
    field_id, option_id = field["id"], field["options"].get(option_name)
    if not option_id:
        return
    graphql(
        "mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){updateProjectV2ItemFieldValue("
        "input:{projectId:$p,itemId:$i,fieldId:$f,value:{singleSelectOptionId:$o}}"
        "){projectV2Item{id}}}",
        p=PROJECT_ID, i=item_id, f=field_id, o=option_id)


def set_text(item_id, field, text):
    graphql(
        "mutation($p:ID!,$i:ID!,$f:ID!,$t:String!){updateProjectV2ItemFieldValue("
        "input:{projectId:$p,itemId:$i,fieldId:$f,value:{text:$t}}"
        "){projectV2Item{id}}}",
        p=PROJECT_ID, i=item_id, f=field["id"], t=text)


def ensure_workstream(fields, name):
    """Add `name` as a Workstream option if it does not exist yet."""
    workstream = fields["Workstream"]
    if name in workstream["options"]:
        return
    options = [{"id": oid, "name": n} for n, oid in workstream["options"].items()]
    options.append({"name": name})
    payload = ",".join(
        "{" + (f'id:"{o["id"]}",' if "id" in o else "") + f'name:"{o["name"]}"' + "}"
        for o in options)
    graphql("mutation($f:ID!){updateProjectV2Field(input:{fieldId:$f,singleSelectOptions:["
            + payload + "]}){projectV2Field{... on ProjectV2SingleSelectField{id}}}}",
            f=workstream["id"])
    print(f"  created workstream: {name}")
    fields.update(load_fields())


def pr_detail(repo, number):
    raw = gh(["pr", "view", str(number), "--repo", repo, "--json",
              "number,title,url,isDraft,state,closedAt,updatedAt,author,labels,"
              "reviewDecision,statusCheckRollup,reviews"])
    return json.loads(raw)


def derive_blocked_on(detail):
    if detail["state"] != "OPEN":
        return None
    if detail["isDraft"]:
        return "Me"
    if detail.get("reviewDecision") == "CHANGES_REQUESTED":
        return "Me"
    failing = [c for c in (detail.get("statusCheckRollup") or [])
               if c.get("conclusion") == "FAILURE"]
    if failing:
        return "CI"
    if detail.get("reviewDecision") == "APPROVED":
        return "Nothing - ready to land"
    return "Reviewer"


def ingest(cleared, existing_urls):
    """Return PR URLs to add, per the per-repo rules."""
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
            raw = gh(["search", "prs", "--repo", repo, "--state", "open",
                      "--limit", "200", "--json", "number,title,url,isDraft",
                      *extra])
            for pr in json.loads(raw):
                if not rule["drafts"] and pr["isDraft"]:
                    continue
                found[pr["url"]] = (repo, pr["number"])
    return {url: v for url, v in found.items()
            if url not in cleared and url not in existing_urls}


def main():
    dry_run = "--dry-run" in sys.argv
    STATE.parent.mkdir(parents=True, exist_ok=True)
    cleared = set(json.loads(STATE.read_text())) if STATE.exists() else set()

    fields = load_fields()
    items = load_items()
    existing = {i["content"].get("url"): i for i in items if i.get("content")}

    # Pass 1: ingest.
    to_add = ingest(cleared, set(existing))
    print(f"ingest: {len(to_add)} new PR(s)")
    for url in to_add:
        print(f"  + {url}")
        if not dry_run:
            gh(["project", "item-add", PROJECT_NUMBER, "--owner", OWNER, "--url", url])
    if to_add and not dry_run:
        items = load_items()
        existing = {i["content"].get("url"): i for i in items if i.get("content")}

    now = datetime.now(timezone.utc)

    # Passes 2-4: classify, refresh, age out.
    for url, item in existing.items():
        repo = item["content"]["repository"]
        if "/" not in repo:
            repo = f"{OWNER}/{repo}"
        try:
            detail = pr_detail(repo, item["content"]["number"])
        except RuntimeError as err:
            print(f"  ! skip {url}: {err}")
            continue

        labels = [l["name"] for l in detail.get("labels", [])]
        workstream, is_new = classify(detail["title"], repo, labels)
        if is_new and not dry_run:
            ensure_workstream(fields, workstream)

        if detail["state"] == "OPEN":
            status = item.get("status") or "Todo"
            blocked = derive_blocked_on(detail)
        else:
            closed = datetime.fromisoformat(detail["closedAt"].replace("Z", "+00:00"))
            age = (now - closed).days
            if age >= CLEAR_AFTER_DAYS:
                print(f"  clear {url} (closed {age}d ago)")
                if not dry_run:
                    graphql("mutation($p:ID!,$i:ID!){archiveProjectV2Item("
                            "input:{projectId:$p,itemId:$i}){item{id}}}",
                            p=PROJECT_ID, i=item["id"])
                    cleared.add(url)
                continue
            status = DONE_MONTH if age >= MONTH_AFTER_DAYS else DONE_WEEK
            blocked = None

        if dry_run:
            print(f"  = {url} -> {workstream} / {status} / {blocked} / {detail['author']['login']}")
            continue

        if item.get("workstream") != workstream:
            set_select(item["id"], fields["Workstream"], workstream)
        if item.get("status") != status:
            set_select(item["id"], fields["Status"], status)
        if blocked and item.get("blocked on") != blocked:
            set_select(item["id"], fields["Blocked on"], blocked)
        set_text(item["id"], fields["PR Author"], detail["author"]["login"])

    if not dry_run:
        STATE.write_text(json.dumps(sorted(cleared), indent=1))
    print(f"cleared tombstones: {len(cleared)}")


if __name__ == "__main__":
    main()
