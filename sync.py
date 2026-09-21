#!/usr/bin/env python3
"""Sync Velox and Presto pull requests into the "Velox + Presto Portfolio" project.

Ingests new PRs, classifies each into a Workstream, refreshes the derived
fields from live PR state, and ages closed PRs out of the board.

Reads are batched: one paginated GraphQL query returns every board item with
its field values *and* the full state of the pull request behind it. Writes
are batched too, many field updates per request. A per-item round trip cost
roughly eight minutes a run, which does not fit a 30-minute schedule.

Cleared items are recorded in state/cleared.json. Archiving alone is not
enough -- an archived item would be re-added by the next ingest pass, so the
tombstone file is what makes "cleared stays cleared" hold.
"""

import json
import pathlib
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from classify import classify, is_parked, UNCLASSIFIED

sys.stdout.reconfigure(line_buffering=True)

PROJECT_ID = "PVT_kwHOAGMDEM4BkDSj"
PROJECT_NUMBER = "11"
OWNER = "karthikeyann"

STATE = pathlib.Path(__file__).parent / "state" / "cleared.json"

# Per-repo ingest rules. `search` terms are appended to the gh search query.
REPO_RULES = {
    # Only cudf work; upstream Velox opens ~490 PRs a month otherwise.
    "facebookincubator/velox": {"search": ["cudf"], "label": "cudf", "drafts": True},
    "rapidsai/velox": {"search": [], "label": None, "drafts": True},
    # Non-draft only, and only PRs touched in the last 90 days: the repo
    # carries long-stale team PRs that are not worth board space.
    "rapidsai/velox-testing": {"search": [], "label": None, "drafts": False,
                               "updated_within_days": 90},
    # Manual additions are preserved; only cudf PRs are auto-ingested.
    "prestodb/presto": {"search": ["cudf"], "label": None, "drafts": True},
}

# Closed PRs get a single Done status; the "merged in the last N days" views
# select on the built-in Closed date instead of shuffling items between
# statuses.
DONE = "Done"
CLEAR_AFTER_DAYS = 30

# GraphQL caps connections at 100; 50 keeps the response comfortably small.
PAGE_SIZE = 50
# Field updates per mutation request.
WRITE_BATCH = 25

BOARD_QUERY = """
query($p:ID!,$cursor:String){
  node(id:$p){... on ProjectV2{items(first:%d,after:$cursor){
    pageInfo{hasNextPage endCursor}
    nodes{
      id
      fieldValues(first:20){nodes{
        ... on ProjectV2ItemFieldSingleSelectValue{
          name field{... on ProjectV2SingleSelectField{name}}}
        ... on ProjectV2ItemFieldTextValue{
          text field{... on ProjectV2Field{name}}}
        ... on ProjectV2ItemFieldDateValue{
          date field{... on ProjectV2Field{name}}}
      }}
      content{... on PullRequest{
        number title url state isDraft closedAt updatedAt additions deletions
        author{login}
        repository{nameWithOwner}
        labels(first:20){nodes{name}}
        latestReviews(first:20){nodes{state author{login}}}
        reviewRequests(first:20){nodes{requestedReviewer{... on User{login}}}}
        commits(last:1){nodes{commit{committedDate statusCheckRollup{state}}}}
      }}
    }
  }}}
}""" % PAGE_SIZE


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
    result = json.loads(gh(args))
    # gh exits 0 on GraphQL-level errors and returns them in the body.
    if result.get("errors"):
        raise RuntimeError(f"GraphQL: {result['errors']}")
    return result


def load_fields():
    """Return {field name: {id, options: {option name: id}}}."""
    data = json.loads(gh(["project", "field-list", PROJECT_NUMBER,
                          "--owner", OWNER, "--format", "json", "-L", "50"]))
    return {f["name"]: {"id": f["id"],
                        "options": {o["name"]: o["id"] for o in f.get("options", [])}}
            for f in data["fields"]}


def fetch_board():
    """Return every board item with its field values and pull-request state."""
    items, cursor = [], None
    while True:
        args = {"p": PROJECT_ID}
        if cursor:
            args["cursor"] = cursor
        page = graphql(BOARD_QUERY, **args)["data"]["node"]["items"]
        for node in page["nodes"]:
            pull = node.get("content") or {}
            if not pull.get("url"):
                continue  # draft issue or non-PR item
            fields = {}
            for value in node["fieldValues"]["nodes"]:
                name = (value.get("field") or {}).get("name")
                if name:
                    fields[name] = (value.get("name") or value.get("text")
                                or value.get("date"))
            items.append({"id": node["id"], "fields": fields, "pr": pull})
        if not page["pageInfo"]["hasNextPage"]:
            return items
        cursor = page["pageInfo"]["endCursor"]


def check_state(pull):
    """Return the overall CI rollup state, or None when there are no checks."""
    commits = (pull.get("commits") or {}).get("nodes") or []
    if not commits:
        return None
    rollup = (commits[0].get("commit") or {}).get("statusCheckRollup")
    return rollup["state"] if rollup else None


def derive_blocked_on(pull):
    """Return the Blocked on value for an open PR.

    Ordered so the most specific signal wins. A changes-requested review the
    author has re-requested is back on the reviewer; one they have not
    re-requested is still theirs to address.
    """
    if pull["state"] != "OPEN":
        return None
    if pull["isDraft"]:
        return "Author"

    latest = (pull.get("latestReviews") or {}).get("nodes") or []
    requested = {(r.get("requestedReviewer") or {}).get("login")
                 for r in ((pull.get("reviewRequests") or {}).get("nodes") or [])}
    changes_requested = [r["author"]["login"] for r in latest
                         if r["state"] == "CHANGES_REQUESTED"]
    approvals = sum(1 for r in latest if r["state"] == "APPROVED")

    if changes_requested:
        # Re-requesting a reviewer puts them back in reviewRequests, which is
        # how a re-review is distinguishable from the original request.
        if any(login in requested for login in changes_requested):
            return "Reviewer"
        return "Author"

    if not latest:
        return "Reviewer"

    failing = check_state(pull) in ("FAILURE", "ERROR")
    if approvals >= 1 and failing:
        return "CI"
    if approvals >= 2 and not failing:
        return "Nothing - ready to land"
    return "Reviewer"


def derive_ci(pull):
    """Map the check rollup onto the CI field."""
    state = check_state(pull)
    if state in ("FAILURE", "ERROR"):
        return "Red"
    if state in ("PENDING", "EXPECTED"):
        return "Running"
    if state == "SUCCESS":
        return "Green"
    return "None"


# Buckets chosen so review effort, not line count, is what the label conveys.
SIZE_BUCKETS = [(10, "XS"), (100, "S"), (500, "M"), (1000, "L")]


def derive_size(pull):
    """Bucket a PR by lines changed. Recomputed every run, since a PR grows."""
    changed = (pull.get("additions") or 0) + (pull.get("deletions") or 0)
    for limit, label in SIZE_BUCKETS:
        if changed < limit:
            return label
    return "XL"


def last_commit_date(pull):
    """Return the last commit date as YYYY-MM-DD, or None.

    Stored as a date rather than an age in days so it cannot go stale between
    runs. It also beats updatedAt as an activity signal: upstream velox runs
    stale[bot], whose comments reset updatedAt and make dormant PRs look
    fresh, while the commit date reflects real work.
    """
    commits = (pull.get("commits") or {}).get("nodes") or []
    if not commits:
        return None
    return (commits[0].get("commit") or {}).get("committedDate", "")[:10] or None


def ensure_workstream(fields, name):
    """Add `name` as a Workstream option if it does not exist yet."""
    workstream = fields["Workstream"]
    if name in workstream["options"]:
        return
    options = [f'{{id:"{oid}",name:"{n}",color:GRAY,description:""}}'
               for n, oid in workstream["options"].items()]
    # color and description are required on ProjectV2SingleSelectFieldOptionInput.
    options.append(f'{{name:"{name}",color:GRAY,description:""}}')
    graphql("mutation($f:ID!){updateProjectV2Field(input:{fieldId:$f,"
            "singleSelectOptions:[" + ",".join(options) + "]})"
            "{projectV2Field{... on ProjectV2SingleSelectField{id}}}}",
            f=workstream["id"])
    print(f"  created workstream: {name}")
    fields.update(load_fields())


def flush_writes(writes):
    """Apply queued field updates, many per request."""
    for start in range(0, len(writes), WRITE_BATCH):
        chunk = writes[start:start + WRITE_BATCH]
        parts = []
        for index, (item_id, field_id, value, kind) in enumerate(chunk):
            payload = {"text": f'text:"{value}"',
                       "date": f'date:"{value}"',
                       "select": f'singleSelectOptionId:"{value}"'}[kind]
            parts.append(
                f'm{index}:updateProjectV2ItemFieldValue(input:{{'
                f'projectId:"{PROJECT_ID}",itemId:"{item_id}",'
                f'fieldId:"{field_id}",value:{{{payload}}}}})'
                f'{{projectV2Item{{id}}}}')
        graphql("mutation{" + " ".join(parts) + "}")
    return len(writes)


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
            window = rule.get("updated_within_days")
            if window:
                since = (datetime.now(timezone.utc)
                         - timedelta(days=window)).date().isoformat()
                extra = [*extra, "--updated", f">={since}"]
            raw = gh(["search", "prs", "--repo", repo, "--state", "open",
                      "--limit", "200", "--json", "number,title,url,isDraft",
                      *extra])
            for pull in json.loads(raw):
                if not rule["drafts"] and pull["isDraft"]:
                    continue
                found[pull["url"]] = repo
    return [url for url in found
            if url not in cleared and url not in existing_urls]


def main():
    dry_run = "--dry-run" in sys.argv
    started = time.time()
    STATE.parent.mkdir(parents=True, exist_ok=True)
    cleared = set(json.loads(STATE.read_text())) if STATE.exists() else set()

    fields = load_fields()
    items = fetch_board()

    to_add = ingest(cleared, {i["pr"]["url"] for i in items})
    print(f"ingest: {len(to_add)} new PR(s)")
    for url in to_add:
        print(f"  + {url}")
        if not dry_run:
            gh(["project", "item-add", PROJECT_NUMBER, "--owner", OWNER,
                "--url", url])
    if to_add and not dry_run:
        items = fetch_board()

    now = datetime.now(timezone.utc)
    writes, archived = [], 0

    for item in items:
        pull = item["pr"]
        current = item["fields"]
        repo = pull["repository"]["nameWithOwner"]
        labels = [l["name"] for l in (pull.get("labels") or {}).get("nodes", [])]
        workstream, is_new = classify(pull["title"], repo, labels)

        if pull["state"] == "OPEN":
            blocked = derive_blocked_on(pull)
            if is_parked(pull["title"], labels):
                status = "Parked"
            else:
                # Clear Parked once the marker is gone, but leave a manual
                # In Progress alone.
                existing = current.get("Status")
                status = existing if existing == "In Progress" else "Todo"
        else:
            closed = datetime.fromisoformat(pull["closedAt"].replace("Z", "+00:00"))
            if (now - closed).days >= CLEAR_AFTER_DAYS:
                print(f"  clear {pull['url']}")
                if not dry_run:
                    graphql("mutation($p:ID!,$i:ID!){archiveProjectV2Item("
                            "input:{projectId:$p,itemId:$i}){item{id}}}",
                            p=PROJECT_ID, i=item["id"])
                    cleared.add(pull["url"])
                archived += 1
                continue
            status, blocked = DONE, None

        if dry_run:
            print(f"  = {pull['url']} -> {workstream} / {status} / {blocked}")
            continue

        # Rules only seed the Workstream. Once a human has set it to anything
        # other than Unclassified, that judgment wins and later runs leave it
        # alone.
        existing_ws = current.get("Workstream")
        if existing_ws in (None, "", UNCLASSIFIED) and workstream != existing_ws:
            if is_new:
                ensure_workstream(fields, workstream)
            option = fields["Workstream"]["options"].get(workstream)
            if option:
                writes.append((item["id"], fields["Workstream"]["id"], option, "select"))

        if current.get("Status") != status:
            option = fields["Status"]["options"].get(status)
            if option:
                writes.append((item["id"], fields["Status"]["id"], option, "select"))

        if blocked and current.get("Blocked on") != blocked:
            option = fields["Blocked on"]["options"].get(blocked)
            if option:
                writes.append((item["id"], fields["Blocked on"]["id"], option, "select"))

        author = pull["author"]["login"]
        if current.get("PR Author") != author:
            writes.append((item["id"], fields["PR Author"]["id"], author, "text"))

        for field_name, wanted in (("CI", derive_ci(pull)),
                                   ("Size", derive_size(pull))):
            if current.get(field_name) != wanted:
                option = fields[field_name]["options"].get(wanted)
                if option:
                    writes.append((item["id"], fields[field_name]["id"],
                                   option, "select"))

        committed = last_commit_date(pull)
        if committed and (current.get("Last commit") or "")[:10] != committed:
            writes.append((item["id"], fields["Last commit"]["id"],
                           committed, "date"))

    if not dry_run:
        applied = flush_writes(writes)
        STATE.write_text(json.dumps(sorted(cleared), indent=1))
        print(f"{len(items)} item(s), {applied} field update(s), "
              f"{archived} archived, {len(cleared)} tombstone(s)")
    print(f"took {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
