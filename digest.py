#!/usr/bin/env python3
"""Weekly digest of the Velox + Presto Portfolio board.

Delivered by opening an issue in this repository: GitHub emails the digest to
anyone watching, which needs no SMTP credentials and keeps a browsable
archive of past weeks. Pass --stdout to print instead.
"""

import argparse
import collections
import datetime as dt

from sync import gh, fetch_board

STALE_DAYS = 14
OWNER_REPO = "karthikeyann/velox-project-sync"


def days_since(stamp):
    """Days since an ISO timestamp or a bare YYYY-MM-DD date."""
    if not stamp:
        return None
    moment = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if moment.tzinfo is None:  # Project DATE fields have no time or zone.
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - moment).days


def section(title, lines):
    return f"## {title}\n\n" + ("\n".join(lines) if lines else "_Nothing._") + "\n"


def build(items):
    open_items = [i for i in items if i["pr"]["state"] == "OPEN"]
    closed = [i for i in items if i["pr"]["state"] != "OPEN"]
    active = [i for i in open_items if i["fields"].get("Status") != "Parked"]
    parked = [i for i in open_items if i["fields"].get("Status") == "Parked"]

    merged_7d = [i for i in closed if (days_since(i["pr"]["closedAt"]) or 99) <= 7]
    ready = [i for i in active
             if i["fields"].get("Blocked on") == "Nothing - ready to land"]
    red = [i for i in active if i["fields"].get("CI") == "Red"]
    triage = [i for i in active if i["fields"].get("Workstream") == "Unclassified"]

    def commit_age(item):
        return days_since(item["fields"].get("Last commit")) or 0

    stalled = sorted((i for i in active
                      if i["fields"].get("Blocked on") == "Reviewer"
                      and commit_age(i) > STALE_DAYS),
                     key=commit_age, reverse=True)

    def link(item, extra=""):
        pull = item["pr"]
        who = item["fields"].get("PR Author", "?")
        return f"- [{pull['title'][:70]}]({pull['url']}) — {who}{extra}"

    out = [f"# Portfolio digest — {dt.date.today():%Y-%m-%d}\n",
           f"**{len(active)} active** · {len(parked)} parked · "
           f"**{len(merged_7d)} merged this week**\n",
           f"CI red: **{len(red)}/{len(active)}** · "
           f"awaiting review >{STALE_DAYS}d: **{len(stalled)}** · "
           f"ready to land: **{len(ready)}** · needs triage: {len(triage)}\n"]

    out.append(section("Ready to land", [link(i) for i in ready]))
    out.append(section(f"Stalled >{STALE_DAYS}d awaiting review",
                       [link(i, f" — {commit_age(i)}d since last commit")
                        for i in stalled[:12]]))
    out.append(section("Needs triage", [link(i) for i in triage[:10]]))

    load = collections.defaultdict(collections.Counter)
    for item in active:
        load[item["fields"].get("PR Author")][item["fields"].get("Blocked on")] += 1
    rows = ["| Author | Open | On author | On reviewer | CI |",
            "|---|---|---|---|---|"]
    for who, counts in sorted(load.items(), key=lambda x: -sum(x[1].values()))[:10]:
        rows.append(f"| {who} | {sum(counts.values())} | {counts['Author']} "
                    f"| {counts['Reviewer']} | {counts['CI']} |")
    out.append(section("Team load", rows))

    work = collections.Counter(i["fields"].get("Workstream") for i in active)
    out.append(section("Workstreams",
                       [f"- {k}: {v}" for k, v in work.most_common()]))
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", action="store_true")
    args = parser.parse_args()

    body = build(fetch_board())
    if args.stdout:
        print(body)
        return
    title = f"Portfolio digest — {dt.date.today():%Y-%m-%d}"
    gh(["issue", "create", "--repo", OWNER_REPO, "--title", title,
        "--body", body])
    print(f"opened issue: {title}")


if __name__ == "__main__":
    main()
