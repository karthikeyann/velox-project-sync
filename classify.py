"""Assign a Workstream to a pull request.

Ordered rules, first match wins. Nothing here guesses: a PR matching no rule
lands in "Unclassified" so it surfaces for triage instead of being filed
somewhere plausible but wrong.

Rules are ordered most-specific first. "Decimal" precedes "GPU Expressions"
because decimal work spans expressions, aggregation and IO, and the decimal
thread is the one worth tracking as a unit.
"""

import re

# Repos whose PRs belong to one workstream regardless of title.
REPO_WORKSTREAM = {
    "rapidsai/velox-testing": "Test Infra",
    "prestodb/presto": "Presto Integration",
}

# Conventional-commit scopes that map straight to a workstream.
SCOPE_WORKSTREAM = {
    "decimal": "Decimal",
    "core": "Core Upstream",
    "exchange": "Exchange",
    "ucx": "Exchange",
    "ucx-exchange": "Exchange",
    "dwio": "IO & Scan",
    "parquet": "IO & Scan",
    "nimble": "IO & Scan",
    "ci": "Build & CI",
    "build": "Build & CI",
}

# Keyword rules matched against title + label names, lowercased.
KEYWORD_WORKSTREAM = [
    (r"exchange|shuffle|outputbuffer|partitioned ?output|\bucx\b", "Exchange"),
    (r"decimal", "Decimal"),
    (r"iceberg|parquet|splitreader|split reader|hybrid scan|\breaders?\b|kvikio"
     r"|asyncdatacache|data cache|hive i/o|\bs3\b|storage|\bscans?\b"
     r"|split batching|minio", "IO & Scan"),
    (r"timestamp|time ?zone|date_add|\bintervals?\b", "Types & Time"),
    (r"memory|\bpools?\b|alloc|\boom\b|spill|\brmm\b|arena|byte target"
     r"|concat batch|batched concat|compression", "GPU Memory"),
    (r"express|simple functions?|\bnvcc\b|evaluat|\budf\b|case without"
     r"|\bbetween\b|subfield filter|predicate|null (semantics|policy)"
     r"|\breplace\b|\bcast\b", "GPU Expressions"),
    (r"join|aggregat|group ?by|groupby|order ?by|window|\boperators?\b"
     r"|\bplan\b|fragment executor", "GPU Operators"),
    (r"\bci\b|\bbuilds?\b|cmake|docker|workflow|pin to|test harness"
     r"|\btests?\b|spark", "Build & CI"),
]

# Scopes too generic to deserve their own workstream.
SCOPE_BLOCKLIST = {"cudf", "velox", "misc", "chore", "test", "tests", "docs", "core"}

UNCLASSIFIED = "Unclassified"


def _scope(title):
    """Return the conventional-commit scope in a title, or None."""
    match = re.match(r"^[a-z]+\(([^)]+)\)!?:", title.strip(), re.I)
    return match.group(1).lower() if match else None


def classify(title, repo, labels=()):
    """Return (workstream, is_new_candidate).

    is_new_candidate is True when the workstream came from an unrecognized
    scope and may still need to be created as a project field option.
    """
    if repo in REPO_WORKSTREAM:
        return REPO_WORKSTREAM[repo], False

    scope = _scope(title)
    if scope in SCOPE_WORKSTREAM:
        return SCOPE_WORKSTREAM[scope], False

    haystack = " ".join([title] + list(labels)).lower()
    for pattern, workstream in KEYWORD_WORKSTREAM:
        if re.search(pattern, haystack):
            return workstream, False

    if scope and scope not in SCOPE_BLOCKLIST:
        return scope.replace("_", " ").replace("-", " ").title(), True

    return UNCLASSIFIED, False
