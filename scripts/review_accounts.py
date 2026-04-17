#!/usr/bin/env python3
"""Interactive review of discovered candidate X accounts.

Presents candidate accounts for human approval/rejection before
they enter the active rotation for proactive engagement discovery.
"""

import sys
import webbrowser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from review_helpers import read_char


def _format_candidate(candidate: dict, index: int, total: int) -> str:
    """Format a candidate account for display."""
    lines = [
        f"{index}/{total}  @{candidate['identifier']}",
    ]

    parts = []
    if candidate.get("relevance_score") is not None:
        parts.append(f"relevance: {candidate['relevance_score']:.2f}")
    if candidate.get("sample_count"):
        parts.append(f"samples: {candidate['sample_count']}")
    if parts:
        lines[0] += f"  ({', '.join(parts)})"

    if candidate.get("name") and candidate["name"] != candidate["identifier"]:
        lines.append(f"     Name: {candidate['name']}")
    if candidate.get("discovery_source"):
        lines.append(f"     Source: {candidate['discovery_source']}")
    if candidate.get("created_at"):
        lines.append(f"     Discovered: {candidate['created_at'][:10]}")

    return "\n".join(lines)


def main():
    with script_context() as (config, db):
        candidates = db.get_candidate_sources("x_account")

        if not candidates:
            print("No candidate accounts pending review.")
            return

        print(f"\n{len(candidates)} candidate account{'s' if len(candidates) != 1 else ''} pending review\n")

        quit_requested = False
        approved = 0
        dismissed = 0

        for i, candidate in enumerate(candidates):
            if quit_requested:
                break

            print(f"{'─' * 60}")
            print(_format_candidate(candidate, i + 1, len(candidates)))
            print()

            while True:
                sys.stdout.write("  [a]pprove  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                sys.stdout.flush()
                choice = read_char().lower()
                print(choice)

                if choice == "o":
                    url = f"https://x.com/{candidate['identifier']}"
                    print(f"  Opening {url}")
                    webbrowser.open(url)
                    continue
                elif choice == "q":
                    quit_requested = True
                elif choice == "a":
                    db.approve_candidate(candidate["id"])
                    print(f"  Approved @{candidate['identifier']} — now active.")
                    approved += 1
                elif choice == "d":
                    db.reject_candidate(candidate["id"])
                    print("  Dismissed.")
                    dismissed += 1
                else:
                    print("  Skipped.")
                break

        print(f"\nDone. {approved} approved, {dismissed} dismissed.")


if __name__ == "__main__":
    main()
