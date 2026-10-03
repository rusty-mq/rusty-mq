#!/usr/bin/env python3
"""Extract T28 soak evidence from a churn-soak log into JSON.

Parses the harness's own output lines:
  SOAK progress: <n> cycles, <secs>s elapsed, journal <b> bytes
  SOAK finished <n> cycles in <dur> (floor was Some(<s>)s)
  test result: ok/FAILED ...

Usage: soak_evidence.py <log> <out.json>
"""

import json
import re
import sys


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    log, out = sys.argv[1], sys.argv[2]
    checkpoints = []
    finished = None
    result = None
    floor = None
    with open(log) as f:
        for line in f:
            m = re.match(
                r"SOAK progress: (\d+) cycles, ([\d.]+)s elapsed, journal (\d+) bytes",
                line,
            )
            if m:
                checkpoints.append(
                    {
                        "cycles": int(m.group(1)),
                        "elapsed_seconds": float(m.group(2)),
                        "journal_bytes": int(m.group(3)),
                    }
                )
                continue
            m = re.match(r"SOAK finished (\d+) cycles in ([^ ]+) \(floor was Some\((\d+)\)s\)", line)
            if m:
                finished = {
                    "cycles": int(m.group(1)),
                    "floor_seconds": int(m.group(3)),
                }
                continue
            m = re.match(r"test result: (ok|FAILED)", line)
            if m:
                result = m.group(1)
    if not checkpoints and result is None:
        print(f"no soak evidence found in {log}", file=sys.stderr)
        sys.exit(1)
    evidence = {
        "log": log,
        "checkpoints": checkpoints,
        "cycles": (finished or checkpoints[-1] if checkpoints else {}).get("cycles"),
        "floor_seconds": (finished or {}).get("floor_seconds"),
        "max_journal_bytes_at_checkpoints": max(
            (c["journal_bytes"] for c in checkpoints), default=None
        ),
        "test_result": result,
        "in_progress": result is None,
    }
    with open(out, "w") as f:
        json.dump(evidence, f, indent=1)
    print(json.dumps(evidence, indent=1))


if __name__ == "__main__":
    main()
