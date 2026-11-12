#!/usr/bin/env python3
"""Prints the next N configs of a sweep list that have not finished or failed yet (used by run-server.sh).

    next_chunk.py LIST DONE_IDS RUN_LOG N

DONE_IDS is the runner's .done_ids (config ids of finished configs); RUN_LOG is the runner's run.log, where
failed configs appear as "FAILED <config id> (reason)". Failed configs are not retried.
"""
import os
import sys


def config_id(line):
    """Same as config_id() in run-profiling.sh: label|op|source|r<rows>|rr<right>|k<keys>|s<sel>|w<words>."""
    res, op, src, rows, right, keys, sel, words = line.split("|")
    heap, gc, cpus = res.split(":")
    rr = right if op in ("join", "union", "cartesian") else "0"
    s = sel if op == "filter" else "-"
    return f"heap{heap}-{gc}-cpu{cpus}|{op}|{src}|r{rows}|rr{rr}|k{keys}|s{s}|w{words}"


def main():
    lst, done_path, log_path, n = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    skip = set()
    if os.path.exists(done_path):
        skip |= {l.strip() for l in open(done_path) if l.strip()}
    if os.path.exists(log_path):
        with open(log_path, errors="replace") as f:
            skip |= {l.split()[1] for l in f if l.startswith("FAILED ") and len(l.split()) > 1}
    out = []
    for line in open(lst):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if config_id(line) not in skip:
            out.append(line)
            if len(out) >= n:
                break
    if out:  # nothing at all when the list is finished: the runners test for an empty file
        print("\n".join(out))


if __name__ == "__main__":
    main()
