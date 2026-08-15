#!/usr/bin/env python3
"""Прогон набора тестов через интерактор для одного или нескольких бинарей.

    python3 harness/bench.py --bin v6=./build/v6 --bin v7=./build/v7
    python3 harness/bench.py --bin v7=./build/v7 --tests 'tests/gen/burst*.txt'
    python3 harness/bench.py --bin v7=./build/v7 --save logs/bench_v7.json
    python3 harness/bench.py --bin v7=./build/v7 --baseline logs/bench_v6.json

Печатает балл по каждому тесту и сумму; при нарушении протокола — VIOLATION и балл 0
(так же, как считает судья). Ненулевой код возврата, если есть нарушения.
"""

import argparse
import glob
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERACTOR = os.path.join(ROOT, "harness", "interactor.py")


def run_one(test, binary, timeout):
    started = time.time()
    try:
        p = subprocess.run(
            [sys.executable, INTERACTOR, test, "--", binary],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"score": 0.0, "verdict": "timeout", "wall": timeout}
    wall = time.time() - started

    res = {"verdict": "violation" if p.returncode else "ok", "wall": wall, "score": 0.0}
    for line in p.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in ("score", "tp", "tdr", "tpot", "tp_comp",
                                            "wait_comp", "tokens"):
            res[parts[0]] = float(parts[1])
    if p.returncode:
        res["score"] = 0.0
        res["error"] = (p.stderr.strip().splitlines() or ["?"])[-1]
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", action="append", required=True, metavar="NAME=PATH")
    ap.add_argument("--tests", default=os.path.join(ROOT, "tests", "gen", "*.txt"))
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--save", help="записать результаты в JSON")
    ap.add_argument("--baseline", help="сравнить с ранее сохранённым JSON")
    args = ap.parse_args()

    bins = []
    for spec in args.bin:
        name, _, path = spec.partition("=")
        bins.append((name, os.path.abspath(path)))
        if not os.path.exists(path):
            sys.exit("нет бинаря: %s" % path)

    tests = sorted(glob.glob(args.tests))
    if not tests:
        sys.exit("не найдено тестов по маске %s" % args.tests)

    baseline = {}
    if args.baseline and os.path.exists(args.baseline):
        baseline = json.load(open(args.baseline))

    header = "%-22s" % "тест" + "".join("%12s" % n for n, _ in bins)
    if len(bins) == 2:
        header += "%10s" % "Δ"
    if baseline:
        header += "%10s" % "Δ base"
    print(header)
    print("-" * len(header))

    results = {}
    totals = {n: 0.0 for n, _ in bins}
    violations = 0
    for test in tests:
        name = os.path.basename(test)[:-4]
        row = "%-22s" % name[:22]
        per_bin = {}
        for bname, bpath in bins:
            r = run_one(test, bpath, args.timeout)
            per_bin[bname] = r
            totals[bname] += r["score"]
            if r["verdict"] != "ok":
                violations += 1
                row += "%12s" % r["verdict"].upper()
            else:
                row += "%12.1f" % r["score"]
        results[name] = per_bin
        if len(bins) == 2:
            a, b = bins[0][0], bins[1][0]
            row += "%+10.1f" % (per_bin[b]["score"] - per_bin[a]["score"])
        if baseline and name in baseline:
            prev = baseline[name][bins[-1][0]]["score"]
            row += "%+10.1f" % (per_bin[bins[-1][0]]["score"] - prev)
        print(row)
        for bname, r in per_bin.items():
            if "error" in r:
                print("    %s: %s" % (bname, r["error"]))

    print("-" * len(header))
    print("%-22s" % "ИТОГО" + "".join("%12.1f" % totals[n] for n, _ in bins))
    print("%-22s" % "макс. время, с" + "".join(
        "%12.1f" % max(results[t][n]["wall"] for t in results) for n, _ in bins))
    if len(bins) == 2:
        a, b = bins[0][0], bins[1][0]
        print("%-22s%22s%+10.1f" % ("разница", "", totals[b] - totals[a]))

    if args.save:
        os.makedirs(os.path.dirname(os.path.abspath(args.save)), exist_ok=True)
        json.dump(results, open(args.save, "w"), indent=1)
        print("сохранено в %s" % args.save)

    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
