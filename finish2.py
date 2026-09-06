#!/usr/bin/env python3
"""finish2.py - build playlists from probe.db history.

Outputs (all keep the original #EXTINF / #EXTVLCOPT lines from index.m3u):
  verified.m3u        every entry that passed in the latest run, index order,
                      feeds of the same channel ordered best first
  verified-best.m3u   one feed per channel (highest score)
  stable.m3u          passed in every run it was seen in (needs >= 2 runs)
  countries/<cc>.m3u  verified.m3u split by the country code in tvg-id
  results-latest.csv  per-URL measurements for inspection
"""
import argparse
import collections
import csv
import math
import os
import re
import sqlite3


def load_index(path):
    blocks, block, header = [], [], "#EXTM3U"
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.rstrip("\r\n")
        if line.startswith("#EXTM3U"):
            header = line
            continue
        if not line.strip():
            continue
        block.append(line)
        if not line.startswith("#"):
            blocks.append(block)
            block = []
    return header, blocks


def score(r):
    """Ordering key for feeds of the same channel: higher is better."""
    s = 0.0
    st = r["startup"] if r["startup"] is not None else 4.0
    s += max(0.0, 4.0 - st) * 15                       # up to 60 for a fast start
    if r["ratio"]:
        s += min(math.log2(max(r["ratio"], 1.0)), 5) * 6  # up to 30 for headroom
    if (r["n_variants"] or 0) > 1:
        s += 8                                         # adaptive ladder
    if r["url"].startswith("https://"):
        s += 4
    if r["res"] and "x" in r["res"]:
        s += min(int(r["res"].split("x")[1]), 1080) / 1080 * 8
    if r["kind"] == "dash":
        s -= 10                                        # not every player handles mpd
    if r["endlist"]:
        s -= 5                                         # looped VOD posing as live
    return round(s, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--index", default="index.m3u")
    ap.add_argument("--db", default="probe.db")
    ap.add_argument("--run", type=int, default=0, help="run id to build from (default: latest finished)")
    ap.add_argument("--history", type=int, default=5, help="runs to consider for stable.m3u")
    ap.add_argument("--no-dash", action="store_true", help="leave DASH (mpd) streams out")
    cfg = ap.parse_args()

    db = sqlite3.connect(cfg.db)
    db.row_factory = sqlite3.Row
    run = db.execute("SELECT * FROM runs WHERE finished IS NOT NULL AND (?=0 OR id=?) ORDER BY id DESC LIMIT 1",
                     (cfg.run, cfg.run)).fetchone()
    if run is None:
        raise SystemExit("no finished run in the database")
    latest = {r["url"]: dict(r) for r in db.execute("SELECT * FROM results WHERE run_id=?", (run["id"],))}
    hist_runs = [r["id"] for r in db.execute(
        "SELECT id FROM runs WHERE finished IS NOT NULL AND id<=? ORDER BY id DESC LIMIT ?", (run["id"], cfg.history))]
    seen, passed = collections.Counter(), collections.Counter()
    q = f"SELECT url, status FROM results WHERE run_id IN ({','.join('?' * len(hist_runs))})"
    for r in db.execute(q, hist_runs):
        seen[r["url"]] += 1
        if r["status"] == "OK":
            passed[r["url"]] += 1

    header, blocks = load_index(cfg.index)
    entries = []
    for b in blocks:
        url = b[-1].strip()
        r = latest.get(url)
        if r is None:
            continue
        ext = next(x for x in b if x.startswith("#EXTINF"))
        tvg = re.search(r'tvg-id="([^"]*)"', ext)
        grp = re.search(r'group-title="([^"]*)"', ext)
        name = ext.rsplit(",", 1)[-1]
        cc = re.search(r"\.([a-z]{2})(?:@|$)", tvg.group(1)) if tvg and tvg.group(1) else None
        entries.append({"block": b, "url": url, "r": r, "score": score(r),
                        "channel": (tvg.group(1).split("@")[0] if tvg and tvg.group(1) else name.lower()),
                        "cc": cc.group(1) if cc else "unknown", "group": grp.group(1) if grp else "",
                        "stable": seen[url] >= 2 and passed[url] == seen[url]})

    def keep(e):
        return e["r"]["status"] == "OK" and not (cfg.no_dash and e["r"]["kind"] == "dash")

    ok = [e for e in entries if keep(e)]
    # order feeds of the same channel best-first while keeping the index's channel order
    first_pos = {}
    for i, e in enumerate(ok):
        first_pos.setdefault(e["channel"], i)
    ok.sort(key=lambda e: (first_pos[e["channel"]], -e["score"]))

    def write(path, items):
        with open(path, "w", encoding="utf-8") as f:
            f.write(header + "\n")
            for e in items:
                f.write("\n".join(e["block"]) + "\n")

    write("verified.m3u", ok)
    best, seen_ch = [], set()
    for e in ok:
        if e["channel"] not in seen_ch:
            seen_ch.add(e["channel"])
            best.append(e)
    write("verified-best.m3u", best)
    stable = [e for e in ok if e["stable"]]
    if len(hist_runs) >= 2:
        write("stable.m3u", stable)
    os.makedirs("countries", exist_ok=True)
    for f in os.listdir("countries"):
        if f.endswith(".m3u"):
            os.remove(os.path.join("countries", f))
    bycc = collections.defaultdict(list)
    for e in ok:
        bycc[e["cc"]].append(e)
    for cc, items in bycc.items():
        write(os.path.join("countries", f"{cc}.m3u"), items)

    cols = ("url", "name", "status", "kind", "reason", "startup", "ratio", "kbps", "declared_kbps",
            "n_variants", "res", "target", "window", "endlist", "attempts_a", "attempts_b")
    with open("results-latest.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols + ("score", "channel", "cc"))
        for e in entries:
            w.writerow([e["r"].get(c) for c in cols] + [e["score"], e["channel"], e["cc"]])

    # ---- report
    n = len(entries)
    by_status = collections.Counter(e["r"]["status"] for e in entries)
    reasons = collections.Counter((e["r"]["reason"] or "").split(":")[0] or e["r"]["reason"]
                                  for e in entries if e["r"]["status"] == "DEAD")
    slow = [e for e in entries if e["r"]["status"] == "SLOW"]
    kinds = collections.Counter(e["r"]["kind"] for e in ok)

    def pct(xs, p):
        xs = sorted(xs)
        return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else float("nan")

    st = [e["r"]["startup"] for e in ok if e["r"]["startup"] is not None]
    ra = [e["r"]["ratio"] for e in ok if e["r"]["ratio"]]
    lines = [
        f"run {run['id']} started {run['started']} took {run['minutes']} min; {run['total']} unique urls",
        f"entries={n} kept={by_status['OK']} ({100*by_status['OK']/n:.0f}%) slow={by_status['SLOW']} ({100*by_status['SLOW']/n:.0f}%) dead={by_status['DEAD']} ({100*by_status['DEAD']/n:.0f}%)",
        f"kept: startup p50={pct(st,50):.2f}s p90={pct(st,90):.2f}s | ratio p50={pct(ra,50):.1f}x p90={pct(ra,90):.1f}x | kinds={dict(kinds)}",
        f"slow: startup>=4s {sum(1 for e in slow if (e['r']['startup'] or 99) >= 4)}, ratio<2 {sum(1 for e in slow if e['r']['ratio'] is not None and e['r']['ratio'] < 2)}",
        f"dead reasons: {reasons.most_common(10)}",
        f"second attempt rescued: {sum(1 for e in ok if (e['r']['attempts_b'] or 0) == 2)} streams (phase B) + {sum(1 for e in entries if (e['r']['attempts_a'] or 0) == 2 and e['r']['status'] != 'DEAD')} (phase A retry)",
        f"channels: {len(best)} distinct in verified-best.m3u from {len(ok)} feeds; stable.m3u={len(stable) if len(hist_runs) >= 2 else 'n/a (1 run)'}",
        f"countries: {len(bycc)}; top {[(cc, len(v)) for cc, v in sorted(bycc.items(), key=lambda kv: -len(kv[1]))[:10]]}",
    ]
    print("\n".join(lines))
    with open("report.txt", "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
