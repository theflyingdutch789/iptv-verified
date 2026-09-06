#!/usr/bin/env python3
"""language.py - build a per-language playlist that keeps every channel.

Takes iptv-org's language playlist (ISO 639-3 code, e.g. mal = Malayalam) and
writes languages/<code>.m3u with the same entries, ordered by how they measured
in the latest probe: fast first, then slow, then unverified. Nothing is dropped:
regional channels are often geo-restricted to their home country and answer 403
from a datacenter probe while playing fine from home.

    python3 language.py mal            # fetches from iptv-org
    python3 language.py mal --file x   # uses a local copy
"""
import argparse
import csv
import os
import subprocess
import sys

ORDER = {"OK": 0, "SLOW": 1, "DEAD": 2}


def load_blocks(text):
    blocks, block, header = [], [], "#EXTM3U"
    for line in text.splitlines():
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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("code", help="ISO 639-3 language code as used by iptv-org, e.g. mal, hin, tam")
    ap.add_argument("--file", default="", help="local playlist instead of downloading")
    ap.add_argument("--results", default="results-latest.csv")
    cfg = ap.parse_args()

    if cfg.file:
        text = open(cfg.file, encoding="utf-8", errors="replace").read()
    else:
        url = f"https://iptv-org.github.io/iptv/languages/{cfg.code}.m3u"
        out = subprocess.run(["curl", "-sSL", "--compressed", "--max-time", "30", url], capture_output=True)
        if out.returncode != 0 or not out.stdout.startswith(b"#EXTM3U"):
            sys.exit(f"could not fetch {url}")
        text = out.stdout.decode("utf-8", "replace")
    header, blocks = load_blocks(text)

    results = {}
    if os.path.exists(cfg.results):
        for r in csv.DictReader(open(cfg.results, encoding="utf-8")):
            results[r["url"]] = r

    def key(b):
        r = results.get(b[-1].strip())
        if r is None:
            return (2, 99.0)
        return (ORDER.get(r["status"], 2), float(r["startup"] or 99))

    ordered = sorted(blocks, key=key)                 # stable: keeps iptv-org's order within a tier
    os.makedirs("languages", exist_ok=True)
    path = os.path.join("languages", f"{cfg.code}.m3u")
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for b in ordered:
            f.write("\n".join(b) + "\n")
    tiers = {"fast": 0, "slow": 0, "unverified": 0}
    for b in blocks:
        r = results.get(b[-1].strip())
        tiers["fast" if r and r["status"] == "OK" else "slow" if r and r["status"] == "SLOW" else "unverified"] += 1
    print(f"{path}: {len(blocks)} channels ({tiers['fast']} fast, {tiers['slow']} slow, {tiers['unverified']} unverified or blocked from the probe's location)")


if __name__ == "__main__":
    main()
