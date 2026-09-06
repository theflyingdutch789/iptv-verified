#!/usr/bin/env python3
"""probe2.py - two-phase IPTV stream prober with SQLite history.

Phase A (manifests, high concurrency)
    Fetch the master playlist, classify it (HLS / DASH / raw MPEG-TS / junk)
    and pick the top variant. Handles gzip bodies and BOMs. Transient
    failures (timeout / connect / reset) get one retry.

Phase B (playback, low concurrency, bandwidth-bound)
    Fetch the media playlist and immediately download the most recent
    *complete* segment, the way a player does before it can show a picture.
    Fetching the media playlist here rather than in phase A matters: live
    windows slide every few seconds, so segments chosen minutes earlier are
    gone. Borderline results are measured a second time with two segments on
    one connection, and the better attempt counts. Concurrency is sized from
    a link-capacity calibration so this machine's own link never becomes the
    bottleneck being measured.

Pass criteria (both configurable): start-up under 4 s, where start-up is
master + media playlist + one complete segment, and the segment downloads at
least 2x faster than it plays.

Each unique URL is probed once. Every run is stored in probe.db so that
finish2.py can build playlists from history rather than a single snapshot.
"""
import argparse
import json
import re
import sqlite3
import subprocess
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

UA_DEFAULT = "Mozilla/5.0"
CAP_BYTES = 262144                      # stop reading a body after this: it is not a playlist
STATS_TAG = "__STATS__"
STATS_FMT = (STATS_TAG + "|%{http_code}|%{time_namelookup}|%{time_connect}|%{time_appconnect}"
             "|%{time_starttransfer}|%{time_total}|%{size_download}|%{speed_download}"
             "|%{content_type}|%{url_effective}\n")
STATS_KEYS = ("http", "dns", "connect", "tls", "ttfb", "total", "size", "speed", "ctype", "effective")
RETRY_REASONS = {"timeout", "connect", "reset"}
CALIBRATE_URLS = (                      # first one that answers 200 is used, 8 parallel downloads
    "https://fsn1-speed.hetzner.com/100MB.bin",
    "http://speedtest.tele2.net/100MB.zip",
    "https://nbg1-speed.hetzner.com/100MB.bin",
    "https://ash-speed.hetzner.com/100MB.bin",
    "http://ipv4.download.thinkbroadband.com/100MB.zip",
)

# ----------------------------------------------------------------------------- curl

class Stat(dict):
    __getattr__ = dict.get


def parse_stats(text):
    out = []
    for line in text.splitlines():
        if not line.startswith(STATS_TAG + "|"):
            continue
        parts = line.split("|", 10)[1:]
        if len(parts) < 10:
            continue
        s = Stat(zip(STATS_KEYS, parts))
        for k in ("dns", "connect", "tls", "ttfb", "total", "size", "speed"):
            try:
                s[k] = float(s[k])
            except (TypeError, ValueError):
                s[k] = None
        s["ctype"] = (s["ctype"] or "").lower()
        out.append(s)
    return out


def run_curl(urls, ua=None, ref=None, maxtime=6, connect=3, cap=None, discard=False):
    """Run one curl process over one or more URLs (same connection when possible).

    Returns dict(body, stats[list, one per transfer], err, killed, truncated,
    wall_first, wall_total). With cap set, reading stops once that many body
    bytes arrived (the process is killed, stats are None): used to recognise
    endless raw streams. truncated is True when --max-time cut the last
    transfer short, so its byte count is partial.
    """
    cmd = ["curl", "-sS", "-L", "--compressed", "--http1.1",
           "--max-time", str(maxtime), "--connect-timeout", str(connect),
           "-A", ua or UA_DEFAULT, "-w", STATS_FMT]
    if ref:
        cmd += ["-e", ref]
    for u in urls:
        cmd += ["-o", "/dev/null" if discard else "-", u]
    t0 = time.monotonic()
    first = None
    killed = False
    buf = bytearray()
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:
        return {"body": b"", "stats": None, "err": f"spawn:{e}", "killed": False, "truncated": False,
                "wall_first": None, "wall_total": 0.0}
    try:
        while True:
            chunk = p.stdout.read1(65536)
            if not chunk:
                break
            if first is None:
                first = time.monotonic() - t0
            buf += chunk
            if cap and len(buf) >= cap:
                p.kill()
                killed = True
                break
        try:
            _, err = p.communicate(timeout=maxtime + 5)
        except subprocess.TimeoutExpired:
            p.kill()
            _, err = p.communicate()
            killed = True
    finally:
        try:
            p.stdout.close()
        except Exception:
            pass
    wall_total = time.monotonic() - t0
    err_text = err.decode("utf-8", "replace").strip()
    base = {"err": err_text, "wall_first": first, "wall_total": wall_total,
            "truncated": "timed out" in err_text.lower()}
    if killed:
        return {**base, "body": bytes(buf), "stats": None, "killed": True}
    marker = STATS_TAG.encode() + b"|"
    if discard:                                   # nothing but stats lines on stdout, one per transfer
        body, tail = b"", bytes(buf)
    else:                                         # body first, then the stats line (body may lack a final newline)
        idx = buf.rfind(marker)
        body = bytes(buf) if idx < 0 else bytes(buf[:idx])
        tail = b"" if idx < 0 else bytes(buf[idx:])
    stats = parse_stats(tail.decode("utf-8", "replace")) or None
    return {**base, "body": body, "stats": stats, "killed": False}


def classify_error(err):
    e = (err or "").lower()
    if "timed out" in e or "timeout" in e:
        return "timeout"
    if "could not resolve" in e:
        return "dns"
    if "refused" in e or "failed to connect" in e or "couldn't connect" in e or "unreachable" in e:
        return "connect"
    if "ssl" in e or "certificate" in e or "tls" in e:
        return "tls"
    if "empty reply" in e or "recv failure" in e or "reset by peer" in e or "connection was reset" in e:
        return "reset"
    if not e:
        return "no_response"
    return "curl:" + re.sub(r"curl: \(\d+\) ", "", e)[:40]

# ----------------------------------------------------------------------------- playlists

def sniff(body, ctype=""):
    head = body[:4096]
    stripped = head.lstrip(b"\xef\xbb\xbf \r\n\t")
    if stripped.startswith(b"#EXTM3U") or b"#EXT-X-" in head or b"#EXTINF" in head:
        return "hls"
    if b"<MPD" in head or "dash+xml" in ctype:
        return "dash"
    if len(body) > 376 and body[0] == 0x47 and body[188] == 0x47:
        return "direct"
    if b"ftyp" in head[:64] or "mp2t" in ctype or ctype.startswith("video/") or ctype.startswith("audio/"):
        return "direct"
    return "other"


def parse_playlist(body):
    text = body.decode("utf-8", "replace").lstrip("﻿ \r\n\t")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return {"kind": "empty"}
    if any(l.startswith("#EXT-X-STREAM-INF") for l in lines):
        variants = []
        for i, l in enumerate(lines):
            if l.startswith("#EXT-X-STREAM-INF"):
                bw = re.search(r"BANDWIDTH=(\d+)", l)
                res = re.search(r"RESOLUTION=(\d+x\d+)", l)
                uri = next((x for x in lines[i + 1:] if not x.startswith("#")), None)
                if uri:
                    variants.append({"bw": int(bw.group(1)) if bw else 0,
                                     "res": res.group(1) if res else None, "uri": uri})
        return {"kind": "master", "variants": variants}
    if any(l.startswith("#EXTINF") for l in lines):
        td = re.search(r"#EXT-X-TARGETDURATION:(\d+)", text)
        segs, dur = [], None
        for l in lines:
            if l.startswith("#EXTINF"):
                m = re.match(r"#EXTINF:\s*([\d.]+)", l)
                dur = float(m.group(1)) if m else None
            elif not l.startswith("#") and dur is not None:
                segs.append((dur, l))
                dur = None
        return {"kind": "media", "target": int(td.group(1)) if td else None,
                "nseg": len(segs), "window": round(sum(d for d, _ in segs), 1),
                "endlist": "#EXT-X-ENDLIST" in text, "segs": segs}
    return {"kind": "not_hls"}

# ----------------------------------------------------------------------------- phase A: master

def phase_a(entry, cfg):
    url, ua, ref = entry["url"], entry["ua"], entry["ref"]
    r = {"url": url, "name": entry["name"], "host": urllib.parse.urlsplit(url).hostname,
         "status": "DEAD", "kind": None, "reason": None, "attempts_a": 1, "attempts_b": 0}
    if not url.startswith(("http://", "https://")):
        r["reason"] = "scheme:" + url.split(":", 1)[0]
        return r
    res = run_curl([url], ua, ref, maxtime=cfg.a_maxtime, connect=cfg.connect, cap=CAP_BYTES)
    if res["killed"]:
        kind = sniff(res["body"])
        r.update(kind=kind, master_ttfb=res["wall_first"], master_total=res["wall_total"])
        if kind == "direct":
            r.update(status="ALIVE", media_url=url)
        else:
            r["reason"] = "not_playlist:oversize"
        return r
    st = res["stats"][-1] if res["stats"] else None
    if st is None:
        r["reason"] = classify_error(res["err"])
        return r
    r.update(dns=st.dns, connect=st.connect, master_ttfb=st.ttfb, master_total=st.total)
    if st.http not in ("200", "206"):
        r["reason"] = "http_" + st.http if st.http != "000" else classify_error(res["err"])
        return r
    kind = sniff(res["body"], st.ctype)
    r["kind"] = kind
    if kind == "dash":
        r.update(status="ALIVE", startup=round(st.total, 2))
        return r
    if kind == "direct":
        r.update(status="ALIVE", media_url=st.effective)
        return r
    if kind != "hls":
        r["reason"] = "not_playlist:" + (st.ctype.split(";")[0] or "unknown")[:30]
        return r
    pl = parse_playlist(res["body"])
    if pl["kind"] == "master":
        if not pl["variants"]:
            r["reason"] = "empty_master"
            return r
        ranked = sorted(pl["variants"], key=lambda v: -v["bw"])
        best = ranked[0]
        r.update(n_variants=len(pl["variants"]), res=best["res"], declared_kbps=best["bw"] // 1000,
                 media_url=urllib.parse.urljoin(st.effective, best["uri"]),
                 # fallbacks if the top variant's playlist is missing: next two by bandwidth
                 media_alts=[urllib.parse.urljoin(st.effective, v["uri"]) for v in ranked[1:3]])
    elif pl["kind"] == "media":
        r.update(n_variants=1, media_url=st.effective, media_is_master=True)
        if not pl["segs"]:
            r["reason"] = "no_segments"
            return r
    else:
        r["reason"] = "master_" + pl["kind"]
        return r
    r["status"] = "ALIVE"
    return r

# ----------------------------------------------------------------------------- phase B: media + segments

def measure_direct(r, entry, cfg):
    res = run_curl([r["media_url"]], entry["ua"], entry["ref"], maxtime=cfg.direct_time,
                   connect=cfg.connect, discard=True)
    st = (res["stats"] or [None])[-1]
    size = (st.size if st else 0) or 0
    if size < 300_000:
        if st is None or st.http in ("200", "206", "000"):
            return {"status": "DEAD", "reason": "direct_no_data"}
        return {"status": "DEAD", "reason": "direct_http_" + st.http}
    startup = st.ttfb or r.get("master_ttfb") or 0.0
    return {"seg_time": round(st.total, 2), "seg_bytes": int(size), "startup": round(startup, 2),
            "kbps": int(size * 8 / max(st.total, 0.1) / 1000),
            "status": "OK" if startup < cfg.max_startup else "SLOW"}


def fetch_media(r, ua, ref, cfg):
    """Fetch the media playlist; if the top variant's is missing, fall back to the next ones."""
    out = {}
    for i, url in enumerate([r["media_url"]] + list(r.get("media_alts") or [])):
        res = run_curl([url], ua, ref, maxtime=cfg.a_maxtime, connect=cfg.connect, cap=CAP_BYTES)
        st = (res["stats"] or [None])[-1]
        if res["killed"] or st is None:
            out = {"status": "DEAD", "reason": "media_" + ("oversize" if res["killed"] else classify_error(res["err"]))}
            break                                      # network trouble: alternatives will not help
        out = {"media_ttfb": st.ttfb, "media_total": st.total}
        if st.http not in ("200", "206"):
            out.update(status="DEAD", reason="media_" + ("http_" + st.http if st.http != "000" else classify_error(res["err"])))
            continue                                   # 404 on one rendition: try the next
        pl = parse_playlist(res["body"])
        if pl["kind"] != "media":
            out.update(status="DEAD", reason="media_" + pl["kind"])
            continue
        if i:
            out["variant_fallback"] = i
        return out, st, pl
    return out, None, None


def fetch_chain(entry, cfg):
    """Fresh master -> media playlist chain, as a player does at press-play time.

    Variant URLs frequently carry session tokens that expire within minutes,
    so phase B never reuses the URL phase A saw: it starts again from the top.
    Returns (fields, media_stat, media_playlist); media_playlist is None on failure.
    """
    url, ua, ref = entry["url"], entry["ua"], entry["ref"]
    res = run_curl([url], ua, ref, maxtime=cfg.a_maxtime, connect=cfg.connect, cap=CAP_BYTES)
    st = (res["stats"] or [None])[-1]
    if res["killed"] or st is None:
        return {"status": "DEAD", "reason": "master_" + ("oversize" if res["killed"] else classify_error(res["err"]))}, None, None
    out = {"master_ttfb": st.ttfb, "master_total": st.total}
    if st.http not in ("200", "206"):
        out.update(status="DEAD", reason=("http_" + st.http) if st.http != "000" else classify_error(res["err"]))
        return out, None, None
    pl = parse_playlist(res["body"])
    if pl["kind"] == "media":                        # the URL is the media playlist itself
        out.update(media_ttfb=0.0, media_total=0.0, n_variants=1)
        return out, st, pl
    if pl["kind"] != "master":
        out.update(status="DEAD", reason="master_" + pl["kind"])
        return out, None, None
    if not pl["variants"]:
        out.update(status="DEAD", reason="empty_master")
        return out, None, None
    ranked = sorted(pl["variants"], key=lambda v: -v["bw"])
    best = ranked[0]
    out.update(n_variants=len(pl["variants"]), res=best["res"], declared_kbps=best["bw"] // 1000)
    tmp = {"media_url": urllib.parse.urljoin(st.effective, best["uri"]),
           "media_alts": [urllib.parse.urljoin(st.effective, v["uri"]) for v in ranked[1:3]]}
    more, st2, pl2 = fetch_media(tmp, ua, ref, cfg)
    out.update(more)
    return out, st2, pl2


def measure_hls(r, entry, cfg, nseg=1):
    ua, ref = entry["ua"], entry["ref"]
    out, st, pl = fetch_chain(entry, cfg)
    if pl is None:
        return out
    out.update(target=pl["target"], nseg=pl["nseg"], window=pl["window"], endlist=int(pl["endlist"]))
    segs = pl["segs"]
    if not segs:
        out.update(status="DEAD", reason="no_segments")
        return out
    # the newest segment may still be being written on some origins: skip it and take the
    # nseg complete ones before it (1 on the first pass, 2 on the borderline re-measure)
    pick = segs[-1 - nseg:-1] if len(segs) > nseg else segs[-nseg:]
    pick = [(d, urllib.parse.urljoin(st.effective, u)) for d, u in pick]
    res2 = run_curl([u for _, u in pick], ua, ref, maxtime=cfg.b_maxtime * nseg, connect=cfg.connect, discard=True)
    stats = res2["stats"] or []
    if not stats or stats[0].http not in ("200", "206") or not stats[0].size:
        code = stats[0].http if stats else None
        out.update(status="DEAD", reason=("segment_http_" + code) if code and code != "000"
                   else "segment_" + classify_error(res2["err"]))
        return out
    pairs = [(d, s) for (d, _), s in zip(pick, stats) if s.http in ("200", "206") and s.size]
    complete = pairs[:-1] if res2["truncated"] else pairs   # a transfer cut short by --max-time is partial
    if complete:
        seg_time = sum(s.total for _, s in complete)
        seg_bytes = sum(s.size for _, s in complete)
        seg_dur = sum(d for d, _ in complete)
        ratio = (seg_dur / seg_time) if seg_dur and seg_time else None
    else:                                                    # only a partial segment: ratio is at most dur/time
        d, s = pairs[0]
        seg_time, seg_bytes, seg_dur = s.total, s.size, d
        ratio = d / max(s.total, 0.1)
    startup = (out.get("master_total") or 0) + (out.get("media_total") or 0) + stats[0].total
    out.update(seg_time=round(seg_time, 2), seg_bytes=int(seg_bytes), seg_dur=round(seg_dur, 2),
               startup=round(startup, 2), ratio=round(ratio, 2) if ratio else None,
               kbps=int(seg_bytes * 8 / seg_dur / 1000) if seg_dur else None)
    if ratio is None:
        out["status"] = "OK" if startup < cfg.max_startup else "SLOW"
    else:
        out["status"] = "OK" if (startup < cfg.max_startup and ratio >= cfg.min_ratio) else "SLOW"
    return out


def phase_b(r, entry, cfg, nseg=1):
    if r["kind"] == "direct":
        return measure_direct(r, entry, cfg)
    return measure_hls(r, entry, cfg, nseg=nseg)


def borderline(r, cfg):
    if r["status"] != "SLOW":
        return False
    st = r.get("startup") or 99
    ra = r.get("ratio")
    return st < cfg.max_startup * 1.5 and (ra is None or ra >= cfg.min_ratio * 0.6)


def merge_better(r, second, cfg):
    """Keep the better of two phase-B measurements."""
    if second.get("status") == "DEAD":
        return r
    st = min(r.get("startup") or 99, second.get("startup") or 99)
    ratios = [x for x in (r.get("ratio"), second.get("ratio")) if x]
    ra = max(ratios) if ratios else None
    r.update(startup=st, ratio=ra)
    if second.get("kbps") and (not r.get("kbps") or second["kbps"] > r["kbps"]):
        r["kbps"] = second["kbps"]
    r["status"] = "OK" if (st < cfg.max_startup and (ra is None or ra >= cfg.min_ratio)) else "SLOW"
    return r

# ----------------------------------------------------------------------------- calibration

def calibrate(seconds=8, per_host=3):
    """Aggregate download capacity in Mbps.

    Several speed-test hosts on different paths are pulled at once (per_host
    connections each) so a slow route to any single host does not pass for
    the local link's ceiling. Returns None when no host answers.
    """
    procs = []
    for url in CALIBRATE_URLS:
        for _ in range(per_host):
            procs.append(subprocess.Popen(["curl", "-sS", "-o", "/dev/null", "--max-time", str(seconds),
                                           "--connect-timeout", "3", "-w", "%{http_code} %{size_download}\n", url],
                                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL))
    total, answered = 0.0, 0
    for p in procs:
        try:
            out, _ = p.communicate(timeout=seconds + 10)
            code, size = (out.decode().split() + ["0", "0"])[:2]
            if code == "200":
                answered += 1
                total += float(size or 0)
        except Exception:
            p.kill()
    return (total * 8 / seconds / 1e6) if answered else None


def size_phase_b(capacity_mbps, per_stream_mbps=6.0, share=0.5, lo=6, hi=32):
    """Workers so that the segment phase uses about half the link at ~6 Mbps each."""
    if not capacity_mbps or capacity_mbps <= 0:
        return None
    return max(lo, min(hi, int(capacity_mbps * share / per_stream_mbps)))

# ----------------------------------------------------------------------------- index + db

def load_index(path):
    entries, block, header = [], [], "#EXTM3U"
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.rstrip("\r\n")
        if line.startswith("#EXTM3U"):
            header = line
            continue
        if not line.strip():
            continue
        block.append(line)
        if not line.startswith("#"):
            ua = ref = None
            name = ""
            for b in block:
                if b.startswith("#EXTINF"):
                    name = b.rsplit(",", 1)[-1]
                m = re.match(r"#EXTVLCOPT:http-user-agent=(.*)", b)
                if m:
                    ua = m.group(1).strip()
                m = re.match(r"#EXTVLCOPT:http-referrer=(.*)", b)
                if m:
                    ref = m.group(1).strip()
            entries.append({"url": line.strip(), "name": name, "ua": ua, "ref": ref, "block": block})
            block = []
    return header, entries


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY, started TEXT, finished TEXT, params TEXT,
  total INT, ok INT, slow INT, dead INT, minutes REAL, capacity_mbps REAL);
CREATE TABLE IF NOT EXISTS results(
  run_id INT, url TEXT, name TEXT, host TEXT, status TEXT, kind TEXT, reason TEXT,
  dns REAL, connect REAL, master_ttfb REAL, master_total REAL, media_ttfb REAL, media_total REAL,
  seg_time REAL, seg_bytes INT, seg_dur REAL, startup REAL, ratio REAL, kbps INT, declared_kbps INT,
  n_variants INT, res TEXT, target INT, window REAL, endlist INT, attempts_a INT, attempts_b INT,
  PRIMARY KEY(run_id, url));
CREATE INDEX IF NOT EXISTS results_url ON results(url);
"""
COLS = ("url", "name", "host", "status", "kind", "reason", "dns", "connect", "master_ttfb", "master_total",
        "media_ttfb", "media_total", "seg_time", "seg_bytes", "seg_dur", "startup", "ratio", "kbps",
        "declared_kbps", "n_variants", "res", "target", "window", "endlist", "attempts_a", "attempts_b")


def save_results(db, run_id, results):
    rows = [tuple([run_id] + [r.get(c) for c in COLS]) for r in results]
    db.executemany(f"INSERT OR REPLACE INTO results(run_id,{','.join(COLS)}) "
                   f"VALUES({','.join('?' * (len(COLS) + 1))})", rows)
    db.commit()

# ----------------------------------------------------------------------------- main

class Progress:
    def __init__(self, label, total, logf):
        self.label, self.total, self.logf = label, total, logf
        self.done, self.t0, self.lock = 0, time.monotonic(), threading.Lock()

    def tick(self, every=500):
        with self.lock:
            self.done += 1
            if self.done % every == 0 or self.done == self.total:
                el = time.monotonic() - self.t0
                eta = el / self.done * (self.total - self.done)
                line = f"{self.label} {self.done}/{self.total} elapsed={el/60:.1f}m eta={eta/60:.1f}m"
                print(line, flush=True)
                self.logf.write(line + "\n")
                self.logf.flush()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--index", default="index.m3u")
    ap.add_argument("--db", default="probe.db")
    ap.add_argument("--workers-a", type=int, default=160, help="manifest phase concurrency")
    ap.add_argument("--workers-b", type=int, default=0, help="segment phase concurrency (0 = size from calibration)")
    ap.add_argument("--max-startup", type=float, default=4.0,
                    help="seconds from opening the URL to holding one complete segment (first picture)")
    ap.add_argument("--min-ratio", type=float, default=2.0, help="segment duration / download time")
    ap.add_argument("--a-maxtime", type=float, default=6, help="per-request cap for playlists")
    ap.add_argument("--b-maxtime", type=float, default=8,
                    help="seconds allowed per segment download (a segment slower than this fails the 2x rule anyway)")
    ap.add_argument("--direct-time", type=float, default=6, help="seconds to sample a raw TS push")
    ap.add_argument("--connect", type=float, default=3, help="connect timeout")
    ap.add_argument("--no-retry", action="store_true", help="skip phase A retry and phase B second attempt")
    ap.add_argument("--limit", type=int, default=0, help="probe only the first N entries (smoke test)")
    ap.add_argument("--urls-sql", default="", help="re-probe only the URLs returned by this query against --db")
    ap.add_argument("--into-run", type=int, default=0, help="write results into this existing run instead of a new one")
    ap.add_argument("--log", default="progress2.log")
    cfg = ap.parse_args()

    header, entries = load_index(cfg.index)
    if cfg.limit:
        entries = entries[:cfg.limit]
    if cfg.urls_sql:
        wanted = {row[0] for row in sqlite3.connect(cfg.db).execute(cfg.urls_sql)}
        entries = [e for e in entries if e["url"] in wanted]
    by_url = {}
    for e in entries:                      # probe each unique URL once
        by_url.setdefault(e["url"], e)
    work = list(by_url.values())
    logf = open(cfg.log, "a")
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    t0 = time.monotonic()

    capacity = None
    if cfg.workers_b <= 0:
        capacity = calibrate()
        cfg.workers_b = size_phase_b(capacity) or 16
        print(f"link capacity ≈ {capacity or 0:.0f} Mbps -> phase B x{cfg.workers_b}"
              + ("" if capacity else " (calibration failed, using default)"), flush=True)
    print(f"{len(entries)} entries, {len(work)} unique urls; phase A x{cfg.workers_a}, phase B x{cfg.workers_b}", flush=True)

    db = sqlite3.connect(cfg.db)
    db.executescript(SCHEMA)
    if cfg.into_run:
        run_id = cfg.into_run
    else:
        cur = db.execute("INSERT INTO runs(started, params, total, capacity_mbps) VALUES(?,?,?,?)",
                         (started, json.dumps(vars(cfg)), len(work), capacity))
        run_id = cur.lastrowid
    db.commit()

    # ---- phase A
    prog = Progress("A", len(work), logf)

    def do_a(e):
        try:
            r = phase_a(e, cfg)
        except Exception as ex:                       # never lose the run to one bad URL
            r = {"url": e["url"], "name": e["name"], "status": "DEAD", "reason": "exception:" + type(ex).__name__,
                 "attempts_a": 1, "attempts_b": 0}
        prog.tick()
        return r
    with ThreadPoolExecutor(max_workers=cfg.workers_a) as ex:
        results = list(ex.map(do_a, work))
    if not cfg.no_retry:
        retry = [i for i, r in enumerate(results) if r["status"] == "DEAD" and r.get("reason") in RETRY_REASONS]
        if retry:
            print(f"A-retry {len(retry)} transient failures", flush=True)
            with ThreadPoolExecutor(max_workers=cfg.workers_a) as ex:
                again = list(ex.map(lambda i: phase_a(work[i], cfg), retry))
            for i, r2 in zip(retry, again):
                r2["attempts_a"] = 2
                results[i] = r2
    save_results(db, run_id, results)
    alive = [i for i, r in enumerate(results) if r["status"] == "ALIVE"]
    print(f"phase A done in {(time.monotonic()-t0)/60:.1f}m: alive={len(alive)} dead={len(results)-len(alive)}", flush=True)

    # ---- phase B
    need_b = [i for i in alive if results[i]["kind"] in ("hls", "direct")]
    for i in alive:
        if results[i]["kind"] == "dash":               # manifest-only check for DASH
            r = results[i]
            r["status"] = "OK" if (r.get("startup") or 99) < cfg.max_startup else "SLOW"
    prog = Progress("B", len(need_b), logf)

    def do_b(i):
        r = results[i]
        try:
            r.update(phase_b(r, by_url[r["url"]], cfg))
        except Exception as ex:
            r.update(status="DEAD", reason="exception_b:" + type(ex).__name__)
        r["attempts_b"] = 1
        prog.tick()
    with ThreadPoolExecutor(max_workers=cfg.workers_b) as ex:
        list(ex.map(do_b, need_b))
    if not cfg.no_retry:
        again = [i for i in need_b if borderline(results[i], cfg)]
        if again:
            print(f"B-retry {len(again)} borderline streams", flush=True)
            prog = Progress("B2", len(again), logf)

            def do_b2(i):
                r = results[i]
                try:
                    merge_better(r, phase_b(r, by_url[r["url"]], cfg, nseg=2), cfg)
                except Exception:
                    pass
                r["attempts_b"] = 2
                prog.tick()
            with ThreadPoolExecutor(max_workers=cfg.workers_b) as ex:
                list(ex.map(do_b2, again))
    for r in results:
        for k in ("media_url", "media_alts", "media_is_master", "variant_fallback"):
            r.pop(k, None)
    save_results(db, run_id, results)

    counts = {s: sum(1 for r in results if r["status"] == s) for s in ("OK", "SLOW", "DEAD")}
    minutes = round((time.monotonic() - t0) / 60, 1)
    if cfg.into_run:                                   # partial re-probe: recount the whole run
        row = db.execute("SELECT sum(status='OK'), sum(status='SLOW'), sum(status='DEAD'), coalesce(minutes,0) "
                         "FROM results JOIN runs ON runs.id=run_id WHERE run_id=?", (run_id,)).fetchone()
        db.execute("UPDATE runs SET finished=?, ok=?, slow=?, dead=?, minutes=? WHERE id=?",
                   (datetime.now(timezone.utc).isoformat(timespec="seconds"), row[0], row[1], row[2],
                    round(row[3] + minutes, 1), run_id))
    else:
        db.execute("UPDATE runs SET finished=?, ok=?, slow=?, dead=?, minutes=? WHERE id=?",
                   (datetime.now(timezone.utc).isoformat(timespec="seconds"), counts["OK"], counts["SLOW"],
                    counts["DEAD"], minutes, run_id))
    db.commit()
    summary = {"run_id": run_id, "unique_urls": len(work), **counts, "minutes": minutes,
               "capacity_mbps": round(capacity) if capacity else None, "workers_b": cfg.workers_b}
    logf.write("DONE " + json.dumps(summary) + "\n")
    logf.close()
    print("DONE", json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
