# stress_v2_3.py
# pip install cloudscraper "requests>=2.32" curl_cffi beautifulsoup4 lxml urllib3

import argparse, csv, random, time, math, threading, logging, sys, shutil
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from typing import Dict, Any, List, Tuple

import requests
import cloudscraper
from requests.adapters import HTTPAdapter

try:
    from curl_cffi import requests as creq
    HAVE_CURLCFFI = True
except Exception:
    HAVE_CURLCFFI = False

URL_TPL = "https://csstats.gg/player/{steam64}"
CF_BAD = ("attention required", "just a moment", "challenge-platform", "error 1015", "rate limited")

# ---------- logging ----------
def setup_logger(level=logging.INFO, log_file: str | None = None):
    logger = logging.getLogger("stress")
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); ch.setLevel(level)
    logger.handlers = [ch]
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt); fh.setLevel(level)
        logger.addHandler(fh)
    return logger

# ---------- heuristics ----------
def is_valid_html(txt: str) -> bool:
    t = txt.lower()
    if '<div id="player-name">' in t: return True
    if "<title>" in t and "player statistics - " in t and "cs2 stats" in t: return True
    if 'link rel="canonical" href="https://csstats.gg/player/' in t: return True
    return False

def looks_like_cf(txt: str, status: int) -> bool:
    if status in (403, 429, 503): return True
    t = txt[:6000].lower()
    return any(s in t for s in CF_BAD)

def percentiles(vals: List[float], ps=(50,90,99)):
    if not vals: return {}
    vals_sorted = sorted(vals)
    out = {}
    for p in ps:
        k = (p/100)*(len(vals_sorted)-1)
        f = math.floor(k); c = math.ceil(k)
        out[int(p)] = round(vals_sorted[f] if f==c else vals_sorted[f] + (vals_sorted[c]-vals_sorted[f])*(k-f), 3)
    return out

# ---------- helpers ----------
def resolve_interpreter(interpreter: str, logger: logging.Logger) -> str:
    """Return a supported JS interpreter for cloudscraper.

    cloudscraper works faster with NodeJS but many environments may not have
    the ``node`` executable available.  Previously the script would silently
    fail when ``node`` was missing which caused worker threads to die and the
    queue to hang forever.  We now check for the presence of NodeJS and
    transparently fall back to ``js2py`` with a warning so that requests can
    still be executed.
    """
    if interpreter == "nodejs" and shutil.which("node") is None:
        logger.warning("node interpreter not found, falling back to js2py")
        return "js2py"
    return interpreter


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.updated = time.perf_counter()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        """Block until a token is available. Returns wait time."""
        start = time.perf_counter()
        while True:
            with self.lock:
                now = time.perf_counter()
                delta = now - self.updated
                if delta > 0:
                    self.tokens = min(self.capacity, self.tokens + delta * self.rate)
                    self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return time.perf_counter() - start
                needed = (1 - self.tokens) / self.rate if self.rate > 0 else 0.1
            time.sleep(min(needed, 0.1))


class ProxyPool:
    """Round-robin proxy pool with per-proxy and global rate limiting."""

    def __init__(self, proxies: List[str], per_rate: float, global_rate: float | None = None):
        self.proxies = proxies
        self.per_rate = per_rate
        self.limiters = {p: RateLimiter(per_rate, per_rate) for p in proxies}
        self.global_limiter = RateLimiter(global_rate, global_rate) if global_rate else None
        self.idx = 0
        self.lock = threading.Lock()

    def acquire(self) -> Tuple[str | None, float]:
        if not self.proxies:
            return None, 0.0
        with self.lock:
            proxy = self.proxies[self.idx]
            self.idx = (self.idx + 1) % len(self.proxies)
        wait = 0.0
        if self.global_limiter:
            wait += self.global_limiter.acquire()
        wait += self.limiters[proxy].acquire()
        return proxy, wait

# ---------- engines ----------
def make_cloudscraper(pool_size: int, interpreter: str):
    params = dict(
        browser={'browser':'chrome','platform':'windows','desktop':True},
        interpreter=interpreter,
        debug=False,
    )
    # "enable_stealth" was added in newer versions of cloudscraper.  For older
    # releases the argument is unknown and would raise a TypeError which used to
    # kill worker threads silently.  Try to enable it and gracefully fall back
    # if the parameter isn't supported.
    try:
        params["enable_stealth"] = True
        s = cloudscraper.create_scraper(**params)
    except TypeError:
        params.pop("enable_stealth", None)
        s = cloudscraper.create_scraper(**params)
    # ВНУТРЕННИЕ ретраи отключены — ретраи делаем сами с контролируемым бэк-оффом
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        pool_block=True,    # важен управляемый пул
        max_retries=0
    )
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def fetch_with_cloudscraper(s, url, timeout_tuple, proxies=None):
    r = s.get(url, timeout=timeout_tuple, proxies=proxies)  # tuple: (connect, read)
    return r.status_code, r.text, len(r.content)

def fetch_with_curlcffi(url, timeout_tuple, proxies=None):
    # curl_cffi принимает один timeout; берём максимум из пары
    to = max(timeout_tuple)
    r = creq.get(url, impersonate="chrome", timeout=to, proxies=proxies)
    return r.status_code, r.text, len(r.content)

# ---------- worker / progress ----------
def run_worker(q: Queue,
               results: list,
               engine: str,
               timeout_tuple,
               max_retries,
               backoff0,
               logger: logging.Logger,
               state: dict,
               verbose_requests: bool,
               hard_timeout: float,
               interpreter: str,
               proxy_pool: ProxyPool | None,
               cache: dict):
    sess = None
    if engine == "cloudscraper":
        try:
            sess = make_cloudscraper(pool_size=state["pool_size"], interpreter=interpreter)
        except Exception as e:
            logger.error(f"failed to create cloudscraper session: {e}")
            return

    while True:
        try:
            idx, url, t_enqueued = q.get(timeout=0.2)
        except Empty:
            return

        try:
            queue_wait = time.perf_counter() - t_enqueued
            with state["lock"]:
                state["inflight"] += 1
                state["start_times"][idx] = time.perf_counter()
                state["queue_waits"].append(queue_wait)

            t0 = time.perf_counter()
            t_start = t0
            status, ok, cf, err, bytes_ = 0, False, False, "", 0
            attempt = 0
            cached = False

            with cache["lock"]:
                entry = cache["data"].get(url)
                if entry and entry[0] > time.time():
                    cached_res = entry[1]
                    cached = True

            if cached:
                elapsed = 0.0
                status = cached_res["status"]
                ok = cached_res["ok"]
                cf = cached_res["cf"]
                err = cached_res["err"]
                bytes_ = cached_res["bytes"]
                with state["lock"]:
                    state["inflight"] -= 1
                    state["done"] += 1
                    state["latencies"].append(elapsed)
                    if ok: state["ok"] += 1
                    if cf: state["cf"] += 1
                    if err: state["errors"][err] = state["errors"].get(err, 0) + 1
                    state["start_times"].pop(idx, None)
                results.append({"idx": idx, "status": status, "ok": ok, "cf": cf,
                                "err": err, "elapsed": elapsed, "bytes": bytes_,
                                "cached": True})
                q.task_done()
                continue

            if verbose_requests:
                logger.info(f"[{idx:04}] start engine={engine}")

            while True:
                # абсолютный дедлайн
                if time.perf_counter() - t_start > hard_timeout:
                    err = "HardTimeout"
                    if verbose_requests:
                        logger.warning(f"[{idx:04}] hard-timeout {hard_timeout}s")
                    break

                attempt += 1
                if proxy_pool:
                    proxy, lw = proxy_pool.acquire()
                    proxies = {"http": proxy, "https": proxy} if proxy else None
                    with state["lock"]:
                        state["limiter_waits"].append(lw)
                else:
                    proxies = None
                try:
                    t_req = time.perf_counter()
                    if engine == "cloudscraper":
                        status, text, bytes_ = fetch_with_cloudscraper(sess, url, timeout_tuple, proxies=proxies)
                    else:
                        status, text, bytes_ = fetch_with_curlcffi(url, timeout_tuple, proxies=proxies)

                    ok = (status == 200 and is_valid_html(text))
                    cf = looks_like_cf(text, status) and not ok
                    err = ""

                    if status == 429:
                        err = "429"
                        with state["lock"]:
                            state["rate_limited"] += 1
                        if verbose_requests:
                            logger.warning(f"[{idx:04}] received 429")

                    if verbose_requests:
                        d = time.perf_counter() - t_req
                        logger.info(f"[{idx:04}] attempt#{attempt} status={status} ok={ok} cf={cf} t={d:.2f}s bytes={bytes_}")

                except requests.exceptions.Timeout:
                    err = "Timeout"
                    if verbose_requests:
                        logger.warning(f"[{idx:04}] attempt#{attempt} timeout")
                except requests.exceptions.RequestException as e:
                    err = e.__class__.__name__
                    if verbose_requests:
                        logger.warning(f"[{idx:04}] attempt#{attempt} err={err}")
                except Exception as e:
                    err = f"{type(e).__name__}"
                    if verbose_requests:
                        logger.warning(f"[{idx:04}] attempt#{attempt} err={err}")

                # Условия выхода: успех / исчерпали попытки / не было исключения (получили ответ)
                if ok or attempt > max_retries or (err == "" and status != 429):
                    break

                # Экспоненциальный бэк-офф с джиттером, но не больше 5с
                delay = backoff0 * (2 ** (attempt - 1)) * random.uniform(0.5, 1.5)
                if verbose_requests:
                    logger.info(f"[{idx:04}] backoff {delay:.2f}s")
                time.sleep(min(delay, 5.0))

            elapsed = time.perf_counter() - t0

            results.append({
                "idx": idx, "status": status, "ok": ok, "cf": cf, "err": err,
                "elapsed": round(elapsed, 3), "bytes": bytes_, "cached": False
            })

            with state["lock"]:
                state["inflight"] -= 1
                state["done"] += 1
                state["latencies"].append(elapsed)
                if ok: state["ok"] += 1
                if cf: state["cf"] += 1
                if err: state["errors"][err] = state["errors"].get(err, 0) + 1
                state["start_times"].pop(idx, None)

            if ok:
                with cache["lock"]:
                    cache["data"][url] = (time.time() + cache["ttl"],
                                             {"status": status, "ok": ok, "cf": cf,
                                              "err": err, "bytes": bytes_})

            if verbose_requests:
                label = "OK" if ok else ("CF" if cf else ("ERR" if err else "BAD"))
                logger.info(f"[{idx:04}] {label} status={status} err={err or '-'} total_t={elapsed:.2f}s")

        except Exception as e:  # catch all to avoid hanging q.join
            logger.exception(f"worker crashed for idx {idx}: {e}")
            with state["lock"]:
                state["inflight"] -= 1
                state["errors"]["Crash"] = state["errors"].get("Crash", 0) + 1
                state["start_times"].pop(idx, None)
        finally:
            q.task_done()

def progress_loop(state: dict, total: int, logger: logging.Logger, every: float, stop_evt: threading.Event):
    while not stop_evt.wait(every):
        with state["lock"]:
            done = state["done"]
            inflight = state["inflight"]
            ok = state["ok"]; cf = state["cf"]; errors = dict(state["errors"])
            lat = list(state["latencies"])
            starts = dict(state["start_times"])
            rl = state["rate_limited"]
            q_waits = list(state["queue_waits"])
        p = percentiles(lat)
        now = time.perf_counter()
        oldest = round(max((now - t for t in starts.values()), default=0.0), 2)
        avg_q = round(sum(q_waits)/len(q_waits),3) if q_waits else 0.0
        logger.info(
            f"[progress] {done}/{total} done | in-flight={inflight} (oldest={oldest}s) | "
            f"ok={ok} | cf={cf} | 429={rl} | errors={errors or {}} | "
            f"avgQ={avg_q}s | p50={p.get(50,'-')}s p90={p.get(90,'-')}s p99={p.get(99,'-')}s"
        )
        if done and rl / done > 0.1:
            logger.warning("high rate of 429 responses detected")
        if avg_q > 5.0:
            logger.warning("queue wait times exceed 5s")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steam64", default="76561197977938104")
    ap.add_argument("--requests", type=int, default=200)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--engine", choices=["cloudscraper","curl_cffi"], default="cloudscraper")
    ap.add_argument("--timeout_connect", type=float, default=6.0)
    ap.add_argument("--timeout_read", type=float, default=20.0)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--backoff", type=float, default=0.35)
    ap.add_argument("--hard-timeout", type=float, default=40.0)
    ap.add_argument("--pool-size", type=int, default=64)
    ap.add_argument("--interpreter", choices=["nodejs","js2py"], default="nodejs")
    ap.add_argument("--cache-ttl", type=float, default=300.0)
    ap.add_argument("--rate-per-proxy", type=float, default=4.0, help="req/s per proxy")
    ap.add_argument("--proxy-start-port", type=int, default=60000)
    ap.add_argument("--proxy-count", type=int, default=10)
    ap.add_argument("--proxy-global-rate", type=float, default=None, help="global req/s limit")

    ap.add_argument("--csv", default=f"stress_{int(time.time())}.csv")
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--progress-every", type=float, default=2.0)
    ap.add_argument("--verbose-requests", action="store_true")
    ap.add_argument("--log-file", default=None)
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])

    args = ap.parse_args()
    level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger = setup_logger(level=level, log_file=args.log_file)

    if args.engine == "curl_cffi" and not HAVE_CURLCFFI:
        logger.error("Install curl_cffi first: pip install curl_cffi")
        raise SystemExit(1)

    # make sure the requested interpreter exists; fall back if necessary
    interpreter = resolve_interpreter(args.interpreter, logger)

    url = URL_TPL.format(steam64=args.steam64)
    logger.info(f"Target: {url} | total={args.requests}, conc={args.concurrency}, engine={args.engine}")

    q = Queue()
    for i in range(args.requests):
        q.put((i, url, time.perf_counter()))

    state = {
        "lock": threading.Lock(),
        "done": 0,
        "inflight": 0,
        "ok": 0,
        "cf": 0,
        "errors": {},
        "latencies": [],
        "start_times": {},
        "pool_size": args.pool_size,
        "rate_limited": 0,
        "queue_waits": [],
        "limiter_waits": []
    }

    results = []
    cache = {"data": {}, "lock": threading.Lock(), "ttl": args.cache_ttl}

    proxies = [f"http://127.0.0.1:{args.proxy_start_port + i}" for i in range(args.proxy_count)] if args.proxy_count > 0 else []
    global_rate = args.proxy_global_rate
    if global_rate is None and proxies:
        global_rate = args.rate_per_proxy * len(proxies)
    proxy_pool = ProxyPool(proxies, per_rate=args.rate_per_proxy, global_rate=global_rate) if proxies else None
    stop_evt = threading.Event()
    prog_thread = None
    if args.progress:
        prog_thread = threading.Thread(target=progress_loop,
            args=(state, args.requests, logger, args.progress_every, stop_evt), daemon=True)
        prog_thread.start()

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        for _ in range(args.concurrency):
            ex.submit(run_worker, q, results, args.engine,
                        (args.timeout_connect, args.timeout_read),
                        args.retries, args.backoff, logger, state,
                        args.verbose_requests, args.hard_timeout, interpreter,
                        proxy_pool, cache)
        q.join()

    stop_evt.set()
    if prog_thread: prog_thread.join(timeout=1.0)

    total = len(results)
    ok = sum(r["ok"] for r in results)
    cf = sum(r["cf"] for r in results)
    rate_limited = state["rate_limited"]
    errs: Dict[str,int] = {}
    for r in results:
        if r["err"]: errs[r["err"]] = errs.get(r["err"],0)+1
    lat = [r["elapsed"] for r in results if r["elapsed"]>0]
    q_waits = state["queue_waits"]
    avg_q = sum(q_waits)/len(q_waits) if q_waits else 0.0
    p = percentiles(lat)

    logger.info("=== SUMMARY ===")
    ok_pct = (ok/total*100) if total else 0
    logger.info(f"total: {total} | ok: {ok} ({ok_pct:.1f}%) | cf-like: {cf} | 429:{rate_limited} | errors: {errs or {}}")
    logger.info(f"avg queue wait: {avg_q:.2f}s | latency p50/p90/p99: {p.get(50,'-')}s / {p.get(90,'-')}s / {p.get(99,'-')}s")

    with open(args.csv,"w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["idx","status","ok","cf","err","elapsed","bytes","cached"])
        w.writeheader(); w.writerows(sorted(results, key=lambda x: x["idx"]))
    logger.info(f"saved: {args.csv}")

if __name__ == "__main__":
    main()
