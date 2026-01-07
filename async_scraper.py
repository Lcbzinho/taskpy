import asyncio
import json
import sys
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
from urllib import robotparser


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


@dataclass
class Selector:
    name: str
    css: str
    attr: Optional[str] = None


def parse_selector_arg(arg: str) -> Selector:
    # format: name=css[ @attr]
    if "=" not in arg:
        raise ValueError(f"Selector must be in name=css[@attr] format: {arg}")
    name, rest = arg.split("=", 1)
    if "@" in rest:
        css, attr = rest.split("@", 1)
        attr = attr.strip() or None
    else:
        css, attr = rest, None
    return Selector(name=name.strip(), css=css.strip(), attr=attr)


class RateLimiter:
    def __init__(self, delay: float = 0.0, per_host: bool = False):
        self.delay = max(0.0, delay)
        self.per_host = per_host
        self._lock = asyncio.Lock()
        self._next_ready_global: float = 0.0
        self._per_host_next: Dict[str, float] = {}

    async def wait(self, url: str) -> None:
        if self.delay <= 0:
            return
        host = urlparse(url).netloc if self.per_host else "__global__"
        async with self._lock:
            now = asyncio.get_event_loop().time()
            if self.per_host:
                next_ready = self._per_host_next.get(host, 0.0)
                if now < next_ready:
                    await asyncio.sleep(next_ready - now)
                    now = asyncio.get_event_loop().time()
                self._per_host_next[host] = now + self.delay
            else:
                if now < self._next_ready_global:
                    await asyncio.sleep(self._next_ready_global - now)
                    now = asyncio.get_event_loop().time()
                self._next_ready_global = now + self.delay


async def fetch(session: aiohttp.ClientSession, url: str, *,
                semaphore: asyncio.Semaphore,
                retries: int = 3,
                timeout_s: float = 20.0,
                rate_limiter: Optional[RateLimiter] = None) -> str:
    backoff = 1.0
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                if rate_limiter is not None:
                    await rate_limiter.wait(url)
                async with session.get(url, timeout=ClientTimeout(total=timeout_s)) as resp:
                    resp.raise_for_status()
                    return await resp.text()
            except Exception as e:
                if attempt == retries:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2


def parse_html(html: str, selectors: List[Selector]) -> Dict[str, List[str]]:
    # prefer lxml if available
    parser = "lxml"
    try:
        import lxml  # noqa: F401
    except Exception:
        parser = "html.parser"
    soup = BeautifulSoup(html, parser)
    result: Dict[str, List[str]] = {}
    if not selectors:
        # Default extraction if no selectors are provided
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        h1s = [h.get_text(strip=True) for h in soup.select("h1")]
        result["title"] = [title] if title else []
        result["h1"] = h1s
        return result

    for sel in selectors:
        nodes = soup.select(sel.css)
        if sel.attr:
            values = [n.get(sel.attr) for n in nodes if n.get(sel.attr)]
        else:
            values = [n.get_text(strip=True) for n in nodes]
        result[sel.name] = values
    return result


class RobotsCache:
    def __init__(self, user_agent: str, session: aiohttp.ClientSession):
        self.user_agent = user_agent
        self.session = session
        self._cache: Dict[str, robotparser.RobotFileParser] = {}
        self._lock = asyncio.Lock()

    async def can_fetch(self, url: str) -> bool:
        host = urlparse(url).netloc
        async with self._lock:
            rp = self._cache.get(host)
            if rp is None:
                robots_url = urlunparse(("https", host, "/robots.txt", "", "", ""))
                text = ""
                try:
                    async with self.session.get(robots_url, timeout=ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                except Exception:
                    # If robots can't be fetched, default allow
                    pass
                rp = robotparser.RobotFileParser()
                if text:
                    rp.parse(text.splitlines())
                else:
                    # With empty rules, robotparser allows by default
                    rp.parse([])
                self._cache[host] = rp
            return rp.can_fetch(self.user_agent, url)


async def scrape_many(urls: List[str], selectors: List[Selector], *,
                      concurrency: int = 10,
                      headers: Optional[Dict[str, str]] = None,
                      delay: float = 0.0,
                      per_host: bool = False,
                      respect_robots: bool = False,
                      logger: Optional[logging.Logger] = None) -> List[Dict]:
    semaphore = asyncio.Semaphore(concurrency)
    headers = headers or DEFAULT_HEADERS
    rate_limiter = RateLimiter(delay=delay, per_host=per_host) if delay > 0 else None
    async with aiohttp.ClientSession(headers=headers) as session:
        robots: Optional[RobotsCache] = RobotsCache(headers.get("User-Agent", "*"), session) if respect_robots else None
        tasks = []
        for url in urls:
            tasks.append(_scrape_one(session, url, selectors, semaphore, rate_limiter, robots, logger))
        return await asyncio.gather(*tasks)


async def _scrape_one(session: aiohttp.ClientSession, url: str,
                      selectors: List[Selector], semaphore: asyncio.Semaphore,
                      rate_limiter: Optional[RateLimiter],
                      robots: Optional[RobotsCache],
                      logger: Optional[logging.Logger]) -> Dict:
    try:
        if robots is not None:
            allowed = await robots.can_fetch(url)
            if not allowed:
                if logger:
                    logger.info(f"Blocked by robots.txt: {url}")
                return {"url": url, "ok": False, "error": "Disallowed by robots.txt"}
        html = await fetch(session, url, semaphore=semaphore, rate_limiter=rate_limiter)
        data = parse_html(html, selectors)
        return {"url": url, "ok": True, "data": data}
    except Exception as e:
        return {"url": url, "ok": False, "error": str(e)}


def _read_urls_from_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]


def _parse_args(argv: List[str]):
    import argparse
    p = argparse.ArgumentParser(description="Async web scraper with CSS selectors (BeautifulSoup + aiohttp)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--url", action="append", help="URL a ser coletada. Pode repetir a opção para várias URLs.")
    g.add_argument("--urls-file", help="Arquivo texto com uma URL por linha.")
    p.add_argument("--selector", action="append",
                   help="Seletores no formato nome=css[@attr]. Pode repetir a opção.")
    p.add_argument("--concurrency", type=int, default=10, help="Número de requisições simultâneas (default: 10)")
    p.add_argument("--output", help="Escreve resultados em JSONL para o caminho informado.")
    p.add_argument("--delay", type=float, default=0.0, help="Atraso mínimo entre requisições (segundos).")
    p.add_argument("--per-host", action="store_true", help="Aplica o atraso por host em vez de globalmente.")
    p.add_argument("--output-format", choices=["jsonl", "csv"], default="jsonl", help="Formato de saída quando --output é usado.")
    p.add_argument("--verbose", action="store_true", help="Ativa logs detalhados.")
    p.add_argument("--respect-robots", action="store_true", help="Respeita regras de robots.txt antes de fazer requisições.")
    return p.parse_args(argv)


def _load_selectors(raw: Optional[List[str]]) -> List[Selector]:
    if not raw:
        return []
    sels: List[Selector] = []
    for arg in raw:
        sels.append(parse_selector_arg(arg))
    return sels


def write_jsonl(path: str, rows: List[Dict]):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: str, rows: List[Dict], selectors: List[Selector]):
    import csv
    # Build header from selectors or defaults
    if selectors:
        cols = [s.name for s in selectors]
    else:
        cols = ["title", "h1"]
    header = ["url"] + cols
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            if not r.get("ok"):
                # still write row with error summary in first selector column
                err = r.get("error", "")
                writer.writerow([r.get("url", "")] + ([f"ERROR: {err}"] + [""] * (len(cols) - 1)))
                continue
            data = r.get("data", {})
            out = [r.get("url", "")]
            for c in cols:
                values = data.get(c, [])
                out.append(" | ".join(values) if isinstance(values, list) else (values or ""))
            writer.writerow(out)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    logger = None
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        logger = logging.getLogger("async_scraper")
    if args.url:
        urls = args.url
    else:
        urls = _read_urls_from_file(args.urls_file)

    selectors = _load_selectors(args.selector)

    results = asyncio.run(
        scrape_many(
            urls,
            selectors,
            concurrency=args.concurrency,
            delay=args.delay,
            per_host=bool(args.per_host),
            respect_robots=bool(args.respect_robots),
            logger=logger,
        )
    )

    # Print a concise summary to stdout
    for r in results:
        if r.get("ok"):
            sizes = {k: len(v) for k, v in r["data"].items()}
            print(f"OK  {r['url']} -> {sizes}")
        else:
            print(f"ERR {r['url']} -> {r.get('error')}")

    if args.output:
        if args.output_format == "jsonl":
            write_jsonl(args.output, results)
        else:
            write_csv(args.output, results, selectors)
        print(f"Resultados salvos em: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
