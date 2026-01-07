import asyncio
import csv
from typing import Dict, List, Optional

import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

POPULAR_URL = "https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm"


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
                rate_limiter: Optional[RateLimiter] = None,
                timeout_s: float = 20.0) -> str:
    async with semaphore:
        if rate_limiter is not None:
            await rate_limiter.wait(url)
        async with session.get(url, timeout=ClientTimeout(total=timeout_s)) as resp:
            resp.raise_for_status()
            return await resp.text()


def parse_chart_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", attrs={"data-testid": "chart-layout-main-column"})
    if not container:
        # fallback: try to find any /title/ links in the page
        anchors = soup.select("a[href*='/title/tt']")
        return [urljoin("https://www.imdb.com", a.get("href")) for a in anchors if a.get("href")]

    ul = container.find("ul")
    if not ul:
        anchors = container.select("a[href*='/title/tt']")
        return [urljoin("https://www.imdb.com", a.get("href")) for a in anchors if a.get("href")]

    links = []
    for li in ul.find_all("li"):
        a = li.find("a", href=True)
        if a and "/title/tt" in a["href"]:
            links.append(urljoin("https://www.imdb.com", a["href"]))
    # dedup while preserving order
    seen = set()
    unique = []
    for u in links:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def parse_movie_details(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    title = None
    date = None

    # Title: try h1 > span, fallback to h1 text, fallback to <title>
    h1 = soup.find("h1")
    if h1:
        span = h1.find("span")
        title = (span.get_text(strip=True) if span else h1.get_text(strip=True)) or None
    if not title and soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Date: look for releaseinfo link
    date_tag = soup.find("a", href=lambda href: href and "releaseinfo" in href)
    if date_tag:
        date = date_tag.get_text(strip=True)

    # Rating: try data-testid score container
    rating_tag = soup.find("div", attrs={"data-testid": "hero-rating-bar__aggregate-rating__score"})
    rating = rating_tag.get_text(strip=True) if rating_tag else None

    # Plot: try data-testid variants, fallback to og:description
    plot_tag = soup.find("span", attrs={"data-testid": "plot-xs_to_m"}) or \
               soup.find("span", attrs={"data-testid": "plot-l"})
    plot = plot_tag.get_text(strip=True) if plot_tag else None
    if not plot:
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            plot = og["content"].strip()

    return {
        "title": title,
        "date": date,
        "rating": rating,
        "plot": plot,
    }


async def scrape_imdb(concurrency: int = 10, delay: float = 0.0) -> List[Dict[str, Optional[str]]]:
    semaphore = asyncio.Semaphore(concurrency)
    limiter = RateLimiter(delay=delay, per_host=True if delay > 0 else False)
    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
        chart_html = await fetch(session, POPULAR_URL, semaphore=semaphore, rate_limiter=limiter)
        links = parse_chart_links(chart_html)

        tasks = [fetch(session, url, semaphore=semaphore, rate_limiter=limiter) for url in links]
        pages = await asyncio.gather(*tasks, return_exceptions=True)

        results: List[Dict[str, Optional[str]]] = []
        for page in pages:
            if isinstance(page, Exception):
                continue
            details = parse_movie_details(page)
            if all(details.get(k) for k in ("title", "date", "rating", "plot")):
                results.append(details)
        return results


def write_csv(path: str, rows: List[Dict[str, Optional[str]]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "date", "rating", "plot"])
        for r in rows:
            writer.writerow([r.get("title"), r.get("date"), r.get("rating"), r.get("plot")])


def _parse_args(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="Async IMDB Most Popular scraper (CSV)")
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--delay", type=float, default=0.0, help="Atraso mínimo por host entre requisições (segundos)")
    p.add_argument("--output", default="movies_async.csv")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    rows = asyncio.run(scrape_imdb(concurrency=args.concurrency, delay=args.delay))
    write_csv(args.output, rows)
    print(f"Salvo {len(rows)} registros em {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
