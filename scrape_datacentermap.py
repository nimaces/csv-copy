#!/usr/bin/env python3
"""Scrape USA data center listings from datacentermap.com."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup


DATA_CENTER_PATH_RE = re.compile(r"/data[-]?cent(er|re)s?/", re.IGNORECASE)


@dataclass
class DataCenter:
    name: str
    url: str
    address: str = ""
    city: str = ""
    state: str = ""
    postal_code: str = ""


def fetch_html(url: str, timeout: int = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_json_ld(soup: BeautifulSoup) -> list[dict]:
    items: list[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not script.string:
            continue
        try:
            payload = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            items.extend([entry for entry in payload if isinstance(entry, dict)])
        elif isinstance(payload, dict):
            items.append(payload)
    return items


def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def slug_to_name(slug: str) -> str:
    return slug.replace("-", " ").strip().title()


def extract_location_from_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts or parts[0].lower() != "usa":
        return "", ""
    state = slug_to_name(parts[1]) if len(parts) >= 2 else ""
    city = slug_to_name(parts[2]) if len(parts) >= 3 else ""
    return state, city


def extract_from_itemlist(
    items: Iterable[dict],
    default_state: str = "",
    default_city: str = "",
) -> list[DataCenter]:
    centers: list[DataCenter] = []
    for item in items:
        if item.get("@type") != "ItemList":
            continue
        for element in item.get("itemListElement", []):
            entry = element.get("item") if isinstance(element, dict) else None
            if not isinstance(entry, dict):
                continue
            name = entry.get("name") or ""
            url = entry.get("url") or ""
            address = ""
            city = ""
            state = ""
            postal = ""
            address_data = entry.get("address")
            if isinstance(address_data, dict):
                address = address_data.get("streetAddress") or ""
                city = address_data.get("addressLocality") or ""
                state = address_data.get("addressRegion") or ""
                postal = address_data.get("postalCode") or ""
            if name and url:
                centers.append(
                    DataCenter(
                        name=normalize_whitespace(name),
                        url=url,
                        address=normalize_whitespace(address),
                        city=normalize_whitespace(city) or default_city,
                        state=normalize_whitespace(state) or default_state,
                        postal_code=normalize_whitespace(postal),
                    )
                )
    return centers


def extract_from_links(
    soup: BeautifulSoup,
    base_url: str,
    default_state: str = "",
    default_city: str = "",
) -> list[DataCenter]:
    centers: list[DataCenter] = []
    seen: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not DATA_CENTER_PATH_RE.search(href):
            continue
        url = href
        if url.startswith("/"):
            url = base_url.rstrip("/") + url
        name = normalize_whitespace(link.get_text(" ", strip=True))
        if not name:
            continue
        if url in seen:
            continue
        seen.add(url)
        address_text = ""
        container = link.find_parent(["li", "article", "div"]) or link.parent
        if container:
            address_candidate = container.find(
                class_=re.compile(r"address|location", re.IGNORECASE)
            )
            if address_candidate:
                address_text = normalize_whitespace(
                    address_candidate.get_text(" ", strip=True)
                )
        centers.append(
            DataCenter(
                name=name,
                url=url,
                address=address_text,
                city=default_city,
                state=default_state,
            )
        )

    return centers


def dedupe_centers(centers: Iterable[DataCenter]) -> list[DataCenter]:
    seen: set[tuple[str, str]] = set()
    unique: list[DataCenter] = []
    for center in centers:
        key = (center.name, center.url)
        if key in seen:
            continue
        seen.add(key)
        unique.append(center)
    return unique


def normalize_url(href: str, base_url: str) -> str:
    if not href:
        return ""
    joined = urljoin(base_url, href)
    parsed = urlparse(joined)
    if parsed.scheme not in {"http", "https"}:
        return ""
    normalized = parsed._replace(query="", fragment="")
    return normalized.geturl()


def classify_location_url(url: str, base_netloc: str) -> str | None:
    parsed = urlparse(url)
    if parsed.netloc != base_netloc:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if not parts or parts[0].lower() != "usa":
        return None
    if len(parts) == 2:
        return "state"
    if len(parts) >= 3:
        return "city"
    return None


def extract_location_links(soup: BeautifulSoup, base_url: str) -> set[str]:
    parsed_base = urlparse(base_url)
    links: set[str] = set()
    for link in soup.find_all("a", href=True):
        normalized = normalize_url(link["href"], base_url)
        if not normalized:
            continue
        if classify_location_url(normalized, parsed_base.netloc):
            links.add(normalized)
    return links


def scrape_centers(start_url: str, dump_html: Path | None = None) -> list[DataCenter]:
    queue: list[str] = [normalize_url(start_url, start_url)]
    visited: set[str] = set()
    centers: list[DataCenter] = []

    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            html = fetch_html(url)
        except (HTTPError, URLError) as exc:
            print(f"Failed to fetch {url}: {exc}", file=sys.stderr)
            continue

        if dump_html and url == start_url:
            dump_html.parent.mkdir(parents=True, exist_ok=True)
            dump_html.write_text(html, encoding="utf-8")

        soup = BeautifulSoup(unescape(html), "html.parser")
        json_ld = parse_json_ld(soup)
        default_state, default_city = extract_location_from_url(url)

        page_centers = extract_from_itemlist(
            json_ld,
            default_state=default_state,
            default_city=default_city,
        )
        if not page_centers:
            page_centers = extract_from_links(
                soup,
                base_url=url,
                default_state=default_state,
                default_city=default_city,
            )
        centers.extend(page_centers)

        for location_url in extract_location_links(soup, base_url=url):
            if location_url not in visited and location_url not in queue:
                queue.append(location_url)

    return dedupe_centers(centers)


def write_csv(centers: Iterable[DataCenter], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "name",
            "url",
            "address",
            "city",
            "state",
            "postal_code",
        ])
        for center in centers:
            writer.writerow(
                [
                    center.name,
                    center.url,
                    center.address,
                    center.city,
                    center.state,
                    center.postal_code,
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape data center listings from datacentermap.com/usa",
    )
    parser.add_argument(
        "--url",
        default="https://www.datacentermap.com/usa/",
        help="URL to scrape (default: https://www.datacentermap.com/usa/)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/datacenters_usa.csv"),
        help="Path to write the CSV output.",
    )
    parser.add_argument(
        "--dump-html",
        type=Path,
        help="Optional path to write the raw HTML for inspection.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    centers = scrape_centers(args.url, dump_html=args.dump_html)

    if not centers:
        print(
            "No data centers found. Consider using --dump-html to inspect the page",
            file=sys.stderr,
        )
        return 1

    write_csv(centers, args.output)
    print(f"Wrote {len(centers)} records to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
