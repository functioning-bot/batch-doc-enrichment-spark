import argparse
import json
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

DOMAINS = [
    "example.com",
    "news.site",
    "docs.example.org",
    "blog.example.net",
    "data.company.io",
]

WORDS = [
    "spark",
    "python",
    "batch",
    "web",
    "document",
    "pipeline",
    "data",
    "processing",
    "enrichment",
    "validation",
    "parquet",
    "partition",
    "performance",
]


def random_html(target_len: int) -> str:
    body = " ".join(random.choices(WORDS, k=max(10, target_len // 6)))
    filler = "".join(random.choices(string.ascii_lowercase + " ", k=target_len))
    return f"<html><body><h1>{body}</h1><p>{filler}</p></body></html>"


def generate_rows(row_count: int, start_time: datetime, domains: list[str]):
    for i in range(row_count):
        domain = random.choice(domains)
        url = f"https://{domain}/article/{i}"
        timestamp = (start_time + timedelta(seconds=i * 5)).isoformat()
        http_status = 200 if random.random() > 0.05 else 404
        size = random.randint(200, 4000)
        yield {
            "doc_id": f"doc_{i}",
            "url": url,
            "fetch_timestamp": timestamp,
            "http_status": http_status,
            "content_type": "text/html",
            "raw_html": random_html(size),
        }


def estimate_rows_for_target(target_bytes: int, avg_row_bytes: int) -> int:
    return max(1, int(target_bytes / avg_row_bytes))


def build_domains(count: int) -> list[str]:
    base = DOMAINS * max(1, (count + len(DOMAINS) - 1) // len(DOMAINS))
    return base[:count]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--target_mb", type=int)
    parser.add_argument("--target_gb", type=float)
    parser.add_argument("--avg_doc_kb", type=int, default=2)
    parser.add_argument("--domains_count", type=int, default=len(DOMAINS))
    parser.add_argument("--seed", type=int, default=13)
    args = parser.parse_args()

    random.seed(args.seed)
    domains = build_domains(args.domains_count)

    avg_row_bytes = max(256, args.avg_doc_kb * 1024)
    if args.target_gb is not None:
        target_bytes = int(args.target_gb * 1024 * 1024 * 1024)
        row_count = estimate_rows_for_target(target_bytes, avg_row_bytes)
    elif args.target_mb:
        target_bytes = int(args.target_mb * 1024 * 1024)
        row_count = estimate_rows_for_target(target_bytes, avg_row_bytes)
    else:
        row_count = args.rows

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = datetime.utcnow()
    with output_path.open("w", encoding="utf-8") as f:
        for row in generate_rows(row_count, start_time, domains):
            f.write(json.dumps(row) + "\n")

    approx_bytes = row_count * avg_row_bytes
    print(
        " ".join(
            [
                f"generated_rows={row_count}",
                f"approx_dataset_size_bytes={approx_bytes}",
                f"output_path={output_path}",
            ]
        )
    )


if __name__ == "__main__":
    main()
