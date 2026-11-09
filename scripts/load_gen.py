#!/usr/bin/env python3
"""Load test for the payments ledger.

Opens some accounts, sends lots of random transfers at the same time, waits for the read
side to catch up, and then checks that no money was created or lost: the total of all
balances must equal the total of the starting deposits.

It only uses the Python standard library, so it runs anywhere with Python 3.9+.

    python3 scripts/load_gen.py --accounts 20 --transfers 500 --workers 16
"""
import argparse
import json
import random
import statistics
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal


def call(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--command-api", default="http://localhost:8081/api/v1")
    p.add_argument("--query-api", default="http://localhost:8082/api/v1")
    p.add_argument("--accounts", type=int, default=20)
    p.add_argument("--transfers", type=int, default=500)
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()
    rnd = random.Random(args.seed)

    run = uuid.uuid4().hex[:6]
    print(f"Opening {args.accounts} accounts (run {run})")
    accounts, seeded = [], Decimal("0")
    for i in range(args.accounts):
        deposit = Decimal(rnd.randint(100, 5000))
        status, body = call("POST", f"{args.command_api}/accounts",
                            {"owner": f"Load {run}-{i:03d}", "currency": "USD", "initialDeposit": str(deposit)})
        assert status == 201, body
        accounts.append(body["id"])
        seeded += deposit

    def one_transfer(_):
        src, dst = rnd.sample(accounts, 2)
        amount = str(Decimal(rnd.randint(1, 30000)) / 100)
        start = time.perf_counter()
        status, body = call("POST", f"{args.command_api}/transfers",
                            {"fromAccountId": src, "toAccountId": dst, "amount": amount},
                            {"Idempotency-Key": str(uuid.uuid4())})
        code = "ACCEPTED" if status == 202 else (body or {}).get("code", str(status))
        return code, (time.perf_counter() - start) * 1000

    print(f"Sending {args.transfers} transfers with {args.workers} workers")
    t0 = time.perf_counter()
    with ThreadPoolExecutor(args.workers) as pool:
        results = list(pool.map(one_transfer, range(args.transfers)))
    elapsed = time.perf_counter() - t0

    outcomes = Counter(code for code, _ in results)
    latencies = sorted(ms for _, ms in results)
    print(f"  {args.transfers / elapsed:.0f} req/s over {elapsed:.1f}s")
    print(f"  latency p50={statistics.median(latencies):.0f}ms p95={latencies[int(len(latencies) * .95) - 1]:.0f}ms")
    print(f"  outcomes: {dict(outcomes)}")

    print("Waiting for the read side to catch up")
    ids = set(accounts)
    deadline = time.time() + 60
    while True:
        _, views = call("GET", f"{args.query_api}/accounts")
        mine = [v for v in views if v["accountId"] in ids]
        total = sum(Decimal(str(v["balance"])) for v in mine)
        if (len(mine) == len(ids) and total == seeded) or time.time() > deadline:
            break
        time.sleep(1)

    negative = [v for v in mine if Decimal(str(v["balance"])) < 0]
    print(f"  seeded {seeded:.2f}, read side total {total:.2f}, negative balances: {len(negative)}")
    if total != seeded or negative:
        raise SystemExit("Conservation check FAILED")
    print("Conservation check passed: no money created or destroyed")


if __name__ == "__main__":
    main()
