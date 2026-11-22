"""Write a deterministic cross-file event-key fixture for scripts/run_fixtures.py."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path


STAMP = "2026-10-01T10:00:00Z"
DAY = "2026-10-01"


def write(path, contents):
    path.write_text(contents, encoding="utf-8", newline="\n")


def event(key, person, ingest, kind="purchase", product="p1", cents="", ts=STAMP):
    return ",".join((key, person, ts, ingest, kind, product, cents)) + "\n"


def create(root):
    case = root / "long_event_keys"
    data = case / "input"
    expected = case / "expected"
    case.mkdir(parents=True)
    for folder in (data / "events", data / "clicks", data / "links", expected):
        folder.mkdir(parents=True)
    write(data / "manifest.json", '{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-02T00:00:00Z"}\n')
    write(data / "products.csv", "product_id,category\np1,books\np2,home\n")
    write(data / "campaigns.csv", "campaign_id,channel\nc_1,email\nc_2,search\n")
    write(data / "links/part.csv", "user_id_a,user_id_b\n")
    write(data / "clicks/part.csv", "click_id,user_id,campaign_id,ts\n"
          "k1,u_alice,c_1,2026-10-01T09:00:00Z\nk2,u_bob,c_2,2026-10-01T09:00:00Z\n")

    zero = "0" * 16
    suffix = zero + "abcdef1234567890"
    alice_keys = [
        "e", "e0", "e" + zero * 2,
        "e" + "1".zfill(16) + zero, "e" + zero + "1".zfill(16),
        "e" + suffix, "e" + suffix.replace("a", "A"), "E" + suffix,
        "e" + "0" * 31, "e" + "0" * 33, "e" + suffix + " ", "other/非ASCII",
    ]
    contested = ["e" + "aa" * 16, "e" + "55" * 16, "e0123456789abcdeffedcba9876543210"]
    with ExitStack() as handles:
        parts = [handles.enter_context((data / "events" / f"part-{i}.csv").open("w", encoding="utf-8", newline="\n"))
                 for i in range(4)]
        for part in parts:
            part.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
        for i, key in enumerate(alice_keys):
            parts[i % 4].write(event(key, "u_alice", STAMP, cents=str(1 << i)))
        # An early physical row wins for one ID, late physical rows win for the others.
        parts[0].write(event(contested[0], "u_bob", STAMP, product="p2", cents="99"))
        later = datetime(2026, 10, 1, 10, 0, 1)
        for i in range(24_576):
            key = contested[i % len(contested)]
            if (i // 12) % 2:
                key = '"' + key + '"'
            person = "u_alice" if i % 2 else "u_ghost"
            ingest = (later + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            parts[i % 4].write(event(key, person, ingest, cents="4096"))
        parts[3].write(event('"' + contested[1] + '"', "u_bob", STAMP, kind="view"))
        # This well-formed earliest copy has an invalid ts. No later good copy survives.
        parts[3].write(event(contested[2], "u_bob", STAMP, cents="8192", ts="invalid"))

    # The 12 distinct spellings credit 1+2+...+2048 cents to Alice. Bob keeps
    # a purchase and a view in one session. No valid event belongs to u_ghost.
    write(expected / "user_daily.csv", "user_id,day,sessions,events,revenue_cents,top_category\n"
          f"u_alice,{DAY},1,12,4095,books\nu_bob,{DAY},1,2,99,home\n")
    write(expected / "campaign_daily.csv", "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n"
          f"c_1,email,{DAY},1,1,4095,1\nc_2,search,{DAY},1,1,99,1\n")
    print(case)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture_root", type=Path)
    create(parser.parse_args().fixture_root)
