"""Build deterministic design fixtures with answers derived from their construction."""
import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path


START = datetime(2028, 2, 28, tzinfo=timezone.utc)
END = START + timedelta(days=3)
DAY_MS = 86_400_000
EPOCH = int(START.timestamp()) * 1000
HEADERS = {
    "events": "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
    "clicks": "click_id,user_id,campaign_id,ts",
    "links": "user_id_a,user_id_b",
}
USER = ("user_id", "day", "sessions", "events", "revenue_cents", "top_category")
CAMPAIGN = ("campaign_id", "channel", "day", "sessions", "purchasing_sessions",
            "revenue_cents", "assisted_sessions")


def iso(date):
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def event(key, who, ts, *, ingest=None, kind="view", product="p1", price=""):
    return ",".join(map(str, (key, who, ts, ingest or iso(START), kind, product, price)))


def click(key, who, campaign, ts):
    return ",".join(map(str, (key, who, campaign, ts)))


def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


class Case:
    def __init__(self, root, name, products=None):
        self.root = root / name
        self.data = self.root / "input"
        self.expected = self.root / "expected"
        self.expected.mkdir(parents=True)
        for group in HEADERS:
            (self.data / group).mkdir(parents=True)
        manifest = {"window_start": iso(START), "window_end": iso(END)}
        (self.data / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        write_csv(self.data / "products.csv", ("product_id", "category"),
                  products or [("p1", "A"), ("p2", "B")])
        write_csv(self.data / "campaigns.csv", ("campaign_id", "channel"),
                  [(f"c_{j}", f"channel-{j}") for j in range(1, 6)])

    def part(self, group, number, lines=(), terminated=True):
        path = self.data / group / f"part-{number:03}.csv"
        with path.open("w", encoding="utf-8", newline="\n") as out:
            out.write(HEADERS[group])
            for line in lines:
                out.write("\n")
                out.write(line)
            if terminated:
                out.write("\n")

    def answers(self, users, campaigns):
        write_csv(self.expected / "user_daily.csv", USER, users)
        write_csv(self.expected / "campaign_daily.csv", CAMPAIGN, campaigns)


def milliseconds(root):
    case = Case(root, "millisecond_edges")
    first = EPOCH + DAY_MS - 300_000
    next_session = first + 3_600_001
    end = EPOCH + 3 * DAY_MS
    ingest_day_1 = iso(START + timedelta(days=1, seconds=1))
    case.part("links", 0)
    case.part("events", 0, [
        event("e1", "A", first, kind="purchase", price="000100"),
        # Leap midnight UTC-04, exactly 30 minutes after the first purchase.
        event("e2", "A", "2028-02-28T20:25:00-04:00", kind="cart"),
        event("e3", "A", next_session, kind="purchase", product="p2", price="250"),
        event("e4", "A", f"000{next_session}", kind="purchase", price="250"),
        event("e5", "B", EPOCH + 1000, ingest=ingest_day_1),
        event("e6", "B", EPOCH + 999, ingest=ingest_day_1),
        event("e7", "B", iso(START)), event("e8", "B", EPOCH - 1),
        event("e9", "B", end - 1), event("ea", "B", end),
    ], terminated=False)
    case.part("clicks", 0, [
        click("old", "A", "c_1", first - 7 * DAY_MS),
        click("too-old", "A", "c_5", first - 7 * DAY_MS - 1),
        click("k00000000000000000000000000000001", "A", "c_2", first),
        click("k00000000000000010000000000000000", "A", "c_3", first),
        click("k00000000000000000000000000000002", "A", "c_4", first + 1),
        click("B-good", "B", "c_1", EPOCH - 7 * DAY_MS),
        click("B-bad", "B", "c_5", EPOCH - 7 * DAY_MS - 1),
    ])
    start, leap, march = ("2028-02-28", "2028-02-29", "2028-03-01")
    case.answers([
        ("A", start, 1, 2, 100, "A"), ("A", leap, 1, 2, 500, "A"),
        ("B", start, 1, 2, 0, None), ("B", march, 1, 1, 0, None),
    ], [
        ("c_1", "channel-1", start, 1, 0, 0, 2), ("c_2", "channel-2", start, 1, 1, 100, 1),
        ("c_3", "channel-3", start, 0, 0, 0, 1), ("c_2", "channel-2", leap, 0, 0, 0, 1),
        ("c_3", "channel-3", leap, 0, 0, 0, 1), ("c_4", "channel-4", leap, 1, 1, 500, 1),
        ("organic", "organic", march, 1, 0, 0, 0),
    ])


def aliases_and_long_lines(root):
    case = Case(root, "wide_aliases_and_long_lines")
    low = "u" + f"{1:032x}"
    high = "u" + f"{1 << 64:032x}"
    zero = "u" + "0" * 32
    small = ["u" + "0" * 31 + digit for digit in ("a", "A")]
    standalone = [low, low.upper(), *small, " u1", "u1 ", "用户甲"]
    people = ["0-unused-representative", zero, *[f"alias{i}" for i in range(512)], high, "共享"]
    leaves = [f"u{1000 + i}" for i in range(96)]
    edges = [f"{a},{b}" for a, b in zip(people, people[1:])]
    edges.extend(f"{who},共享" for who in leaves)
    edges.extend([f'{high},"共享"', f"{zero},{zero}", f'"{low},共享', f"{low},", ",共享"])
    for i in range(19):
        case.part("links", i, reversed(edges[i::19]))
    for i in range(30):
        case.part("events", i, [event(f"leaf-{j}", leaves[j], EPOCH) for j in range(i, len(leaves), 30)])
    stamp = iso(START)
    later = iso(START + timedelta(seconds=1))
    # The common 4 MiB prefix crosses both the read-buffer and largest chunk
    # boundaries. Its suffix alone distinguishes the two IDs.
    prefix = "事件" + "x" * (4 * 1024 * 1024 + 253)
    case.part("events", 30, [
        event(prefix + "坏", "ghost", EPOCH, kind="purchase", price="２５"),
        event(prefix + "好", high, EPOCH, kind="purchase", product="missing", price="42"),
    ], terminated=False)
    case.part("events", 31, [
        event(prefix + "坏", "ghost", EPOCH, ingest=later, kind="purchase", price="999"),
        event(prefix + "好", "ghost", EPOCH, ingest=later),
        *[event(f"other{i}", who, stamp) for i, who in enumerate(standalone)],
    ])
    case.part("clicks", 0, [
        click("k" + "0" * 31 + "a", zero, "c_1", EPOCH),
        click("k" + "0" * 31 + "A", high, "c_2", EPOCH),
        click("点击", "共享", "c_3", EPOCH),
    ])
    for i in range(1, 11):
        case.part("clicks", i)
    day = START.date()
    case.answers([
        (people[0], day, 1, len(leaves) + 1, 42, "unknown"),
        *[(who, day, 1, 1, 0, None) for who in standalone],
    ], [
        ("c_1", "channel-1", day, 0, 0, 0, 1), ("c_2", "channel-2", day, 1, 1, 42, 1),
        ("c_3", "channel-3", day, 0, 0, 0, 1),
        ("organic", "organic", day, len(standalone), 0, 0, 0),
    ])


def group_thresholds(root):
    products = [(f"p{i}", f"cat{i:03}") for i in range(260)]
    case = Case(root, "attribution_thresholds_and_wide_categories", products)
    case.part("links", 0)
    # Every scheduled hour starts its own session. Only the last session buys.
    # All clicks are at the first session start, tied across two campaigns.
    groups = [(1, 0), (2, 1), (16, 2), (63, 15), (64, 16), (64, 17), (65, 17)]
    user_days = defaultdict(lambda: [0, 0, 0, None])
    campaign_days = defaultdict(lambda: [0, 0, 0, 0])
    for g, (sessions, clicks) in enumerate(groups):
        who = f"purchaser-{g}"
        rows = []
        for hour in range(sessions):
            instant = START + timedelta(hours=hour)
            revenue = 10_000_000 if hour == sessions - 1 else 0
            rows.append(event(f"e{g}-{hour}", who, iso(instant), kind="purchase" if revenue else "view",
                              product="p259", price=str(revenue)))
            day = instant.date()
            daily = user_days[who, day]
            daily[0] += 1
            daily[1] += 1
            daily[2] += revenue
            if revenue:
                daily[3] = "cat259"
            credited = "c_1" if clicks else "organic"
            total = campaign_days[credited, day]
            total[0] += 1
            total[1] += bool(revenue)
            total[2] += revenue
            total[3] += bool(clicks)
            if clicks > 1:
                campaign_days["c_2", day][3] += 1
        case.part("events", g, reversed(rows))
        case.part("clicks", g, (
            click(f"k{g}-{i:02}", who, f"c_{i % 2 + 1}", EPOCH) for i in reversed(range(clicks))))
    case.answers([(who, day, *v) for (who, day), v in sorted(user_days.items())], [
        (cid, "organic" if cid == "organic" else f"channel-{cid[-1]}", day, *v)
        for (cid, day), v in sorted(campaign_days.items())
    ])


def create(root):
    for case in (milliseconds, aliases_and_long_lines, group_thresholds):
        case(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    create(parser.parse_args().root)
