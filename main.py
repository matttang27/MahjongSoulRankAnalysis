#!/usr/bin/env python3
"""
Mahjong Soul Rank Analysis - data fetcher

Uses the Amae Koromo API to fetch game records for a given time range and mode,
then stores a simplified representation into a local SQLite database using the
schema described in README.md.

Endpoint pattern (four-player south example from README):
  https://5-data.amae-koromo.com/api/v2/pl4/games/{end_ms}/{start_ms}?limit=100000&descending=true&mode={mode}

Notes:
- The API path takes milliseconds since epoch, but the response JSON's startTime/endTime
  fields appear to be seconds since epoch. We store endTime as returned (seconds) in DB.
- Pagination: we request with `descending=true` and repeatedly move the end_ms backward to
  the oldest game's endTime (in ms) minus 1 until we pass the start_ms or the API returns empty.
- Players are recorded in order as player1..player4 with their level, score, gradingScore.

CLI usage:
  python main.py --mode 12 --start-ms 1659809600000 --end-ms 1759828200000 --db games.sqlite

If start/end are not provided, defaults to [now-30d, now].

Modes (from README):
- Gold South: 9
- Jade South: 12
- Throne South: 16
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

BASE_URL = "https://5-data.amae-koromo.com/api/v2/pl4/games/{end}/{start}?{query}"

# Desired column order for schema and inserts: group rank with each player
INSERT_COLUMNS: List[str] = [
    "id", "mode", "startTime",
    "player1_level", "player1_score", "player1_gradingScore", "player1_rank",
    "player2_level", "player2_score", "player2_gradingScore", "player2_rank",
    "player3_level", "player3_score", "player3_gradingScore", "player3_rank",
    "player4_level", "player4_score", "player4_gradingScore", "player4_rank",
]


def now_ms() -> int:
    return int(time.time() * 1000)


def default_window_ms(days: int = 30) -> Tuple[int, int]:
    end = now_ms()
    start = end - days * 24 * 60 * 60 * 1000
    return start, end


def ensure_schema(conn: sqlite3.Connection) -> None:
    def create_games_with_desired_order() -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                id TEXT PRIMARY KEY,
                mode INTEGER,
                startTime INTEGER,
                player1_level INTEGER,
                player1_score INTEGER,
                player1_gradingScore INTEGER,
                player1_rank INTEGER,
                player2_level INTEGER,
                player2_score INTEGER,
                player2_gradingScore INTEGER,
                player2_rank INTEGER,
                player3_level INTEGER,
                player3_score INTEGER,
                player3_gradingScore INTEGER,
                player3_rank INTEGER,
                player4_level INTEGER,
                player4_score INTEGER,
                player4_gradingScore INTEGER,
                player4_rank INTEGER
            );
            """
        )

    # Check if table exists
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='games'")
    row = cur.fetchone()
    if row is None:
        create_games_with_desired_order()
        conn.commit()
        return

    # Table exists: check columns and migrate if necessary
    cur = conn.execute("PRAGMA table_info(games)")
    cols_info = cur.fetchall()
    current_cols = [r[1] for r in cols_info]
    if current_cols == INSERT_COLUMNS:
        # Already in desired order
        return

    # Ensure all required columns exist or can be derived
    existing_set = set(current_cols)
    required_set = set(INSERT_COLUMNS)
    missing = required_set - existing_set

    # Create new table with desired order
    conn.execute("DROP TABLE IF EXISTS games_new")
    conn.execute("BEGIN")
    conn.execute(
        """
        CREATE TABLE games_new (
            id TEXT PRIMARY KEY,
            mode INTEGER,
            startTime INTEGER,
            player1_level INTEGER,
            player1_score INTEGER,
            player1_gradingScore INTEGER,
            player1_rank INTEGER,
            player2_level INTEGER,
            player2_score INTEGER,
            player2_gradingScore INTEGER,
            player2_rank INTEGER,
            player3_level INTEGER,
            player3_score INTEGER,
            player3_gradingScore INTEGER,
            player3_rank INTEGER,
            player4_level INTEGER,
            player4_score INTEGER,
            player4_gradingScore INTEGER,
            player4_rank INTEGER
        )
        """
    )

    # Helper: SQL expr for rank with original-order tie-break
    def rank_expr(player_idx: int) -> str:
        # player_idx: 1..4; fields playerN_score
        s = ["player1_score", "player2_score", "player3_score", "player4_score"]
        i = player_idx - 1
        # greater-than counts
        gt_terms = [f"(CASE WHEN {s[j]} > {s[i]} THEN 1 ELSE 0 END)" for j in range(4) if j != i]
        # equals from earlier players only
        eq_terms = [f"(CASE WHEN {s[j]} = {s[i]} THEN 1 ELSE 0 END)" for j in range(i)];
        sum_terms = " + ".join(gt_terms + eq_terms) if (gt_terms or eq_terms) else "0"
        return f"(1 + {sum_terms})"

    select_cols: List[str] = [
        "id", "mode", "startTime",
        "player1_level", "player1_score", "player1_gradingScore", ("player1_rank" if "player1_rank" in existing_set else rank_expr(1)),
        "player2_level", "player2_score", "player2_gradingScore", ("player2_rank" if "player2_rank" in existing_set else rank_expr(2)),
        "player3_level", "player3_score", "player3_gradingScore", ("player3_rank" if "player3_rank" in existing_set else rank_expr(3)),
        "player4_level", "player4_score", "player4_gradingScore", ("player4_rank" if "player4_rank" in existing_set else rank_expr(4)),
    ]

    conn.execute(
        "INSERT INTO games_new (" + ", ".join(INSERT_COLUMNS) + ") "
        "SELECT " + ", ".join(select_cols) + " FROM games"
    )
    conn.execute("DROP TABLE games")
    conn.execute("ALTER TABLE games_new RENAME TO games")
    conn.commit()


def build_url(end_ms: int, start_ms: int, mode: int, limit: int = 100000, descending: bool = True) -> str:
    params = {
        "limit": str(limit),
        "descending": "true" if descending else "false",
        "mode": str(mode),
    }
    query = urllib.parse.urlencode(params)
    return BASE_URL.format(end=end_ms, start=start_ms, query=query)


def http_get_json(url: str, timeout: int = 30) -> Any:
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; MahjongSoulRankAnalysis/1.0)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        return json.loads(data)


def to_ms(x: int) -> int:
    # Heuristic: treat values >= 10^12 as milliseconds, otherwise seconds -> ms
    return x if x >= 10**12 else x * 1000


def to_seconds(x: int) -> int:
    # Heuristic: treat values >= 10^12 as milliseconds, convert to seconds
    return x // 1000 if x >= 10**12 else x


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Mahjong Soul games into SQLite (Amae Koromo API)")
    parser.add_argument("--mode", type=int, choices=[9, 12, 16], required=False, default=12,
                        help="Game mode: 9=Gold South, 12=Jade South, 16=Throne South (default: 12)")
    parser.add_argument("--start-ms", type=int, required=False, help="Start time in ms since epoch (default: now-30d)")
    parser.add_argument("--end-ms", type=int, required=False, help="End time in ms since epoch (default: now)")
    parser.add_argument("--db", type=str, required=False, default="games.sqlite", help="SQLite database file path")
    parser.add_argument("--limit", type=int, required=False, default=100000, help="API page limit (default 100000)")
    parser.add_argument("--sleep", type=float, required=False, default=0.5, help="Sleep between pages in seconds")
    parser.add_argument("--max-pages", type=int, required=False, default=0,
                        help="Optional cap on number of pages to fetch (0 = no cap)")
    parser.add_argument("--recent", type=int, required=False, default=0,
                        help="Fetch the N most recent games (ignores --start-ms). 0 = disabled.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    return parser.parse_args(argv)


def extract_row(game: Dict[str, Any], mode_override: Optional[int] = None) -> Optional[Tuple[Any, ...]]:
    try:
        gid = game.get("_id") or game.get("uuid")
        if not gid:
            return None
        mode = int(game.get("modeId")) if game.get("modeId") is not None else (mode_override if mode_override is not None else None)
        if mode is None:
            return None
        # Use startTime from API (seconds). Fall back to endTime if missing.
        start_time_sec = int(game.get("startTime") if game.get("startTime") is not None else game["endTime"])  # seconds
        players = game.get("players") or []
        if len(players) != 4:
            return None
        # Extract metrics in order
        levels: List[int] = []
        scores: List[int] = []
        grades: List[int] = []
        for i in range(4):
            p = players[i]
            levels.append(int(p.get("level", 0)))
            scores.append(int(p.get("score", 0)))
            grades.append(int(p.get("gradingScore", 0)))

        # Compute placement ranks 1..4 by score desc.
        # Tie-break ONLY by original order (earlier index = higher rank).
        # This intentionally ignores gradingScore and accountId for tiebreaks.
        tie_keys: List[Tuple[int, int]] = []
        for i in range(4):
            tie_keys.append((-scores[i], i))
        order = sorted(range(4), key=lambda i: tie_keys[i])
        ranks = [0, 0, 0, 0]
        for pos, idx in enumerate(order):
            ranks[idx] = pos + 1

        vals: List[int] = [
            levels[0], scores[0], grades[0], ranks[0],
            levels[1], scores[1], grades[1], ranks[1],
            levels[2], scores[2], grades[2], ranks[2],
            levels[3], scores[3], grades[3], ranks[3],
        ]
        return (
            gid,
            mode,
            start_time_sec,
            *vals,
        )
    except Exception:
        return None


def insert_games(
    conn: sqlite3.Connection,
    games: List[Dict[str, Any]],
    mode_override: Optional[int] = None,
    verbose: bool = False,
) -> Tuple[int, int]:
    placeholders = ",".join(["?"] * len(INSERT_COLUMNS))
    sql = (
        "INSERT OR IGNORE INTO games (" + ", ".join(INSERT_COLUMNS) + ") "
        f"VALUES ({placeholders})"
    )
    ok = 0
    skipped = 0
    cur = conn.cursor()
    for idx, g in enumerate(games, start=1):
        row = extract_row(g, mode_override=mode_override)
        if verbose and row is not None:
            try:
                players = g.get("players")
                print(f"  [rec {idx}] players={len(players) if isinstance(players, list) else 'non-list'} row_len={len(row)} id={g.get('_id') or g.get('uuid')}")
            except Exception:
                pass
        if row is None:
            if verbose:
                # Try to identify reason
                gid = g.get("_id") or g.get("uuid")
                players = g.get("players")
                reason = []
                if not gid:
                    reason.append("no id")
                if not isinstance(players, list) or len(players) != 4:
                    reason.append(f"players count != 4 (got {len(players) if isinstance(players, list) else 'non-list'})")
                mode_val = g.get("modeId")
                if mode_val is None and mode_override is None:
                    reason.append("no modeId and no override")
                print(f"  [skip rec {idx}] invalid row: {', '.join(reason) if reason else 'unknown'}")
            skipped += 1
            continue
        try:
            cur.execute(sql, row)
            if cur.rowcount > 0:
                ok += 1
            else:
                if verbose:
                    gid = g.get("_id") or g.get("uuid")
                    print(f"  [skip rec {idx}] duplicate (id={gid})")
                skipped += 1
        except sqlite3.IntegrityError:
            # Duplicate primary key
            if verbose:
                gid = g.get("_id") or g.get("uuid")
                print(f"  [skip rec {idx}] IntegrityError duplicate (id={gid})")
            skipped += 1
        except Exception as e:
            if verbose:
                gid = g.get("_id") or g.get("uuid")
                print(f"  [skip rec {idx}] exception during insert (id={gid}): {e}")
            skipped += 1
    conn.commit()
    return ok, skipped


def insert_games_capped(
    conn: sqlite3.Connection,
    games: List[Dict[str, Any]],
    mode_override: Optional[int],
    remaining: int,
    seen_ids: Optional[set[str]] = None,
    verbose: bool = False,
) -> Tuple[int, int, bool, int]:
    """
    Insert games in order until `remaining` inserts succeed (or page ends).
    Returns: (inserted, skipped, reached_cap, processed)
    - inserted: count successfully inserted (new rows)
    - skipped: count skipped (invalid rows or duplicates)
    - reached_cap: True if remaining reached 0 during this page
    - processed: number of items from `games` list we consumed
    """
    placeholders = ",".join(["?"] * len(INSERT_COLUMNS))
    sql = (
        "INSERT OR IGNORE INTO games (" + ", ".join(INSERT_COLUMNS) + ") "
        f"VALUES ({placeholders})"
    )
    inserted = 0
    skipped = 0
    processed = 0
    cap_reached = False
    cur = conn.cursor()
    local_seen = seen_ids if seen_ids is not None else set()

    for idx, g in enumerate(games, start=1):
        processed += 1
        gid = g.get("_id") or g.get("uuid")
        if gid is not None and gid in local_seen:
            if verbose:
                print(f"  [skip rec {idx}] already seen in this run (id={gid})")
            skipped += 1
            continue
        row = extract_row(g, mode_override=mode_override)
        if verbose and row is not None:
            try:
                players = g.get("players")
                print(f"  [rec {idx}] players={len(players) if isinstance(players, list) else 'non-list'} row_len={len(row)} id={gid}")
            except Exception:
                pass
        if row is None:
            if verbose:
                players = g.get("players")
                reason = []
                if not gid:
                    reason.append("no id")
                if not isinstance(players, list) or len(players) != 4:
                    reason.append(f"players count != 4 (got {len(players) if isinstance(players, list) else 'non-list'})")
                mode_val = g.get("modeId")
                if mode_val is None and mode_override is None:
                    reason.append("no modeId and no override")
                print(f"  [skip rec {idx}] invalid row: {', '.join(reason) if reason else 'unknown'}")
            skipped += 1
            continue
        try:
            cur.execute(sql, row)
            if gid is not None:
                local_seen.add(gid)
            # Count only successful insertions (new rows). If OR IGNORE ignored due to dup,
            # the rowcount may be 0; treat as skipped.
            if cur.rowcount > 0:
                inserted += 1
                remaining -= 1
                if remaining <= 0:
                    cap_reached = True
                    break
            else:
                if verbose:
                    print(f"  [skip rec {idx}] duplicate (id={gid})")
                skipped += 1
        except sqlite3.IntegrityError:
            if verbose:
                print(f"  [skip rec {idx}] IntegrityError duplicate (id={gid})")
            skipped += 1
        except Exception as e:
            if verbose:
                print(f"  [skip rec {idx}] exception during insert (id={gid}): {e}")
            skipped += 1

    conn.commit()
    return inserted, skipped, cap_reached, processed


def fetch_and_store(
    db_path: str,
    mode: int,
    start_ms: int,
    end_ms: int,
    limit: int,
    sleep_s: float,
    max_pages: int = 0,
    recent_count: int = 0,
    verbose: bool = False,
) -> None:
    with sqlite3.connect(db_path) as conn:
        ensure_schema(conn)
        pages = 0
        total_inserted = 0
        current_end = end_ms
        seen_ids: set[str] = set()

        while current_end >= start_ms:
            # Adjust per-page limit if we're in recent mode
            page_limit = limit
            if recent_count and recent_count > 0:
                remaining = max(0, recent_count - total_inserted)
                if remaining <= 0:
                    print("Target reached; stopping.")
                    break
                page_limit = min(limit, remaining)

            url = build_url(end_ms=current_end, start_ms=start_ms, mode=mode, limit=page_limit, descending=True)
            if verbose:
                print(f"Fetching: {url}")
            try:
                data = http_get_json(url)
            except Exception as e:
                print(f"Request failed: {e}", file=sys.stderr)
                # Backoff and retry once quickly; if fails again, break this page
                time.sleep(min(2.0, sleep_s))
                try:
                    data = http_get_json(url)
                except Exception as e2:
                    print(f"Retry failed: {e2}", file=sys.stderr)
                    break

            if not isinstance(data, list) or len(data) == 0:
                print("No more data returned; stopping.")
                break

            if recent_count and recent_count > 0:
                remaining = max(0, recent_count - total_inserted)
                inserted, skipped, cap, processed = insert_games_capped(
                    conn, data, mode_override=mode, remaining=remaining, seen_ids=seen_ids, verbose=verbose
                )
                total_inserted += inserted
                pages += 1
                if verbose:
                    print(
                        f"Page {pages}: received {len(data)} records -> inserted {inserted}, skipped {skipped}. Total inserted: {total_inserted}"
                    )
                if cap:
                    print("Reached requested recent count; stopping.")
                    break
            else:
                inserted, skipped = insert_games(conn, data, mode_override=mode, verbose=verbose)
                total_inserted += inserted
                pages += 1
                if verbose:
                    print(
                        f"Page {pages}: received {len(data)} records -> inserted {inserted}, skipped {skipped}. Total inserted: {total_inserted}"
                    )

            # Determine next end_ms from oldest startTime in this page, convert to ms and move back.
            try:
                oldest_start_sec = min(
                    int((g.get("startTime") if g.get("startTime") is not None else g.get("endTime", 0)))
                    for g in data if isinstance(g, dict)
                )
            except ValueError:
                # If something odd, break out
                print("Could not determine oldest startTime; stopping.")
                break
            next_end_ms = to_ms(oldest_start_sec) - 1
            if verbose:
                print(
                    f"Boundary: oldest_start_sec={oldest_start_sec}, current_end_ms={current_end} -> proposed next_end_ms={next_end_ms}"
                )

            # Progress guard: if boundary didn't move to an earlier second, force a 1s step back
            current_end_sec = to_seconds(current_end)
            proposed_end_sec = to_seconds(next_end_ms)
            if proposed_end_sec >= current_end_sec:
                fallback_next = current_end_sec * 1000 - 1000
                if fallback_next >= current_end:
                    fallback_next = current_end - 1000  # ensure movement
                if verbose:
                    print(
                        f"No boundary progress (proposed {proposed_end_sec} >= current {current_end_sec}). Fallback next_end_ms={fallback_next}"
                    )
                next_end_ms = fallback_next

            if next_end_ms < start_ms:
                print("Reached start boundary; stopping.")
                break
            current_end = next_end_ms

            if max_pages and pages >= max_pages:
                print("Max pages reached; stopping.")
                break

            if sleep_s > 0:
                time.sleep(sleep_s)

        print(f"Done. Pages: {pages}, total inserted: {total_inserted}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    # Determine time window depending on recent mode
    if args.recent and args.recent > 0:
        # Recent mode: ignore start-ms, default end-ms to now if missing
        start_ms = 0
        end_ms = args.end_ms if args.end_ms is not None else now_ms()
        if args.end_ms is None and args.verbose:
            print(f"Recent mode: using end_ms={end_ms}, start_ms=0 (ignored)")
    else:
        if args.start_ms is None or args.end_ms is None:
            # Default to last 30 days if not provided
            start_default, end_default = default_window_ms(30)
            start_ms = args.start_ms if args.start_ms is not None else start_default
            end_ms = args.end_ms if args.end_ms is not None else end_default
            print(f"Using default window (30d) for missing values -> start_ms={start_ms}, end_ms={end_ms}")
        else:
            start_ms = args.start_ms
            end_ms = args.end_ms

    if end_ms < start_ms:
        print("Error: end-ms must be >= start-ms", file=sys.stderr)
        return 2

    try:
        fetch_and_store(
            db_path=args.db,
            mode=args.mode,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=args.limit,
            sleep_s=args.sleep,
            max_pages=args.max_pages,
            recent_count=args.recent,
            verbose=args.verbose,
        )
        return 0
    except KeyboardInterrupt:
        print("Interrupted by user.")
        return 130
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
