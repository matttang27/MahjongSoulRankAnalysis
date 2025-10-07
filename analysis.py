#!/usr/bin/env python3
"""
Mahjong Soul Rank Analysis - analysis utilities

Reads the SQLite DB produced by main.py and computes simple summaries:
- summary: aggregate by player rank (1..4) with counts, averages of score/grade/level, and percentage share.
- export: export normalized player rows to CSV for external analysis.

Filters:
- --mode: filter by mode (9, 12, 16)
- --start-ms/--end-ms: filter by startTime window in ms

Example:
  python analysis.py summary --mode 12 --start-ms <ms> --end-ms <ms>
  python analysis.py export --csv out.csv --mode 12
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

DB_DEFAULT = "games.sqlite"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mahjong Soul analysis")
    sub = parser.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=DB_DEFAULT, help="Path to SQLite DB (default: games.sqlite)")
    common.add_argument("--mode", type=int, choices=[9, 12, 16], help="Filter by mode")
    common.add_argument("--start-ms", type=int, help="Filter by startTime >= this (ms)")
    common.add_argument("--end-ms", type=int, help="Filter by startTime <= this (ms)")

    p1 = sub.add_parser("summary", parents=[common], help="Show summary stats by rank")

    p2 = sub.add_parser("export", parents=[common], help="Export normalized rows to CSV")
    p2.add_argument("--csv", required=True, help="Output CSV file path")

    p3 = sub.add_parser("rank-correlation", parents=[common], help="Correlate player level with placement & scores")
    p3.add_argument("--level-bin", default="level", choices=["level", "level10"], help="Grouping: level=exact, level10=by 10s (e.g., 10300)")

    p4 = sub.add_parser("compare-levels", parents=[common], help="Head-to-head comparison between two levels (within the same match)")
    p4.add_argument("--level-a", type=int, required=True, help="Level (or bucket) for A")
    p4.add_argument("--level-b", type=int, required=True, help="Level (or bucket) for B")
    p4.add_argument("--level-bin", default="level", choices=["level", "level10"], help="Grouping for matching levels")
    p4.add_argument("--csv", help="Optional CSV to write per-instance pairs (A vs B)")

    # compare-all: iterate preset level ranges and write one summary row per matchup to CSV
    p5 = sub.add_parser("compare-all", parents=[common], help="Compare all level pairs in predefined ranges (for all levels in the ranges)")
    p5.add_argument("--csv", help="Optional CSV to write all pairs")
    p5.add_argument("--level-bin", default="level", choices=["level", "level10"], help="Grouping: level=exact, level10=by 10s (e.g., 10300)")

    return parser.parse_args(argv)


def where_clause(args: argparse.Namespace) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if args.mode is not None:
        clauses.append("mode = ?")
        params.append(args.mode)
    # startTime in DB is stored in seconds; incoming filters are ms
    if args.start_ms is not None:
        clauses.append("startTime >= ?")
        params.append(args.start_ms // 1000)
    if args.end_ms is not None:
        clauses.append("startTime <= ?")
        params.append(args.end_ms // 1000)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def iter_player_rows(conn: sqlite3.Connection, args: argparse.Namespace) -> Iterable[Tuple[int, int, int, int]]:
    """
    Yield normalized rows (rank, level, score, grade) for all four players.
    """
    where, params = where_clause(args)
    sql = (
        "SELECT player1_rank, player1_level, player1_score, player1_gradingScore, "
        "player2_rank, player2_level, player2_score, player2_gradingScore, "
        "player3_rank, player3_level, player3_score, player3_gradingScore, "
        "player4_rank, player4_level, player4_score, player4_gradingScore "
        "FROM games" + where
    )
    for row in conn.execute(sql, params):
        # row: 16 columns
        for i in range(4):
            rank = row[i * 4]
            level = row[i * 4 + 1]
            score = row[i * 4 + 2]
            grade = row[i * 4 + 3]
            yield int(rank), int(level), int(score), int(grade)


def bucket_level(level: int, mode: str) -> int:
    if mode == "level10":
        return (level // 10) * 10
    return level


def do_rank_correlation(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    from collections import defaultdict

    # Aggregates per level bucket
    counts = defaultdict(int)
    place1 = defaultdict(int)
    place2 = defaultdict(int)
    place3 = defaultdict(int)
    place4 = defaultdict(int)
    sum_score = defaultdict(int)
    sum_grade = defaultdict(int)
    sum_place = defaultdict(int)

    total = 0
    for rank, level, score, grade in iter_player_rows(conn, args):
        lb = bucket_level(level, args.level_bin)
        counts[lb] += 1
        if rank == 1:
            place1[lb] += 1
        elif rank == 2:
            place2[lb] += 1
        elif rank == 3:
            place3[lb] += 1
        elif rank == 4:
            place4[lb] += 1
        sum_score[lb] += score
        sum_grade[lb] += grade
        sum_place[lb] += rank
        total += 1

    if total == 0:
        print("No rows match the filter.")
        return

    # Header
    print("level,count,p1%,p2%,p3%,p4%,avg_place,avg_score,avg_grade")
    for lb in sorted(counts.keys()):
        c = counts[lb]
        p1 = place1[lb] / c * 100.0
        p2 = place2[lb] / c * 100.0
        p3 = place3[lb] / c * 100.0
        p4 = place4[lb] / c * 100.0
        avg_place = sum_place[lb] / c
        avg_score = sum_score[lb] / c
        avg_grade = sum_grade[lb] / c
        print(f"{lb},{c},{p1:.2f},{p2:.2f},{p3:.2f},{p4:.2f},{avg_place:.3f},{avg_score:.2f},{avg_grade:.2f}")


def do_summary(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    from collections import defaultdict

    counts = defaultdict(int)
    sum_score = defaultdict(int)
    sum_grade = defaultdict(int)
    sum_level = defaultdict(int)

    total = 0
    for rank, level, score, grade in iter_player_rows(conn, args):
        counts[rank] += 1
        sum_score[rank] += score
        sum_grade[rank] += grade
        sum_level[rank] += level
        total += 1

    if total == 0:
        print("No rows match the filter.")
        return

    print("rank,count,percent,avg_score,avg_grade,avg_level")
    for rank in sorted(counts.keys()):
        c = counts[rank]
        pct = (c / total) * 100.0
        avg_score = sum_score[rank] / c
        avg_grade = sum_grade[rank] / c
        avg_level = sum_level[rank] / c
        print(f"{rank},{c},{pct:.2f},{avg_score:.2f},{avg_grade:.2f},{avg_level:.2f}")


def do_export(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    where, params = where_clause(args)
    sql = (
        "SELECT id, mode, startTime, "
        "player1_level, player1_score, player1_gradingScore, player1_rank, "
        "player2_level, player2_score, player2_gradingScore, player2_rank, "
        "player3_level, player3_score, player3_gradingScore, player3_rank, "
        "player4_level, player4_score, player4_gradingScore, player4_rank "
        "FROM games" + where
    )

    # Normalize 4 players into 4 rows
    headers = ["id", "mode", "startTime", "player", "rank", "level", "score", "gradingScore"]
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for row in conn.execute(sql, params):
            id_, mode, start_time = row[0], row[1], row[2]
            # For each player, write a row
            for i in range(4):
                level = row[3 + i*4]
                score = row[4 + i*4]
                grade = row[5 + i*4]
                rank = row[6 + i*4]
                w.writerow([id_, mode, start_time, i+1, rank, level, score, grade])


def do_compare_levels(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    if args.level_a == args.level_b:
        print("Error: --level-a and --level-b must be different", file=sys.stderr)
        return

    where, params = where_clause(args)
    sql = (
        "SELECT id, startTime, "
        "player1_level, player1_score, player1_gradingScore, player1_rank, "
        "player2_level, player2_score, player2_gradingScore, player2_rank, "
        "player3_level, player3_score, player3_gradingScore, player3_rank, "
        "player4_level, player4_score, player4_gradingScore, player4_rank "
        "FROM games" + where
    )

    # Aggregates
    instances = 0
    sum_rank_diff = 0
    sum_score_diff = 0
    sum_grade_diff = 0
    a_better = 0
    b_better = 0
    # ties are impossible because ranks are unique (1..4)

    writer = None
    f = None
    if args.csv:
        headers = [
            "id", "startTime",
            "playerA", "levelA", "rankA", "scoreA", "gradeA",
            "playerB", "levelB", "rankB", "scoreB", "gradeB",
            "diff_rank", "diff_score", "diff_grade"
        ]
        f = open(args.csv, "w", newline="", encoding="utf-8")
        writer = csv.writer(f)
        writer.writerow(headers)

    try:
        for row in conn.execute(sql, params):
            gid = row[0]
            st = row[1]
            # Extract players into a list of dicts for convenience
            players = []
            for i in range(4):
                level = int(row[2 + i*4])
                score = int(row[3 + i*4])
                grade = int(row[4 + i*4])
                rank = int(row[5 + i*4])
                players.append({
                    "idx": i+1,
                    "level": level,
                    "bucket": bucket_level(level, args.level_bin),
                    "score": score,
                    "grade": grade,
                    "rank": rank,
                })

            groupA = [p for p in players if p["bucket"] == args.level_a]
            groupB = [p for p in players if p["bucket"] == args.level_b]
            if not groupA or not groupB:
                continue

            for pa in groupA:
                for pb in groupB:
                    diff_rank = pa["rank"] - pb["rank"]
                    diff_score = pa["score"] - pb["score"]
                    diff_grade = pa["grade"] - pb["grade"]
                    instances += 1
                    sum_rank_diff += diff_rank
                    sum_score_diff += diff_score
                    sum_grade_diff += diff_grade
                    if diff_rank < 0:
                        a_better += 1
                    elif diff_rank > 0:
                        b_better += 1
                    # no tie case since ranks are unique
                    if writer is not None:
                        writer.writerow([
                            gid, st,
                            pa["idx"], pa["level"], pa["rank"], pa["score"], pa["grade"],
                            pb["idx"], pb["level"], pb["rank"], pb["score"], pb["grade"],
                            diff_rank, diff_score, diff_grade
                        ])
    finally:
        if f is not None:
            f.close()

    if instances == 0:
        print("No head-to-head instances found for the provided levels and filters.")
        return

    avg_rank_diff = sum_rank_diff / instances
    avg_score_diff = sum_score_diff / instances
    avg_grade_diff = sum_grade_diff / instances

    # CSV-like summary line for easy copying
    print("level_a,level_b,instances,a_better,b_better,avg_rank_diff,avg_score_diff,avg_grade_diff")
    print(f"{args.level_a},{args.level_b},{instances},{a_better},{b_better},{avg_rank_diff:.4f},{avg_score_diff:.2f},{avg_grade_diff:.2f}")


def compute_compare_summary(conn: sqlite3.Connection, args: argparse.Namespace, level_a: int, level_b: int) -> Optional[Tuple[int, int, float, float, float, int]]:
    """Compute head-to-head summary for a given pair of buckets. Returns
    (instances, a_better, b_better, avg_rank_diff, avg_score_diff, avg_grade_diff)
    or None if no instances were found."""
    if level_a == level_b:
        return None

    where, params = where_clause(args)
    sql = (
        "SELECT id, startTime, "
        "player1_level, player1_score, player1_gradingScore, player1_rank, "
        "player2_level, player2_score, player2_gradingScore, player2_rank, "
        "player3_level, player3_score, player3_gradingScore, player3_rank, "
        "player4_level, player4_score, player4_gradingScore, player4_rank "
        "FROM games" + where
    )

    instances = 0
    sum_rank_diff = 0
    sum_score_diff = 0
    sum_grade_diff = 0
    a_better = 0
    b_better = 0
    # no ties tracked; ranks are unique

    for row in conn.execute(sql, params):
        players = []
        for i in range(4):
            level = int(row[2 + i*4])
            score = int(row[3 + i*4])
            grade = int(row[4 + i*4])
            rank = int(row[5 + i*4])
            players.append({
                "idx": i+1,
                "level": level,
                "bucket": bucket_level(level, args.level_bin),
                "score": score,
                "grade": grade,
                "rank": rank,
            })

        groupA = [p for p in players if p["bucket"] == level_a]
        groupB = [p for p in players if p["bucket"] == level_b]
        if not groupA or not groupB:
            continue

        for pa in groupA:
            for pb in groupB:
                diff_rank = pa["rank"] - pb["rank"]
                diff_score = pa["score"] - pb["score"]
                diff_grade = pa["grade"] - pb["grade"]
                instances += 1
                sum_rank_diff += diff_rank
                sum_score_diff += diff_score
                sum_grade_diff += diff_grade
                if diff_rank < 0:
                    a_better += 1
                elif diff_rank > 0:
                    b_better += 1
                # no tie branch since ranks are unique

    if instances == 0:
        return None

    avg_rank_diff = sum_rank_diff / instances
    avg_score_diff = sum_score_diff / instances
    avg_grade_diff = sum_grade_diff / instances
    return (instances, a_better, avg_rank_diff, avg_score_diff, avg_grade_diff, b_better)


def do_compare_all(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    """Write one CSV row per matchup across predefined level ranges per mode."""
    # Predefined ranges; use tuple() to concatenate ranges where needed
    level_configs: List[Tuple[Iterable[int], Iterable[int], int]] = [
        (range(10301, 10304), tuple(range(10301, 10304)) + tuple(range(10401, 10404)), 9),   # Gold vs Gold+Master
        (range(10401, 10404), tuple(range(10401, 10404)) + tuple(range(10501, 10504)), 12),                                      # Master vs Master+Saint
        (range(10501, 10504), tuple(range(10501, 10504)) + tuple([10701]), 16),                                      # Saint vs Saint+Celestial
        ([10701], range(10701, 10721), 16),                                      # Celestial vs Celestial
    ]

    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["level_a", "level_b", "mode", "instances", "a_better", "b_better", "avg_rank_diff", "avg_score_diff", "avg_grade_diff"])

        for level_range_a, level_range_b, mode in level_configs:
            print(level_range_a, level_range_b, mode)
            prev_mode = args.mode
            args.mode = mode
            try:
                for level_a in level_range_a:
                    for level_b in level_range_b:
                        if level_a >= level_b:
                            continue
                        summary = compute_compare_summary(conn, args, level_a, level_b)
                        if summary is None:
                            continue
                        instances, a_better, avg_rank_diff, avg_score_diff, avg_grade_diff, b_better = summary
                        w.writerow([level_a, level_b, mode, instances, a_better, b_better,
                                   f"{avg_rank_diff:.4f}", f"{avg_score_diff:.2f}", f"{avg_grade_diff:.2f}"])
            finally:
                args.mode = prev_mode


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        with sqlite3.connect(args.db) as conn:
            if args.cmd == "summary":
                do_summary(conn, args)
            elif args.cmd == "export":
                do_export(conn, args)
            elif args.cmd == "rank-correlation":
                do_rank_correlation(conn, args)
            elif args.cmd == "compare-levels":
                do_compare_levels(conn, args)
            elif args.cmd == "compare-all":
                do_compare_all(conn, args)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
