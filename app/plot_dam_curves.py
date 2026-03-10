#!/usr/bin/env python3
"""
Plot DAM matching curves (supply/demand bid stacks) from da_bid data.

Usage:
    python3 plot_dam_curves.py YYYY-MM-DD [PERIOD]

Examples:
    python3 plot_dam_curves.py 2026-01-07          # period 1
    python3 plot_dam_curves.py 2026-01-07 1        # period 1
    python3 plot_dam_curves.py 2026-01-07 48       # period 48 (12:00)
"""

import sys
from decimal import Decimal
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import psycopg2
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT


def fetch_bids(conn, delivery_date, period):
    """Fetch all bids for a given date and period."""
    cur = conn.cursor()
    cur.execute("""
        SELECT side, price, volume_bid, volume_matched
        FROM da_bid
        WHERE delivery_date = %s AND period = %s
        ORDER BY side, price
    """, (delivery_date, period))
    rows = cur.fetchall()
    cur.close()
    return rows


def build_curve(bids, side):
    """
    Build cumulative volume curve for one side.

    Supply (sell): sorted by price ascending — cheapest first.
    Demand (buy): sorted by price descending — most willing buyer first.

    Returns two lists of (x, y) points for matched and unmatched segments.
    Each bid produces two points: (start_vol, price), (end_vol, price).
    """
    side_bids = [(float(b[1]), float(b[2]), float(b[3])) for b in bids if b[0] == side]

    if side == 'sell':
        side_bids.sort(key=lambda b: b[0])   # price ascending
    else:
        side_bids.sort(key=lambda b: -b[0])  # price descending

    matched_x, matched_y = [], []
    unmatched_x, unmatched_y = [], []

    cum_vol = 0.0

    for price, vol_bid, vol_matched in side_bids:
        if vol_matched > 0:
            matched_x.extend([cum_vol, cum_vol + vol_matched])
            matched_y.extend([price, price])
            cum_vol += vol_matched
        else:
            unmatched_x.extend([cum_vol, cum_vol + vol_bid])
            unmatched_y.extend([price, price])
            cum_vol += vol_bid

    return matched_x, matched_y, unmatched_x, unmatched_y


def plot_curves(delivery_date, period, bids, output_path):
    """Generate the matching curve plot."""
    fig, ax = plt.subplots(figsize=(14, 8))

    # Build curves
    sm_x, sm_y, su_x, su_y = build_curve(bids, 'sell')
    dm_x, dm_y, du_x, du_y = build_curve(bids, 'buy')

    # Plot matched curves (line plot — pairs of points form horizontal segments)
    if sm_x and sm_y:
        ax.plot(sm_x, sm_y, color='#E74C3C', linewidth=1.8,
                label='Supply matched energy', zorder=3)
    if dm_x and dm_y:
        ax.plot(dm_x, dm_y, color='#2C3E80', linewidth=1.8,
                label='Demand matched energy', zorder=3)

    # Plot unmatched curves
    if su_x and su_y:
        ax.plot(su_x, su_y, color='#D4A017', linewidth=1.8,
                label='Supply energy', zorder=2)
    if du_x and du_y:
        ax.plot(du_x, du_y, color='#1ABC9C', linewidth=1.8,
                label='Demand energy', zorder=2)

    # Format
    hour = (period - 1) // 4
    quarter = (period - 1) % 4
    start_min = quarter * 15
    end_min = start_min + 15
    time_str = f"{hour:02d}:{start_min:02d}-{hour:02d}:{end_min:02d}"

    ax.set_title(f"DAM Matching Curves — {delivery_date}, period {period} ({time_str})",
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Volume (MW)', fontsize=12)
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12)

    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x:,.0f}'))

    ax.legend(loc='upper right', fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3)
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 plot_dam_curves.py YYYY-MM-DD [PERIOD]")
        sys.exit(1)

    delivery_date = sys.argv[1]
    period = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, port=DB_PORT
    )

    bids = fetch_bids(conn, delivery_date, period)
    conn.close()

    if not bids:
        print(f"No bids found for {delivery_date} period {period}")
        sys.exit(1)

    print(f"Fetched {len(bids)} bids for {delivery_date} period {period}")

    output_path = Path(f'/app/downloads/dam_curve_{delivery_date}_p{period}.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_curves(delivery_date, period, bids, output_path)


if __name__ == '__main__':
    main()
