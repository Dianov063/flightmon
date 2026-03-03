#!/usr/bin/env python3
"""
Flight History Monitor v3 - OpenSky Network API
"""

import os
import requests
import sqlite3
import time
import schedule
import argparse
from datetime import datetime, timezone

DB_PATH = os.environ.get('DB_PATH', '/data/flights.db' if os.path.isdir('/data') else 'flights.db')
CHECK_INTERVAL_MINUTES = 10
OPENSKY_USER = os.environ.get('OPENSKY_USER', '')
OPENSKY_PASS = os.environ.get('OPENSKY_PASS', '')

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS aircraft (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign TEXT UNIQUE NOT NULL,
            label TEXT,
            registration TEXT,
            active INTEGER DEFAULT 1,
            added_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign TEXT NOT NULL,
            flight_id TEXT UNIQUE,
            registration TEXT,
            aircraft_type TEXT,
            from_iata TEXT,
            from_name TEXT,
            from_city TEXT,
            to_iata TEXT,
            to_name TEXT,
            to_city TEXT,
            departure_utc TEXT,
            arrival_utc TEXT,
            duration_min INTEGER,
            status TEXT,
            owner TEXT,
            saved_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS poll_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign TEXT,
            status TEXT,
            new_flights INTEGER DEFAULT 0,
            polled_at TEXT DEFAULT (datetime('now'))
        )
    """)
    existing = conn.execute("SELECT COUNT(*) FROM aircraft").fetchone()[0]
    if existing == 0:
        conn.execute(
            "INSERT OR IGNORE INTO aircraft (callsign, label, registration) VALUES (?,?,?)",
            ("WSA9677", "Solaris Aero", "RA-67677")
        )
    conn.commit()
    conn.close()

def add_aircraft(callsign: str, label: str = "", registration: str = ""):
    callsign = callsign.strip().upper()
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO aircraft (callsign, label, registration) VALUES (?,?,?)",
            (callsign, label, registration)
        )
        conn.execute("UPDATE aircraft SET active=1, label=?, registration=? WHERE callsign=?",
                     (label, registration, callsign))
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def remove_aircraft(callsign: str):
    callsign = callsign.strip().upper()
    conn = get_conn()
    conn.execute("UPDATE aircraft SET active=0 WHERE callsign=?", (callsign,))
    conn.commit()
    conn.close()

def list_aircraft():
    conn = get_conn()
    rows = conn.execute("""
        SELECT a.callsign, a.label, a.registration, a.active,
               COUNT(f.id) as flight_count,
               MAX(f.departure_utc) as last_seen
        FROM aircraft a
        LEFT JOIN flights f ON f.callsign = a.callsign
        GROUP BY a.callsign
        ORDER BY a.active DESC, a.added_at
    """).fetchall()
    conn.close()
    return rows

def ts_to_utc(ts):
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def fetch_opensky(callsign: str):
    callsign_padded = callsign.ljust(8)
    now = int(time.time())
    begin = now - 7 * 24 * 3600

    url = "https://opensky-network.org/api/flights/callsign"
    params = {"callsign": callsign_padded, "begin": begin, "end": now}
    auth = (OPENSKY_USER, OPENSKY_PASS) if OPENSKY_USER and OPENSKY_PASS else None

    try:
        r = requests.get(url, params=params, auth=auth, timeout=30)
        if r.status_code == 404:
            return []
        if r.status_code == 429:
            print(f"Rate limit OpenSky")
            return None
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP {e.response.status_code}")
        return None
    except requests.RequestException as e:
        print(f"Network error: {e}")
        return None

def parse_opensky_flight(item: dict, callsign: str) -> dict:
    dep_utc = ts_to_utc(item.get("firstSeen"))
    arr_utc = ts_to_utc(item.get("lastSeen"))

    duration = None
    if item.get("firstSeen") and item.get("lastSeen"):
        try:
            duration = int((int(item["lastSeen"]) - int(item["firstSeen"])) / 60)
        except Exception:
            pass

    from_icao = item.get("estDepartureAirport") or ""
    to_icao   = item.get("estArrivalAirport")   or ""
    flight_id = f"{item.get('icao24','')}-{item.get('firstSeen','')}"

    if arr_utc:
        status = "landed"
    elif dep_utc:
        status = "active"
    else:
        status = "unknown"

    return {
        "flight_id":    flight_id,
        "callsign":     callsign,
        "registration": item.get("icao24", ""),
        "aircraft_type": "",
        "from_iata":    from_icao,
        "from_name":    from_icao,
        "from_city":    "",
        "to_iata":      to_icao,
        "to_name":      to_icao,
        "to_city":      "",
        "departure_utc": dep_utc,
        "arrival_utc":   arr_utc,
        "duration_min":  duration,
        "status":        status,
        "owner":         "",
    }

def save_flights(flights_list: list, callsign: str) -> int:
    conn = get_conn()
    new_count = 0
    for item in flights_list:
        f = parse_opensky_flight(item, callsign)
        if not f["flight_id"]:
            continue
        conn.execute("""
            INSERT OR IGNORE INTO flights
            (callsign, flight_id, registration, aircraft_type,
             from_iata, from_name, from_city,
             to_iata, to_name, to_city,
             departure_utc, arrival_utc, duration_min, status, owner)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            f["callsign"], f["flight_id"], f["registration"], f["aircraft_type"],
            f["from_iata"], f["from_name"], f["from_city"],
            f["to_iata"], f["to_name"], f["to_city"],
            f["departure_utc"], f["arrival_utc"], f["duration_min"],
            f["status"], f["owner"]
        ))
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            new_count += 1
    conn.commit()
    conn.close()
    return new_count

def poll_aircraft(callsign: str):
    print(f"Polling {callsign} via OpenSky...")
    data = fetch_opensky(callsign)
    conn = get_conn()
    if data is None:
        conn.execute("INSERT INTO poll_log (callsign, status) VALUES (?,?)", (callsign, "error"))
        conn.commit(); conn.close()
        return
    new = save_flights(data, callsign)
    conn.execute("INSERT INTO poll_log (callsign, status, new_flights) VALUES (?,?,?)",
                 (callsign, "ok", new))
    conn.commit(); conn.close()
    print(f"Done: {len(data)} records, {new} new")

def monitor_all():
    conn = get_conn()
    callsigns = [r[0] for r in conn.execute(
        "SELECT callsign FROM aircraft WHERE active=1").fetchall()]
    conn.close()
    for cs in callsigns:
        poll_aircraft(cs)

def show_history(callsign: str = None, limit: int = 20):
    conn = get_conn()
    if callsign:
        rows = conn.execute("""
            SELECT callsign, departure_utc, from_iata, from_city,
                   to_iata, to_city, duration_min, status, aircraft_type
            FROM flights WHERE callsign=?
            ORDER BY departure_utc DESC LIMIT ?
        """, (callsign.upper(), limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT callsign, departure_utc, from_iata, from_city,
                   to_iata, to_city, duration_min, status, aircraft_type
            FROM flights ORDER BY departure_utc DESC LIMIT ?
        """, (limit,)).fetchall()
    conn.close()
    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--add",      metavar="CALLSIGN")
    parser.add_argument("--remove",   metavar="CALLSIGN")
    parser.add_argument("--label",    default="")
    parser.add_argument("--reg",      default="")
    parser.add_argument("--list",     action="store_true")
    parser.add_argument("--history",  nargs="?", const="", metavar="CALLSIGN")
    parser.add_argument("--fetch",    metavar="CALLSIGN")
    parser.add_argument("--interval", type=int, default=CHECK_INTERVAL_MINUTES)
    args = parser.parse_args()

    init_db()

    if args.add:
        add_aircraft(args.add, args.label, args.reg); return
    if args.remove:
        remove_aircraft(args.remove); return
    if args.list:
        list_aircraft(); return
    if args.history is not None:
        show_history(args.history if args.history else None); return
    if args.fetch:
        poll_aircraft(args.fetch.upper()); return

    monitor_all()
    schedule.every(args.interval).minutes.do(monitor_all)
    try:
        while True:
            schedule.run_pending()
            time.sleep(15)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
