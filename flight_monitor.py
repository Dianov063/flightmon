#!/usr/bin/env python3
"""
Flight History Monitor v2
- Правильный парсер (проверен на реальных данных FR24)
- Поддержка нескольких самолётов
- Управление через аргументы командной строки
"""

import os
import requests
import sqlite3
import json
import time
import schedule
import argparse
from datetime import datetime

# ─── Настройки ────────────────────────────────────────────────────────────────
DB_PATH = os.environ.get('DB_PATH', '/data/flights.db' if os.path.isdir('/data') else 'flights.db')
CHECK_INTERVAL_MINUTES = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.flightradar24.com/",
    "Origin": "https://www.flightradar24.com",
}

# ─── База данных ───────────────────────────────────────────────────────────────
def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS aircraft (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign     TEXT UNIQUE NOT NULL,
            label        TEXT,
            registration TEXT,
            active       INTEGER DEFAULT 1,
            added_at     TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign      TEXT NOT NULL,
            flight_id     TEXT UNIQUE,
            registration  TEXT,
            aircraft_type TEXT,
            from_iata     TEXT,
            from_name     TEXT,
            from_city     TEXT,
            to_iata       TEXT,
            to_name       TEXT,
            to_city       TEXT,
            departure_utc TEXT,
            arrival_utc   TEXT,
            duration_min  INTEGER,
            status        TEXT,
            owner         TEXT,
            saved_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS poll_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign    TEXT,
            status      TEXT,
            new_flights INTEGER DEFAULT 0,
            polled_at   TEXT DEFAULT (datetime('now'))
        )
    """)

    # Добавляем WSA9677 по умолчанию
    existing = conn.execute("SELECT COUNT(*) FROM aircraft").fetchone()[0]
    if existing == 0:
        conn.execute(
            "INSERT OR IGNORE INTO aircraft (callsign, label, registration) VALUES (?,?,?)",
            ("WSA9677", "Solaris Aero", "RA-67677")
        )
        print("✅ Добавлен по умолчанию: WSA9677 (Solaris Aero, RA-67677)")

    conn.commit()
    conn.close()

# ─── Управление самолётами ─────────────────────────────────────────────────────
def add_aircraft(callsign: str, label: str = "", registration: str = ""):
    callsign = callsign.strip().upper()
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO aircraft (callsign, label, registration) VALUES (?,?,?)",
            (callsign, label, registration)
        )
        # Реактивировать если был на паузе
        conn.execute("UPDATE aircraft SET active=1, label=?, registration=? WHERE callsign=?",
                     (label, registration, callsign))
        conn.commit()
        print(f"✅ Добавлен: {callsign}" + (f" ({label})" if label else "") +
              (f" [{registration}]" if registration else ""))
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        conn.close()

def remove_aircraft(callsign: str):
    callsign = callsign.strip().upper()
    conn = get_conn()
    conn.execute("UPDATE aircraft SET active=0 WHERE callsign=?", (callsign,))
    conn.commit()
    conn.close()
    print(f"⏸️  Мониторинг {callsign} приостановлен (история сохранена в БД)")

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

    print(f"\n{'═'*72}")
    print(f"  СПИСОК САМОЛЁТОВ")
    print(f"{'═'*72}")
    print(f"  {'':3} {'ПОЗЫВНОЙ':<12} {'НАЗВАНИЕ':<20} {'БОРТ':<12} {'РЕЙСОВ':<8} ПОСЛЕДНИЙ ВЫЛЕТ")
    print(f"  {'─'*68}")
    for row in rows:
        cs, label, reg, active, count, last = row
        icon = "🟢" if active else "⏸️ "
        print(f"  {icon} {cs:<12} {str(label or '—'):<20} {str(reg or '—'):<12} {count:<8} {str(last or 'нет данных')}")
    print(f"{'═'*72}\n")

# ─── Запрос к FlightRadar24 ────────────────────────────────────────────────────
def fetch_fr24(callsign: str) -> dict | None:
    url = "https://api.flightradar24.com/common/v1/flight/list.json"
    params = {"fetchBy": "flight", "page": 1, "limit": 25, "query": callsign}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        print(f"  ⚠️  HTTP {e.response.status_code}")
        return None
    except requests.RequestException as e:
        print(f"  ❌ Сетевая ошибка: {e}")
        return None

# ─── Парсинг (проверен на реальных данных FR24) ────────────────────────────────
def ts_to_utc(ts) -> str | None:
    if not ts:
        return None
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def parse_flight(item: dict, callsign: str) -> dict:
    ident   = item.get("identification", {})
    status  = item.get("status", {})
    ac      = item.get("aircraft", {})
    owner   = item.get("owner") or item.get("airline") or {}
    airport = item.get("airport", {})
    times   = item.get("time", {})

    origin = airport.get("origin") or {}
    dest   = airport.get("destination") or {}

    # Приоритет: реальное время > расписание
    real      = times.get("real") or {}
    scheduled = times.get("scheduled") or {}
    other     = times.get("other") or {}

    dep_ts = real.get("departure") or scheduled.get("departure")
    arr_ts = real.get("arrival")   or scheduled.get("arrival") or other.get("eta")

    duration = None
    if dep_ts and arr_ts:
        try:
            duration = int((int(arr_ts) - int(dep_ts)) / 60)
        except Exception:
            pass

    status_text = ((status.get("generic") or {}).get("status") or {}).get("text", "unknown")

    return {
        "flight_id":     ident.get("id", ""),
        "callsign":      callsign,
        "registration":  ac.get("registration", ""),
        "aircraft_type": (ac.get("model") or {}).get("code", ""),
        "from_iata":     ((origin.get("code") or {}).get("iata") or ""),
        "from_name":     origin.get("name", ""),
        "from_city":     ((origin.get("position") or {}).get("region") or {}).get("city", ""),
        "to_iata":       ((dest.get("code") or {}).get("iata") or ""),
        "to_name":       dest.get("name", ""),
        "to_city":       ((dest.get("position") or {}).get("region") or {}).get("city", ""),
        "departure_utc": ts_to_utc(dep_ts),
        "arrival_utc":   ts_to_utc(arr_ts),
        "duration_min":  duration,
        "status":        status_text,
        "owner":         owner.get("name", ""),
    }

def save_flights(flights_list: list, callsign: str) -> int:
    conn = get_conn()
    new_count = 0
    for item in flights_list:
        f = parse_flight(item, callsign)
        if not f["flight_id"]:
            continue
        conn.execute("""
            INSERT OR IGNORE INTO flights
            (callsign, flight_id, registration, aircraft_type,
             from_iata, from_name, from_city,
             to_iata,   to_name,   to_city,
             departure_utc, arrival_utc, duration_min, status, owner)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            f["callsign"], f["flight_id"], f["registration"], f["aircraft_type"],
            f["from_iata"], f["from_name"], f["from_city"],
            f["to_iata"],   f["to_name"],   f["to_city"],
            f["departure_utc"], f["arrival_utc"], f["duration_min"],
            f["status"], f["owner"]
        ))
        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            new_count += 1
            fi = f['from_iata'] or '?'
            ti = f['to_iata']   or '?'
            fc = f"({f['from_city']})" if f['from_city'] else ""
            tc = f"({f['to_city']})"   if f['to_city']   else ""
            dep = f['departure_utc'] or '?'
            print(f"    ✈  {dep}  {fi}{fc:12} → {ti}{tc:12}  [{f['status']}]")
    conn.commit()
    conn.close()
    return new_count

# ─── Опрос ────────────────────────────────────────────────────────────────────
def poll_aircraft(callsign: str):
    print(f"\n  🔍 Опрос {callsign} ...", end=" ", flush=True)
    data = fetch_fr24(callsign)
    conn = get_conn()

    if not data:
        conn.execute("INSERT INTO poll_log (callsign, status) VALUES (?,?)", (callsign, "error"))
        conn.commit(); conn.close()
        return

    flights_list = ((data.get("result") or {}).get("response") or {}).get("data") or []
    print(f"получено {len(flights_list)} записей")

    new = save_flights(flights_list, callsign)
    conn.execute("INSERT INTO poll_log (callsign, status, new_flights) VALUES (?,?,?)",
                 (callsign, "ok", new))
    conn.commit(); conn.close()

    print(f"    💾 Новых: {new}" if new else "    ℹ️  Новых рейсов нет")

def monitor_all():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'─'*50}\n⏰ {now} UTC")
    conn = get_conn()
    callsigns = [r[0] for r in conn.execute(
        "SELECT callsign FROM aircraft WHERE active=1").fetchall()]
    conn.close()
    if not callsigns:
        print("  ⚠️  Нет активных самолётов")
        return
    for cs in callsigns:
        poll_aircraft(cs)

# ─── История ──────────────────────────────────────────────────────────────────
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

    title = f" — {callsign.upper()}" if callsign else " — ВСЕ САМОЛЁТЫ"
    print(f"\n{'═'*78}\n  ИСТОРИЯ РЕЙСОВ{title}\n{'═'*78}")
    print(f"  {'ПОЗЫВНОЙ':<10} {'ВЫЛЕТ UTC':<20} {'ОТКУДА':<16} {'КУДА':<16} {'ДЛИТ.':<8} СТАТУС")
    print(f"  {'─'*73}")
    for cs, dep, fi, fc, ti, tc, dur, st, ac in rows:
        fr = f"{fi}({fc})" if fc else fi or "?"
        to = f"{ti}({tc})" if tc else ti or "?"
        dur_s = f"{dur//60}ч{dur%60:02d}м" if dur else "—"
        print(f"  {cs:<10} {str(dep):<20} {fr:<16} {to:<16} {dur_s:<8} {st}")
    print(f"{'═'*78}\n")

# ─── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="✈  Flight History Monitor v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python flight_monitor.py                             # запустить мониторинг
  python flight_monitor.py --fetch WSA9677             # разовая проверка
  python flight_monitor.py --add SVR777                # добавить самолёт
  python flight_monitor.py --add SVR777 --label "VIP" --reg RA-12345
  python flight_monitor.py --remove SVR777             # пауза мониторинга
  python flight_monitor.py --list                      # список самолётов
  python flight_monitor.py --history                   # вся история
  python flight_monitor.py --history WSA9677           # история одного
  python flight_monitor.py --interval 5                # проверка каждые 5 мин
        """
    )
    parser.add_argument("--add",      metavar="CALLSIGN", help="Добавить самолёт в мониторинг")
    parser.add_argument("--remove",   metavar="CALLSIGN", help="Приостановить мониторинг самолёта")
    parser.add_argument("--label",    default="",         help="Название (для --add)")
    parser.add_argument("--reg",      default="",         help="Бортовой номер (для --add)")
    parser.add_argument("--list",     action="store_true",help="Показать список самолётов")
    parser.add_argument("--history",  nargs="?", const="", metavar="CALLSIGN",
                                                           help="Показать историю рейсов")
    parser.add_argument("--fetch",    metavar="CALLSIGN", help="Разовая проверка одного самолёта")
    parser.add_argument("--interval", type=int, default=CHECK_INTERVAL_MINUTES,
                                                           help="Интервал проверки (минуты)")
    args = parser.parse_args()

    init_db()

    if args.add:
        add_aircraft(args.add, args.label, args.reg)
        return
    if args.remove:
        remove_aircraft(args.remove)
        return
    if args.list:
        list_aircraft()
        return
    if args.history is not None:
        show_history(args.history if args.history else None)
        return
    if args.fetch:
        poll_aircraft(args.fetch.upper())
        show_history(args.fetch.upper(), limit=5)
        return

    # ─── Режим мониторинга ────────────────────────────────────────────────────
    list_aircraft()
    monitor_all()
    schedule.every(args.interval).minutes.do(monitor_all)
    print(f"\n⏰ Мониторинг запущен. Проверка каждые {args.interval} мин. Ctrl+C — стоп\n")
    try:
        while True:
            schedule.run_pending()
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n👋 Мониторинг остановлен")

if __name__ == "__main__":
    main()
