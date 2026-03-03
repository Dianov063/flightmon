#!/usr/bin/env python3
"""
FlightMon - Web Server
Flask API + дашборд для Railway деплоя
"""

import os
import threading
import schedule
import time
from flask import Flask, jsonify, request, send_from_directory
from flight_monitor import (
    init_db, monitor_all, add_aircraft, remove_aircraft,
    list_aircraft as get_aircraft_list, show_history,
    get_conn, DB_PATH
)

app = Flask(__name__, static_folder='static')

# Инициализация БД при загрузке модуля (работает и с gunicorn)
init_db()
threading.Thread(target=monitor_all, daemon=True).start()

# ─── API эндпоинты ─────────────────────────────────────────────────────────────

@app.route('/api/flights')
def api_flights():
    """Получить историю рейсов"""
    callsign = request.args.get('callsign', '').upper() or None
    limit    = int(request.args.get('limit', 100))

    conn = get_conn()
    if callsign:
        rows = conn.execute("""
            SELECT callsign, flight_id, registration, aircraft_type,
                   from_iata, from_name, from_city,
                   to_iata, to_name, to_city,
                   departure_utc, arrival_utc, duration_min, status, owner, saved_at
            FROM flights WHERE callsign=?
            ORDER BY departure_utc DESC LIMIT ?
        """, (callsign, limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT callsign, flight_id, registration, aircraft_type,
                   from_iata, from_name, from_city,
                   to_iata, to_name, to_city,
                   departure_utc, arrival_utc, duration_min, status, owner, saved_at
            FROM flights
            ORDER BY departure_utc DESC LIMIT ?
        """, (limit,)).fetchall()
    conn.close()

    keys = ['callsign','flight_id','registration','aircraft_type',
            'from_iata','from_name','from_city',
            'to_iata','to_name','to_city',
            'departure_utc','arrival_utc','duration_min','status','owner','saved_at']

    return jsonify([dict(zip(keys, r)) for r in rows])


@app.route('/api/aircraft')
def api_aircraft():
    """Список самолётов"""
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

    keys = ['callsign','label','registration','active','flight_count','last_seen']
    return jsonify([dict(zip(keys, r)) for r in rows])


@app.route('/api/aircraft', methods=['POST'])
def api_add_aircraft():
    """Добавить самолёт"""
    data = request.get_json()
    callsign = (data.get('callsign') or '').strip().upper()
    label    = data.get('label', '')
    reg      = data.get('registration', '')

    if not callsign:
        return jsonify({'error': 'callsign required'}), 400

    add_aircraft(callsign, label, reg)
    # Сразу запускаем проверку в фоне
    threading.Thread(target=monitor_one, args=(callsign,), daemon=True).start()
    return jsonify({'ok': True, 'callsign': callsign})


@app.route('/api/aircraft/<callsign>', methods=['DELETE'])
def api_remove_aircraft(callsign):
    """Остановить мониторинг"""
    remove_aircraft(callsign.upper())
    return jsonify({'ok': True})


@app.route('/api/stats')
def api_stats():
    """Общая статистика"""
    conn = get_conn()
    total   = conn.execute("SELECT COUNT(*) FROM flights").fetchone()[0]
    active  = conn.execute("SELECT COUNT(*) FROM aircraft WHERE active=1").fetchone()[0]
    routes  = conn.execute("SELECT COUNT(DISTINCT from_iata||to_iata) FROM flights").fetchone()[0]
    last    = conn.execute("SELECT from_iata, to_iata, departure_utc FROM flights ORDER BY departure_utc DESC LIMIT 1").fetchone()
    polls   = conn.execute("SELECT COUNT(*) FROM poll_log WHERE status='ok'").fetchone()[0]
    conn.close()

    return jsonify({
        'total_flights':  total,
        'active_aircraft': active,
        'unique_routes':  routes,
        'total_polls':    polls,
        'last_flight': {
            'from': last[0], 'to': last[1], 'dep': last[2]
        } if last else None
    })


@app.route('/api/fetch', methods=['POST'])
def api_fetch():
    """Запустить проверку вручную"""
    callsign = (request.get_json() or {}).get('callsign', '').upper() or None
    if callsign:
        threading.Thread(target=monitor_one, args=(callsign,), daemon=True).start()
    else:
        threading.Thread(target=monitor_all, daemon=True).start()
    return jsonify({'ok': True, 'message': f'Fetch started for {callsign or "all"}'})


@app.route('/api/log')
def api_log():
    """Последние записи лога"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT callsign, status, new_flights, polled_at
        FROM poll_log ORDER BY polled_at DESC LIMIT 50
    """).fetchall()
    conn.close()
    keys = ['callsign','status','new_flights','polled_at']
    return jsonify([dict(zip(keys, r)) for r in rows])


# ─── Статические файлы (дашборд) ───────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


# ─── Вспомогательные функции ───────────────────────────────────────────────────

def monitor_one(callsign: str):
    from flight_monitor import poll_aircraft
    poll_aircraft(callsign)


def background_scheduler():
    """Фоновый поток с расписанием"""
    interval = int(os.environ.get('CHECK_INTERVAL_MINUTES', 10))
    schedule.every(interval).minutes.do(monitor_all)
    print(f"⏰ Фоновый мониторинг: каждые {interval} мин")
    while True:
        schedule.run_pending()
        time.sleep(15)


# ─── Запуск ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    init_db()

    # Первая проверка при старте
    threading.Thread(target=monitor_all, daemon=True).start()

    # Запускаем планировщик в фоне
    t = threading.Thread(target=background_scheduler, daemon=True)
    t.start()

    port = int(os.environ.get('PORT', 5000))
    print(f"🚀 FlightMon запущен на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
