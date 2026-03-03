#!/usr/bin/env python3
"""
FlightMon - Web Server
Flask API + дашборд для Railway плоя
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
