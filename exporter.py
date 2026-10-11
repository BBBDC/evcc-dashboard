#!/usr/bin/env python3
"""
evcc & Solar-Log Telemetry Exporter
Sammelt Daten aus evcc REST-API, InfluxDB, SQLite Charging DB und SolarLog DB
und erzeugt data.json für das Web-Dashboard.
"""

import sys
import os
import json
import sqlite3
import asyncio
import aiohttp
import configparser
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

BASE_DIR = Path("/home/admin/evcc-dashboard")
BOT_DIR = Path("/home/admin/evcc-bot")
CONFIG_PATH = BOT_DIR / "config.ini"

CHARGING_DB_PATH = Path.home() / ".local" / "share" / "evcc_bot" / "charging.db"
SOLARLOG_DB_PATH = Path.home() / ".local" / "share" / "solarlog_monitor" / "solarlog.db"

def load_config():
    config = configparser.ConfigParser()
    if CONFIG_PATH.exists():
        config.read(CONFIG_PATH)
    return config

config = load_config()
EVCC_URL = config.get("evcc", "url", fallback="http://localhost:7070").rstrip("/")
INFLUX_URL = config.get("influx", "url", fallback="http://localhost:8086")
INFLUX_TOKEN = config.get("influx", "token", fallback="")
INFLUX_ORG = config.get("influx", "org", fallback="home")
INFLUX_BUCKET = config.get("influx", "bucket", fallback="evcc")


async def fetch_evcc_state() -> Optional[Dict[str, Any]]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{EVCC_URL}/api/state", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("result", data)
    except Exception as e:
        print(f"Warnung evcc API: {e}", file=sys.stderr)
    return None


def fetch_influx_stats() -> Dict[str, Any]:
    stats = {
        "energy_kwh": 0.0,
        "max_power_w": 0,
        "avg_power_w": 0,
        "date": datetime.now().strftime("%d.%m.%Y")
    }
    if not INFLUX_TOKEN:
        return stats
    try:
        from influxdb_client import InfluxDBClient
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=10_000)
        query_api = client.query_api()

        today = datetime.now(timezone.utc).date()
        start = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)

        flux_energy = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start.isoformat()})
          |> filter(fn: (r) => r._measurement == "pvPower")
          |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
          |> integral(unit: 1h)
        '''
        tables = query_api.query(flux_energy)
        total_wh = 0.0
        for table in tables:
            for record in table.records:
                val = record.get_value()
                if val is not None:
                    total_wh += val
        stats["energy_kwh"] = round(total_wh / 1000, 2)

        flux_stats = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start.isoformat()})
          |> filter(fn: (r) => r._measurement == "pvPower")
          |> keep(columns: ["_value"])
          |> reduce(
              identity: {{min: 999999.0, max: 0.0, sum: 0.0, count: 0}},
              fn: (r, accumulator) => ({{
                min: if r._value < accumulator.min then r._value else accumulator.min,
                max: if r._value > accumulator.max then r._value else accumulator.max,
                sum: accumulator.sum + r._value,
                count: accumulator.count + 1
              }})
            )
        '''
        tables = query_api.query(flux_stats)
        max_power = 0.0
        avg_power = 0.0
        for table in tables:
            for record in table.records:
                max_power = record.values.get("max", 0)
                count = record.values.get("count", 0)
                sum_val = record.values.get("sum", 0)
                if count > 0:
                    avg_power = sum_val / count
        stats["max_power_w"] = round(max_power)
        stats["avg_power_w"] = round(avg_power)
        client.close()
    except Exception as e:
        print(f"Warnung InfluxDB: {e}", file=sys.stderr)
    return stats


def fetch_charging_db() -> Dict[str, Any]:
    result = {
        "vehicles": {},
        "recent_sessions": [],
        "monthly_totals": []
    }
    if not CHARGING_DB_PATH.exists():
        return result

    try:
        conn = sqlite3.connect(CHARGING_DB_PATH, timeout=5.0)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Letzte 15 Ladevorgänge
        cur.execute("""
            SELECT id, vehicle_id, vehicle_title, kwh, charged_at, note
            FROM charging_sessions
            ORDER BY charged_at DESC, id DESC
            LIMIT 15
        """)
        result["recent_sessions"] = [dict(r) for r in cur.fetchall()]

        # Zusammenfassung je Fahrzeug
        now = datetime.now()
        cur_month = now.strftime("%Y-%m")
        cur_year = now.strftime("%Y")

        cur.execute("SELECT DISTINCT vehicle_title FROM charging_sessions")
        titles = [r[0] for r in cur.fetchall()]

        for title in titles:
            cur.execute("""
                SELECT 
                    COUNT(id) as total_count,
                    COALESCE(SUM(kwh), 0.0) as total_kwh,
                    COALESCE(SUM(CASE WHEN charged_at LIKE ? THEN kwh ELSE 0.0 END), 0.0) as month_kwh,
                    COALESCE(SUM(CASE WHEN charged_at LIKE ? THEN kwh ELSE 0.0 END), 0.0) as year_kwh
                FROM charging_sessions
                WHERE vehicle_title = ?
            """, (f"{cur_month}%", f"{cur_year}%", title))
            row = cur.fetchone()

            cur.execute("""
                SELECT charged_at, kwh, note
                FROM charging_sessions
                WHERE vehicle_title = ?
                ORDER BY charged_at DESC, id DESC
                LIMIT 1
            """, (title,))
            last_row = cur.fetchone()

            result["vehicles"][title] = {
                "total_count": row["total_count"] if row else 0,
                "total_kwh": round(row["total_kwh"], 2) if row else 0.0,
                "month_kwh": round(row["month_kwh"], 2) if row else 0.0,
                "year_kwh": round(row["year_kwh"], 2) if row else 0.0,
                "latest_charge": dict(last_row) if last_row else None
            }

        # Monatliche Summen für Verlaufs-Chart (letzte 6 Monate)
        cur.execute("""
            SELECT substr(charged_at, 1, 7) as month_str, round(sum(kwh), 2) as kwh_sum, count(id) as count
            FROM charging_sessions
            GROUP BY month_str
            ORDER BY month_str DESC
            LIMIT 6
        """)
        result["monthly_totals"] = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        print(f"Warnung Charging DB: {e}", file=sys.stderr)

    return result


def fetch_solarlog_db() -> Dict[str, Any]:
    result = {
        "inverters": [],
        "total_pac": 0.0,
        "total_daysum_kwh": 0.0,
        "active_count": 0,
        "error_count": 0,
        "last_update": None
    }
    if not SOLARLOG_DB_PATH.exists():
        return result

    try:
        conn = sqlite3.connect(SOLARLOG_DB_PATH, timeout=5.0)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            SELECT wr_number, last_update, pac, daysum, status, error, status_text
            FROM inverter_status
            ORDER BY wr_number ASC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        total_pac = 0.0
        total_daysum = 0.0
        active_count = 0
        error_count = 0
        latest_ts = None

        for r in rows:
            pac = float(r.get("pac") or 0.0)
            daysum = float(r.get("daysum") or 0.0)
            status = r.get("status")
            error = r.get("error")
            last_up = r.get("last_update")

            total_pac += pac
            total_daysum += daysum
            if status == 2:  # 2 = Einspeisen / Betrieb
                active_count += 1
            if error and error != 0:
                error_count += 1
            if last_up and (not latest_ts or last_up > latest_ts):
                latest_ts = last_up

            r["daysum_kwh"] = round(daysum / 1000.0, 2)
            r["pac_kw"] = round(pac / 1000.0, 2)

        result["inverters"] = rows
        result["total_pac"] = round(total_pac, 1)
        result["total_daysum_kwh"] = round(total_daysum / 1000.0, 2)
        result["active_count"] = active_count
        result["error_count"] = error_count
        result["last_update"] = latest_ts
    except Exception as e:
        print(f"Warnung SolarLog DB: {e}", file=sys.stderr)

    return result


async def generate_payload() -> Dict[str, Any]:
    now = datetime.now()
    evcc_state = await fetch_evcc_state() or {}
    influx_stats = fetch_influx_stats()
    charging_data = fetch_charging_db()
    solarlog_data = fetch_solarlog_db()

    # EVCC Aufbereitung
    pv_list = evcc_state.get("pv", [])
    total_pv_power = sum(item.get("power", 0) for item in pv_list)
    grid_power = evcc_state.get("grid", {}).get("power", 0)
    battery_soc = evcc_state.get("batterySoc")
    battery_power = None
    battery_obj = evcc_state.get("battery")
    if isinstance(battery_obj, dict):
        battery_power = battery_obj.get("power")
    elif isinstance(battery_obj, list) and len(battery_obj) > 0:
        battery_power = sum(b.get("power", 0) for b in battery_obj if isinstance(b, dict))

    # Hausverbrauch (homePower)
    home_power = evcc_state.get("homePower")
    if home_power is None:
        # Formel: PV + Grid (positiv) - Batterie (falls ladend)
        home_power = max(0, total_pv_power + grid_power - (battery_power or 0))

    # Fahrzeuge
    vehicles_dict = {}
    for vid, vdata in (evcc_state.get("vehicles") or {}).items():
        if isinstance(vdata, dict):
            title = vdata.get("title") or vid
            vehicles_dict[vid] = {
                "id": vid,
                "title": title,
                "capacity": vdata.get("capacity"),
                "mode": vdata.get("mode"),
                "minSoc": vdata.get("minSoc"),
                "soc": vdata.get("soc"),
                "odometer": vdata.get("odometer"),
                "connected": False,
                "charging": False,
                "charge_power": 0
            }

    # Loadpoints abgleichen
    for lp in evcc_state.get("loadpoints", []):
        lp_title = lp.get("vehicleTitle")
        lp_vehicle = lp.get("vehicle")
        for vid, vinfo in vehicles_dict.items():
            if (lp_title and vinfo["title"] == lp_title) or (isinstance(lp_vehicle, dict) and lp_vehicle.get("title") == vinfo["title"]):
                if lp.get("vehicleSoc") is not None:
                    vinfo["soc"] = lp.get("vehicleSoc")
                if lp.get("charging") is not None:
                    vinfo["charging"] = bool(lp.get("charging"))
                if lp.get("connected") is not None:
                    vinfo["connected"] = bool(lp.get("connected"))
                if lp.get("chargePower") is not None:
                    vinfo["charge_power"] = lp.get("chargePower")

    # Solar Vorhersage
    forecast_raw = evcc_state.get("forecast", {}).get("solar", {})
    forecast = {
        "today_kwh": round(forecast_raw.get("today", {}).get("energy", 0) / 1000.0, 2),
        "tomorrow_kwh": round(forecast_raw.get("tomorrow", {}).get("energy", 0) / 1000.0, 2),
        "dayAfterTomorrow_kwh": round(forecast_raw.get("dayAfterTomorrow", {}).get("energy", 0) / 1000.0, 2),
    }

    payload = {
        "meta": {
            "version": "1.0.0",
            "timestamp": now.isoformat(),
            "display_time": now.strftime("%d.%m.%Y %H:%M:%S"),
            "display_date": now.strftime("%d.%m.%Y"),
        },
        "energy": {
            "pv_power_w": round(total_pv_power),
            "pv_power_kw": round(total_pv_power / 1000.0, 2),
            "grid_power_w": round(grid_power),
            "grid_power_kw": round(grid_power / 1000.0, 2),
            "grid_mode": "export" if grid_power < 0 else "import",
            "home_power_w": round(home_power),
            "home_power_kw": round(home_power / 1000.0, 2),
            "battery_soc": battery_soc,
            "battery_power_w": battery_power,
            "pv_plants": [
                {
                    "title": p.get("title", "Unbekannt"),
                    "power_w": p.get("power", 0),
                    "energy_kwh": round(p.get("energy", 0) / 1000.0, 2) if "energy" in p else None
                }
                for p in pv_list
            ]
        },
        "influx_today": influx_stats,
        "forecast": forecast,
        "vehicles": list(vehicles_dict.values()),
        "charging_stats": charging_data,
        "solarlog": solarlog_data
    }
    return payload


def export_and_push(single_run=True):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    payload = loop.run_until_complete(generate_payload())
    loop.close()

    json_file = BASE_DIR / "data.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ data.json geschrieben.")

    # Git Commit & Push
    try:
        status = subprocess.run(["git", "status", "--porcelain", "data.json"], cwd=BASE_DIR, capture_output=True, text=True)
        if status.stdout.strip():
            subprocess.run(["git", "add", "data.json"], cwd=BASE_DIR, check=True)
            subprocess.run(["git", "commit", "-m", f"chore(data): auto-update telemetry {datetime.now().strftime('%d.%m %H:%M')}"], cwd=BASE_DIR, check=True)
            subprocess.run(["git", "push", "origin", "main"], cwd=BASE_DIR, check=True)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 Push zu GitHub erfolgreich.")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Keine Änderungen in data.json.")
    except Exception as e:
        print(f"Fehler bei Git Push: {e}", file=sys.stderr)


if __name__ == "__main__":
    export_and_push()
