#!/usr/bin/env python3
"""
evcc & Solar-Log Telemetry Exporter v2.0
Sammelt Daten aus:
- evcc REST-API (Live PV, Grid, Home, Fahrzeuge, Solar-Vorhersage)
- InfluxDB (Heutiger Tagesertrag, Spitzenleistung, Durchschnitt)
- SQLite Charging DB (Manuelle Fahrzeug-Ladevorgänge)
- SolarLog DB (Alle 17 Wechselrichter mit detailliertem Status)
- Open-Meteo & DWD Bright Sky (Wetter, Sonnenschein & Ladefenster-Empfehlungen)
"""

import sys
import os
import json
import sqlite3
import asyncio
import aiohttp
import configparser
import subprocess
import math
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

BASE_DIR = Path("/home/admin/evcc-dashboard")
BOT_DIR = Path("/home/admin/evcc-bot")
CONFIG_PATH = BOT_DIR / "config.ini"

CHARGING_DB_PATH = Path.home() / ".local" / "share" / "evcc_bot" / "charging.db"
SOLARLOG_DB_PATH = Path.home() / ".local" / "share" / "solarlog_monitor" / "solarlog.db"

# Zeitzone & Koordinaten Kempten (Allgäu)
LOCAL_TZ = ZoneInfo("Europe/Berlin")
LAT = 47.7479
LON = 10.3077

# Dynamische Zyklus-Intervalle (Sekunden)
DAY_INTERVAL_SEC = 120    # 2 Minuten tagsüber bei Sonnenschein oder Ladevorgang
NIGHT_INTERVAL_SEC = 900  # 15 Minuten nachts im Ruhemodus (schont GitHub API & Pages)


def get_sun_times(dt: Optional[datetime] = None) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Berechnet Sonnenaufgang und Sonnenuntergang für Kempten (Allgäu)
    via NOAA-Algorithmus (offline, mathematisch exakt auf Tagesbasis).
    Rückgabe: (sunrise_dt, sunset_dt) in Europe/Berlin.
    """
    if dt is None:
        dt = datetime.now(LOCAL_TZ)
    d = dt.date()
    n = (datetime(d.year, d.month, d.day) - datetime(2000, 1, 1, 12, 0)).days + 0.0008
    j_star = n - LON / 360.0
    M = (357.5291 + 0.98560028 * j_star) % 360.0
    C = 1.9148 * math.sin(math.radians(M)) + 0.02 * math.sin(math.radians(2 * M)) + 0.0003 * math.sin(math.radians(3 * M))
    lambda_val = (M + C + 180.0 + 102.9372) % 360.0
    j_transit = 2451545.0 + j_star + 0.0053 * math.sin(math.radians(M)) - 0.0069 * math.sin(math.radians(2 * lambda_val))
    sin_delta = math.sin(math.radians(lambda_val)) * math.sin(math.radians(23.44))
    cos_delta = math.cos(math.asin(sin_delta))
    cos_omega_0 = (math.sin(math.radians(-0.833)) - math.sin(math.radians(LAT)) * sin_delta) / (math.cos(math.radians(LAT)) * cos_delta)
    if cos_omega_0 > 1.0 or cos_omega_0 < -1.0:
        return None, None
    omega_0 = math.degrees(math.acos(cos_omega_0))
    j_rise = j_transit - omega_0 / 360.0
    j_set = j_transit + omega_0 / 360.0

    def jd_to_dt(jd):
        ts = (jd - 2440587.5) * 86400.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(LOCAL_TZ)

    return jd_to_dt(j_rise), jd_to_dt(j_set)


def get_dynamic_interval(payload: Dict[str, Any], day_sec: int = DAY_INTERVAL_SEC, night_sec: int = NIGHT_INTERVAL_SEC) -> int:
    """
    Ermittelt dynamisch das nächste Abrufintervall:
    - Tag (Sonnenaufgang - 15m bis Sonnenuntergang + 30m): Tag-Intervall (z.B. 120s)
    - PV-Leistung > 50 W: Tag-Intervall (z.B. 120s)
    - Aktiver Fahrzeug-Ladevorgang (auch nachts): Tag-Intervall (z.B. 120s)
    - Nacht (ohne Sonne & ohne Laden): Nacht-Intervall (z.B. 900s / 15 Min)
    """
    now = datetime.now(LOCAL_TZ)
    sunrise, sunset = get_sun_times(now)

    pv_power = payload.get("energy", {}).get("pv_power_w", 0) or 0
    vehicles = payload.get("vehicles", [])
    is_charging = any(
        v.get("charging") is True or (v.get("charge_power", 0) or 0) > 0
        for v in vehicles
    )
    wb_charging = payload.get("wallbox", {}).get("is_charging") is True or (payload.get("wallbox", {}).get("power_w", 0) > 100)

    is_day = False
    if sunrise and sunset:
        is_day = (sunrise - timedelta(minutes=15)) <= now <= (sunset + timedelta(minutes=30))

    now_str = now.strftime("%H:%M:%S")
    sunrise_str = sunrise.strftime("%H:%M") if sunrise else "--:--"
    sunset_str = sunset.strftime("%H:%M") if sunset else "--:--"

    if pv_power > 50:
        print(f"[{now_str}] ☀️ PV-Leistung aktiv ({pv_power} W) -> Tag-Intervall ({day_sec}s).")
        return day_sec

    if is_charging or wb_charging:
        print(f"[{now_str}] 🔌 Fahrzeug lädt aktiv (Wallbox) -> Schnell-Intervall ({day_sec}s).")
        return day_sec

    if is_day:
        print(f"[{now_str}] ☀️ Tagfenster aktiv ({sunrise_str} – {sunset_str} Uhr) -> Tag-Intervall ({day_sec}s).")
        return day_sec

    print(f"[{now_str}] 🌙 Nachtmodus aktiv (Sonne ging um {sunset_str} Uhr unter, Aufgang morgen {sunrise_str} Uhr) -> Nacht-Intervall ({night_sec}s / {round(night_sec/60)} Min).")
    return night_sec


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

        # Prüfe vorhandene Spalten in charging_sessions
        cur.execute("PRAGMA table_info(charging_sessions)")
        cols = [r["name"] for r in cur.fetchall()]
        has_cost = "total_cost" in cols
        has_price = "price_per_kwh" in cols
        has_transfer = "transfer_status" in cols

        # Prüfe Tabelle vehicle_settings
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vehicle_settings'")
        has_settings = cur.fetchone() is not None

        vehicle_prices = {}
        if has_settings:
            cur.execute("SELECT vehicle_id, vehicle_title, price_per_kwh FROM vehicle_settings")
            for r in cur.fetchall():
                vehicle_prices[r["vehicle_title"]] = float(r["price_per_kwh"])
                vehicle_prices[r["vehicle_id"]] = float(r["price_per_kwh"])

        # Letzte 20 Ladevorgänge
        price_col = "COALESCE(price_per_kwh, 0.30) as price_per_kwh," if has_price else "0.30 as price_per_kwh,"
        cost_col = "COALESCE(total_cost, round(kwh * 0.30, 2)) as total_cost," if has_cost else "round(kwh * 0.30, 2) as total_cost,"
        transfer_col = "COALESCE(transfer_status, 'NONE') as transfer_status, transfer_at," if has_transfer else "'NONE' as transfer_status, NULL as transfer_at,"

        cur.execute(f"""
            SELECT id, vehicle_id, vehicle_title, kwh, {price_col} {cost_col} {transfer_col} charged_at, note
            FROM charging_sessions
            ORDER BY charged_at DESC, id DESC
            LIMIT 20
        """)
        result["recent_sessions"] = [dict(r) for r in cur.fetchall()]

        # Zusammenfassung je Fahrzeug
        now = datetime.now()
        cur_month = now.strftime("%Y-%m")
        cur_year = now.strftime("%Y")

        cur.execute("SELECT DISTINCT vehicle_title FROM charging_sessions")
        titles = set(r[0] for r in cur.fetchall())
        for t in vehicle_prices.keys():
            if not t.startswith("db:"):
                titles.add(t)

        for title in titles:
            cost_expr = "COALESCE(total_cost, kwh * COALESCE(price_per_kwh, 0.30))" if has_cost else "(kwh * 0.30)"

            cur.execute(f"""
                SELECT 
                    COUNT(id) as total_count,
                    COALESCE(SUM(kwh), 0.0) as total_kwh,
                    COALESCE(SUM({cost_expr}), 0.0) as total_cost,
                    COALESCE(SUM(CASE WHEN charged_at LIKE ? THEN kwh ELSE 0.0 END), 0.0) as month_kwh,
                    COALESCE(SUM(CASE WHEN charged_at LIKE ? THEN {cost_expr} ELSE 0.0 END), 0.0) as month_cost,
                    COALESCE(SUM(CASE WHEN charged_at LIKE ? THEN kwh ELSE 0.0 END), 0.0) as year_kwh,
                    COALESCE(SUM(CASE WHEN charged_at LIKE ? THEN {cost_expr} ELSE 0.0 END), 0.0) as year_cost
                FROM charging_sessions
                WHERE vehicle_title = ?
            """, (f"{cur_month}%", f"{cur_month}%", f"{cur_year}%", f"{cur_year}%", title))
            row = cur.fetchone()

            cur.execute(f"""
                SELECT charged_at, kwh, {price_col} {cost_col} note
                FROM charging_sessions
                WHERE vehicle_title = ?
                ORDER BY charged_at DESC, id DESC
                LIMIT 1
            """, (title,))
            last_row = cur.fetchone()

            v_price = vehicle_prices.get(title, 0.30)

            result["vehicles"][title] = {
                "price_per_kwh": v_price,
                "total_count": row["total_count"] if row else 0,
                "total_kwh": round(row["total_kwh"], 2) if row else 0.0,
                "total_cost": round(row["total_cost"], 2) if row else 0.0,
                "month_kwh": round(row["month_kwh"], 2) if row else 0.0,
                "month_cost": round(row["month_cost"], 2) if row else 0.0,
                "year_kwh": round(row["year_kwh"], 2) if row else 0.0,
                "year_cost": round(row["year_cost"], 2) if row else 0.0,
                "latest_charge": dict(last_row) if last_row else None
            }

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
        "standby_count": 0,
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
        standby_count = 0
        error_count = 0
        latest_ts = None

        for r in rows:
            pac = float(r.get("pac") or 0.0)
            daysum = float(r.get("daysum") or 0.0)
            status = r.get("status") or 0
            error = r.get("error") or 0
            last_up = r.get("last_update")

            total_pac += pac
            total_daysum += daysum

            # Status-Kategorisierung für Filter & Badges
            if error and error > 0:
                error_count += 1
                state_category = "error"
                state_badge = f"Fehler {error}"
            elif pac > 0:
                active_count += 1
                state_category = "active"
                state_badge = "Einspeisung"
            else:
                standby_count += 1
                state_category = "standby"
                state_badge = "Standby (0 W)"

            # Detaillierter Text
            status_desc = "Normalbetrieb"
            if status == 0:
                status_desc = "Standby / Nacht"
            elif status == 1:
                status_desc = "Warten auf Sonne"
            elif status == 2:
                status_desc = "Einspeisung"
            elif status == 3:
                status_desc = "MPP-Betrieb"
            elif status == 7:
                status_desc = "Normalbetrieb"

            if last_up and (not latest_ts or last_up > latest_ts):
                latest_ts = last_up

            r["daysum_kwh"] = round(daysum / 1000.0, 2)
            r["pac_kw"] = round(pac / 1000.0, 2)
            r["state_category"] = state_category
            r["state_badge"] = state_badge
            r["status_desc"] = status_desc

        result["inverters"] = rows
        result["total_pac"] = round(total_pac, 1)
        result["total_daysum_kwh"] = round(total_daysum / 1000.0, 2)
        result["active_count"] = active_count
        result["standby_count"] = standby_count
        result["error_count"] = error_count
        result["last_update"] = latest_ts
    except Exception as e:
        print(f"Warnung SolarLog DB: {e}", file=sys.stderr)

    return result


_LAST_WEATHER_CACHE: Optional[Dict[str, Any]] = None


def _init_weather_cache_from_disk():
    global _LAST_WEATHER_CACHE
    try:
        data_file = Path(__file__).parent / "data.json"
        if data_file.exists():
            with open(data_file, "r", encoding="utf-8") as f:
                d = json.load(f)
                wi = d.get("weather_intelligence")
                if wi and wi.get("dwd", {}).get("temp_c") is not None:
                    _LAST_WEATHER_CACHE = wi
    except Exception:
        pass


_init_weather_cache_from_disk()


async def fetch_weather_and_charging_recommendations(evcc_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fragt Open-Meteo und DWD Bright Sky ab und berechnet die besten Ladefenster
    basierend auf der 220 kWp Solarprognose.
    """
    weather_data = {
        "location": "Kempten (Allgäu)",
        "dwd": {"condition": "unbekannt", "temp_c": None, "cloud_cover": None},
        "open_meteo": {"temp_c": None, "cloud_cover": None, "sunshine_today_h": None},
        "charging_windows": [],
        "days_overview": []
    }

    try:
        async with aiohttp.ClientSession() as session:
            # 1. DWD Bright Sky
            try:
                dwd_url = f"https://api.brightsky.dev/current_weather?lat={LAT}&lon={LON}"
                async with session.get(dwd_url, timeout=aiohttp.ClientTimeout(total=4)) as r:
                    if r.status == 200:
                        d = await r.json()
                        w = d.get("weather", {})
                        weather_data["dwd"] = {
                            "condition": w.get("condition", "trocken"),
                            "temp_c": w.get("temperature"),
                            "cloud_cover": w.get("cloud_cover")
                        }
            except Exception as e:
                print(f"Warnung DWD API: {e}", file=sys.stderr)

            # 2. Open-Meteo Wetter & Globalstrahlung & Sonnenzeiten
            try:
                om_url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&hourly=cloudcover,direct_normal_irradiance,temperature_2m&daily=weathercode,temperature_2m_max,temperature_2m_min,sunshine_duration,sunrise,sunset&timezone=Europe%2FBerlin&forecast_days=3"
                async with session.get(om_url, timeout=aiohttp.ClientTimeout(total=4)) as r:
                    if r.status == 200:
                        om = await r.json()
                        daily = om.get("daily", {})
                        sunshine_sec = daily.get("sunshine_duration", [0, 0, 0])
                        weather_data["open_meteo"]["sunshine_today_h"] = round(sunshine_sec[0] / 3600.0, 1) if sunshine_sec else 0
                        weather_data["open_meteo"]["sunshine_tomorrow_h"] = round(sunshine_sec[1] / 3600.0, 1) if len(sunshine_sec) > 1 else 0
                        
                        sr_list = daily.get("sunrise", [])
                        ss_list = daily.get("sunset", [])
                        if sr_list and sr_list[0]:
                            weather_data["open_meteo"]["sunrise"] = sr_list[0].split("T")[-1] if "T" in sr_list[0] else sr_list[0]
                        if ss_list and ss_list[0]:
                            weather_data["open_meteo"]["sunset"] = ss_list[0].split("T")[-1] if "T" in ss_list[0] else ss_list[0]

                        hourly_cc = om.get("hourly", {}).get("cloudcover", [])
                        if hourly_cc:
                            now_hour = datetime.now().hour
                            weather_data["open_meteo"]["cloud_cover"] = hourly_cc[min(now_hour, len(hourly_cc) - 1)]
            except Exception as e:
                print(f"Warnung Open-Meteo API: {e}", file=sys.stderr)

        # Fallback Sonnenzeiten via NOAA-Berechnung falls Open-Meteo fehlt
        if not weather_data["open_meteo"].get("sunrise") or not weather_data["open_meteo"].get("sunset"):
            sr, ss = get_sun_times()
            if sr and ss:
                weather_data["open_meteo"]["sunrise"] = sr.strftime("%H:%M")
                weather_data["open_meteo"]["sunset"] = ss.strftime("%H:%M")

    except Exception as e:
        print(f"Warnung Wetterabruf allgemein: {e}", file=sys.stderr)

    # 3. Ladefenster-Berechnung AUSSCHLIESSLICH aus Fronius 3er-Verbund (18.67 kWp)
    # Formel: WR 1 (Fronius rechts) + WR 2 (100%) + WR 3 (66.7%) = 8/3 ≈ 2.667 * Fronius
    # Skalierungsfaktor gegenüber der 220 kWp Gesamtanlage: 18.67 / 220.0 ≈ 0.08485
    RATIO_3WR = (7.0 * (8.0 / 3.0)) / 220.0

    try:
        solar_fc = evcc_state.get("forecast", {}).get("solar", {})
        timeseries = solar_fc.get("timeseries", [])
        
        days_map = {}
        for ts, w in timeseries:
            dt = datetime.fromtimestamp(ts)
            day_str = dt.strftime("%Y-%m-%d")
            if day_str not in days_map:
                days_map[day_str] = []
            # Nur Leistung der 3 Lade-Wechselrichter hochrechnen!
            scaled_power_w = w * RATIO_3WR
            days_map[day_str].append((dt, scaled_power_w))

        today_str = datetime.now().strftime("%Y-%m-%d")
        tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        day_after_str = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

        day_labels = {
            today_str: "Heute",
            tomorrow_str: "Morgen",
            day_after_str: "Übermorgen"
        }

        # Ziel-Ladeleistung des Nutzers (Standard: 7.5 kW = 7–8 kW)
        TARGET_CHARGE_KW = 7.5
        TARGET_CHARGE_W = TARGET_CHARGE_KW * 1000.0

        all_charging_points = []

        for day_str in [today_str, tomorrow_str, day_after_str]:
            if day_str not in days_map:
                continue
            points = days_map[day_str]
            label = day_labels.get(day_str, day_str)

            # Serie für interaktiven Schieberegler im Dashboard sammeln
            for dt, p_w in points:
                all_charging_points.append({
                    "t": dt.strftime("%Y-%m-%d %H:%M"),
                    "day": label,
                    "hour": dt.strftime("%H:%M"),
                    "kw": round(p_w / 1000.0, 2)
                })

            # Wallbox Schwellenwerte:
            # Volle Ladeleistung (7–8 kW Schwellwert)
            # 3-phasiges Laden (ab 6A = 4.1 kW)
            # 1-phasiges Laden (ab 6A = 1.4 kW)
            full_power_points = [p for p in points if p[1] >= TARGET_CHARGE_W]
            good_points_3p = [p for p in points if p[1] >= 4100]
            good_points_1p = [p for p in points if p[1] >= 1400]

            max_power_w = max((p[1] for p in points), default=0)
            max_kw = max_power_w / 1000.0
            avg_power_target = (sum(p[1] for p in full_power_points) / len(full_power_points)) if full_power_points else ((sum(p[1] for p in good_points_3p) / len(good_points_3p)) if good_points_3p else 0)

            if full_power_points:
                start_full = full_power_points[0][0].strftime("%H:%M")
                end_full = full_power_points[-1][0].strftime("%H:%M")
                hours_count = round(len(full_power_points) * 15 / 60, 1)
                window_str = f"{start_full} – {end_full} Uhr"
                rating = "Optimal (Volle 7–8 kW Leistung)"
                rating_code = "optimal"
                badge_color = "emerald"
                comment = f"Spitze {round(max_kw, 1)} kW • Volle Ladeleistung (7–8 kW) durchgehend für ~{hours_count} Std. rein solar gedeckt!"
            elif good_points_3p:
                start_3p = good_points_3p[0][0].strftime("%H:%M")
                end_3p = good_points_3p[-1][0].strftime("%H:%M")
                window_str = f"{start_3p} – {end_3p} Uhr (Teilladung)"
                rating = "Gut (3-Phasen Teilladung 4–7 kW)"
                rating_code = "good"
                badge_color = "amber"
                comment = f"Spitze {round(max_kw, 1)} kW • 3-phasiges PV-Laden möglich (bis zu {round(max_kw, 1)} kW)"
            elif good_points_1p:
                start_1p = good_points_1p[0][0].strftime("%H:%M")
                end_1p = good_points_1p[-1][0].strftime("%H:%M")
                window_str = f"{start_1p} – {end_1p} Uhr (1-phasig)"
                rating = "Mäßig (1-phasig ab 1.4 kW)"
                rating_code = "moderate"
                badge_color = "slate"
                comment = "Nur für 1-phasiges Erhaltungsladen geeignet"
            else:
                window_str = "Kein sonniges Ladefenster"
                rating = "Kein Solarüberschuss"
                rating_code = "none"
                badge_color = "slate"
                comment = "Wenig bis keine Sonneneinstrahlung"

            weather_data["charging_windows"].append({
                "day_label": label,
                "date": datetime.strptime(day_str, "%Y-%m-%d").strftime("%d.%m.%Y"),
                "window": window_str,
                "target_kw": TARGET_CHARGE_KW,
                "max_kw": round(max_kw, 1),
                "avg_kw": round(avg_power_target / 1000.0, 1),
                "rating": rating,
                "rating_code": rating_code,
                "badge_color": badge_color,
                "comment": comment
            })

            # Tageszusammenfassung der 3 Lade-Wechselrichter
            total_day_kwh = (solar_fc.get("today" if label == "Heute" else ("tomorrow" if label == "Morgen" else "dayAfterTomorrow"), {}).get("energy", 0) / 1000.0) * RATIO_3WR
            weather_data["days_overview"].append({
                "day_label": label,
                "date": datetime.strptime(day_str, "%Y-%m-%d").strftime("%d.%m.%Y"),
                "energy_kwh": round(total_day_kwh, 1),
                "max_kw": round(max_kw, 1),
                "charging_possible": bool(full_power_points or good_points_3p)
            })

        weather_data["charging_timeseries"] = all_charging_points
        weather_data["default_target_kw"] = TARGET_CHARGE_KW


    except Exception as e:
        print(f"Fehler bei Ladefenster-Berechnung: {e}", file=sys.stderr)

    global _LAST_WEATHER_CACHE
    if weather_data.get("dwd", {}).get("temp_c") is not None or weather_data.get("charging_windows"):
        _LAST_WEATHER_CACHE = weather_data
    elif _LAST_WEATHER_CACHE:
        return _LAST_WEATHER_CACHE

    return weather_data



def fetch_auth_data() -> Dict[str, Any]:
    try:
        if str(BOT_DIR) not in sys.path:
            sys.path.insert(0, str(BOT_DIR))
        from bot.auth import get_exportable_auth_payload
        return get_exportable_auth_payload()
    except Exception as e:
        print(f"Warnung Auth-Payload: {e}", file=sys.stderr)
        return {
            "bot_username": "evcc_bayrhof_bot",
            "group_chat_id": -1003964768309,
            "group_title": "EVCC Bayrhof Steuerung",
            "authorized_ids": ["15231751"],
            "authorized_usernames": ["bdc_gumb_drop"],
            "users": []
        }


async def fetch_wallbox_telemetry() -> Dict[str, Any]:
    try:
        if str(BOT_DIR) not in sys.path:
            sys.path.insert(0, str(BOT_DIR))
        from bot.wallbox import get_wallbox_instance
        wb = get_wallbox_instance()
        telemetry = await wb.get_telemetry()
        return telemetry
    except Exception as e:
        print(f"Warnung Wallbox-Telemetrie: {e}", file=sys.stderr)
        return {
            "online": False,
            "error_msg": str(e),
            "work_state": "UNKNOWN",
            "status_label": "Nicht erreichbar",
            "current_amps": 10,
            "power_w": 0,
            "power_kw": 0.0,
            "auto_mode": True
        }


async def generate_payload() -> Dict[str, Any]:
    now = datetime.now()
    evcc_state = await fetch_evcc_state() or {}
    influx_stats = fetch_influx_stats()
    charging_data = fetch_charging_db()
    solarlog_data = fetch_solarlog_db()
    weather_and_charging = await fetch_weather_and_charging_recommendations(evcc_state)
    wallbox_data = await fetch_wallbox_telemetry()

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

    # Fronius 3er-Verbund (exklusiv für Autoladung)
    fronius_plant = next((p for p in pv_list if "fronius" in p.get("title", "").lower()), None)
    fronius_w = fronius_plant.get("power", 0) if fronius_plant else 0
    wr1_w = round(fronius_w)
    wr2_w = round(fronius_w)
    wr3_w = round(fronius_w * (2.0 / 3.0))
    charging_pv_total_w = wr1_w + wr2_w + wr3_w
    charging_pv_total_kw = round(charging_pv_total_w / 1000.0, 2)

    wb_buffer_kw = float(wallbox_data.get("house_buffer_kw", 3.0))
    wb_single_phase = bool(wallbox_data.get("single_phase_mode", False))
    wb_watt_per_amp = 230.0 if wb_single_phase else 690.0
    wb_min_amps = int(wallbox_data.get("min_current", 6))
    wb_max_amps = int(wallbox_data.get("max_current", 16))

    # Dynamische Startschwelle: Hauspuffer + 10% Reserve + Mindestladung (6A)
    target_charge_kw = round((wb_min_amps * wb_watt_per_amp) / 1000.0, 2)
    threshold_kw = wallbox_data.get("start_threshold_kw")
    if not threshold_kw:
        threshold_kw = round(((wb_buffer_kw * 1000.0 * 1.10) + (wb_min_amps * wb_watt_per_amp)) / 1000.0, 2)
    threshold_w = threshold_kw * 1000.0

    is_charging_ready = charging_pv_total_w >= threshold_w
    diff_to_threshold_w = charging_pv_total_w - threshold_w
    diff_to_threshold_kw = round(diff_to_threshold_w / 1000.0, 2)
    progress_pct = min(100.0, round((charging_pv_total_w / threshold_w) * 100.0, 1)) if threshold_w > 0 else 100.0

    charging_readiness = {
        "threshold_kw": threshold_kw,
        "threshold_w": threshold_w,
        "target_charge_kw": target_charge_kw,
        "buffer_kw": wb_buffer_kw,
        "buffer_with_margin_kw": round(wb_buffer_kw * 1.10, 2),
        "min_current": wb_min_amps,
        "max_current": wb_max_amps,
        "current_kw": charging_pv_total_kw,
        "current_w": charging_pv_total_w,
        "is_ready": is_charging_ready,
        "diff_kw": diff_to_threshold_kw,
        "progress_percent": progress_pct
    }

    charging_pv_system = {
        "title": "Fronius 3er-Verbund (Auto-Lade-PV)",
        "installed_kwp": 18.67,
        "total_power_w": charging_pv_total_w,
        "total_power_kw": charging_pv_total_kw,
        "charging_readiness": charging_readiness,
        "wr1": {
            "title": "Fronius rechts (Messwert)",
            "power_w": wr1_w,
            "power_kw": round(wr1_w / 1000.0, 2),
            "ratio_percent": 100
        },
        "wr2": {
            "title": "Wechselrichter 2 (Partner-WR)",
            "power_w": wr2_w,
            "power_kw": round(wr2_w / 1000.0, 2),
            "ratio_percent": 100
        },
        "wr3": {
            "title": "Wechselrichter 3 (Zusatz-WR)",
            "power_w": wr3_w,
            "power_kw": round(wr3_w / 1000.0, 2),
            "ratio_percent": 66.7
        }
    }

    payload = {
        "meta": {
            "version": "2.2.0",
            "timestamp": now.isoformat(),
            "display_time": now.strftime("%d.%m.%Y %H:%M:%S"),
            "display_date": now.strftime("%d.%m.%Y"),
            "location": "Kempten (Allgäu)"
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
        "charging_pv_system": charging_pv_system,
        "charging_readiness": charging_readiness,
        "wallbox": wallbox_data,
        "influx_today": influx_stats,
        "weather_intelligence": weather_and_charging,
        "vehicles": list(vehicles_dict.values()),
        "charging_stats": charging_data,
        "solarlog": solarlog_data,
        "auth": fetch_auth_data()
    }
    return payload


def export_and_push() -> tuple[bool, Dict[str, Any]]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    payload = loop.run_until_complete(generate_payload())
    loop.close()

    json_file = BASE_DIR / "data.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    now_str = datetime.now().strftime("%d.%m %H:%M:%S")
    print(f"[{now_str}] ✅ data.json geschrieben.")

    pushed = False
    # Git Commit & Push mit Historien-Schutz (--amend wenn möglich)
    try:
        status = subprocess.run(["git", "status", "--porcelain", "data.json"], cwd=BASE_DIR, capture_output=True, text=True)
        if status.stdout.strip():
            subprocess.run(["git", "add", "data.json"], cwd=BASE_DIR, check=True)
            
            last_msg = subprocess.run(["git", "log", "-1", "--pretty=%B"], cwd=BASE_DIR, capture_output=True, text=True).stdout.strip()
            commit_msg = f"chore(data): auto-update telemetry {datetime.now().strftime('%d.%m %H:%M')}"
            
            if last_msg.startswith("chore(data):"):
                subprocess.run(["git", "commit", "--amend", "--date=now", "-m", commit_msg], cwd=BASE_DIR, check=True)
                subprocess.run(["git", "push", "-f", "origin", "main"], cwd=BASE_DIR, check=True)
            else:
                subprocess.run(["git", "commit", "-m", commit_msg], cwd=BASE_DIR, check=True)
                subprocess.run(["git", "push", "origin", "main"], cwd=BASE_DIR, check=True)

            print(f"[{now_str}] 🚀 Telemetrie erfolgreich zu GitHub übertragen.")
            pushed = True
        else:
            print(f"[{now_str}] Keine Änderungen in data.json festgestellt.")
    except Exception as e:
        print(f"[{now_str}] ⚠️ Fehler bei Git Push: {e}", file=sys.stderr)

    return pushed, payload


def run_daemon(day_interval_sec: int = DAY_INTERVAL_SEC, night_interval_sec: int = NIGHT_INTERVAL_SEC):
    print(f"⚡ Starte EVCC Dashboard Exporter Daemon v2.1 (Tag: {day_interval_sec}s, Nacht: {night_interval_sec}s / {round(night_interval_sec/60)}m)...")
    import time
    while True:
        sleep_sec = day_interval_sec
        try:
            pushed, payload = export_and_push()
            sleep_sec = get_dynamic_interval(payload, day_interval_sec, night_interval_sec)
        except Exception as e:
            print(f"Unerwarteter Exporter-Fehler: {e}", file=sys.stderr)
            sleep_sec = day_interval_sec
        time.sleep(sleep_sec)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("--daemon", "-d", "daemon"):
        day_iv = DAY_INTERVAL_SEC
        night_iv = NIGHT_INTERVAL_SEC
        if len(sys.argv) > 2 and sys.argv[2].isdigit():
            day_iv = int(sys.argv[2])
        if len(sys.argv) > 3 and sys.argv[3].isdigit():
            night_iv = int(sys.argv[3])
        run_daemon(day_iv, night_iv)
    else:
        export_and_push()
