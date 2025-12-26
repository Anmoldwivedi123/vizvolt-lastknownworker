import psycopg2
import time
import os
from datetime import datetime



DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "sslmode": "require"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

UPSERT_SQL = """
INSERT INTO last_known_data
SELECT DISTINCT ON (dd.imei)
    dd.imei,
    dl.asset_name,

    dd.gpsiat, dd.latitude, dd.longitude, dd.direction, dd.speed,
    dd.disttravelled_all, dd.disttravelled_today,

    dd.bmsiat, dd.cc, dd.voltage, dd.current, dd.soc,

    dd.maxvoltagecellvalue, dd.maxvltagecellnumber,
    dd.minvoltagecellvalue, dd.minvoltagecellnumber,

    dd.ChargeDischargeStatus, dd.ChargingCurrent,
    dd.dischargingcurrent, dd.DeviceStatus,

    dd.serial, dd.barcode,

    dd.cellVolt1, dd.cellVolt2, dd.cellVolt3, dd.cellVolt4,
    dd.cellVolt5, dd.cellVolt6, dd.cellVolt7, dd.cellVolt8,
    dd.cellVolt9, dd.cellVolt10, dd.cellVolt11, dd.cellVolt12,
    dd.cellVolt13, dd.cellVolt14, dd.cellVolt15, dd.cellVolt16,

    dd.cellTemp1, dd.cellTemp2, dd.cellTemp3, dd.cellTemp4,
    dd.cellTemp5, dd.cellTemp6, dd.cellTemp7, dd.cellTemp8,
    dd.cellTemp9, dd.cellTemp10, dd.cellTemp11, dd.cellTemp12,
    dd.cellTemp13, dd.cellTemp14, dd.cellTemp15, dd.cellTemp16,

    dd.charging, dd.avgrangekm, dd.maxrangekm, dd.minrangekm,

    dd.created_at,
    NOW()

FROM device_data dd
INNER JOIN device_list dl
    ON dl.imei = dd.imei

ORDER BY dd.imei, dd.created_at DESC

ON CONFLICT (imei)
DO UPDATE SET
    assetname = EXCLUDED.assetname,

    gpsiat = EXCLUDED.gpsiat,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    direction = EXCLUDED.direction,
    speed = EXCLUDED.speed,

    disttravelled_all = EXCLUDED.disttravelled_all,
    disttravelled_today = EXCLUDED.disttravelled_today,

    bmsiat = EXCLUDED.bmsiat,
    cc = EXCLUDED.cc,
    voltage = EXCLUDED.voltage,
    current = EXCLUDED.current,
    soc = EXCLUDED.soc,

    maxvoltagecellvalue = EXCLUDED.maxvoltagecellvalue,
    maxvltagecellnumber = EXCLUDED.maxvltagecellnumber,
    minvoltagecellvalue = EXCLUDED.minvoltagecellvalue,
    minvoltagecellnumber = EXCLUDED.minvoltagecellnumber,

    ChargeDischargeStatus = EXCLUDED.ChargeDischargeStatus,
    ChargingCurrent = EXCLUDED.ChargingCurrent,
    dischargingcurrent = EXCLUDED.dischargingcurrent,
    DeviceStatus = EXCLUDED.DeviceStatus,

    serial = EXCLUDED.serial,
    barcode = EXCLUDED.barcode,

    cellVolt1 = EXCLUDED.cellVolt1,
    cellVolt2 = EXCLUDED.cellVolt2,
    cellVolt3 = EXCLUDED.cellVolt3,
    cellVolt4 = EXCLUDED.cellVolt4,
    cellVolt5 = EXCLUDED.cellVolt5,
    cellVolt6 = EXCLUDED.cellVolt6,
    cellVolt7 = EXCLUDED.cellVolt7,
    cellVolt8 = EXCLUDED.cellVolt8,
    cellVolt9 = EXCLUDED.cellVolt9,
    cellVolt10 = EXCLUDED.cellVolt10,
    cellVolt11 = EXCLUDED.cellVolt11,
    cellVolt12 = EXCLUDED.cellVolt12,
    cellVolt13 = EXCLUDED.cellVolt13,
    cellVolt14 = EXCLUDED.cellVolt14,
    cellVolt15 = EXCLUDED.cellVolt15,
    cellVolt16 = EXCLUDED.cellVolt16,

    cellTemp1 = EXCLUDED.cellTemp1,
    cellTemp2 = EXCLUDED.cellTemp2,
    cellTemp3 = EXCLUDED.cellTemp3,
    cellTemp4 = EXCLUDED.cellTemp4,
    cellTemp5 = EXCLUDED.cellTemp5,
    cellTemp6 = EXCLUDED.cellTemp6,
    cellTemp7 = EXCLUDED.cellTemp7,
    cellTemp8 = EXCLUDED.cellTemp8,
    cellTemp9 = EXCLUDED.cellTemp9,
    cellTemp10 = EXCLUDED.cellTemp10,
    cellTemp11 = EXCLUDED.cellTemp11,
    cellTemp12 = EXCLUDED.cellTemp12,
    cellTemp13 = EXCLUDED.cellTemp13,
    cellTemp14 = EXCLUDED.cellTemp14,
    cellTemp15 = EXCLUDED.cellTemp15,
    cellTemp16 = EXCLUDED.cellTemp16,

    charging = EXCLUDED.charging,
    avgrangekm = EXCLUDED.avgrangekm,
    maxrangekm = EXCLUDED.maxrangekm,
    minrangekm = EXCLUDED.minrangekm,

    source_created_at = EXCLUDED.source_created_at,
    updated_at = NOW();
"""

def main():
    print("Last Known Data worker started...")
    while True:
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(UPSERT_SQL)
            conn.commit()
            cur.close()
            conn.close()
            print(f"Updated last_known_data @ {datetime.now()}")
        except Exception as e:
            print("Worker error:", e)

        time.sleep(2)

if __name__ == "__main__":
    main()
