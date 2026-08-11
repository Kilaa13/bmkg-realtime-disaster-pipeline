import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook
import re

def parse_bmkg_datetime(date_str):
    """
    Mengubah format tanggal BMKG (misal '09 Agu 2026' / '09 Agu 2026, 12:00:00')
    menjadi format ISO standar 'YYYY-MM-DD HH:MM:SS' untuk Snowflake.
    """
    if not date_str or pd.isna(date_str):
        return None

    date_str = str(date_str).strip()

    month_map = {
        'Mei': 'May',
        'Agu': 'Aug',
        'Agus': 'Aug',
        'Okt': 'Oct',
        'Des': 'Dec'
    }

    for id_month, en_month in month_map.items():
        date_str = re.sub(rf'\b{id_month}\b', en_month, date_str, flags=re.IGNORECASE)

    try:
        parsed_dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(parsed_dt):
            return parsed_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        pass
    
    return None

def run_load_snowflake(**context):
    
    gold_path = '/opt/airflow/data/gold/earthquake_summary.csv'
    if context and 'ti' in context:
        pulled_gold = context['ti'].xcom_pull(key='gold_file_path', task_ids='gold_task')
        if pulled_gold:
            gold_path = pulled_gold

    df_gold = pd.read_csv(gold_path)

    silver_path = '/opt/airflow/data/silver/earthquake_detail.csv'
    if context and 'ti' in context:
        pulled_silver = context['ti'].xcom_pull(key='silver_file_path', task_ids='silver_task')
        if pulled_silver:
            silver_path = pulled_silver

    df_silver = pd.read_csv(silver_path)
    conn_obj = BaseHook.get_connection('snowflake_conn')

    ctx = snowflake.connector.connect(
        user=conn_obj.login,
        password=conn_obj.password,
        account=conn_obj.extra_dejson.get('account', 'evuiaqu-gn98819'),
        warehouse='COMPUTE_WH',
        database='BMKG_DB'
    )
    cs = ctx.cursor()

    for _, row in df_gold.iterrows():
        query_gold = f"""
        INSERT INTO BMKG_DB.KPI.EARTHQUAKE_SUMMARY 
        (kategori_kedalaman, total_kejadian, avg_magnitude, max_magnitude)
        VALUES ('{row['kategori_kedalaman']}', {row['total_kejadian']}, {row['avg_magnitude']}, {row['max_magnitude']});
        """
        cs.execute(query_gold)

    for idx, row in df_silver.iterrows():
        raw_wilayah = row.get('wilayah', row.get('Wilayah', ''))
        if pd.notna(raw_wilayah) and raw_wilayah:
           
            wilayah_bersih = re.sub(r'^\d+\s*km\s*\w+\s*', '', str(raw_wilayah)).strip()
        else:
            wilayah_bersih = ''

        wilayah_clean = wilayah_bersih.replace("'", "''")
        
        raw_potensi = row.get("potensi", row.get("Potensi", ""))
        potensi_clean = (
            str(raw_potensi).replace("'", "''") if pd.notna(raw_potensi) else ""
        )
        
        event_id = row.get('event_id', f"eq_{idx}")
        raw_dt = row.get('datetime', row.get('DateTime', row.get('Tanggal', '')))
        parsed_dt = parse_bmkg_datetime(raw_dt)
        dt_sql_val = f"'{parsed_dt}'" if parsed_dt else "CURRENT_TIMESTAMP()"

        mag = row.get('magnitude', row.get('Magnitude', 0))
        depth = row.get('kedalaman_km', row.get('Kedalaman', 0))
        kat_depth = row.get('kategori_kedalaman', '')
        lat = row.get('latitude', row.get('Latitude', 0))
        lon = row.get('longitude', row.get('Longitude', 0))
        coords = f"{lat},{lon}"

        query_silver = f"""
        INSERT INTO BMKG_DB.SILVER.EARTHQUAKE_DETAIL 
        (event_id, datetime, wilayah, magnitude, kedalaman_km, kategori_kedalaman, latitude, longitude, coordinates, potensi)
        VALUES ('{event_id}', {dt_sql_val}, '{wilayah_clean}', {mag}, {depth}, '{kat_depth}', {lat}, {lon}, '{coords}', '{potensi_clean}');
        """
        cs.execute(query_silver)
        
    cs.execute("COMMIT;")
    cs.close()
    ctx.close()
    print('[SNOWFLAKE SUCCESS] Data Gold & Silver berhasil dimasukkan ke Snowflake!')
