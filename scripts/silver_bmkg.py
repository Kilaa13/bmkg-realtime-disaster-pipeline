import json
from pathlib import Path
import pandas as pd
from scripts.notifications import send_telegram_msg



def check_and_alert_disaster(df):
    """Mengecek apakah ada gempa dengan magnitudo >= 6.0."""
    high_mag_eq = df[df['magnitude'] >= 6.0]
    
    if not high_mag_eq.empty:
        for _, row in high_mag_eq.iterrows():
            msg = (
                f"🚨 *PERINGATAN GEMPA BUMI POTENSIAL* 🚨\n\n"
                f"📍 *Wilayah:* {row.get('wilayah', 'N/A')}\n"
                f"📊 *Magnitudo:* `{row.get('magnitude')} SR`\n"
                f"🌊 *Kedalaman:* `{row.get('kedalaman_km')} km` ({row.get('kategori_kedalaman')})\n"
                f"⏰ *Waktu Kejadian:* `{row.get('datetime')}`"
            )
            send_telegram_msg(msg)
            
def run_silver_bmkg(**context):
  bronze_path = Path('/opt/airflow/data/bronze/gempaterkini.json')
  if context and 'ti' in context:
    pulled_path = context['ti'].xcom_pull(
        key='bronze_file_path', task_ids='bronze_task'
    )
    if pulled_path:
      bronze_path = Path(pulled_path)

  with open(bronze_path, 'r', encoding='utf-8') as f:
    raw_data = json.load(f)

  gempa_list = raw_data.get('Infogempa', {}).get('gempa', [])
  if isinstance(gempa_list, dict):
    gempa_list = [gempa_list]

  df = pd.DataFrame(gempa_list)

  if not df.empty:
    if 'Coordinates' in df.columns:
      coords = df['Coordinates'].str.split(',', expand=True)
      df['latitude'] = pd.to_numeric(coords[0], errors='coerce')
      df['longitude'] = pd.to_numeric(coords[1], errors='coerce')

    if 'Magnitude' in df.columns:
      df['magnitude'] = pd.to_numeric(df['Magnitude'], errors='coerce')

    if 'Kedalaman' in df.columns:
      df['kedalaman_km'] = (
          df['Kedalaman'].str.extract(r'(\d+)').astype(float)
      )

      def categorize_depth(depth):
        if depth <= 60:
          return 'Dangkal'
        elif depth <= 300:
          return 'Menengah'
        else:
          return 'Dalam'

      df['kategori_kedalaman'] = df['kedalaman_km'].apply(categorize_depth)

  output_dir = Path('/opt/airflow/data/silver')
  output_dir.mkdir(parents=True, exist_ok=True)
  output_path = output_dir / 'gempaterkini_clean.csv'

  df.to_csv(output_path, index=False)
  print(
      f'[SILVER SUCCESS] Data berhasil dibersihkan & disimpan di {output_path}'
  )

  if context and 'ti' in context:
    context['ti'].xcom_push(key='silver_file_path', value=str(output_path))
            
  return str(output_path)