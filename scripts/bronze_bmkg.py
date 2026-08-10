import json
from pathlib import Path
import requests


def run_bronze_bmkg(**context):
  url = 'https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json'

  # Tambahkan User-Agent agar tidak diblokir (403 Forbidden) oleh BMKG
  headers = {
      'User-Agent': (
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          ' (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      )
  }

  response = requests.get(url, headers=headers, timeout=15)
  response.raise_for_status()

  data = response.json()

  # Simpan ke folder bronze
  output_dir = Path('/opt/airflow/data/bronze')
  output_dir.mkdir(parents=True, exist_ok=True)

  output_path = output_dir / 'gempaterkini.json'
  with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

  print(f'[BRONZE SUCCESS] Data berhasil disimpan di {output_path}')

  # Kirim path file ke XCom untuk dibaca task selanjutnya (Silver)
  if context and 'ti' in context:
    context['ti'].xcom_push(key='bronze_file_path', value=str(output_path))

  return str(output_path)