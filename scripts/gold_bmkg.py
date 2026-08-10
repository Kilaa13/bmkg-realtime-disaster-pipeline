from pathlib import Path
import pandas as pd


def run_gold_bmkg(**context):
  # Ambil path dari Silver via XCom
  silver_path = Path('/opt/airflow/data/silver/gempaterkini_clean.csv')
  if context and 'ti' in context:
    pulled_path = context['ti'].xcom_pull(
        key='silver_file_path', task_ids='silver_task'
    )
    if pulled_path:
      silver_path = Path(pulled_path)

  df = pd.read_csv(silver_path)

  # Agregasi Ringkasan KPI
  if 'kategori_kedalaman' in df.columns and 'magnitude' in df.columns:
    summary_df = (
        df.groupby('kategori_kedalaman')
        .agg(
            total_kejadian=('magnitude', 'count'),
            avg_magnitude=('magnitude', 'mean'),
            max_magnitude=('magnitude', 'max'),
        )
        .reset_index()
    )

    summary_df['avg_magnitude'] = summary_df['avg_magnitude'].round(2)
  else:
    summary_df = pd.DataFrame()

  # Simpan ke Gold (.csv)
  output_dir = Path('/opt/airflow/data/gold')
  output_dir.mkdir(parents=True, exist_ok=True)
  output_path = output_dir / 'earthquake_summary.csv'

  summary_df.to_csv(output_path, index=False)
  print(f'[GOLD SUCCESS] Ringkasi KPI berhasil disimpan di {output_path}')

  if context and 'ti' in context:
    context['ti'].xcom_push(key='gold_file_path', value=str(output_path))

  return str(output_path)