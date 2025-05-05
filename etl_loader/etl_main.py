import argparse
import logging
import os
from datetime import datetime
from etl.extract import list_matching_objects, download_file
from etl.transform import transform_data
from etl.load import load_to_staging
from etl.utils import ensure_dir

# Логирование
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_etl(entity: str, start_month: str, end_month: str, load_id: str):
    logger.info(f"🚀 Запуск ETL: entity={entity}, с {start_month} по {end_month}, load_id={load_id}")
    bucket = os.getenv("MINIO_BUCKET", "srcdata")
    temp_dir = "/tmp/etl_files"
    ensure_dir(temp_dir)

    try:
        files = list_matching_objects(bucket, entity, start_month, end_month)
        logger.info(f"🔍 Найдено {len(files)} файлов: {files}")
        for key in files:
            local_path = os.path.join(temp_dir, os.path.basename(key))
            download_file(bucket, key, local_path)
            df = transform_data(local_path, entity)
            load_to_staging(df, entity, key, load_id)
            logger.info(f"✔ Файл {key} обработан и загружен.")
    except Exception as e:
        logger.error(f"❌ Ошибка ETL: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity", type=str, required=True)
    parser.add_argument("--start", type=str, required=False)
    parser.add_argument("--end", type=str, required=True)
    parser.add_argument("--load_id", type=str, required=True, help="Уникальный идентификатор загрузки (например, 20240501_123456789)")
    args = parser.parse_args()

    if not args.start:
        args.start = args.end

    run_etl(args.entity, args.start, args.end, args.load_id)

