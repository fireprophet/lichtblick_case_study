import boto3
import os
from typing import List
import re

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )

def list_matching_objects(bucket: str, entity: str, start: str, end: str) -> List[str]:
    print(f"[DEBUG] list_matching_objects(bucket={bucket}, entity={entity}, start={start}, end={end})")
    s3 = get_s3_client()

    # Проверка подключения
    try:
        s3.head_bucket(Bucket=bucket)
        print("[DEBUG] ✅ Доступ к S3 получен")
    except Exception as e:
        print(f"[DEBUG] ❌ Ошибка доступа к S3: {e}")
        raise

    response = s3.list_objects_v2(Bucket=bucket)
    matching_files = []

    entities = [e.strip() for e in entity.split(",")]

    for obj in response.get("Contents", []):
        key = obj["Key"]
        for ent in entities:
            pattern = r"^(\d{6})\d{8}_" + re.escape(ent) + r"\.csv$"
            match = re.match(pattern, key)
            if match:
                yyyymm = match.group(1)
                if start <= yyyymm <= end:
                    print(f"[DEBUG] ✅ Файл подходит: {key}")
                    matching_files.append(key)
                else:
                    print(f"[DEBUG] ⏩ Пропуск {key} — {yyyymm} не в диапазоне")
    return matching_files


def download_file(bucket: str, key: str, dest_path: str):
    s3 = get_s3_client()
    s3.download_file(bucket, key, dest_path)