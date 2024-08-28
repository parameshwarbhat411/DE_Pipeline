import tempfile

import psycopg2
import csv
from datetime import datetime
from minio import Minio
import os
from minio.error import S3Error

def extract_data():
    conn = psycopg2.connect(host="localhost", dbname="data_engineering", user="postgres", password="password", port= 5432)
    cursor = conn.cursor()
    cursor.execute("Select * from sensor_data;")
    data = cursor.fetchall()
    conn.close()
    return data

def transform_data(data):
    transformed = []
    for row in data:
        transformed.append({
            "id": row[0],
            "sensor_id": row[1],
            "temperature": row[2],
            "humidity": row[3],
            "recorded_at": row[4],
        })
    return transformed

def load_data(transformed):
    with tempfile.NamedTemporaryFile(mode="w", newline="", delete=False, suffix=".csv") as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=["id", "sensor_id", "temperature", "humidity", "recorded_at"])
        writer.writeheader()
        writer.writerows(transformed)

        # Get the name of the temporary file
        temp_file_name = temp_file.name

    # Initialize MinIO client
    minio_client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )

    # Upload the file to the MinIO bucket
    try:
        with open(temp_file_name, "rb") as file_data:
            file_stat = os.stat(temp_file_name)
            minio_client.put_object(
                "datalake-ci-cd-test",
                object_name=f"sensor_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
                data=file_data,
                length=file_stat.st_size,
                content_type="application/csv"
            )
        print(f"file uploaded successfully to MinIO bucket 'datalake-ci-cd-test'.")
    except S3Error as e:
        print(f"Failed to upload temporary file to MinIO: {e}")
    finally:
        # Cleanup: delete the temporary file
        os.remove(temp_file_name)

if __name__ == "__main__":
    data = extract_data()
    transformed = transform_data(data)
    load_data(transformed)