import unittest
from unittest import mock
from unittest.mock import patch, MagicMock, mock_open
import psycopg2
from io import StringIO
import os
from etl import extract_data, transform_data, load_data

class TestETLProcess(unittest.TestCase):

    @patch('psycopg2.connect')
    @patch('os.environ.get')
    def test_extract_data(self, mock_getenv, mock_connect):
        # Simulate running inside Docker by setting the environment variable
        mock_getenv.return_value = "RUNNING_IN_DOCKER"

        # Set up mock cursor and connection
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = [
            (1, 101, 22.5, 60.1, "2023-01-01 10:00:00"),
            (2, 102, 23.0, 61.2, "2023-01-01 11:00:00"),
        ]
        mock_connect.return_value = mock_conn

        # Run the function
        data = extract_data()

        # Assertions
        mock_connect.assert_called_once_with(
            host="host.docker.internal", dbname="data_engineering", user="postgres", password="password", port=5432
        )
        mock_cursor.execute.assert_called_once_with("Select * from sensor_data;")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], (1, 101, 22.5, 60.1, "2023-01-01 10:00:00"))

    @patch('psycopg2.connect')
    @patch('os.environ.get')
    def test_extract_data_local(self, mock_getenv, mock_connect):
        # Simulate running locally by not setting the environment variable
        mock_getenv.return_value = None

        # Set up mock cursor and connection
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = [
            (1, 101, 22.5, 60.1, "2023-01-01 10:00:00"),
            (2, 102, 23.0, 61.2, "2023-01-01 11:00:00"),
        ]
        mock_connect.return_value = mock_conn

        # Run the function
        data = extract_data()

        # Assertions
        mock_connect.assert_called_once_with(
            host="localhost", dbname="data_engineering", user="postgres", password="password", port=5432
        )
        mock_cursor.execute.assert_called_once_with("Select * from sensor_data;")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], (1, 101, 22.5, 60.1, "2023-01-01 10:00:00"))

    def test_transform_data(self):
        # Input data
        data = [
            (1, 101, 22.5, 60.1, "2023-01-01 10:00:00"),
            (2, 102, 23.0, 61.2, "2023-01-01 11:00:00"),
        ]

        # Expected transformed data
        expected_transformed = [
            {"id": 1, "sensor_id": 101, "temperature": 22.5, "humidity": 60.1, "recorded_at": "2023-01-01 10:00:00"},
            {"id": 2, "sensor_id": 102, "temperature": 23.0, "humidity": 61.2, "recorded_at": "2023-01-01 11:00:00"},
        ]

        # Run the function
        transformed = transform_data(data)

        # Assertions
        self.assertEqual(transformed, expected_transformed)

    @patch('builtins.open', new_callable=mock_open, read_data=b"data" * 256)
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.remove')
    @patch('os.stat')
    @patch('etl.Minio')
    def test_load_data(self, mock_minio, mock_stat, mock_remove, mock_tempfile, mock_open):
        # Mock temp file
        mock_temp = MagicMock()
        mock_temp.name = 'tempfile.csv'
        mock_tempfile.return_value.__enter__.return_value = mock_temp

        # Mock stat result
        mock_stat.return_value.st_size = 1024

        # Mock MinIO client
        mock_minio_client = MagicMock()
        mock_minio.return_value = mock_minio_client

        # Mock transformed data
        transformed = [
            {"id": 1, "sensor_id": 101, "temperature": 22.5, "humidity": 60.1, "recorded_at": "2023-01-01 10:00:00"}
        ]

        # Run the function
        load_data(transformed)

        # Assertions
        mock_tempfile.assert_called_once_with(mode="w", newline="", delete=False, suffix=".csv")
        mock_stat.assert_called_once_with('tempfile.csv')
        mock_minio.assert_called_once_with(
            "localhost:9000",
            access_key="admin",
            secret_key="password",
            secure=False
        )
        mock_minio_client.put_object.assert_called_once_with(
            "datalake-ci-cd-test",
            object_name=mock.ANY,
            data=mock.ANY,
            length=1024,
            content_type="application/csv"
        )
        mock_remove.assert_called_once_with('tempfile.csv')

if __name__ == "__main__":
    unittest.main()
