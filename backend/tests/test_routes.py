import pytest
from unittest.mock import patch, MagicMock

# UT-01


@patch('api.app.get_db_conn')
def test_health_ok(mock_get_db_conn, client):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock the three queries executed in the /api/health route sequentially
    mock_cursor.fetchone.side_effect = [
        {"now_utc": "2023-10-10 10:00:00Z"},
        {"n": 5},
        {"n": 5}
    ]

    response = client.get('/api/health')
    assert response.status_code == 200
    data = response.get_json()
    assert "status" in data
    assert data["status"] == "ok"

# UT-02


@patch('api.app.get_db_conn')
def test_bikes_current_returns_list(mock_get_db_conn, client):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchall.return_value = [
        {"station_id": 1, "name": "Station A", "available_bikes": 10},
        {"station_id": 2, "name": "Station B", "available_bikes": 15}
    ]

    response = client.get('/api/bikes/current')
    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) == 2

# UT-03


def test_forecast_missing_station_id(client):
    response = client.get('/api/bikes/forecast')  # No query params
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data

# UT-04


@patch('api.app.get_db_conn')
def test_weather_current_has_temperature(mock_get_db_conn, client):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db_conn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = {
        "observed_at": "2023-10-10T10:00:00Z",
        "temperature": 15.5
    }

    response = client.get('/api/weather/current')
    assert response.status_code == 200
    data = response.get_json()
    assert "temperature" in data

# UT-05


def test_login_missing_fields(client):
    response = client.post('/api/login', json={})
    assert response.status_code in (400, 401)
    assert "error" in response.get_json()

# UT-06


def test_predict_missing_params(client):
    response = client.get('/api/bikes/predict')
    assert response.status_code == 400
    assert "error" in response.get_json()
