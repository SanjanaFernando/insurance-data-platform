from datetime import date

from extractor.extract_weather import extract, month_chunks, to_frame


def test_month_chunks_split_on_month_boundaries():
    chunks = list(month_chunks(date(2024, 1, 15), date(2024, 3, 10)))
    assert chunks == [(date(2024, 1, 15), date(2024, 1, 31)),
                      (date(2024, 2, 1), date(2024, 2, 29)),
                      (date(2024, 3, 1), date(2024, 3, 10))]


def test_to_frame_shape_and_columns():
    payload = {"daily": {"time": ["2024-01-01", "2024-01-02"],
                         "temperature_2m_max": [30.1, 29.5], "precipitation_sum": [0.0, 12.4]}}
    df = to_frame(payload)
    assert list(df.columns) == ["weather_date", "temp_max_c", "precip_mm"]
    assert len(df) == 2


def test_extract_combines_chunks_and_removes_duplicates(monkeypatch):
    def fake_fetch(session, start, end):
        return {"daily": {"time": [start.isoformat(), end.isoformat()],
                          "temperature_2m_max": [30, 31], "precipitation_sum": [0, 1]}}
    monkeypatch.setattr("extractor.extract_weather.fetch_chunk", fake_fetch)
    df = extract(date(2024, 1, 1), date(2024, 2, 29), session=object())
    assert df["weather_date"].is_unique
    assert len(df) == 4
