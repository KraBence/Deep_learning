import pandas as pd
import requests
import os
from datetime import datetime

def download_financial_data(instrument, resolution, start_date, end_date,
                            data_source='alphavantage', api_key=None,
                            data_dir='../data'):
    """
    Letölti a pénzügyi adatokat és menti egyetlen CSV fájlba.

    Args:
        instrument (str): Pl. 'EURUSD', 'US500', 'XAUUSD', stb.
        resolution (str): '1min', '5min', '15min', '30min', '1H', '1D'
        start_date (str): 'YYYY-MM-DD'
        end_date (str): 'YYYY-MM-DD'
        data_source (str): 'alphavantage', 'polygon', 'finnhub'
        api_key (str): API kulcs
        data_dir (str): Mentési könyvtár

    Returns:
        str: A mentett fájl elérési útja
    """

    os.makedirs(data_dir, exist_ok=True)

    # Adatok letöltése
    data = fetch_data_from_api(instrument, resolution, start_date, end_date, data_source, api_key)

    if data is None or data.empty:
        print(f"⚠️ Nem sikerült adatot letölteni: {instrument} ({resolution})")
        return None

    # Timestamp átalakítása olvasható formátumra
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    data['timestamp'] = data['timestamp'].dt.strftime('%Y-%m-%d %H:%M')

    # CSV mentése
    filename = f"{instrument}_{resolution}.csv"
    filepath = os.path.join(data_dir, filename)
    data.to_csv(filepath, index=False)
    print(f"✅ Adatok mentve: {filepath}")

    return filepath


def fetch_data_from_api(instrument, resolution, start_date, end_date, data_source, api_key):
    """
    Adatok letöltése a kiválasztott API-ról
    """
    resolution_map = {
        'alphavantage': {
            '1min': '1min', '5min': '5min', '15min': '15min',
            '30min': '30min', '1H': '60min', '1D': 'daily'
        },
        'polygon': {
            '1min': 'minute', '5min': '5minute', '15min': '15minute',
            '30min': '30minute', '1H': 'hour', '1D': 'day'
        }
    }

    try:
        if data_source == 'alphavantage':
            return fetch_alphavantage(instrument, resolution, start_date, end_date, api_key, resolution_map)
        elif data_source == 'polygon':
            return fetch_polygon(instrument, resolution, start_date, end_date, api_key, resolution_map)
        elif data_source == 'finnhub':
            return fetch_finnhub(instrument, resolution, start_date, end_date, api_key)
        else:
            print(f"Ismeretlen adatforrás: {data_source}")
            return create_sample_data(start_date, end_date, resolution)

    except Exception as e:
        print(f"⚠️ Hiba az adatletöltés során: {e}")
        return create_sample_data(start_date, end_date, resolution)


def fetch_alphavantage(instrument, resolution, start_date, end_date, api_key, res_map):
    """Alpha Vantage adatletöltés (teszt verzió)"""
    print(f"Alpha Vantage letöltés: {instrument} ({resolution})")
    # TODO: Itt lehet beépíteni a valós API hívást
    return create_sample_data(start_date, end_date, resolution)


def fetch_polygon(instrument, resolution, start_date, end_date, api_key, res_map):
    """Polygon.io adatletöltés (teszt verzió)"""
    print(f"Polygon.io letöltés: {instrument} ({resolution})")
    # TODO: Valós API-hívás itt
    return create_sample_data(start_date, end_date, resolution)


def fetch_finnhub(instrument, resolution, start_date, end_date, api_key):
    """Finnhub adatletöltés (teszt verzió)"""
    print(f"Finnhub letöltés: {instrument} ({resolution})")
    # TODO: Valós API-hívás itt
    return create_sample_data(start_date, end_date, resolution)


def create_sample_data(start_date, end_date, resolution):
    """
    Minta adatok generálása teszteléshez.
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    freq_map = {
        '1min': '1min', '5min': '5min', '15min': '15min',
        '30min': '30min', '1H': '1H', '1D': '1D'
    }
    freq = freq_map.get(resolution, '1D')

    date_range = pd.date_range(start=start, end=end, freq=freq)
    prices = 100 + pd.Series(range(len(date_range))) * 0.1

    data = pd.DataFrame({
        'timestamp': [int(x.timestamp() * 1000) for x in date_range],
        'open': prices,
        'high': prices + 0.5,
        'low': prices - 0.5,
        'close': prices + 0.2
    })
    return data


def download_all_assets(data_source='alphavantage', api_key=None, data_dir='../data'):
    """
    Összes instrumentum és időfelbontás letöltése egyben.
    """

    instruments = ['EURUSD', 'XAUUSD', 'US30']
    resolutions = ['1min', '5min', '15min', '30min', '1H']

    start_date = '2025-01-01'
    end_date = '2025-09-30'

    all_files = []
    for inst in instruments:
        for res in resolutions:
            print(f"\n📊 Letöltés: {inst} - {res}")
            f = download_financial_data(inst, res, start_date, end_date,
                                        data_source=data_source,
                                        api_key=api_key,
                                        data_dir=data_dir)
            if f:
                all_files.append(f)

    print(f"\n✅ Összesen {len(all_files)} fájl mentve.")
    return all_files


# Példa futtatás
if __name__ == "__main__":
    API_KEY = "your_api_key_here"
    download_all_assets(data_source='alphavantage', api_key=API_KEY)