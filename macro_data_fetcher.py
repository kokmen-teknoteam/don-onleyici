import requests
import pandas as pd
import config
from datetime import datetime, timedelta

def get_forecast_agri_data(lat=config.LATITUDE, lon=config.LONGITUDE, days=5):
    """
    Belirtilen koordinat için gelecek günlerin saatlik hava tahmin verisini çeker.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(getattr(config, 'HOURLY_FORECAST_VARIABLES', config.HOURLY_VARIABLES)),
        "timezone": "auto",
        "forecast_days": days
    }
    response = requests.get(config.FORECAST_API_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    hourly_data = data.get("hourly", {})
    
    if not hourly_data:
        raise ValueError("API'den beklenen 'hourly' tahmin verisi alınamadı.")
        
    df = pd.DataFrame(hourly_data)
    df["time"] = pd.to_datetime(df["time"])
    return df

def get_historical_agri_data(lat=config.LATITUDE, lon=config.LONGITUDE, days_back=365):
    """
    Geçmiş hava verisini çeker. (Otomatik Etiketleme ve Eğitim için)
    """
    end_date = datetime(2023, 12, 31)
    start_date = end_date - timedelta(days=days_back)
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(config.HOURLY_VARIABLES),
        "timezone": "auto",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    response = requests.get(config.HISTORICAL_API_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    hourly_data = data.get("hourly", {})
    
    if not hourly_data:
        raise ValueError("API'den beklenen 'hourly' geçmiş verisi alınamadı.")
        
    df = pd.DataFrame(hourly_data)
    df["time"] = pd.to_datetime(df["time"])
    return df

def get_historical_nasa_data(lat=config.LATITUDE, lon=config.LONGITUDE, days_back=10):
    """
    NASA POWER API üzerinden geçmiş saatlik tarımsal meteoroloji verilerini çeker.
    Dikkat: NASA POWER gelecek tahminleri sunmaz, geçmiş veriler (Model Eğitimi) için mükemmeldir.
    Veriler 'Near Real-Time' olduğu için genellikle 1-2 gün gecikmelidir.
    """
    end_date = datetime.now() - timedelta(days=2) # NASA data is 1-2 days delayed
    start_date = end_date - timedelta(days=days_back)
    
    params = {
        "parameters": ",".join(config.NASA_VARIABLES),
        "community": "AG", # Agroclimatology
        "longitude": lon,
        "latitude": lat,
        "format": "JSON",
        "start": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d")
    }
    
    response = requests.get(config.NASA_POWER_API_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    parameters = data.get("properties", {}).get("parameter", {})
    
    if not parameters:
        raise ValueError("NASA API'den beklenen parametreler alınamadı.")
        
    timestamps = list(parameters[config.NASA_VARIABLES[0]].keys())
    
    records = []
    for ts in timestamps:
        row = {"time": ts}
        for var in config.NASA_VARIABLES:
            row[var] = parameters.get(var, {}).get(ts, None)
        records.append(row)
        
    df = pd.DataFrame(records)
    # Convert '2023010123' string format to real datetime
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d%H")
    return df

if __name__ == "__main__":
    print(f"[{config.LATITUDE}, {config.LONGITUDE}] koordinatları için API üzerinden makro veriler çekiliyor...\n")
    try:
        # 1. Open-Meteo Forecast
        df_forecast = get_forecast_agri_data(days=3)
        print("======== İLK 5 SAATLİK TAHMİN VERİSİ (OPEN-METEO) ========")
        print(df_forecast.head())
        
        output_file = "sample_macro_data.csv"
        df_forecast.to_csv(output_file, index=False)
        print(f"\n[BAŞARILI] Örnek Tahmin verisi '{output_file}' dosyasına kaydedildi!")
        
        print("\n------------------------------------------------------------\n")
        
        # 2. NASA POWER Historical
        print(f"[{config.LATITUDE}, {config.LONGITUDE}] koordinatları için NASA POWER Geçmiş verileri çekiliyor...\n")
        df_nasa = get_historical_nasa_data(days_back=5)
        print("======== İLK 5 SAATLİK GEÇMİŞ VERİ (NASA POWER) ========")
        print(df_nasa.head())
        
        nasa_output_file = "sample_nasa_historical.csv"
        df_nasa.to_csv(nasa_output_file, index=False)
        print(f"\n[BAŞARILI] Örnek NASA verisi '{nasa_output_file}' dosyasına kaydedildi!")
        
    except Exception as e:
        print(f"[HATA]: Veri çekme servisi başarısız oldu: {str(e)}")
