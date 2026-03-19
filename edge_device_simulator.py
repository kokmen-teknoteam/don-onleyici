import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime
import config
import sys
import pandas as pd
import macro_data_fetcher

USE_SATELLITE_DATA = "--satellite-data" in sys.argv
USE_LIVE_SATELLITE = "--live-satellite" in sys.argv
DATASET_PATH_SATELLITE = "sample_macro_data.csv"

current_state = {
    "temperature_2m": 4.5,    
    "humidity": 65.0,         
    "soil_temperature": 5.5,  
    "soil_moisture": 0.35,    
    "wind_speed": 12.0        
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("\n[Sanal Edge] MQTT Broker'a (HiveMQ) başarıyla bağlanıldı!")
    else:
        print(f"\n[Sanal Edge] MQTT Bağlantı hatası, Kod: {rc}")

def simulate_frost_drift():
    current_state["temperature_2m"] -= random.uniform(0.1, 0.4)
    current_state["soil_temperature"] -= random.uniform(0.05, 0.15)
    
    if current_state["temperature_2m"] < -5.0:
        print("\n[Sensör Simülatörü] Gece döngüsü bitti, Güneş doğuyor. Sensör değerleri normale (Sabah) sıfırlanıyor...")
        current_state["temperature_2m"] = random.uniform(4.0, 6.0)
        current_state["soil_temperature"] = random.uniform(4.0, 6.0)
        current_state["humidity"] = random.uniform(60.0, 70.0)
        current_state["soil_moisture"] = random.uniform(0.3, 0.4)
        current_state["wind_speed"] = random.uniform(8.0, 15.0)
        
    current_state["soil_moisture"] -= random.uniform(0.002, 0.006)
    if current_state["soil_moisture"] < 0:
        current_state["soil_moisture"] = 0.0
        
    current_state["humidity"] += random.uniform(0.5, 1.5)
    if current_state["humidity"] > 100:
        current_state["humidity"] = 100.0
        
    current_state["wind_speed"] -= random.uniform(0.2, 0.6)
    if current_state["wind_speed"] < 0:
        current_state["wind_speed"] = 0.0

def generate_payload():
    simulate_frost_drift()
    payload = {
        "device_id": "VIRTUAL_ESP32_DRIFT",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "latitude": config.LATITUDE,
        "longitude": config.LONGITUDE,
        "metrics": {
            "temperature_2m": round(current_state["temperature_2m"], 2),
            "humidity": round(current_state["humidity"], 2),
            "soil_temperature": round(current_state["soil_temperature"], 2),
            "soil_moisture": round(current_state["soil_moisture"], 3),
            "wind_speed": round(current_state["wind_speed"], 2)
        }
    }
    return json.dumps(payload)

def start_simulation():
    client = mqtt.Client(client_id="Virtual_Edge_Node_" + str(random.randint(1000,9999)))
    client.on_connect = on_connect
    
    print(f"[Sanal Edge Gateway] {config.MQTT_BROKER} bağlanılıyor...")
    client.connect(config.MQTT_BROKER, config.MQTT_PORT, 60)
    client.loop_start() 
    
    try:
        
        # 1. MOD: CANLI GERÇEK UYDU AKIŞI (ŞU AN VE GELECEK TAHMİNİ)
        if USE_LIVE_SATELLITE:
            print("[Sanal Edge] MOD: CANLI METEROROJOİ UYDU VERİSİ (Şu Anki Gerçek Hava)")
            try:
                # API'den şu anın gerçek 7 günlük hava tahminini kopartıyoruz
                df_live = macro_data_fetcher.get_forecast_agri_data(config.LATITUDE, config.LONGITUDE)
                print(f"[API] Open-Meteo'dan Koordinatlar ({config.LATITUDE}, {config.LONGITUDE}) için ŞU ANKİ Canlı Hava Durumu çekildi!")
                
                # Sadece şu andan (eski saatleri kırparak) sonraki tahminleri filtrele
                current_time = datetime.utcnow()
                df_live['time_dt'] = pd.to_datetime(df_live['time'])
                df_future = df_live[df_live['time_dt'] >= current_time].copy()
                
                if df_future.empty:
                     df_future = df_live # Hata varsa tüm tabloyu oynat
                     
                for index, row in df_future.iterrows():
                    payload = {
                        "device_id": "LIVE_SATELLITE_SENSOR_01",
                        "timestamp": str(row["time"]).replace(" ", "T") + "Z",
                        "latitude": config.LATITUDE,
                        "longitude": config.LONGITUDE,
                        "metrics": {
                            "temperature_2m": round(row["temperature_2m"], 2),
                            "humidity": round(row["relative_humidity_2m"], 2),
                            # Uydudan GERÇEK Toprak Nemini ve Isısını doğrudan çekiyoruz:
                            "soil_temperature": round(row.get("soil_temperature_0cm", row["temperature_2m"] - 0.5), 2),
                            "soil_moisture": round(row.get("soil_moisture_0_to_1cm", 0.35), 3),
                            "wind_speed": round(row["wind_speed_10m"], 2)
                        }
                    }
                    client.publish(config.MQTT_TOPIC_SENSORS, json.dumps(payload), qos=1) 
                    print(f" [CANLI UYDU AKIŞI] ŞU AN/GELECEK Saat: {row['time']} | Sıcaklık: {row['temperature_2m']}°C")
                    time.sleep(3.0) # Her 3 saniyede bir, gelecek 1 saatin hava durumunu AI Motoruna yolla (Hızlı Gelecek Replay'i)
                print("[Sanal Edge] 7 Günlük Canlı Meteoroloji Tahmini Tamamlandı.")
            except Exception as e:
                print(f"[HATA] Canlı uydu verisi çekilemedi: {e}")
                
        # 2. MOD: GEÇMİŞ ZAMAN MAKİNESİ (SATELLITE REPLAY)
        elif USE_SATELLITE_DATA:
            print("[Sanal Edge] MOD: GEÇMİŞ UYDU VERİSİ (Replay Streaming / Zaman Makinesi)")
            try:
                df = pd.read_csv(DATASET_PATH_SATELLITE)
                frost_indices = df[df['temperature_2m'] <= 1.0].index
                if len(frost_indices) > 0:
                    start_idx = max(0, frost_indices[0] - 10) 
                    df_subset = df.iloc[start_idx : start_idx + 100]
                else:
                    df_subset = df.head(100)
                    
                for index, row in df_subset.iterrows():
                    payload = {
                        "device_id": "PAST_SATELLITE_STATION_01",
                        "timestamp": str(row.get("time", datetime.utcnow().isoformat())) + "Z",
                        "latitude": config.LATITUDE,
                        "longitude": config.LONGITUDE,
                        "metrics": {
                            "temperature_2m": round(row["temperature_2m"], 2),
                            "humidity": round(row["relative_humidity_2m"], 2),
                            "soil_temperature": round(row["temperature_2m"] - 0.5, 2), 
                            "soil_moisture": 0.20, 
                            "wind_speed": round(row["wind_speed_10m"], 2)
                        }
                    }
                    client.publish(config.MQTT_TOPIC_SENSORS, json.dumps(payload), qos=1) 
                    print(f" [BİLİNEN/GEÇMİŞ UYDU YAYINI] Tarih: {row.get('time', 'Bilinmiyor')} | Sıcaklık: {row['temperature_2m']}°C")
                    time.sleep(3.0) 
                print("[Sanal Edge] Gerçek Geçmiş Uydu Yayını (Zaman Makinesi Replay) Tamamlandı.")
            except FileNotFoundError:
                print(f"[HATA] {DATASET_PATH_SATELLITE} uydu verisi bulunamadı!")
            
        # 3. MOD: NORMAL DRIFT SİMÜLASYONU
        else:
            print("[Sanal Edge] MOD: SENSÖR DRİFT SİMÜLASYONU (Sonsuz Döngü)\n")
            while True:
                payload = generate_payload()
                client.publish(config.MQTT_TOPIC_SENSORS, payload, qos=1) 
                print(f" [SIMÜLE YAYIN] Data: {payload}")
                time.sleep(3) 
    except KeyboardInterrupt:
        print("\n[Sanal Edge] Yayın durduruldu.")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    start_simulation()
