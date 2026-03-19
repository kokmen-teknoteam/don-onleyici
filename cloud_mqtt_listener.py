import paho.mqtt.client as mqtt
import json
import random
import config
import time
import joblib
import pandas as pd

from data_processor import DataBlender

# Blender'ı ayağa kaldırıp Makro tahminleri RAM'e alıyoruz
blender = DataBlender(macro_data_file="sample_macro_data.csv")

try:
    rf_model_hyperlocal = joblib.load("frost_model_hyperlocal.joblib")
    print("\n[Sistem] 🧠 Motor 1: Hiper-Yerel AI Modeli yüklendi!")
except FileNotFoundError:
    print("\n[Sistem] HATA: Hiper-Yerel model bulunamadı.")
    rf_model_hyperlocal = None

try:
    rf_model_kaggle = joblib.load("frost_model_kaggle.joblib")
    print("[Sistem] 🧠 Motor 2: Kaggle Dünya AI Modeli yüklendi!")
except FileNotFoundError:
    print("[Sistem] HATA: Kaggle modeli bulunamadı.")
    rf_model_kaggle = None

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"\n[Bulut] 'Çift Motorlu (Ensemble)' IoT Bulut Katmanına başarıyla bağlanıldı!")
        print(f"[Bulut] Abone olunan kanal: {config.MQTT_TOPIC_SENSORS}\n")
        client.subscribe(config.MQTT_TOPIC_SENSORS, qos=1)
    else:
        print(f"[Bulut] Bağlantı Hatası: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    
    try:
        raw_sensor_data = json.loads(payload)
        enriched_vector = blender.merge_sensor_with_macro(raw_sensor_data)
        
        print(f"\n============= [ÇİFT MOTORLU (ENSEMBLE) INFERENCE KÖPRÜSÜ] =============")
        temp = enriched_vector.get("micro_temperature")
        
        if rf_model_hyperlocal is not None and rf_model_kaggle is not None and temp is not None:
            
            features_hyperlocal = pd.DataFrame([{
                "temperature_2m": enriched_vector.get("micro_temperature"),
                "relative_humidity_2m": enriched_vector.get("micro_humidity"),
                "dew_point_2m": enriched_vector.get("macro_dew_point"),
                "wind_speed_10m": enriched_vector.get("micro_wind", enriched_vector.get("macro_wind"))
            }])
            
            features_kaggle = pd.DataFrame([{
                "temp_2m": enriched_vector.get("micro_temperature"),
                "humedad_rel_2m": enriched_vector.get("micro_humidity"),
                "punto_rocio_2m": enriched_vector.get("macro_dew_point"),
                "viento_10m": enriched_vector.get("micro_wind", enriched_vector.get("macro_wind")),
                "humedad_suelo": raw_sensor_data["metrics"].get("soil_moisture", 0.25)
            }])
            
            pred_local = rf_model_hyperlocal.predict(features_hyperlocal)[0]
            prob_local = rf_model_hyperlocal.predict_proba(features_hyperlocal)[0][1]
            
            pred_kaggle = rf_model_kaggle.predict(features_kaggle)[0]
            prob_kaggle = rf_model_kaggle.predict_proba(features_kaggle)[0][1]
            
            print(f" 🧠 [Motor 1 / YEREL] Risk: %{round(prob_local * 100, 1)} | Sonuç: {'🚨 RİSKLİ' if pred_local==1 else '🟢 NORMAL'}")
            print(f" 🧠 [Motor 2 / DÜNYA] Risk: %{round(prob_kaggle * 100, 1)} | Sonuç: {'🚨 RİSKLİ' if pred_kaggle==1 else '🟢 NORMAL'}")
            
            print("\n  >> OTONOM MÜDAHALE MERKEZİ (ACTUATION) <<")
            
            # --- GUI ve Frontend (Dashboard) için Anlık Durum JSON'u Gönder ---
            status_dict = {
                "timestamp": enriched_vector.get("exact_sensor_time"),
                "device_id": enriched_vector.get("device_id"),
                "micro_temperature": temp,
                "micro_humidity": enriched_vector.get("micro_humidity"),
                "micro_soil_temp": enriched_vector.get("micro_soil_temp"),
                "micro_wind": enriched_vector.get("micro_wind"),
                "macro_dew_point": enriched_vector.get("macro_dew_point"),
                "prob_local": round(prob_local * 100, 1),
                "prob_kaggle": round(prob_kaggle * 100, 1),
                "alert_level": 0 
            }
            
            if pred_local == 1 and pred_kaggle == 1:
                 print("  🚨 [KIRMIZI ALARM]: İKİ YAPAY ZEKA UZMANI OY BİRLİĞİ İLE RADYASYON DONU İLAN ETTİ! (POMPALAR AÇILDI)")
                 status_dict["alert_level"] = 2
            elif pred_local == 1 or pred_kaggle == 1:
                 print("  ⚠️ [SARI UYARI]: Yalnızca bir model risk görüyor. (ÇİFTÇİYE SMS ATILDI / POMPALAR UYKU MODUNDA)")
                 status_dict["alert_level"] = 1
            else:
                 print("  ✅ [GÜVENLİ ONAYI]: Her iki Makine Öğrenimi de tarlada dona elverişsiz hava şartlarında hemfikir.")
                 status_dict["alert_level"] = 0
                 
            # JSON olarak GUI'nin çekmesi için diske dondur.
            with open("latest_status.json", "w") as f:
                json.dump(status_dict, f)
                 
        else:
            if temp is not None and temp <= 1.0:
                print(f"  > [UYARI] Modeller aktif değil. Isı donma tehlikesinde: {temp}°C")
            
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f" -> Otonom İşleme Hatası : {e}")

def start_listening():
    client = mqtt.Client(client_id="Cloud_Dual_AI_Node_" + str(random.randint(1000,9999)))
    client.on_connect = on_connect
    client.on_message = on_message
    
    print(f"[Sistem] Endüstriyel Bulut MQTT ({config.MQTT_BROKER}) dinleniyor...")
    client.connect(config.MQTT_BROKER, config.MQTT_PORT, 60)
    
    try:
        client.loop_forever() 
    except KeyboardInterrupt:
        print("\n[Bulut] Dinleme Durduruldu.")
    finally:
        client.disconnect()

if __name__ == "__main__":
    start_listening()
