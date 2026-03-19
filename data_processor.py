import pandas as pd
from datetime import datetime

class DataBlender:
    def __init__(self, macro_data_file="sample_macro_data.csv", fallback_macro_file="sample_nasa_historical.csv"):
        """
        Makro uydu/tahmin verilerini sistem başladığında RAM'e (önbelleğe) yükler. 
        Bu sayede milisaniyelik IoT akışımız makronun diske/apiye gitme gecikmesini beklemeden zenginleşir.
        """
        try:
            self.macro_df = pd.read_csv(macro_data_file)
            self.macro_df["time"] = pd.to_datetime(self.macro_df["time"])
            self.macro_df = self.macro_df.set_index("time")
            print(f"[DataBlender] '{macro_data_file}' belleğe başarıyla yüklendi. (Satır: {len(self.macro_df)})")
        except FileNotFoundError:
            try:
                # İkincil kaynak (Fallback API)
                self.macro_df = pd.read_csv(fallback_macro_file)
                self.macro_df["time"] = pd.to_datetime(self.macro_df["time"])
                self.macro_df = self.macro_df.set_index("time")
                print(f"[DataBlender] UYARI: Tahmin dosyası bulunamadı, fallback NASA verisi yüklendi.")
            except FileNotFoundError:
                print("[DataBlender] HATA: Hiçbir Makro veri dosyası diskte bulunamadı!")
                self.macro_df = pd.DataFrame()

    def merge_sensor_with_macro(self, sensor_payload):
        """
        Saniyede bir gelen mikro sensör JSON'u (Sensor Payload) ile 
        ona zaman olarak en yakın saatlik makro (Uydu) veriyi harmanlar.
        Yapay Zeka (AI) için zenginleştirilmiş Feature Vector (Özellik Vektörü) döndürür.
        """
        if self.macro_df.empty:
            return sensor_payload # Eğer makro çökmüşse, tarlaya kör uçuş yaptırma, sadece ham sensör ile devam et (Hata Toleransı)
            
        try:
            # 1. Sensörün o anki iletilen tam zamanı (Örn: 14:15:32)
            sensor_time = pd.to_datetime(sensor_payload["timestamp"])
            
            # 2. En yakın saate yuvarlama - Round to nearest hour (Örn: 14:00:00)
            nearest_hour = sensor_time.round("h").tz_localize(None) 
            sensor_time_naive = sensor_time.tz_localize(None)
            
            if self.macro_df.index.tz is not None:
                self.macro_df.index = self.macro_df.index.tz_localize(None)
                
            # 3. DataFrame indeksinde en yakın saati "get_indexer(method='nearest')" ile hızlıca buluyoruz
            idx = self.macro_df.index.get_indexer([nearest_hour], method="nearest")[0]
            
            if idx != -1:
                closest_macro_row = self.macro_df.iloc[idx].to_dict()
                
                # 4. Asimetrik Veri Harmanlama ve Özellik Mühendisliği (Feature Engineering)
                enriched_data = {
                    "device_id": sensor_payload.get("device_id"),
                    "exact_sensor_time": sensor_payload["timestamp"],
                    "matched_macro_hour": str(self.macro_df.index[idx]),
                    
                    # Mikro Özellikler (Tarladan Anlık Gelen)
                    "micro_temperature": sensor_payload["metrics"]["temperature_2m"],
                    "micro_humidity": sensor_payload["metrics"]["humidity"],
                    "micro_soil_temp": sensor_payload["metrics"]["soil_temperature"],
                    "micro_wind": sensor_payload["metrics"]["wind_speed"],
                    
                    # Makro Özellikler (Uydudan/Meteorolojik Modelden O Saat İçin Çekilen)
                    "macro_dew_point": closest_macro_row.get(
                        "dew_point_2m", closest_macro_row.get("T2MDEW")
                    ), 
                    "macro_generic_temp": closest_macro_row.get(
                        "temperature_2m", closest_macro_row.get("T2M")
                    ),
                    "macro_wind": closest_macro_row.get(
                        "wind_speed_10m", closest_macro_row.get("WS10M")
                    )
                }
                
                # Sırf teşhis için sentetik özellik (Risk Diferansiyeli): Makro Tahmin ile Tarlanın Sensör Uyuşmazlığı Ne Kadar?
                if enriched_data.get("macro_generic_temp") is not None:
                    # Tarlanın bölgesel soğukluğa göre farkı (Burası bir çöküntü alanı mı?)
                    enriched_data["temp_diff_micro_macro"] = round(
                        enriched_data["micro_temperature"] - enriched_data["macro_generic_temp"], 2
                    )
                    
                return enriched_data
            else:
                return sensor_payload
                
        except Exception as e:
            print(f"[DataBlender] Zaman hizalama asimetri hatası: {e}")
            return sensor_payload
