import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

MODEL_FILENAME = "frost_model_kaggle.joblib"
DATASET_PATH = "archive/DATASET.csv"

def auto_label_frost_events_spanish(df):
    """
    Kaggle (Peru) Veri Setini (İspanyolca) agronomik termodinamik kurallarla etiketler.
    1 = Don Tehdidi Oldu (Frost), 0 = Don Yok
    """
    print("[Kaggle AI Motoru] Veri seti bilimsel kurallarla (Auto-Labeling) etiketleniyor...")
    
    # Koşulları vektör tabanlı mantık uygulayarak hızla atıyoruz
    condition1 = df['temp_2m'] <= 1.0
    condition2 = (df['temp_2m'] <= 2.5) & (abs(df['temp_2m'] - df['punto_rocio_2m']) <= 1.5)
    
    df['frost_target'] = np.where(condition1 | condition2, 1, 0)
    return df

def generate_and_train_kaggle_model():
    print(f"\n=================================================================")
    print(f"|    KAGGLE (PERU) BİLİMSEL VERİ SETİ AI MOTORU EĞİTİMİ         |")
    print(f"=================================================================\n")
    print(f"[Aşama 1]: '{DATASET_PATH}' dosyası RAM'e alınıyor...")
    
    df = pd.read_csv(DATASET_PATH)
    
    # Sadece sensörlerin tam olduğu yerleri alıyoruz
    df = df.dropna()
    print(f"[Aşama 1 Başarılı]: Kaggle Verisi yüklendi. Orijinal Satır Sayısı: {len(df)}")
    
    # RAM şişmesini önlemek ve eğitimi saniyeler içinde bitirmek için Rastgele 200.000 satır (Örneklem) alıyoruz.
    # Bu da yaklaşık 22 yıllık bir tarlanın saatlik verisine bedeldir, A/B testi için fazlasıyla yeterlidir.
    if len(df) > 200000:
        print("[Sistem] Hız optimizasyonu için dev veritabanından rastgele 200.000 satır örneklem (Sample) çekiliyor...")
        df = df.sample(n=200000, random_state=42)
    
    print("\n[Aşama 2]: Otonom Don Etiketlemesi (Auto-Labeling) başlatıldı...")
    df_labeled = auto_label_frost_events_spanish(df)
    
    num_frosts = df_labeled['frost_target'].sum()
    print(f"-> Etiketleme Bitti! 200.000 saatlik bu örneklemde tespit edilen Radyasyon Donu Olayı: {num_frosts} Saat")
    
    # Özellikler (Features - İspanyolca Kolon İsimleri)
    features = [
        "temp_2m",         # Sıcaklık
        "humedad_rel_2m",  # Bağıl Nem
        "punto_rocio_2m",  # Çiğ Noktası
        "viento_10m",      # 10m Rüzgar
        "humedad_suelo"    # Toprak Nemi (Peru setinde ekstra olarak var!)
    ]
    
    X = df_labeled[features]
    y = df_labeled['frost_target']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    print(f"\n[Aşama 3]: Scikit-Learn 'Random Forest' Modeli Eğitiliyor (Kaggle Version)...")
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced", n_jobs=-1)
    
    rf_model.fit(X_train, y_train)
    
    print("\n[Aşama 4]: Eğitim Başarılı! \nModel %20 ayrılmış test (sınav) verisi üzerinden geçiriliyor...")
    y_pred = rf_model.predict(X_test)
    
    acc = accuracy_score(y_test, y_pred)
    print(f"\n======== KAGGLE DÜNYA MODELİ SONUÇLARI (PERFORMANS) ========")
    print(f"Genel Doğruluk (Accuracy) Raporu   : %{round(acc * 100, 2)}")
    
    print("\nDetaylı Makine Öğrenmesi Raporu (Classification Report):")
    print(classification_report(y_test, y_pred, target_names=["Normal (0)", "Don Riski (1)"]))
    
    importance = rf_model.feature_importances_
    print("\nÖzellik Önem Sırası (Peru Açık Hava Verisine Göre Model Karar Alırken Neye Baktı?):")
    for fname, imp in zip(features, importance):
         print(f" -> {fname} : %{round(imp*100, 2)}")
    
    joblib.dump(rf_model, MODEL_FILENAME)
    print(f"\n[SİSTEM]: Kaggle Ağırlık Dosyası '{MODEL_FILENAME}' başarıyla donduruldu ve kaydedildi.")

if __name__ == "__main__":
    generate_and_train_kaggle_model()
