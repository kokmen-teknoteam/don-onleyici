import pandas as pd
import numpy as np
import joblib
import config
from macro_data_fetcher import get_historical_agri_data
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

MODEL_FILENAME = "frost_model_hyperlocal.joblib"

def auto_label_frost_events(df):
    """
    Otonom Etiketleyici: Geçmiş hava durumu saatlerini termodinamik/zirai kurallarla etiketler.
    1 = Don Tehdidi Oldu (Frost), 0 = Don Yok
    Kural: 
    - Hava sıcaklığı 1.0°C altındaysa kesin "Radyasyon veya Adveksiyon" donudur.
    - Hava 2.5°C altında VE Çiğ noktasına çok yakınsa (Nem doygunluğu) KESİN DON (Frost) riski olarak sınıflandırılır.
    """
    print("[AI Motoru] Agronomik termodinamik şartlara göre 26.000 satır veri otonom olarak işlenip '1/0' etiketleniyor...")
    
    # Koşulları vektör tabanlı mantık (Pandas Vectorization) uygulayarak hızla atıyoruz
    condition1 = df['temperature_2m'] <= 1.0
    condition2 = (df['temperature_2m'] <= 2.5) & (abs(df['temperature_2m'] - df['dew_point_2m']) <= 1.5)
    
    # Hedef Kolon. Yapay zekaya verilecek "Sınav Sorularının Cevapları" (Ground Truth)
    df['frost_target'] = np.where(condition1 | condition2, 1, 0)
    
    return df

def generate_and_train_hyperlocal_model():
    print(f"\n=================================================================")
    print(f"|  HİPER-YEREL (KENDİ DON VERİMİZİ ÖĞRENEN) AI MOTORU EĞİTİMİ   |")
    print(f"=================================================================\n")
    print(f"[Aşama 1]: {config.LATITUDE}, {config.LONGITUDE} koordinatları için son 1 YILLIK ARŞİV verisi indiriliyor...")
    print("(Geriye dönük 8.700+ saatlik hava durumu verisi... Lütfen bekleyiniz)")
    
    # 1 yıllık sürekli saatlik veri Open-Meteo Archive API üzerinden çekiliyor
    df_history = get_historical_agri_data(days_back=365)
    
    # Sensör boşlukları olan NaN (Eksik Veri) satırları eleniyor (Data Cleaning)
    df_history = df_history.dropna()
    
    print(f"[Aşama 1 Başarılı]: Veri çekildi. Veritabanındaki Toplam Saat / Satır: {len(df_history)}")
    
    print("\n[Aşama 2]: Otonom Don Etiketlemesi (Auto-Labeling) başlatıldı...")
    df_labeled = auto_label_frost_events(df_history)
    
    num_frosts = df_labeled['frost_target'].sum()
    print(f"-> Etiketleme Bitti! Son 3 yılda bu tarlada fiilen tespit edilen Donma/Frost Saati: {num_frosts} Saat")
    
    # Modeli Eğiteceğimiz Bilgi Kolonları (Feature Selection)
    features = [
        "temperature_2m", 
        "relative_humidity_2m", 
        "dew_point_2m", 
        "wind_speed_10m"
    ]
    
    X = df_labeled[features]
    y = df_labeled['frost_target']
    
    # Verimizi Test (%20) ve Gerçek Eğitim (%80) Olarak İkiye Bölüyoruz (Cross-Validation Vorbereitung)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    print(f"\n[Aşama 3]: Scikit-Learn 'Random Forest' (Rastgele Orman Sınıflandırıcısı) Yapay Zekası Eğitiliyor (Training)...")
    # Ağaç sayısı 100, sınıf dengesizliği olduğu için (don olaran nadir durum) class_weight="balanced" kullanıyoruz.
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced", n_jobs=-1)
    
    # Beyni Yetiştirme: (Sınav Sorularına Nasıl Çözüm Üreteceğini Öğren)
    rf_model.fit(X_train, y_train)
    
    print("\n[Aşama 4]: Eğitim Başarılı! \nHepsi kenara ayrılmış (modelin daha önce hiç görmediği) %20'lik Test Verisi üzerinde SINAV Uygulanıyor (Inference)...")
    y_pred = rf_model.predict(X_test)
    
    # METRİKLER (Model ne kadar isabetli?)
    acc = accuracy_score(y_test, y_pred)
    print(f"\n======== HİPER-YEREL MODEL SONUÇLARI (PERFORMANS) ========")
    print(f"Genel Doğruluk (Accuracy) Raporu   : %{round(acc * 100, 2)}")
    
    print("\nDetaylı Makine Öğrenmesi Raporu (Classification Report):")
    # Sadece doğruluğa değil, don olaylarını ne kadar yakaladığına (Recall) bilimsel olarak da bakıyoruz
    print(classification_report(y_test, y_pred, target_names=["Normal (0)", "Don Riski (1)"]))
    
    # Feature Importance (Hangi sensöre daha çok güvendi?)
    importance = rf_model.feature_importances_
    print("\nÖzellik Önem Sırası (Model Karar Alırken En Çok Hangi Bilgiye Güvendi?):")
    for fname, imp in zip(features, importance):
         print(f" -> {fname} : %{round(imp*100, 2)}")
    
    # Eğitilen Nöral Yolu Dünyaya Açılması İçin Kaydet.
    joblib.dump(rf_model, MODEL_FILENAME)
    print(f"\n[SİSTEM]: Tebrikler! Eğitilen Yapay Zeka Ağırlış Dosyası '{MODEL_FILENAME}' adıyla projenin ana dizinine donduruldu.")

if __name__ == "__main__":
    generate_and_train_hyperlocal_model()
