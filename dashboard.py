import streamlit as st
import json
import time
import subprocess
import os
import requests
import folium
from streamlit_folium import st_folium

# Sayfa Konfigürasyonu
st.set_page_config(page_title="Zirai Don Erken Uyarı | AI Dashboard", layout="wide", page_icon="❄️")

# Arka Plan Servis İşlemleri (Subprocess Manager)
if "proc_listener" not in st.session_state:
    st.session_state.proc_listener = None
if "proc_simulator" not in st.session_state:
    st.session_state.proc_simulator = None

def stop_services():
    try:
        if st.session_state.proc_listener:
            st.session_state.proc_listener.kill()
            st.session_state.proc_listener = None
        if st.session_state.proc_simulator:
            st.session_state.proc_simulator.kill()
            st.session_state.proc_simulator = None
    except Exception:
        pass
        
    # Acımasız Temizlik (Streamlit Refreshinden kaçan zombi process avcısı)
    try:
        subprocess.run(["powershell", "-Command", "Get-WmiObject Win32_Process | Where-Object { $_.CommandLine -match 'edge_device_simulator.py' -or $_.CommandLine -match 'cloud_mqtt_listener.py' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"], capture_output=True)
    except Exception:
        pass

#=======================================================
# HEDEF KONUM OTOMASYONU VE IP GEOLOCATION (İLK KURULUM SİHİRBAZI)
#=======================================================
is_first_time = False
city_name = "Bilinmiyor"

try:
    with open("target_location.json", "r") as f:
        loc = json.load(f)
        current_lat = float(loc.get("lat"))
        current_lon = float(loc.get("lon"))
except Exception:
    is_first_time = True
    try:
        res = requests.get("https://ipapi.co/json/", timeout=3).json()
        current_lat = float(res.get("latitude", 38.3552))
        current_lon = float(res.get("longitude", 38.3095))
        city_name = res.get("city", "Bulunamadı")
    except:
        current_lat = 38.3552
        current_lon = 38.3095

#=======================================================
# SOL MENÜ: SİSTEM KONTROL VE BAŞLATMA
#=======================================================
st.sidebar.header("🕹️ Sistem Kontrol Merkezi")

data_mode = st.sidebar.radio(
    "📡 Yapay Zeka Hangi Veriyi Yorumlasın?",
    (
        "Sanal Drift Simülasyonu", 
        "Hafıza Oynatımı (Önceki Don Felaketi)", 
        "ŞU ANKİ Canlı Uydu Tahmini (Seçili Konum)"
    ),
    help="Buluta (MQTT) basılacak veri senaryosu."
)

if st.sidebar.button("▶️ Sistemi Başlat", use_container_width=True):
    stop_services() 
    time.sleep(0.5)
    
    st.session_state.proc_listener = subprocess.Popen(["python", "cloud_mqtt_listener.py"])
    
    if data_mode == "Hafıza Oynatımı (Önceki Don Felaketi)":
        st.session_state.proc_simulator = subprocess.Popen(["python", "edge_device_simulator.py", "--satellite-data"])
    elif data_mode == "ŞU ANKİ Canlı Uydu Tahmini (Seçili Konum)":
        st.session_state.proc_simulator = subprocess.Popen(["python", "edge_device_simulator.py", "--live-satellite"])
    else:
        st.session_state.proc_simulator = subprocess.Popen(["python", "edge_device_simulator.py"])
        
    st.sidebar.success("✅ Servisler Otonom Başlatıldı!")

if st.sidebar.button("🛑 Sistemi Durdur", use_container_width=True):
    stop_services()
    st.sidebar.warning("⛔ Tüm Servisler Durduruldu!")

st.sidebar.markdown("---")
st.sidebar.info("💡 **Otomasyon:** Tarlanızın yerini sadece 1 kez seçip onayladıktan sonra veritabanına mühürlenir. Sistem her yeniden başladığında sadece o lokasyonu ezbere bilir ve size sormaz.")

#=======================================================
# ANA EKRAN KONTROL PANELİ VE HARİTA
#=======================================================
st.title("❄️ Otonom Zirai Don Erken Uyarı Kontrol Paneli")

if is_first_time:
    st.warning(f"🕵️‍♂️ **Otonom Cihaz Konumu Tespiti (Canlı Uydu):** Sistem uykudan ilk defa uyandı. Edge donanımının IP Geolocation taramasıyla fiziksel olarak **{city_name}** (Enlem: {current_lat}, Boylam: {current_lon}) dolaylarında kurulduğu saptandı.")
    col_ask1, col_ask2 = st.columns([1, 1])
    with col_ask1:
        if st.button("✅ Evet, Cihaz (Tarlam) Tam Olarak Burada. (Hafızaya Kaydet)", use_container_width=True):
            with open("target_location.json", "w") as f:
                json.dump({"lat": current_lat, "lon": current_lon}, f)
            st.success("Tesisat konumu kalıcı olarak hafızaya mühürlendi! Kurulum Başarılı.")
            time.sleep(1)
            st.rerun()
    with col_ask2:
        st.info("👈 Eğer otomasyon bölgeyi doğru tespit ettiyse buradan onaylayınız; değilse hemen alttaki interaktif haritadan asıl kuruluma tıklayınız.")

st.subheader("🌍 Tarlanın Kesin Kurulacağı Lokasyonu Seçin")
st.write("Sistem tarlanın konumunu veri tabanında (target_location) kalıcı olarak saklar. Girdiğinizde en son seçtiğiniz yer çıkar. Konumu değiştirmek için tıklamanız yeterlidir:")

# Harita Modu Seçimi (Arayüz Zenginleştirmesi)
map_style = st.radio(
    "🗺️ Harita Görünüm Katmanı:",
    ["Yüksek Çözünürlüklü Uydu (Satellite)", "Klasik Şematik (OpenStreetMap)", "Fiziki Arazi (Terrain)"],
    horizontal=True
)

if map_style == "Yüksek Çözünürlüklü Uydu (Satellite)":
    # Yüksek Çözünürrüklü Uydu + Yer isimlerinin birlikte olduğu Hybrid (Google) Harita Kaynağı 
    m_tiles = 'http://mt0.google.com/vt/lyrs=y&hl=tr&x={x}&y={y}&z={z}'
    m_attr = 'Google Satellite Hybrid'
elif map_style == "Fiziki Arazi (Terrain)":
    m_tiles = 'OpenTopoMap'
    m_attr = 'Map style: © OpenTopoMap'
else:
    m_tiles = 'OpenStreetMap'
    m_attr = '© OpenStreetMap contributors'

# Folium Haritası Dinamik Katman Oluşturucu
m = folium.Map(location=[current_lat, current_lon], zoom_start=9, tiles=m_tiles, attr=m_attr)

# Mavi İkonun görünür olması için manuel stil atandı
folium.Marker(
    [current_lat, current_lon], 
    popup="Cihazın Konumu", 
    tooltip="Seçili Akıllı Tarla Modülü",
    icon=folium.Icon(color="darkblue", icon="info-sign")
).add_to(m)

# Ekran kararamsı hatasını filtreyle, 'Tıklanınca ikon yok olması/Önbellek (Cache)' hatasını Dinamik Key ile ÖNLÜYORUZ!
map_data = st_folium(m, height=450, use_container_width=True, key=f"main_map_{current_lat}_{current_lon}", returned_objects=["last_clicked"])

if map_data and map_data.get("last_clicked"):
    clicked_lat = map_data["last_clicked"]["lat"]
    clicked_lon = map_data["last_clicked"]["lng"]
    if round(clicked_lat, 4) != round(current_lat, 4):
        with open("target_location.json", "w") as f:
            json.dump({"lat": clicked_lat, "lon": clicked_lon}, f)
        st.success("📍 YENİ KOORDİNAT HAFIZAYA MÜHÜRLENDİ! Geçerli olması için lütfen Soldan Sistemi DURDURUP, sonra BAŞLATIN.")
        time.sleep(1)
        st.rerun()

st.markdown("---")
st.markdown(f"📍 **Seçili ve Kayıtlı Donanım Konumu:** `{round(current_lat,4)} Enlem`, `{round(current_lon,4)} Boylam`")

def load_data():
    try:
        with open("latest_status.json", "r") as f:
            return json.load(f)
    except Exception:
        return None

@st.fragment(run_every=2)
def live_dashboard():
    data = load_data()
    
    if data:
        st.subheader(f"📡 Canlı Telemetri Paneli (Edge Sensörleri)", divider="blue")
        col1, col2, col3, col4 = st.columns(4)
        
        temp_val = data['micro_temperature']
        col1.metric("🌡️ Hava Sıcaklığı", f"{temp_val} °C", "DONDURUCU EŞİK" if temp_val < 2.0 else "Normal", delta_color="inverse")
        col2.metric("💧 Bağıl Nem", f"% {data['micro_humidity']}")
        col3.metric("🌱 Nem (Toprak)", f"% {data['micro_soil_temp']}") 
        col4.metric("💨 Rüzgar / Çiğ N.", f"{data['micro_wind']} km/h / {data['macro_dew_point']} °C")
        
        st.markdown("---")
        st.subheader("🧠 Hibrit Yapay Zeka Oylama Konseyi", divider="gray")
        
        c1, c2 = st.columns(2)
        with c1:
            st.info("### 🌍 Hiper-Yerel Model (Bölge UZMANI)")
            risk_local = data['prob_local']
            st.metric("Öngörülen Radyasyon Donu Riski", f"%{risk_local}")
            st.progress(int(risk_local), "Kırmızı Alarm Tehlike Eşiği: %50")
            
        with c2:
            st.info("### 🌎 Kaggle Dünya Modeli (Toprak UZMANI)")
            risk_kaggle = data['prob_kaggle']
            st.metric("Öngörülen Radyasyon Donu Riski", f"%{risk_kaggle}")
            st.progress(int(risk_kaggle), "Kırmızı Alarm Tehlike Eşiği: %50")
            
        st.markdown("---")
        st.subheader("🚨 Otonom Müdahale (Su Pompası Aktüatörü)")
        if data['alert_level'] == 2:
            st.error("#### 🔴 KIRMIZI TEYİT ALARMI: İki Yapay Zeka Modeli OY BİRLİĞİ İle Don Tehlikesini Onayladı! \n\n**Aksiyon Merkezi:** Tarladaki Su/Sis Pompaları ve Isıtıcı Fanlar **GERÇEK ZAMANLI OLARAK AÇILDI (ON)** !")
        elif data['alert_level'] == 1:
            st.warning("#### 🟡 SARI (ŞÜPHE/UYARI) TEHLİKESİ: Modellerden sadece biri dona gidebilecek bir reaksiyon sezdi. (Genelde Toprak Nemi kaynaklı farklılıklar). \n\n**Aksiyon Merkezi:** Pompalar Otonom Olarak 'STANDBY (Uyku)' modunda ateşlemeye hazır bekletiliyor. SMS Gönderildi.")
        else:
            st.success("#### 🟢 GÜVENLİ (SAĞLAM): Çift Motorlu AI don riski tespit etmedi. Açık hava, Toprak Nemi veya Rüzgar donu engelliyor. \n\n**Aksiyon Merkezi:** Bitki adaptasyonu stabil. Pompalar **KAPALI (OFF)**. Sistem otonom su ve enerji tasarrufu sağlıyor.")
            
        st.caption(f"Yayıncı Cihaz: `{data['device_id']}` | Son Eşzamanlanma Zamanı: `{data['timestamp']}`")
    else:
        st.warning("📡 MQTT Bağlantısı veya Telemetri bekleniyor... Lütfen Mod Seçip Sistemi 'Başlat' Tuşuyla Başlatın.")

live_dashboard()
