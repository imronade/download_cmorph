import os
import time
import random
import requests
from datetime import datetime, timedelta
from tqdm import tqdm
import concurrent.futures
import logging
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cmorph_download.log"),
        logging.StreamHandler()
    ]
)

# Base URL untuk data CMORPH
BASE_URL = "https://www.ncei.noaa.gov/data/cmorph-high-resolution-global-precipitation-estimates/access/30min/8km"

# Folder untuk menyimpan data
OUTPUT_DIR = "CMORPH_Data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# File untuk menyimpan laporan kegagalan download
FAILED_DOWNLOADS_FILE = "failed_downloads.json"

def get_remote_file_size(url):
    """Get file size from remote URL using HEAD request"""
    try:
        response = requests.head(url, allow_redirects=True)
        if response.status_code == 200:
            return int(response.headers.get('content-length', 0))
        elif response.status_code == 404:
            logging.warning(f"File tidak ditemukan: {url}")
            return -1
        else:
            logging.warning(f"Gagal mendapatkan ukuran file {url}: Status code {response.status_code}")
            return -1
    except Exception as e:
        logging.error(f"Error saat mengecek ukuran file {url}: {str(e)}")
        return -1

def check_file_integrity(url, local_path):
    """Check if local file size matches remote file size"""
    if not os.path.exists(local_path):
        return False
    
    remote_size = get_remote_file_size(url)
    if remote_size == -1:  # Error getting remote size
        return True  # Assume file is OK to avoid unnecessary downloads
    
    local_size = os.path.getsize(local_path)
    
    if remote_size != local_size:
        logging.warning(f"File size mismatch for {local_path}. Remote: {remote_size}, Local: {local_size}")
        return False
    
    return True

def download_file(url, local_path, max_retries=3, check_size=True):
    """Download file dari URL dan simpan ke path lokal dengan percobaan ulang"""
    # Buat direktori jika belum ada
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Cek apakah file sudah ada dan ukurannya sesuai
    if os.path.exists(local_path):
        if check_size:
            if check_file_integrity(url, local_path):
                logging.info(f"File {local_path} sudah ada dan ukurannya valid, lewati")
                return True
            else:
                logging.warning(f"File {local_path} ukurannya tidak sesuai, menghapus dan mengunduh ulang")
                try:
                    os.remove(local_path)
                except Exception as e:
                    logging.error(f"Gagal menghapus file {local_path}: {str(e)}")
                    return False
        else:
            logging.info(f"File {local_path} sudah ada, lewati")
            return True
    
    # Coba download dengan percobaan ulang
    retries = 0
    while retries < max_retries:
        try:
            # Download file
            response = requests.get(url, stream=True)
            
            # Cek apakah URL valid
            if response.status_code == 404:
                logging.warning(f"File tidak ditemukan: {url}")
                return False
            
            response.raise_for_status()
            
            # Simpan file
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verifikasi ukuran file setelah download
            if check_size:
                if not check_file_integrity(url, local_path):
                    logging.warning(f"Verifikasi ukuran file gagal setelah download: {local_path}")
                    if retries < max_retries - 1:
                        retries += 1
                        continue
                    else:
                        return False
            
            # Jeda waktu acak antara 3-8 detik
            sleep_time = random.uniform(3, 8)
            time.sleep(sleep_time)
            
            logging.info(f"Berhasil mengunduh {url} ke {local_path}")
            return True
            
        except Exception as e:
            retries += 1
            if retries < max_retries:
                logging.warning(f"Percobaan ke-{retries} gagal untuk {url}: {str(e)}. Mencoba lagi...")
                # Jeda waktu lebih lama sebelum mencoba lagi
                sleep_time = random.uniform(5, 10)
                time.sleep(sleep_time)
            else:
                logging.error(f"Gagal mengunduh {url} setelah {max_retries} percobaan: {str(e)}")
                return False

def download_day(year, month, day, check_latest=True):
    """Download semua file untuk satu hari tertentu"""
    # Format tanggal untuk URL dan path
    date_str = f"{year:04d}{month:02d}{day:02d}"
    
    # Buat folder untuk tahun/bulan
    year_month_dir = os.path.join(OUTPUT_DIR, f"{year:04d}", f"{month:02d}")
    os.makedirs(year_month_dir, exist_ok=True)
    
    # URL untuk hari tersebut
    day_url = f"{BASE_URL}/{year:04d}/{month:02d}/{day:02d}/"
    
    # Untuk setiap file pada hari tersebut (24 file untuk data perjam)
    success_count = 0
    failed_files = []
    
    # Daftar file untuk hari ini
    day_files = []
    for hour in range(24):
        file_name = f"CMORPH_V1.0_ADJ_8km-30min_{date_str}{hour:02d}.nc"
        file_url = f"{day_url}{file_name}"
        local_file_path = os.path.join(year_month_dir, file_name)
        day_files.append((hour, file_url, local_file_path))
    
    # Jika diminta untuk mengecek file terbaru, ambil 2 file terakhir yang diunduh
    if check_latest:
        # Cari file terbaru (jika ada)
        existing_files = [(h, url, path) for h, url, path in day_files if os.path.exists(path)]
        if len(existing_files) >= 2:
            # Ambil 2 file terakhir
            latest_files = sorted(existing_files, key=lambda x: x[0], reverse=True)[:2]
            for hour, file_url, local_file_path in latest_files:
                # Cek dan verifikasi file
                if not check_file_integrity(file_url, local_file_path):
                    logging.info(f"File terbaru {local_file_path} ukurannya tidak sesuai, menghapus dan mengunduh ulang")
                    try:
                        os.remove(local_file_path)
                    except Exception as e:
                        logging.error(f"Gagal menghapus file {local_file_path}: {str(e)}")
                    # Tandai untuk diunduh
                    day_files[hour] = (hour, file_url, local_file_path)  # Perbarui status file
    
    # Proses semua file yang perlu diunduh
    for hour, file_url, local_file_path in day_files:
        # Download file jika belum ada atau perlu diunduh ulang
        if download_file(file_url, local_file_path, max_retries=3, check_size=True):
            success_count += 1
        else:
            failed_files.append({
                "date": f"{year:04d}-{month:02d}-{day:02d}",
                "hour": hour,
                "url": file_url,
                "local_path": local_file_path
            })

    return success_count, failed_files

def check_data_completeness(start_date, end_date, verify_size=True):
    """Memeriksa kelengkapan data yang sudah didownload"""
    missing_data = []
    current_date = start_date
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day
        date_str = f"{year:04d}{month:02d}{day:02d}"
        
        year_month_dir = os.path.join(OUTPUT_DIR, f"{year:04d}", f"{month:02d}")
        
        missing_hours = []
        for hour in range(24):
            file_name = f"CMORPH_V1.0_ADJ_8km-30min_{date_str}{hour:02d}.nc"
            local_file_path = os.path.join(year_month_dir, file_name)
            day_url = f"{BASE_URL}/{year:04d}/{month:02d}/{day:02d}/"
            file_url = f"{day_url}{file_name}"
            
            # Cek keberadaan file dan validasi ukuran jika diminta
            file_exists = os.path.exists(local_file_path)
            file_valid = True
            
            if file_exists and verify_size:
                file_valid = check_file_integrity(file_url, local_file_path)
            
            if not file_exists or not file_valid:
                missing_hours.append({
                    "hour": hour,
                    "url": file_url,
                    "local_path": local_file_path,
                    "reason": "missing" if not file_exists else "invalid_size"
                })
        
        if missing_hours:
            missing_data.append({
                "date": f"{year:04d}-{month:02d}-{day:02d}",
                "missing_hours": missing_hours
            })
        
        current_date += timedelta(days=1)
    
    return missing_data

def save_failed_downloads(failed_files):
    """Menyimpan daftar file yang gagal didownload ke file JSON"""
    if failed_files:
        with open(FAILED_DOWNLOADS_FILE, 'w') as f:
            json.dump(failed_files, f, indent=4)
        logging.info(f"Laporan kegagalan download disimpan di {FAILED_DOWNLOADS_FILE}")
    else:
        if os.path.exists(FAILED_DOWNLOADS_FILE):
            os.remove(FAILED_DOWNLOADS_FILE)
            logging.info(f"Tidak ada kegagalan download, file {FAILED_DOWNLOADS_FILE} dihapus")

def retry_failed_downloads(failed_files):
    """Mencoba mengunduh ulang file-file yang gagal didownload"""
    if not failed_files:
        logging.info("Tidak ada file yang perlu diunduh ulang")
        return []
    
    logging.info(f"Mencoba mengunduh ulang {len(failed_files)} file yang gagal")
    still_failed = []
    
    for file_info in tqdm(failed_files, desc="Retrying failed downloads"):
        url = file_info["url"]
        local_path = file_info["local_path"]
        
        if download_file(url, local_path, max_retries=3):
            logging.info(f"Berhasil mengunduh ulang {url}")
        else:
            still_failed.append(file_info)
    
    return still_failed

def main():
    total_files = 0
    successful_downloads = 0
    all_failed_files = []
    
    # Rentang waktu yang akan diunduh
    start_date = datetime(1998, 1, 1)
    end_date = datetime(2000, 12, 31)
    current_date = start_date
    
    # Hitung total file yang akan diunduh
    date_count = (end_date - start_date).days + 1
    total_files = date_count * 24  # 24 file per hari (data setiap 1 jam)
    
    logging.info(f"Mulai mengunduh {total_files} file CMORPH dari {start_date.strftime('%Y-%m-%d')} hingga {end_date.strftime('%Y-%m-%d')}")
    
    # Loop untuk setiap hari dalam rentang waktu
    with tqdm(total=date_count, desc="Download Progress") as pbar:
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day
            
            logging.info(f"Mengunduh data untuk tanggal {current_date.strftime('%Y-%m-%d')}")
            # Pastikan untuk memeriksa 2 file terbaru setiap hari
            success_count, failed_files = download_day(year, month, day, check_latest=True)
            successful_downloads += success_count
            all_failed_files.extend(failed_files)
            
            # Pindah ke hari berikutnya
            current_date += timedelta(days=1)
            pbar.update(1)
    
    # Simpan laporan kegagalan download
    save_failed_downloads(all_failed_files)
    
    # Tampilkan ringkasan hasil download
    logging.info(f"Proses download selesai. Berhasil mengunduh {successful_downloads} dari {total_files} file.")
    if all_failed_files:
        logging.info(f"Terdapat {len(all_failed_files)} file yang gagal diunduh. Lihat {FAILED_DOWNLOADS_FILE} untuk detailnya.")
    
    # Periksa kelengkapan data dan validasi ukuran file
    logging.info("Memeriksa kelengkapan data dan validasi ukuran file...")
    missing_data = check_data_completeness(start_date, end_date, verify_size=True)
    
    if missing_data:
        total_missing = sum(len(day["missing_hours"]) for day in missing_data)
        logging.info(f"Terdeteksi {total_missing} file yang hilang atau tidak valid dari {len(missing_data)} hari.")
        
        # Flatkan daftar file yang hilang untuk mencoba diunduh ulang
        files_to_retry = []
        for day in missing_data:
            for hour_info in day["missing_hours"]:
                # Jika file ada tapi ukurannya tidak valid, hapus terlebih dahulu
                if hour_info.get("reason") == "invalid_size" and os.path.exists(hour_info["local_path"]):
                    try:
                        os.remove(hour_info["local_path"])
                        logging.info(f"Menghapus file tidak valid: {hour_info['local_path']}")
                    except Exception as e:
                        logging.error(f"Gagal menghapus file {hour_info['local_path']}: {str(e)}")
                
                files_to_retry.append({
                    "date": day["date"],
                    "hour": hour_info["hour"],
                    "url": hour_info["url"],
                    "local_path": hour_info["local_path"]
                })
        
        # Coba unduh ulang file yang hilang
        logging.info("Mencoba mengunduh ulang file-file yang hilang atau tidak valid...")
        still_missing = retry_failed_downloads(files_to_retry)
        
        # Simpan laporan file yang masih hilang
        if still_missing:
            with open("missing_files.json", 'w') as f:
                json.dump(still_missing, f, indent=4)
            logging.info(f"Masih ada {len(still_missing)} file yang tidak berhasil diunduh. Lihat missing_files.json untuk detailnya.")
        else:
            logging.info("Semua file berhasil diunduh setelah percobaan ulang!")
    else:
        logging.info("Semua data lengkap dan valid.")

if __name__ == "__main__":
    main()