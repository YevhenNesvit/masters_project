import os
from pathlib import Path
import json

# --- 1. НАЛАШТУЙТЕ ЦІ 2 ЗМІННІ ---

# Вкажіть шлях до вашої папки з фото, яку ви завантажували (де лежать '105 YCG' і т.д.)
LOCAL_PHOTO_DIR = Path("/home/yevhen/Проекти/masters_project/yachts") 

# Ваш базовий URL в Cloudflare R2
BASE_URL = "https://pub-59edec60055841149d71125f2e73e658.r2.dev/yachts/yachts"

# ------------------------------------

photo_map = {}

print(f"Scanning {LOCAL_PHOTO_DIR}...")

# Ефективно скануємо папки першого рівня
for entry in os.scandir(LOCAL_PHOTO_DIR):
    # entry.name - це назва папки, наприклад '105 YCG' або 'FLYING FOX'
    if entry.is_dir():
        yacht_name_upper = entry.name 
        
        # Шукаємо всі файли .jpg всередині цієї папки і сортуємо їх
        file_paths = sorted(Path(entry.path).glob('*.jpg'))
        
        if file_paths:
            # Створюємо повні URL-адреси
            urls = [f"{BASE_URL}/{yacht_name_upper}/{file.name}" for file in file_paths]
            
            # Зберігаємо у наш словник (ключ - назва яхти)
            photo_map[yacht_name_upper] = urls

# Зберігаємо результат у JSON-файл, щоб не робити це щоразу
output_file = 'photo_map.json'
with open(output_file, 'w') as f:
    json.dump(photo_map, f, indent=2)

print(f"Done! Found photos for {len(photo_map)} yachts.")
print(f"Map saved to '{output_file}'.")
