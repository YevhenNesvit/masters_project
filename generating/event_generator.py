import pandas as pd
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

# --- Налаштування ---
N_USERS = 101      # Скільки користувачів згенерувати
N_EVENTS = 1515   # Скільки подій згенерувати
# --------------------

# Завантажуємо реальні ID яхт
try:
    # Переконайтеся, що назва файлу правильна
    yacht_df = pd.read_csv('yachts_data_filled.csv') 
    # Припускаємо, що у вас є колонка 'Name', яка є унікальним ідентифікатором
    YACHT_IDS = yacht_df['id'].tolist()

    print(f"Знайдено {len(YACHT_IDS)} унікальних ID яхт.")
except FileNotFoundError:
    print("ПОМИЛКА: Файл 'yachts_data_cleaned - yachts_data_cleaned.csv' не знайдено.")
    print("Використовую фіктивні ID яхт.")
    YACHT_IDS = [f'yacht_id_{i}' for i in range(1, 501)] 


# Словник типів подій та їхньої "ваги" (для запису в БД)
EVENT_TYPES = {
    'view': 2,
    'wishlist': 4,
    'chat_owner': 6,
    'start_booking': 8,
    'book': 10
}

# --- ✅ ГОЛОВНА ЗМІНА ---
# Нові ваги для ГЕНЕРАЦІЇ, що імітують воронку
# (на 100 переглядів припадає ~20 в обране, ~5 чатів, ~2 початку броні, 1 бронь)
GENERATION_WEIGHTS = {
    'view': 100,
    'wishlist': 20,
    'chat_owner': 5,
    'start_booking': 2,
    'book': 1
}

marina_to_country_map = {
    # Італія
    'Amalfi Coast': 'Italy',
    'Sardinia': 'Italy',
    'Ligurian Riviera': 'Italy',
        
    # Греція
    'Mykonos': 'Greece',
        
    # Франція
    'Calvi': 'France',
    'Corsica': 'France',
    'French Riviera': 'France',
    'Cannes': 'France',
        
    # Іспанія
    'Ibiza': 'Spain',
    'Mallorca': 'Spain',
    'The Balearics': 'Spain',
    
    # США
    'Virgin Islands': 'USA',
    'Alaska': 'USA',
    'Florida': 'USA',
    'New England': 'USA',
        
    # ОАЕ
    'Abu Dhabi': 'UAE',
    'Dubai': 'UAE',
        
    # Австралія
    'Sydney': 'Australia',
    'Whitsundays': 'Australia',
        
    # Інші
    'Komodo': 'Indonesia',
        
    # Регіони (не країни)
    'Scandinavia': 'Norway'
}

# -------------------------

# Опції для поля "досвід"
SAILING_EXP_OPTIONS = ['none', 'beginner', 'intermediate', 'pro']

# Ініціалізуємо Faker
fake = Faker()

print("Починаємо генерацію даних...")

# --- 1. Генерація користувачів (Users) ---
users_data = []
for _ in range(N_USERS):
    user_id = str(uuid.uuid4()) # Використовуємо UUID як заміну CUID
    created_at = fake.date_time_between(start_date='-2y', end_date='now')
    
    # Генеруємо бюджети (я повернув None, щоб деякі були порожніми)
    budget_min = random.randint(5000, 100000)
    budget_max = None
    if budget_min is not None:
        budget_max = budget_min + random.randint(100000, 600000)

    user = {
        'id': user_id,
        'email': fake.unique.email(),
        'password_hash': fake.sha256(), # Генеруємо фіктивний хеш
        'country': random.choice(list(set(marina_to_country_map.values()))),
        'role': 'lessee',
        'sailingExp': random.choice(SAILING_EXP_OPTIONS),
        'budgetMin': budget_min,
        'budgetMax': budget_max,
        'has_skipper_licence': random.choice([True, False]),
        'createdAt': created_at
    }
    users_data.append(user)

users_df = pd.DataFrame(users_data)
users_df.to_csv('generated_users.csv', index=False)
print(f"✅ Створено {len(users_df)} користувачів у 'generated_users.csv'")


# --- 2. Генерація подій (Events) ---

# Створюємо списки для зваженого випадкового вибору
event_names = list(EVENT_TYPES.keys())
# Створюємо список ваг у правильному порядку
generation_weights_list = [GENERATION_WEIGHTS[name] for name in event_names]

events_data = []
user_ids_list = users_df['id'].tolist() # Беремо ID зі згенерованих юзерів

for _ in range(N_EVENTS):
    
    # Використовуємо random.choices() з новими вагами "воронки"
    event_type = random.choices(event_names, weights=generation_weights_list, k=1)[0]
    
    user = random.choice(user_ids_list)
    
    # Знаходимо час створення юзера, щоб подія не була раніше
    user_created_at = users_df.loc[users_df['id'] == user, 'createdAt'].iloc[0]
    
    event = {
        'id': str(uuid.uuid4()),
        'userId': user,
        'yachtId': random.choice(YACHT_IDS),
        'type': event_type,
        'weight': EVENT_TYPES[event_type], # Записуємо оригінальну вагу (2, 4, ... 10)
        'ts': fake.date_time_between(start_date=user_created_at, end_date='now')
    }
    events_data.append(event)

events_df = pd.DataFrame(events_data)
events_df = events_df.sort_values(by='ts') # Сортуємо для логічності

events_df.to_csv('generated_events.csv', index=False)
print(f"✅ Створено {len(events_df)} подій у 'generated_events.csv'")

# --- 3. (Опціонально) Перевірка розподілу подій ---
print("\n--- Перевірка розподілу згенерованих подій ---")
print(events_df['type'].value_counts(normalize=True).sort_index() * 100)

print("\n🎉 Генерація завершена!")
