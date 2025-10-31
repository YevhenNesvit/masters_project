import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
import pickle
from sqlalchemy import create_engine
from tqdm import tqdm

class YachtRecommender:
    """
    Content-based yacht recommender using KNN
    для холодного старту (схожі яхти на основі поточної)
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Args:
            df: DataFrame з яхтами (yachts_data_filled.csv)
        """
        self.df = df.copy()
        self.feature_matrix = None
        self.knn_model = None
        self.scaler = None
        self.yacht_id_to_idx = {}
        self.idx_to_yacht_id = {}
        
    def prepare_features(self):
        """
        Створює feature matrix для KNN
        """
        df = self.df.copy()
        
        # 1. NUMERICAL FEATURES (нормалізовані)
        numerical_features = ['guests', 'cabins', 'crew', 'length', 'year', 'rating']
        
        # Заповнюємо missing values медіаною
        # for col in numerical_features:
        #     df[col] = pd.to_numeric(df[col], errors='coerce')
        #     df[col] = df[col].fillna(df[col].median())
        
        # 2. DERIVED FEATURES
        # df['guests_per_cabin'] = df['guests'] / df['cabins'].replace(0, 1)
        # df['crew_per_guest'] = df['crew'] / df['guests'].replace(0, 1)
        # df['space_per_guest'] = df['length'] / df['guests'].replace(0, 1)
        
        # 3. PRICE FEATURES (важливо для схожості!)
        df['avg_price'] = (
            df['summerLowSeasonPrice'] + 
            df['summerHighSeasonPrice'] +
            df['winterLowSeasonPrice'] + 
            df['winterHighSeasonPrice']
        ) / 4
        # df['price_per_guest'] = df['avg_price'] / df['guests'].replace(0, 1)
        
        # Log transform для ціни (щоб зменшити вплив outliers)
        df['log_price'] = np.log1p(df['avg_price'])
        
        # 4. RATING
        # df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(4.0)
        
        # 5. CATEGORICAL FEATURES (one-hot encoding)
        # Type
        type_dummies = pd.get_dummies(df['type'], prefix='type')
        
        # Base Marina (топ-15 марін, решта → "Other")
        # top_marinas = df['baseMarina'].value_counts().head(15).index
        # df['marina_grouped'] = df['baseMarina'].apply(
        #     lambda x: x if x in top_marinas else 'Other'
        # )
        # marina_dummies = pd.get_dummies(df['marina_grouped'], prefix='marina')
        
        # Country
        country_dummies = pd.get_dummies(df['country'], prefix='country')
        
        # 6. COMBINE ALL FEATURES
        feature_cols = (
            numerical_features + 
            [# ['guests_per_cabin', 'crew_per_guest', 'space_per_guest',
             'log_price', # 'price_per_guest',
             'rating']
        )
        
        numerical_df = df[feature_cols]
        
        # Concatenate з categorical
        feature_matrix = pd.concat([
            numerical_df,
            type_dummies,
            # marina_dummies,
            country_dummies
        ], axis=1)
        
        # Заповнюємо будь-які залишкові NaN нулями
        feature_matrix = feature_matrix.fillna(0)
        
        print(f"✅ Feature matrix створено: {feature_matrix.shape}")
        print(f"   Numerical features: {len(feature_cols)}")
        print(f"   Type dummies: {type_dummies.shape[1]}")
        # print(f"   Marina dummies: {marina_dummies.shape[1]}")
        print(f"   Country dummies: {country_dummies.shape[1]}")
        
        return feature_matrix
    
    def fit(self, n_neighbors=11, metric='cosine'):
        """
        Тренує KNN модель
        
        Args:
            n_neighbors: скільки сусідів шукати (11 = 10 recommendations + сама яхта)
            metric: 'cosine', 'euclidean', 'manhattan'
        """
        print(f"\n🔧 Тренування KNN моделі (n_neighbors={n_neighbors}, metric={metric})...")
        
        # Prepare features
        self.feature_matrix = self.prepare_features()
        
        # Normalize features (важливо для euclidean/manhattan)
        if metric in ['euclidean', 'manhattan']:
            self.scaler = StandardScaler()
            feature_matrix_scaled = self.scaler.fit_transform(self.feature_matrix)
        else:
            # Для cosine не потрібна нормалізація
            feature_matrix_scaled = self.feature_matrix.values
        
        # Створюємо mapping yacht_id ↔ index
        if 'id' in self.df.columns:
            self.yacht_id_to_idx = {yacht_id: idx for idx, yacht_id in enumerate(self.df['id'])}
            self.idx_to_yacht_id = {idx: yacht_id for yacht_id, idx in self.yacht_id_to_idx.items()}
        else:
            # Якщо немає колонки 'id', використовуємо row index
            self.yacht_id_to_idx = {idx: idx for idx in range(len(self.df))}
            self.idx_to_yacht_id = self.yacht_id_to_idx
        
        # Train KNN
        self.knn_model = NearestNeighbors(
            n_neighbors=n_neighbors,
            metric=metric,
            algorithm='auto',  # автоматично обирає ball_tree, kd_tree або brute
            n_jobs=-1  # використовує всі CPU cores
        )
        
        self.knn_model.fit(feature_matrix_scaled)
        
        print(f"✅ KNN модель натренована!")
        
        return self
    
    def recommend(self, yacht_id, top_k=10, filters=None):
        """
        Рекомендує схожі яхти на основі yacht_id
        
        Args:
            yacht_id: ID яхти (з колонки 'id' або row index)
            top_k: скільки рекомендацій повернути
            filters: dict з фільтрами (опціонально)
                {
                    'max_price': 50000,
                    'min_guests': 8,
                    'countries': ['Italy', 'France'],
                    'types': ['Motor Yachts']
                }
        
        Returns:
            DataFrame з рекомендованими яхтами
        """
        if self.knn_model is None:
            raise ValueError("Модель не натренована! Спочатку викличте .fit()")
        
        # Знаходимо index яхти
        if yacht_id not in self.yacht_id_to_idx:
            raise ValueError(f"Yacht ID {yacht_id} не знайдено в датасеті")
        
        yacht_idx = self.yacht_id_to_idx[yacht_id]
        
        # Отримуємо feature vector цієї яхти
        if self.scaler:
            yacht_features = self.scaler.transform([self.feature_matrix.iloc[yacht_idx]])
        else:
            yacht_features = self.feature_matrix.iloc[yacht_idx].values.reshape(1, -1)
        
        # Знаходимо k найближчих сусідів
        distances, indices = self.knn_model.kneighbors(yacht_features)
        
        # Видаляємо саму яхту (перший елемент)
        distances = distances[0][1:]
        indices = indices[0][1:]
        
        # Конвертуємо distance в similarity score (для cosine: 1 - distance)
        if self.knn_model.metric == 'cosine':
            similarities = 1 - distances
        else:
            # Для euclidean/manhattan: нормалізуємо до [0, 1]
            max_dist = distances.max() if distances.max() > 0 else 1
            similarities = 1 - (distances / max_dist)
        
        # Створюємо DataFrame з рекомендаціями
        recommendations = []
        for idx, similarity in zip(indices, similarities):
            yacht_data = self.df.iloc[idx].copy()
            yacht_data['similarity_score'] = similarity
            recommendations.append(yacht_data)
        
        recommendations_df = pd.DataFrame(recommendations)
        
        # Застосовуємо фільтри (якщо є)
        if filters:
            if 'max_price' in filters:
                recommendations_df = recommendations_df[
                    recommendations_df['summerLowSeasonPrice'] <= filters['max_price']
                ]
            
            if 'min_guests' in filters:
                recommendations_df = recommendations_df[
                    recommendations_df['guests'] >= filters['min_guests']
                ]
            
            if 'countries' in filters and filters['countries']:
                recommendations_df = recommendations_df[
                    recommendations_df['country'].isin(filters['countries'])
                ]
            
            if 'types' in filters and filters['types']:
                recommendations_df = recommendations_df[
                    recommendations_df['type'].isin(filters['types'])
                ]
        
        # Повертаємо топ-K
        return recommendations_df.head(top_k)
    
    def get_yacht_info(self, yacht_id):
        """
        Повертає інформацію про яхту
        """
        if yacht_id not in self.yacht_id_to_idx:
            return None
        
        yacht_idx = self.yacht_id_to_idx[yacht_id]
        return self.df.iloc[yacht_idx]
    
    def save_model(self, filepath='yacht_recommender.pkl'):
        """
        Зберігає модель на диск
        """
        with open(filepath, 'wb') as f:
            pickle.dump({
                'knn_model': self.knn_model,
                'feature_matrix': self.feature_matrix,
                'scaler': self.scaler,
                'yacht_id_to_idx': self.yacht_id_to_idx,
                'idx_to_yacht_id': self.idx_to_yacht_id,
                'df': self.df
            }, f)
        print(f"✅ Модель збережена у {filepath}")
    
    @classmethod
    def load_model(cls, filepath='yacht_recommender.pkl'):
        """
        Завантажує модель з диску
        """
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        recommender = cls(data['df'])
        recommender.knn_model = data['knn_model']
        recommender.feature_matrix = data['feature_matrix']
        recommender.scaler = data['scaler']
        recommender.yacht_id_to_idx = data['yacht_id_to_idx']
        recommender.idx_to_yacht_id = data['idx_to_yacht_id']
        
        print(f"✅ Модель завантажена з {filepath}")
        return recommender


# ============================================
# ВИКОРИСТАННЯ
# ============================================

if __name__ == "__main__":
    # 1. Завантажуємо дані
    engine = create_engine("postgresql+psycopg2://yachts_user:3lcFBrVjtsHmAabuUbRlzvq0lldzmBbn@dpg-d3qvbkgdl3ps73c8tfh0-a.frankfurt-postgres.render.com:5432/yachts")

    # SQL-запит (можна вибрати конкретну таблицю або зробити join)
    query = "SELECT * FROM yachts;"

    # Зчитуємо дані у DataFrame
    raw_conn = engine.raw_connection()

    try:
        df = pd.read_sql(query, raw_conn)
    finally:
        raw_conn.close()  # обов’язково закрити!
    
    print(f"Завантажено {len(df)} яхт")
    
    # 2. Створюємо та тренуємо recommender
    # ВАЖЛИВО: n_neighbors = 21, щоб отримати 20 рекомендацій + саму яхту
    # recommender = YachtRecommender(df)
    # recommender.fit(n_neighbors=21, metric='cosine')
    
    # print("\n🚀 Починаємо генерацію рекомендацій для всіх яхт...")
    
    # all_recommendations_data = []
    
    # # 3. Проходимо по кожній яхті і генеруємо рекомендації
    # all_yacht_ids = df['id'].unique()
    
    # # tqdm - це просто для гарного progress bar
    # for yacht_id in tqdm(all_yacht_ids, desc="Генерація рекомендацій"):
    #     try:
    #         # Отримуємо 20 найкращих рекомендацій
    #         recs_df = recommender.recommend(yacht_id, top_k=20)
            
    #         # Отримуємо тільки список ID
    #         recs_ids = recs_df['id'].tolist()
            
    #         # Додаємо у наш список
    #         all_recommendations_data.append({
    #             'yacht_id': yacht_id,
    #             'cold_recommendations': recs_ids
    #         })
    #     except ValueError as e:
    #         print(f"Помилка для yacht_id {yacht_id}: {e}")
    #         all_recommendations_data.append({
    #             'yacht_id': yacht_id,
    #             'cold_recommendations': [] # Порожній список у разі помилки
    #         })

    # print(f"\n✅ Успішно згенеровано рекомендації для {len(all_recommendations_data)} яхт.")
    
    # # 4. Створюємо фінальний DataFrame
    # recs_to_upload_df = pd.DataFrame(all_recommendations_data)
    # recs_to_upload_df['yacht_id_text'] = recs_to_upload_df['yacht_id_text'].astype(str)

    # # --- ОСЬ ЦЕЙ ВАЖЛИВИЙ РЯДОК ---
    # # Він перетворює [UUID('...')] на ['...']
    # recs_to_upload_df['cold_recommendations_text'] = recs_to_upload_df['cold_recommendations_text'].apply(
    #     lambda uuid_list: str([str(uuid_obj) for uuid_obj in uuid_list])
    # )
    
    # # 5. Зберігаємо у CSV (для бекапу)
    # csv_filename = 'similar_yachts.csv'
    # recs_to_upload_df.to_csv(csv_filename, index=False)
    # print(f"💾 Рекомендації збережено у {csv_filename}")

    # # 6. Завантажуємо в PostgreSQL
    # table_name = 'cold_recommendations'
    # print(f"📤 Завантажуємо дані в PostgreSQL (таблиця: {table_name})...")
    
    # try:
    #     # Визначаємо типи даних для колонок
    #     # Це важливо, щоб 'cold_recommendations' стала масивом у Postgres
        
    #     # Завантажуємо DataFrame в SQL
    #     recs_to_upload_df.to_sql(
    #         table_name, 
    #         engine, 
    #         if_exists='replace',  # 'replace' - повністю перестворює таблицю
    #         index=False
    #     )
        
    #     print(f"✅ Дані успішно завантажено в таблицю '{table_name}'!")
        
    # except Exception as e:
    #     print(f"❌ Помилка під час завантаження в базу даних: {e}")

    recommender = YachtRecommender(df)
    recommender.fit(n_neighbors=11, metric='cosine')
    
    # 3. Зберігаємо модель (для production)
    recommender.save_model('yacht_recommender.pkl')
    
    # 4. ПРИКЛАД: Рекомендації для першої яхти
    yacht_id = df['id'].iloc[0]  # Перша яхта
    
    print(f"\n{'='*60}")
    print(f"Поточна яхта: {df.iloc[0]['name']}")
    print(f"Тип: {df.iloc[0]['type']}")
    print(f"Гостей: {df.iloc[0]['guests']}")
    print(f"Marina: {df.iloc[0]['baseMarina']}")
    print(f"Rating: {df.iloc[0]['rating']}")
    print(f"Ціна: ${df.iloc[0]['summerLowSeasonPrice']:,.0f}/день")
    print(f"{'='*60}\n")
    
    # Отримуємо рекомендації
    recommendations = recommender.recommend(yacht_id, top_k=10)
    
    print("🎯 ТОП-10 СХОЖИХ ЯХТ:\n")
    for i, row in recommendations.iterrows():
        print(f"{row.name + 1}. {row['name']}")
        print(f"   Similarity: {row['similarity_score']:.3f}")
        print(f"   Type: {row['type']} | Guests: {row['guests']} | "
              f"Price: ${row['summerLowSeasonPrice']:,.0f}/день")
        print(f"   Rating: {row['rating']}")
        print(f"   Marina: {row['baseMarina']} ({row['country']})")
        print()
    
    # 5. ПРИКЛАД З ФІЛЬТРАМИ
    # print(f"\n{'='*60}")
    # print("🔍 РЕКОМЕНДАЦІЇ З ФІЛЬТРАМИ (budget <$30k, min 10 гостей)")
    # print(f"{'='*60}\n")
    
    # filtered_recs = recommender.recommend(
    #     yacht_id, 
    #     top_k=11,
    #     filters={
    #         'max_price': 30000,
    #         'min_guests': 10
    #     }
    # )
    
    # for i, row in filtered_recs.iterrows():
    #     print(f"{i + 1}. {row['name']} - ${row['summerLowSeasonPrice']:,.0f}/день")

# metrics_to_test = ['cosine', 'euclidean', 'manhattan']
# results = {}

# for metric in metrics_to_test:
#     print(f"\n{'='*50}")
#     print(f"Testing metric: {metric}")
#     print(f"{'='*50}")
    
#     rec = YachtRecommender(df)
#     rec.fit(n_neighbors=11, metric=metric)
    
#     # Тестуємо на кількох яхтах
#     sample_yachts = df['id'].sample(10)
    
#     for yacht_id in sample_yachts:
#         recs = rec.recommend(yacht_id, top_k=5)
#         results[f"{metric}_{yacht_id}"] = recs
        
#     # Перевіряємо diversity
#     all_recommended_ids = set()
#     for yacht_id in sample_yachts:
#         recs = rec.recommend(yacht_id, top_k=10)
#         all_recommended_ids.update(recs['id'])
    
#     diversity = len(all_recommended_ids) / len(df)
#     print(f"Diversity (catalog coverage): {diversity:.2%}")
