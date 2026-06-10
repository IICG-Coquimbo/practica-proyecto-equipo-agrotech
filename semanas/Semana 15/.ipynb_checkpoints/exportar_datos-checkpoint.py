import os
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

def generar_respaldo_csv():
    print("🔌 Conectando a MongoDB Atlas para exportar datos...")
    load_dotenv()
    
    mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://agrotech_sebastiancastillo:agrotechbigdata2026@cluster0.7z77rka.mongodb.net/?appName=Cluster0")
    db_name = os.getenv("DB_NAME", "db_g9agrotech")
    collection_name = os.getenv("PROCESSED_COLLECTION", "processed_data")
    
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        col = db[collection_name]
        
        documentos = list(col.find({}))
        
        if not documentos:
            print("⚠️ Colección procesada vacía. Intentando con 'raw_data'...")
            col = db["raw_data"]
            documentos = list(col.find({}))
            
        if not documentos:
            print("❌ No se encontraron datos en la base de datos.")
            return False
            
        df_mongo = pd.DataFrame(documentos)
        
        if "_id" in df_mongo.columns:
            df_mongo = df_mongo.drop(columns=["_id"])
            
        if "fecha_inicio" in df_mongo.columns and "fecha" not in df_mongo.columns:
            df_mongo = df_mongo.rename(columns={"fecha_inicio": "fecha"})
            
        # 📌 AQUÍ ESTÁ EL CAMBIO: Forzamos la ruta exacta donde estás ejecutando app.py
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        nombre_csv = os.path.join(ruta_actual, "datos_agrotech_dashboard.csv")
        
        df_mongo.to_csv(nombre_csv, index=False, encoding="utf-8")
        print(f"✅ ¡Archivo creado con éxito en la ruta absoluta!")
        print(f"📍 Ubicación: {nombre_csv}")
        return True
        
    except Exception as e:
        print(f"❌ Error durante la exportación: {e}")
        return False

if __name__ == "__main__":
    generar_respaldo_csv()