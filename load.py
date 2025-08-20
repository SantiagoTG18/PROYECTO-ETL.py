import os
import pandas as pd
import sqlite3
from datetime import datetime

def guardar_datos(datos, base_output_path):
    """
    Guarda los datos en formato CSV y SQLite en la ruta especificada
    Retorna diccionario con rutas y metadatos
    """
    print("\nIniciando carga de datos...")
    
    if datos is None or len(datos) == 0:
        return {"error": "No hay datos para guardar"}
    
    # Crear directorios si no existen 
    os.makedirs(base_output_path, exist_ok=True)
    
    # Generar nombre base con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"datos_consolidados_{timestamp}"
    
    resultados = {}
    
    try:
        # 1. Guardar como CSV
        csv_path = os.path.join(base_output_path, f"{base_name}.csv")
        datos.to_csv(csv_path, index=False, sep=';', encoding='utf-8')
        print(f"\n CSV guardado en: {csv_path}")
        resultados['csv'] = {
            'ruta': csv_path,
            'filas': len(datos),
            'columnas': list(datos.columns)
        }
        
        # 2. Guardar como SQLite
        db_path = os.path.join(base_output_path, f"{base_name}.db")
        tabla_name = "datos_consolidados"
        
        conn = sqlite3.connect(db_path)
        datos.to_sql(tabla_name, conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"SQLite guardado en: {db_path}")
        print(f" - Tabla creada: '{tabla_name}'")
        
        resultados['sqlite'] = {
            'ruta': db_path,
            'tabla': tabla_name,
            'filas': len(datos),
            'columnas': list(datos.columns)
        }
        return resultados
        
    except Exception as e:
        print(f"\n Error al guardar datos: {str(e)}")
        return {"error": str(e)}
    
def verificar_estructura_db(db_path, tabla_name, columnas_esperadas):
    """Verificar que la tabla de SQLite sea correcta"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Obtener columnas de la tabla
        cursor.execute(f"PRAGMA table_info({tabla_name})")
        columnas_db = [col[1] for col in cursor.fetchall()]
        
        conn.close()
        
        # Verificar que todas las columnas esperadas estén presentes 
        return all(col in columnas_db for col in columnas_esperadas)
    
    except Exception as e:
        print(f"Error al verificar estructura: {str(e)}")
        return False