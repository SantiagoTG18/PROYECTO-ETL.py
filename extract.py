import pandas as pd
import os
from utils.file_utils import detectar_separador

def extraer_archivos(ruta):
    """
    Extrae datos de archivos CSV en la ruta especificada
    Retorna lista de DataFrames y metadatos
    """
    print("Iniciando extracción de datos...")
    
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"ERROR: Ruta no encontrada:\n{ruta}")
    
    archivos_csv = [f for f in os.listdir(ruta) if f.endswith('.csv')]
    print(f"\nArchivos CSV encontrados: {len(archivos_csv)}")
    
    if not archivos_csv:
        return [], []
    
    dataframes = []
    metadatos = []
    
    for archivo in archivos_csv:
        file_path = os.path.join(ruta, archivo)
        print(f"\nProcesando: {archivo}")
        
        encoding, separador = detectar_separador(file_path)
        
        if not encoding or not separador:
            print(" No se pudo determinar encoding/separador")
            continue
            
        print(f" - Encoding: {encoding}")
        print(f" - Separador: '{separador}'")
        
        try:
            df = pd.read_csv(
                file_path,
                sep=separador,
                encoding=encoding,
                on_bad_lines='skip',
                engine='python',
                dtype=str
            )
            
            print(f"  Archivo cargado - Filas: {len(df)}")
            dataframes.append(df)
            metadatos.append({
                'nombre_archivo': archivo,
                'encoding': encoding,
                'separador': separador,
                'filas': len(df)
            })
            
        except Exception as e:
            print(f"  Error al leer archivo: {str(e)[:100]}")
            continue
    
    return dataframes, metadatos

