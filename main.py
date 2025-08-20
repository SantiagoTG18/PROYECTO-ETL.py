import os
import tkinter as tk
from tkinter import filedialog
from extract import extraer_archivos
from transform import transformar_datos, enriquecer_con_maestro_equipos
from transform import enriquecer_con_ubicaciones_tecnicas
from load import guardar_datos

def seleccionar_archivo(titulo, tipos_archivo):
    """Muestra diálogo para seleccionar archivo"""
    root = tk.Tk()
    root.withdraw()
    return filedialog.askopenfilename(title=titulo, filetypes=tipos_archivo)

def seleccionar_carpeta(titulo):
    """Muestra diálogo para seleccionar carpeta"""
    root = tk.Tk()
    root.withdraw()
    return filedialog.askdirectory(title=titulo)

def main():
    print("\n" + "="*50)
    print("ETL PARA PROCESAMIENTO DE DATOS")
    print("="*50)
    
    # 1. Selección de carpetas
    print("\n[1/5] Selecciona la carpeta con los archivos CSV de origen")
    ruta_origen = seleccionar_carpeta("Seleccionar carpeta de origen")
    if not ruta_origen:
        print("\n No se seleccionó carpeta de origen. Saliendo...")
        return

    print("\n[2/5] Selecciona la carpeta donde guardar los resultados")
    ruta_salida = seleccionar_carpeta("Seleccionar carpeta de destino")
    if not ruta_salida:
        print("\n No se seleccionó carpeta de destino. Saliendo...")
        return

    try:
        # 2. Extracción
        print("\n" + "="*50)
        print("[3/5] FASE DE EXTRACCIÓN")
        dataframes, metadatos = extraer_archivos(ruta_origen)
        
        if not dataframes:
            print("\n No se encontraron datos válidos para procesar")
            return
        
        # 3. Transformación
        print("\n" + "="*50)
        print("[4/5] FASE DE TRANSFORMACIÓN")
        datos_transformados, reporte = transformar_datos(dataframes)
        
        if datos_transformados is None:
            print("\n Error en transformación - ver reporte anterior")
            return
        
        # 4. Enriquecimientos
        print("\n" + "="*50)
        print("[5/5] FASE DE ENRIQUECIMIENTO")
        
        # Maestro de equipos
        print("\n► Selecciona el archivo Maestro_Equipos.csv (opcional)")
        ruta_maestro = seleccionar_archivo(
            "Seleccionar Maestro_Equipos.csv",
            [("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if ruta_maestro:
            datos_transformados = enriquecer_con_maestro_equipos(datos_transformados, ruta_maestro)
        else:
            print("\n○ No se seleccionó archivo maestro. Continuando...")
        
        # Ubicaciones técnicas
        print("\n► Selecciona el archivo Ubicaciones_tecnicas.csv (opcional)")
        ruta_ubicaciones = seleccionar_archivo(
            "Seleccionar Ubicaciones_tecnicas.csv",
            [("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if ruta_ubicaciones:
            datos_transformados = enriquecer_con_ubicaciones_tecnicas(datos_transformados, ruta_ubicaciones)
        else:
            print("\n○ No se seleccionó archivo de ubicaciones. Continuando...")
        
        # 5. Carga
        print("\n" + "="*50)
        print("FASE DE CARGA DE RESULTADOS")
        resultados_carga = guardar_datos(datos_transformados, ruta_salida)
        
        print("\n" + "="*50)
        print("RESUMEN FINAL")
        if 'error' in resultados_carga:
            print(f"\n Error al guardar resultados: {resultados_carga['error']}")
        else:
            print("\n✓ Proceso completado con éxito")
            print(f"\nArchivos generados en: {ruta_salida}")
            for formato, info in resultados_carga.items():
                print(f"\n• {formato.upper()}:")
                print(f"  Ruta: {info['ruta']}")
                print(f"  Filas: {info['filas']}")
                print(f"  Columnas: {len(info['columnas'])}")
        
    except Exception as e:
        print(f"\n ERROR NO CONTROLADO: {str(e)}")
    
    input("\nPresiona Enter para salir...")

if __name__ == "__main__":
    main()