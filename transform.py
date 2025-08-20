import pandas as pd

def transformar_datos(dataframes):
    """Transforma los datos con filtrado estricto de columnas"""
    print("\nIniciando transformación avanzada de datos...")
    
    if not dataframes:
        return None, {"error": "No hay datos para transformar"}
    
    try:
        datos_completos = pd.concat(dataframes, ignore_index=True)
        datos_completos.columns = datos_completos.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        datos_completos.columns = datos_completos.columns.str.upper().str.strip()
        
        datos_limpios = datos_completos.dropna(how='all')
        datos_limpios = datos_limpios.loc[:, ~datos_limpios.columns.str.contains('^UNNAMED')]
        
        COLUMNAS_REQUERIDAS = {
            'ACTIVITY_WORK_TYPE': ['ACTIVITY_WORK_TYPE', 'TIPO_ACTIVIDAD', 'WORK_TYPE'],
            'WORK_ORDER_SUBTYPE': ['WORK_ORDER_SUBTYPE', 'SUBTIPO_OT', 'ORDER_SUBTYPE'],
            'TIPO_DE_INVENTARIO': ['TIPO_DE_INVENTARIO', 'INVENTARIO_TIPO', 'TIPO_INV'],
            'GRUPO_DE_INVENTARIOS': ['GRUPO_DE_INVENTARIOS', 'GRUPO_INV', 'INVENTORY_GROUP'],
            'ORDEN_DE_TRABAJO': ['ORDEN_DE_TRABAJO', 'WORK_ORDER', 'OT'],
            'ID_ALIADO': ['ID_ALIADO', 'ALIADO_ID', 'PARTNER_ID'],
            'ID_EXTERNO_DE_RECURSO': ['ID_EXTERNO_DE_RECURSO', 'EXTERNAL_RESOURCE_ID', 'RECURSO_ID'],
            'NUMERO_DE_CUENTA': ['NÚMERO_DE_CUENTA', 'NUMERO_DE_CUENTA', 'ACCOUNT_NUMBER', 'NÃšMERO_DE_CUENTA'],
            'FECHA_DE_RUTA': ['FECHA_DE_RUTA', 'ROUTE_DATE', 'FECHA_RUTA'],
            'CIUDAD': ['CIUDAD', 'NOMBRE_CIUDAD', 'CITY'],
            'NODO': ['NODO', 'NODE', 'UBICACION_TECNICA'],
            'LLAVE': ['LLAVE', 'KEY', 'CODIGO_EQUIPO']
        }
        
        datos_limpios.columns = datos_limpios.columns.str.upper().str.strip()
        mapeo_columnas = {}
        columnas_encontradas = set()
        
        for col_std, alternativas in COLUMNAS_REQUERIDAS.items():
            for alternativa in alternativas:
                alt_upper = alternativa.upper().strip()
                if alt_upper in datos_limpios.columns:
                    mapeo_columnas[alt_upper] = col_std
                    columnas_encontradas.add(col_std)
                    break
                
        datos_limpios = datos_limpios.rename(columns=mapeo_columnas)
        columnas_a_conservar = [col for col in COLUMNAS_REQUERIDAS.keys() if col in datos_limpios.columns]
        datos_filtrados = datos_limpios[columnas_a_conservar].copy()
        
        reporte = {
            'total_filas_original': sum(len(df) for df in dataframes),
            'total_filas_final': len(datos_filtrados),
            'columnas_conservadas': columnas_a_conservar,
            'columnas_faltantes': [col for col in COLUMNAS_REQUERIDAS.keys() if col not in columnas_encontradas],
            'columnas_renombradas': list(mapeo_columnas.items()),
            'columnas_originales': datos_completos.columns.tolist(),
            'columnas_eliminadas': [col for col in datos_completos.columns if col.upper().strip() not in mapeo_columnas],
            'filas_eliminadas': len(datos_completos) - len(datos_filtrados)
        }
        
        print("\nTRANSFORMACIÓN COMPLETA")
        print(f"• Filas originales: {reporte['total_filas_original']}")
        print(f"• Filas finales: {reporte['total_filas_final']}")
        print(f"\nCOLUMNAS CONSERVADAS: {', '.join(reporte['columnas_conservadas'])}")
        
        if reporte['columnas_renombradas']:
            print("\nRENOMBRAMIENTOS REALIZADOS:")
            for original, nuevo in reporte['columnas_renombradas']:
                print(f"- {original} → {nuevo}")
                
        if reporte['columnas_faltantes']:
            print(f"\nCOLUMNAS NO ENCONTRADAS: {', '.join(reporte['columnas_faltantes'])}")
        
        return datos_filtrados, reporte
        
    except Exception as e:
        print(f"\n Error en transformación: {str(e)}")
        return None, {"error": str(e)}

def enriquecer_con_maestro_equipos(datos, ruta_maestro):
    """Enriquece los datos con información del maestro de equipos"""
    print("\nIniciando enriquecimiento con maestro de equipos...")
    
    try:
        maestro = pd.read_csv(ruta_maestro, sep=';', encoding='latin1')
        
        columnas_maestro = {
            'fabricante': 'FABRICANTE',
            'familia': 'FAMILIA',
            'referencia': 'REFERENCIA',
            'tecnologia': 'TECNOLOGIA',
            'llave': 'LLAVE'
        }
        
        maestro.columns = maestro.columns.str.strip().str.lower()
        maestro = maestro.rename(columns={k:v for k,v in columnas_maestro.items() if k in maestro.columns})
        
        maestro['LLAVE'] = maestro['LLAVE'].astype(str).str.replace('@', '').str.strip()
        
        columnas_faltantes = [col for col in columnas_maestro.values() if col not in maestro.columns]
        if columnas_faltantes:
            raise ValueError(f"Columnas faltantes en el maestro: {columnas_faltantes}")
        
        maestro = maestro[list(columnas_maestro.values())].drop_duplicates(subset=['LLAVE'])
        
        if 'TIPO_DE_INVENTARIO' not in datos.columns:
            raise ValueError("No se encontró columna 'TIPO_DE_INVENTARIO' en los datos")
        
        datos_enriquecidos = pd.merge(
            datos,
            maestro,
            left_on='TIPO_DE_INVENTARIO',
            right_on='LLAVE',
            how='left'
        ).drop(columns=['LLAVE'])
        
        print("\n Enriquecimiento completado con éxito")
        print("Columnas añadidas: MAESTRO_FABRICANTE, MAESTRO_FAMILIA, MAESTRO_REFERENCIA, MAESTRO_TECNOLOGIA")
        
        return datos_enriquecidos.rename(columns={
            'FABRICANTE': 'MAESTRO_FABRICANTE',
            'FAMILIA': 'MAESTRO_FAMILIA',
            'REFERENCIA': 'MAESTRO_REFERENCIA',
            'TECNOLOGIA': 'MAESTRO_TECNOLOGIA'
        })
        
    except Exception as e:
        print(f"\n Error durante el enriquecimiento: {str(e)}")
        print("Se añadirán las columnas vacías")
        
        for col in ['MAESTRO_FABRICANTE', 'MAESTRO_FAMILIA', 'MAESTRO_REFERENCIA', 'MAESTRO_TECNOLOGIA']:
            datos[col] = None
            
        return datos

def enriquecer_con_ubicaciones_tecnicas(datos, ruta_ubicaciones):
    """Enriquece los datos con información de ubicaciones técnicas"""
    print("\nIniciando enriquecimiento con ubicaciones técnicas...")
    
    try:
        # Leer el archivo de ubicaciones
        ubicaciones = pd.read_csv(ruta_ubicaciones, sep=';', encoding='latin1')
        ubicaciones.columns = ubicaciones.columns.str.strip().str.upper()
        
        # Columnas requeridas
        cols_requeridas = ['ID', 'ESTADO', 'TIPOLOGIA_RED', 'OPERA', 'ALIADO_ZONIFICADO']
        
        # Verificar columnas
        faltantes = [col for col in cols_requeridas if col not in ubicaciones.columns]
        if faltantes:
            raise ValueError(f"Faltan columnas: {', '.join(faltantes)}")
        
        # Procesar
        ubicaciones = ubicaciones[cols_requeridas].drop_duplicates('ID')
        
        if 'NODO' not in datos.columns:
            print("Advertencia: No se encontró columna 'NODO' en los datos")
            for col in ['UBICACION_ESTADO', 'UBICACION_TIPOLOGIA', 'UBICACION_OPERA', 'UBICACION_ALIADO_ZONA']:
                datos[col] = None
            return datos
            
        # Hacer el merge
        datos = pd.merge(
            datos,
            ubicaciones,
            left_on='NODO',
            right_on='ID',
            how='left'
        ).drop(columns=['ID'])
        
        # Renombrar columnas
        renombres = {
            'ESTADO': 'UBICACION_ESTADO',
            'TIPOLOGIA_RED': 'UBICACION_TIPOLOGIA',
            'OPERA': 'UBICACION_OPERA',
            'ALIADO_ZONIFICADO': 'UBICACION_ALIADO_ZONA'
        }
        datos = datos.rename(columns=renombres)
        
        print(" Enriquecimiento completado")
        return datos
        
    except Exception as e:
        print(f" Error: {str(e)}")
        for col in ['UBICACION_ESTADO', 'UBICACION_TIPOLOGIA', 'UBICACION_OPERA', 'UBICACION_ALIADO_ZONA']:
            datos[col] = None
        return datos