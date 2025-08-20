def detectar_separador(archivo, encodings=['utf-8', 'latin1', 'iso-8859-1']):
    """Detecta el separador y encoding probando alternativas comunes"""
    for encoding in encodings: 
        try:
            with open(archivo, 'r', encoding=encoding) as f:
                lineas = [f.readline() for _ in range(5)]
                
            # Contar separadores potenciales 
            sep_counts = {',': 0, ';': 0, '\t': 0, '|': 0}
            for linea in lineas:
                for sep in sep_counts:
                    sep_counts[sep] += linea.count(sep)
                    
            # Determinar el separador más común
            separador = max(sep_counts, key=sep_counts.get)
            return encoding, separador
        except:
            continue
    return None, None