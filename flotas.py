import pandas as pd
import pyarrow.parquet as pq
import glob

def generar_maestro_vehiculos(ruta_data, salida):
    print("🔍 Escaneando archivos para indexar flotas y placas...")
    # Buscamos todos los consolidados
    archivos = glob.glob(f"{ruta_data}/**/consolidado_*.parquet", recursive=True)
    
    listas_unicas = []
    
    for archivo in archivos:
        # Solo leemos las columnas necesarias para ir rápido
        df = pd.read_parquet(archivo, columns=['placa', 'flota'])
        listas_unicas.append(df.drop_duplicates())
    
    # Consolidamos y eliminamos duplicados finales
    maestro = pd.concat(listas_unicas).drop_duplicates().reset_index(drop=True)
    
    # Guardamos el archivo maestro
    maestro.to_parquet(salida, index=False)
    print(f"✅ Maestro generado con {len(maestro)} vehículos en: {salida}")

# Uso:
# generar_maestro_vehiculos('./1/data', './1/config/maestro_vehiculos.parquet')