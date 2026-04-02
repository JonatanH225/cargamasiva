import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

def generar_historico_un_ano(id_cliente=1, num_vehiculos=5000):
    print(f"🚀 Iniciando generación histórica para Cliente ID: {id_cliente}")
    
    # 1. Configuración de rango: Marzo 2025 a Marzo 2026
    fecha_inicio = datetime(2023, 1, 1)
    fecha_fin = datetime(2026, 3, 1)
    
    # 2. Vehículos y Flotas estables
    vehiculos = [f"ABC{i:03d}" for i in range(1, num_vehiculos + 1)]
    flotas = [f"Flota_{i:02d}" for i in range(1, 51)]
    
    # Generamos el mapa una sola vez para que sea consistente
    mapa_vehiculo_flota = {v: np.random.choice(flotas) for v in vehiculos}
    
    fecha_actual = fecha_inicio
    
    # --- CICLO DE GENERACIÓN DE DATA MENSUAL ---
    while fecha_actual <= fecha_fin:
        anio = fecha_actual.strftime("%Y")
        mes = fecha_actual.strftime("%m")
        
        path = f"{id_cliente}/data/{anio}_{mes}/"
        if not os.path.exists(path):
            os.makedirs(path)
        
        dias_del_mes = pd.date_range(
            start=fecha_actual, 
            end=fecha_actual + relativedelta(day=31), 
            freq='D'
        )
        dias_del_mes = dias_del_mes[dias_del_mes.month == fecha_actual.month]
        
        print(f"📅 Generando {anio}-{mes} ({len(dias_del_mes)} días)...")
        
        data_mes = []
        for v_id in vehiculos:
            flota = mapa_vehiculo_flota[v_id]
            for dia in dias_del_mes:
                distancia = round(np.random.uniform(10, 450), 2)
                data_mes.append({
                    "id_cliente": id_cliente,
                    "fecha": dia,
                    "placa": v_id,
                    "flota": flota,
                    "distancia": distancia,
                    "combustible": round(distancia * np.random.uniform(0.12, 0.30), 2),
                    "combustible_idle": round(np.random.uniform(0.1, 5.0), 2),
                    "aceleraciones": np.random.randint(0, 25),
                    "frenadas": np.random.randint(0, 20),
                    "giros": np.random.randint(0, 30),
                    "excesos_velocidad": np.random.randint(0, 15),
                    "maxima_velocidad": round(np.random.uniform(60, 140), 1),
                    "velocidad_promedio": round(np.random.uniform(20, 100), 1),
                    "trayectos_realizados": np.random.randint(1, 15)
                })
        
        df_mes = pd.DataFrame(data_mes)
        file_name = f"{path}consolidado_{anio}_{mes}.parquet"
        df_mes.to_parquet(file_name, engine='pyarrow', compression='snappy', index=False)
        
        print(f"✅ Guardado: {file_name}")
        fecha_actual += relativedelta(months=1)

    # --- NUEVO: GENERACIÓN DEL MAESTRO DE VEHÍCULOS ---
    print("\n📦 Generando archivo maestro de Flotas y Placas...")
    
    config_path = f"{id_cliente}/config/"
    if not os.path.exists(config_path):
        os.makedirs(config_path)
    
    # Convertimos el diccionario mapa_vehiculo_flota en un DataFrame
    df_maestro = pd.DataFrame(list(mapa_vehiculo_flota.items()), columns=['placa', 'flota'])
    
    # Guardamos en la carpeta config del cliente
    maestro_file = f"{config_path}maestro_vehiculos.parquet"
    df_maestro.to_parquet(maestro_file, index=False)
    
    print(f"✅ Maestro listo en: {maestro_file} ({len(df_maestro)} registros)")
    print("\n" + "="*40)
    print("✨ PROCESO COMPLETO ✨")
    print("="*40)

if __name__ == "__main__":
    generar_historico_un_ano(id_cliente=1)