import json
import os
import random

def generar_usuarios_con_placas(id_cliente=1, total_vehiculos=5000, num_usuarios=20):
    print(f"Generando permisos personalizados para {num_usuarios} usuarios...")
    
    path_usuarios = f"1/usuarios/"
    if not os.path.exists(path_usuarios):
        os.makedirs(path_usuarios)

    # 1. Creamos el universo total de placas (las 5000 que existen en tus Parquet)
    todas_las_placas = [f"ABC{i:03d}" for i in range(1, total_vehiculos + 1)]
    
    nombres = [
        "Andrés", "Beatriz", "Carlos", "Diana", "Eduardo", 
        "Fabiola", "Gabriel", "Hilda", "Iván", "Julieta",
        "Kevin", "Laura", "Mauricio", "Natalia", "Óscar", 
        "Patricia", "Ricardo", "Sandra", "Tomás", "Verónica"
    ]

    usuarios_dict = {}

    for i in range(num_usuarios):
        id_usuario = f"user_{i+1:03d}"
        nombre_real = nombres[i]
        
        # --- LÓGICA DE ASIGNACIÓN ESPECIAL ---
        
        if i == 0:
            # EL USUARIO "DIOS": Tiene acceso a TODO (5,000 placas)
            placas_usuario = todas_las_placas
            nombre_real = "Administrador Master"
            print(f"👑 Configurando usuario Master: {nombre_real}")
            
        elif i == 1:
            # EL USUARIO RESTRINGIDO: Solo 10 placas al azar
            placas_usuario = random.sample(todas_las_placas, 10)
            nombre_real = "Analista Junior (10 Placas)"
            print(f"🛡️ Configurando usuario restringido: {nombre_real}")
            
        else:
            # EL RESTO: Variables al azar (entre 50 y 500 placas)
            cantidad_asignada = random.randint(50, 500)
            placas_usuario = random.sample(todas_las_placas, cantidad_asignada)

        usuarios_dict[id_usuario] = {
            "nombre": nombre_real,
            "id_cliente": id_cliente,
            "placas_autorizadas": sorted(placas_usuario)
        }

    # Guardar el JSON
    file_path = f"{path_usuarios}permisos.json"
    with open(file_path, "w", encoding='utf-8') as f:
        json.dump(usuarios_dict, f, indent=4, ensure_ascii=False)

    print("-" * 30)
    print(f"✅ Archivo de permisos actualizado: {file_path}")
    print(f"Usuario 1: {usuarios_dict['user_001']['nombre']} ({len(usuarios_dict['user_001']['placas_autorizadas'])} placas)")
    print(f"Usuario 2: {usuarios_dict['user_002']['nombre']} ({len(usuarios_dict['user_002']['placas_autorizadas'])} placas)")
    print("-" * 30)

if __name__ == "__main__":
    generar_usuarios_con_placas()