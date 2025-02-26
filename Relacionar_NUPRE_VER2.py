import sqlite3
import os
from pathlib import Path
import fitz
from PIL import Image
import io
from datetime import datetime

# Solicitar rutas de entrada
gpkg_path = input("Ingrese la ruta del geopackage:") 
ruta = input("Ingrese la ruta donde está el modelo de captura:")

ruta_origin = []
ruta_modificada_list = []
extensiones = []

# Conectar al GeoPackage
conn = sqlite3.connect(gpkg_path)
cursor = conn.cursor()

# Crear tabla temporal para almacenar T_id y extensión
cursor.execute("""
    CREATE TEMPORARY TABLE temp_extensiones (
        T_id INTEGER,
        extension TEXT
    )
""")

# Consulta inicial para obtener datos de la tabla cca_adjunto
consulta0 = """ 
SELECT archivo, T_id, cca_construccion_adjunto, cca_fuenteadminstrtiva_adjunto, 
       cca_interesado_adjunto, cca_unidadconstruccion_adjunto, cca_predio_adjunto
FROM cca_adjunto 
"""
cursor.execute(consulta0)
resultados1 = cursor.fetchall() 
print("consulta0: ", resultados1)

# Recopilar rutas y extensiones
for resultado in resultados1:
    archivo, t_id, cca_construccion, cca_fuenteadmin, cca_interesado, cca_unidadconstruccion, cca_predio = resultado
    
    ruta_completa = os.path.join(ruta, archivo)
    ruta_origin.append(ruta_completa)
    
    # Se obtiene la extensión a partir del nombre del archivo
    ruta_objeto = Path(ruta_completa)
    extension = ruta_objeto.suffix.lower()  # en minúsculas
    # IMPORTANTE: Se guarda T_id y extensión en el orden correcto
    extensiones.append((t_id, extension))

# Insertar datos en la tabla temporal
cursor.executemany("""
    INSERT INTO temp_extensiones (T_id, extension)
    VALUES (?, ?)
""", extensiones)

# Verificar contenido de la tabla temporal
consulta1 = "SELECT extension, T_id FROM temp_extensiones"
cursor.execute(consulta1)
resultados_temp = cursor.fetchall()
print("consulta 1", resultados_temp)

# Consulta adicional para relacionar predios (solo para verificación)
consulta2 = """ 
SELECT adjunto.T_id, cons.T_id, predio.T_id, predio.numero_predial
FROM cca_adjunto AS adjunto
    JOIN cca_construccion AS cons ON cons.T_id = adjunto.cca_construccion_adjunto
    JOIN cca_predio AS predio ON predio.T_id = cons.predio
"""
cursor.execute(consulta2)
resu = cursor.fetchall() 
print("consultanumerosprediales: ", resu)

# Actualizar tipo de archivo en cca_adjunto
consulta_tipo = """
UPDATE cca_adjunto
SET tipo_archivo = CASE 
    WHEN cca_construccion_adjunto IS NOT NULL AND cca_unidadconstruccion_adjunto IS NOT NULL THEN 'construcciones'
    WHEN cca_fuenteadminstrtiva_adjunto IS NOT NULL THEN 'fuenteadministrativa'
    WHEN cca_interesado_adjunto IS NOT NULL THEN 'interesado'
    WHEN cca_unidadconstruccion_adjunto IS NOT NULL THEN 'unidad de construcción'
    WHEN cca_predio_adjunto IS NOT NULL THEN 'predio'
    ELSE tipo_archivo
END;
"""
cursor.execute(consulta_tipo)

consultaDependencia = """
UPDATE cca_adjunto
SET dependencia_ucons = CASE 
    WHEN dependencia_ucons = '1' AND tipo_archivo = 'unidad de construcción' THEN 'Estructura'
    WHEN dependencia_ucons = '2' AND tipo_archivo = 'unidad de construcción' THEN 'Acabados_Principales'
    WHEN dependencia_ucons = '3' AND tipo_archivo = 'unidad de construcción' THEN 'Baño'
    WHEN dependencia_ucons = '4' AND tipo_archivo = 'unidad de construcción' THEN 'Cocina'
    WHEN dependencia_ucons = '5' AND tipo_archivo = 'unidad de construcción' THEN 'Complemento_Industria'
    ELSE dependencia_ucons
END;
"""
cursor.execute(consultaDependencia)

# Consulta para construir rutas únicas para 'construcciones'
e = """
CREATE TEMPORARY TABLE temp_rutas_unicas AS
SELECT 
    adjunto.T_Id AS T_id,
    'DCIM/' || 'CO_' || predio.numero_predial || SUBSTR(CAST(adjunto.T_Id AS TEXT), 1, INSTR(CAST(adjunto.T_Id AS TEXT), '.') - 1) AS RutaBase,
    e.extension AS Extension,
    COUNT(*) OVER (PARTITION BY 'DCIM/' || 'CO_' || predio.numero_predial || CAST(adjunto.T_Id AS TEXT)) AS Duplicados,
    ROW_NUMBER() OVER (
        PARTITION BY 'DCIM/' || 'CO_' || predio.numero_predial || CAST(adjunto.T_Id AS TEXT)
        ORDER BY adjunto.T_Id
    ) AS Sufijo
FROM cca_adjunto AS adjunto
JOIN cca_construccion AS cons ON cons.T_id = adjunto.cca_construccion_adjunto
JOIN cca_predio AS predio ON predio.T_id = cons.predio
JOIN temp_extensiones AS e ON adjunto.T_id = e.T_id
WHERE adjunto.tipo_archivo = 'construcciones';
"""
cursor.execute(e)

c = """
UPDATE cca_adjunto
SET ruta_modificada = (
    SELECT 
        CASE 
            WHEN temp_rutas_unicas.Duplicados = 1 THEN temp_rutas_unicas.RutaBase || temp_rutas_unicas.Extension
            ELSE temp_rutas_unicas.RutaBase || '-' || temp_rutas_unicas.Sufijo || temp_rutas_unicas.Extension
        END
    FROM temp_rutas_unicas
    WHERE temp_rutas_unicas.T_id = cca_adjunto.T_Id
)
WHERE tipo_archivo = 'construcciones';
"""
cursor.execute(c)

u = """
UPDATE cca_adjunto
SET ruta_modificada = (
    SELECT 
        'DCIM/' || 'FA_' || predio.numero_predial || e.extension
    FROM cca_adjunto AS adjunto
    JOIN cca_fuenteadministrativa cf ON cf.T_id = adjunto.cca_fuenteadminstrtiva_adjunto 
    JOIN cca_fuenteadministrativa_derecho cfd ON cfd.fuente_administrativa = cf.T_Id 
    JOIN cca_derecho d ON d.T_Id = cfd.derecho 
    JOIN cca_predio AS predio ON predio.T_id = d.predio
    JOIN temp_extensiones AS e ON adjunto.T_id = e.T_id
    WHERE adjunto.cca_fuenteadminstrtiva_adjunto = cf.T_Id
) 
WHERE tipo_archivo = 'fuenteadministrativa';
"""
cursor.execute(u)

i = """
UPDATE cca_adjunto
SET ruta_modificada = (
    SELECT 
        'DCIM/' || GROUP_CONCAT(
            CASE 
                WHEN adjunto.cca_interesado_adjunto = i.T_Id THEN 'In_' || predio.numero_predial
            END, ','
        ) || MAX(e.extension)
    FROM cca_adjunto AS adjunto
    JOIN cca_interesado AS i ON i.T_Id = adjunto.cca_interesado_adjunto
    JOIN cca_derecho AS d ON d.interesado = i.T_Id
    JOIN cca_predio AS predio ON predio.T_id = d.predio
    JOIN temp_extensiones AS e ON adjunto.T_id = e.T_id
    WHERE adjunto.cca_interesado_adjunto = i.T_Id
    GROUP BY adjunto.cca_interesado_adjunto
) 
WHERE tipo_archivo = 'interesado';
"""
cursor.execute(i)

h = """
CREATE TEMPORARY TABLE temp_rutas_unicas2 AS
SELECT 
    adjunto.T_Id AS T_id,
    'DCIM/' || 'UC_' || predio.numero_predial || '_' || adjunto.dependencia_ucons AS RutaBase,
    e.extension AS Extension,
    COUNT(*) OVER (PARTITION BY 'DCIM/' || 'UC_' || predio.numero_predial || '_' || adjunto.dependencia_ucons || e.extension) AS Duplicados,
    ROW_NUMBER() OVER (
        PARTITION BY 'DCIM/' || 'UC_' || predio.numero_predial || '_' || adjunto.dependencia_ucons || e.extension 
        ORDER BY adjunto.T_Id
    ) AS Sufijo
FROM cca_adjunto AS adjunto
JOIN cca_unidadconstruccion AS u ON u.T_Id = adjunto.cca_unidadconstruccion_adjunto
JOIN cca_construccion AS c ON u.construccion = c.T_Id
JOIN cca_predio AS predio ON predio.T_Id = c.predio
JOIN temp_extensiones AS e ON adjunto.T_id = e.T_id
WHERE adjunto.cca_unidadconstruccion_adjunto = u.T_Id 
  AND adjunto.cca_construccion_adjunto IS NULL 
  AND adjunto.tipo_archivo = 'unidad de construcción';
"""
cursor.execute(h)

p = """
UPDATE cca_adjunto
SET ruta_modificada = (
    SELECT 
        CASE 
            WHEN temp_rutas_unicas2.Duplicados = 1 THEN temp_rutas_unicas2.RutaBase || temp_rutas_unicas2.Extension
            ELSE temp_rutas_unicas2.RutaBase || '-' || temp_rutas_unicas2.Sufijo || temp_rutas_unicas2.Extension
        END
    FROM temp_rutas_unicas2
    WHERE temp_rutas_unicas2.T_id = cca_adjunto.T_Id
)
WHERE tipo_archivo = 'unidad de construcción';
"""
cursor.execute(p)

d = """
UPDATE cca_adjunto
SET ruta_modificada = (
    SELECT 
        'DCIM/' || 'PE_' || predio.numero_predial || e.extension
    FROM cca_adjunto AS adjunto
    JOIN cca_predio AS predio ON predio.T_id = adjunto.cca_predio_adjunto
    JOIN temp_extensiones AS e ON adjunto.T_id = e.T_id
    WHERE adjunto.cca_predio_adjunto = predio.T_Id  
)
WHERE tipo_archivo = 'predio';
"""
cursor.execute(d)

# Confirmar cambios en la base de datos
conn.commit()

# Consulta final para obtener la ruta modificada y el archivo
consulta_final = "SELECT ruta_modificada, archivo, T_Id FROM cca_adjunto"
cursor.execute(consulta_final)
resultados2 = cursor.fetchall()
print("Ruta Modificada: ", resultados2)

# Variables para log
modificados = 0
no_modificados = 0
detalles_no_modificados = []

# Iterar sobre los resultados y renombrar archivos
for resultado in resultados2:
    ruta_modificada, archivo, t_id = resultado
    if archivo is not None and ruta_modificada is not None:
        archivo_original = os.path.join(ruta, archivo)
        nuevo_archivo = os.path.join(ruta, ruta_modificada)
        try:
            if os.path.exists(archivo_original):
                os.rename(archivo_original, nuevo_archivo)
                print(f"Archivo renombrado de {archivo_original} a {nuevo_archivo}")
                modificados += 1
            else:
                observacion = "El archivo original no existe."
                print(observacion, archivo_original)
                no_modificados += 1
                detalles_no_modificados.append(f"T_id: {t_id}, archivo: {archivo}, observacion: {observacion}")
        except Exception as e:
            observacion = f"Error renombrando: {e}"
            print(observacion)
            no_modificados += 1
            detalles_no_modificados.append(f"T_id: {t_id}, archivo: {archivo}, observacion: {observacion}")
    else:
        observacion = "Registro incompleto (archivo o ruta_modificada es None)."
        print(observacion, resultado)
        no_modificados += 1
        detalles_no_modificados.append(f"T_id: {t_id}, archivo: {archivo}, observacion: {observacion}")

# Preparar el log con marca de tiempo
log_text = []
log_text.append(f"Registro de Modificaciones - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log_text.append(f"Total de registros procesados: {len(resultados2)}")
log_text.append(f"Registros modificados exitosamente: {modificados}")
log_text.append(f"Registros NO modificados: {no_modificados}")
if no_modificados > 0:
    log_text.append("Detalle de registros no modificados:")
    for detalle in detalles_no_modificados:
        log_text.append(" - " + detalle)
log_text.append("\nSugerencia: Revisar los registros no modificados para confirmar la existencia de archivos y consistencia en la base de datos.")

# Escribir el log en un archivo txt en la misma ruta del script
with open("modificaciones_log.txt", "w", encoding="utf-8") as log_file:
    log_file.write("\n".join(log_text))

print("Log generado: modificaciones_log.txt")

conn.close()
