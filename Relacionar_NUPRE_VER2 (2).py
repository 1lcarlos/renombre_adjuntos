import sqlite3
import os
from pathlib import Path
##import fitz
##from PIL import Image
import io

##NOTA MAS IMPORTANTE OJO AL MOMENTO DE CORRER EL SCRIPT VA A MODIFICAR LA RUTA ORIGINAL POR LO QUE AL CORRER EL SCRIPT POR 
#SEGUNDA VEZ SALDRA ERROR POR QUE COMO SE CAMBIO EL NOMBRE DEL DOCUMENTO ORIGINAL ESO YA NO EXISTE, ENTONCES EL SISTEMA NO ENCONTRARA
#NINGUN ARCHIVO
#Ruta a tu archivo GeoPackage
gpkg_path = input("Ingrese la ruta del geopackage:") 
ruta = input("Ingrese la ruta donde esta el modelo de captura:")
ruta_origin = []
ruta_modificada = []
extensiones = []

# Conectar al GeoPackage
conn = sqlite3.connect(gpkg_path)
cursor = conn.cursor()
# Tabla temporal
cursor.execute("""
    create temporary table temp_extensiones (
        T_id integer,
        extension text
    )
""")
#Id para poder hacer los respectivos cambios
consulta0 = """ 
select archivo, T_id, cca_construccion_adjunto, cca_fuenteadminstrtiva_adjunto, 
       cca_interesado_adjunto, cca_unidadconstruccion_adjunto, cca_predio_adjunto
from cca_adjunto 
"""
cursor.execute(consulta0)
resultados1 = cursor.fetchall() 
print("consulta0: ",resultados1)

# Iterar sobre los resultados y procesar la información
for resultado in resultados1:
    archivo, t_id, cca_construccion, cca_fuenteadmin, cca_interesado, cca_unidadconstruccion, cca_predio = resultado
    
    # Crear la ruta completa del archivo
    ruta_completa = os.path.join(ruta, archivo)
    ruta_origin.append(ruta_completa)
    
    # Convertir a objeto Path para obtener la extensión
    ruta_objeto = Path(ruta_completa)
    extension = ruta_objeto.suffix.lower()  # Convertir a minúsculas
    extensiones.append((extension, t_id))


# Opcionalmente, si solo deseas imprimir las listas al final, fuera del ciclo:
#print(f"Rutas completas: {ruta_origin}")
#print(f"Extensiones y t_id: {extensiones}")

# Insertar los datos en la tabla temporal
cursor.executemany("""
    insert into temp_extensiones (T_id, extension)
    values (?, ?)
""", extensiones)


# Consulta para verificar el contenido de la tabla temporal ##ojo t_id es extensiones, y extensiones es t_id corregir 
consulta1 = """
select extension,T_id from temp_extensiones
"""
cursor.execute(consulta1)
resultados_temp = cursor.fetchall()
print("consulta 1",resultados_temp)



consulta2 = """ 
select adjunto.T_id, cons.T_id, predio.T_id, predio.numero_predial
from cca_adjunto as adjunto
    join cca_construccion as cons on cons.T_id = adjunto.cca_construccion_adjunto
    join cca_predio as predio on predio.T_id = cons.predio
"""
cursor.execute(consulta2)
resu = cursor.fetchall() 
print("consultanumerosprediales: ",resu)

###############################################################################################################################################################################


# Mostrar la lista de extensiones con T_id y los resultados de la tabla temporal, verificacion por si el codigo falla
# util para verificar que esta captando
#print("extensiones y T_id:", extensiones)
#print("contenido de la tabla temporal:", resultados_temp)

#Consulta para definir el tipo de archivo
consulta2 = """
update cca_adjunto
set tipo_archivo = case 
    when cca_construccion_adjunto is not null and cca_unidadconstruccion_adjunto is not null then 'construcciones'
    when cca_fuenteadminstrtiva_adjunto is not null then 'fuenteadministrativa'
    when cca_interesado_adjunto is not null then 'interesado'
    when cca_unidadconstruccion_adjunto is not null then 'unidad de construcción'
    when cca_predio_adjunto is not null then 'predio'
    else tipo_archivo
end;
"""
cursor.execute(consulta2)

consultaDependencia = """
update cca_adjunto
set dependencia_ucons = case 
    when dependencia_ucons = '1' and tipo_archivo = 'unidad de construcción' then 'Estructura'
    when dependencia_ucons = '2' and tipo_archivo = 'unidad de construcción' then 'Acabados_Principales'
    when dependencia_ucons = '3' and tipo_archivo = 'unidad de construcción' then 'Baño'
    when dependencia_ucons = '4' and tipo_archivo = 'unidad de construcción' then 'Cocina'
    when dependencia_ucons = '5' and tipo_archivo = 'unidad de construcción' then 'Complemento_Industria'
    else dependencia_ucons
end;
"""
cursor.execute(consultaDependencia)


# ##si sirveee


e="""
-- Crear una tabla temporal para almacenar las rutas base y su contador
CREATE TEMPORARY TABLE temp_rutas_unicas AS
SELECT 
    adjunto.T_Id AS T_id,
    'DCIM/' || 'CO_' || predio.numero_predial || SUBSTR(e.T_id, 1, INSTR(e.T_id, '.') - 1) AS RutaBase,
    SUBSTR(e.T_id, INSTR(e.T_id, '.')) AS Extension,
    COUNT(*) OVER (PARTITION BY 'DCIM/' || 'CO_' || predio.numero_predial || e.T_id) AS Duplicados,
    ROW_NUMBER() OVER (
        PARTITION BY 'DCIM/' || 'CO_' || predio.numero_predial || e.T_id 
        ORDER BY adjunto.T_Id
    ) AS Sufijo
FROM cca_adjunto AS adjunto
JOIN cca_construccion AS cons ON cons.T_id = adjunto.cca_construccion_adjunto
JOIN cca_predio AS predio ON predio.T_id = cons.predio
JOIN temp_extensiones AS e ON adjunto.T_Id = e.extension
WHERE adjunto.tipo_archivo = 'construcciones';
"""


c= """
-- Actualizar la tabla `cca_adjunto` con las rutas únicas
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


u= """
UPDATE cca_adjunto
SET ruta_modificada = (
select 
case 
            -- Caso 2: cca_fuenteadministrativa
            when adjunto.cca_fuenteadminstrtiva_adjunto = cf.T_Id then 
                'DCIM/' || 'FA_'|| predio.numero_predial || e.T_id
            end as ruta_modificada
            from cca_adjunto as adjunto
            	join cca_fuenteadministrativa cf  on cf.T_id = adjunto.cca_fuenteadminstrtiva_adjunto 
            	join cca_fuenteadministrativa_derecho cfd on cfd.fuente_administrativa =cf.T_Id 
            	join cca_derecho d on d.T_Id = cfd.derecho 
                join cca_predio as predio on predio.T_id = d.predio
                join temp_extensiones as e on adjunto.T_id = e.extension
                where cca_adjunto.cca_fuenteadminstrtiva_adjunto = cf.T_Id
) WHERE tipo_archivo = 'fuenteadministrativa';
""" 

i = """
UPDATE cca_adjunto
SET ruta_modificada = (
    SELECT 
        'DCIM/' || 
        string_agg(
            CASE 
                WHEN adjunto.cca_interesado_adjunto = i.T_Id THEN 
                    'In_' || predio.numero_predial
            END, ',' -- Separador entre las rutas concatenadas
        ) || MAX(e.T_id)
    FROM cca_adjunto AS adjunto
    JOIN cca_interesado AS i ON i.T_Id = adjunto.cca_interesado_adjunto
    JOIN cca_derecho AS d ON d.interesado = i.T_Id
    JOIN cca_predio AS predio ON predio.T_id = d.predio
    JOIN temp_extensiones AS e ON adjunto.T_id = e.extension
    WHERE cca_adjunto.cca_interesado_adjunto = i.T_Id
    GROUP BY adjunto.cca_interesado_adjunto
) WHERE tipo_archivo = 'interesado';

"""

h= """
-- Crear una tabla temporal para almacenar las rutas base y su contador
CREATE TEMPORARY TABLE temp_rutas_unicas2 AS
SELECT 
    adjunto.T_Id AS T_id,
    'DCIM/' || 'UC_' || predio.numero_predial || '_' || adjunto.dependencia_ucons AS RutaBase,
    e.T_id AS Extension,
    COUNT(*) OVER (PARTITION BY 'DCIM/' || 'UC_' || predio.numero_predial || '_' || adjunto.dependencia_ucons || e.T_id) AS Duplicados,
    ROW_NUMBER() OVER (
        PARTITION BY 'DCIM/' || 'UC_' || predio.numero_predial || '_' || adjunto.dependencia_ucons || e.T_id 
        ORDER BY adjunto.T_Id
    ) AS Sufijo
FROM cca_adjunto AS adjunto
JOIN cca_unidadconstruccion AS u ON u.T_Id = adjunto.cca_unidadconstruccion_adjunto
JOIN cca_construccion AS c ON u.construccion = c.T_Id
JOIN cca_predio AS predio ON predio.T_Id = c.predio
JOIN temp_extensiones AS e ON adjunto.T_id = e.extension
WHERE adjunto.cca_unidadconstruccion_adjunto = u.T_Id 
  AND adjunto.cca_construccion_adjunto IS NULL 
  AND adjunto.tipo_archivo = 'unidad de construcción';
"""

p= """
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

d= """

UPDATE cca_adjunto
SET ruta_modificada = (
select 
case 
            -- Caso 4: cca_predio
            when adjunto.cca_predio_adjunto = predio.T_Id then 
                'DCIM/' || 'PE_'|| predio.numero_predial || e.T_id
            end as ruta_modificada
            from cca_adjunto as adjunto
                join cca_predio as predio on predio.T_id = adjunto.cca_predio_adjunto
                join temp_extensiones as e on adjunto.T_id = e.extension
                WHERE cca_adjunto.cca_predio_adjunto = predio.T_Id  )
            WHERE tipo_archivo = 'predio';     

"""
cursor.execute(e)
cursor.execute(c)
cursor.execute(u)
cursor.execute(i)
cursor.execute(h)
cursor.execute(p)
cursor.execute(d)

# Confirmar los cambios

conn.commit()

consulta1 = """
select ruta_modificada, archivo T_id from cca_adjunto 
"""
cursor.execute(consulta1)
resultados2 = cursor.fetchall()
print("Ruta Modificada: ",resultados2)

for resultado in resultados2:
    ruta_modificada, archivo = resultado  # Desempaquetar la tupla correctamente
    if archivo is not None and ruta_modificada is not None:  # Verifica que archivo no sea None
        # Mostrar el archivo original y el nuevo archivo renombrado
        archivo_original = os.path.join(ruta, archivo)
        nuevo_archivo = os.path.join(ruta, ruta_modificada)
        print(f"Archivo original: {archivo_original}, Nuevo archivo: {nuevo_archivo}")

        if os.path.exists(archivo_original):
            try:
                    os.rename(archivo_original, nuevo_archivo)  # Renombrar archivo
                    print(f"Archivo renombrado de {archivo_original} a {nuevo_archivo}")
            except Exception as e:
                    print(f"Error renombrando {archivo_original}: {e}")
        else:
                print(f"El archivo original {archivo_original} no existe.")        
    else:
        print(f"Archivo es None en el resultado: {resultado}")



'''# Reducir tamaño de archivos PDF e imágenes
def reducir_pdf(ruta_pdf):
    doc = fitz.open(ruta_pdf)
    for page in doc:
        # Iterar sobre las imágenes de cada página
        for img in page.get_images(full=True):
            xref = img[0]  # Obtener el identificador de la imagen
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]  # Obtener los bytes de la imagen

            # Cargar la imagen en PIL para reducir su tamaño
            image = Image.open(io.BytesIO(image_bytes))
            image = image.convert("RGB")  # Convertir si es necesario
            image = image.resize((int(image.width / 2), int(image.height / 2)))  # Reducir el tamaño a la mitad, por ejemplo

            # Guardar la imagen comprimida en un nuevo archivo temporal
            image_path = "temp_image.jpg"
            image.save(image_path, quality=60, optimize=True)

            # Reemplazar la imagen original en el PDF por la imagen comprimida
            new_xref = doc.add_image(image_path)
            page.replace_image(xref, new_xref)

    doc.save(ruta_pdf, deflate=True)  # Guardar el PDF comprimido


def reducir_imagen(ruta_imagen):
    with Image.open(ruta_imagen) as img:
        img = img.convert("RGB")  # Asegurar compatibilidad
        img.save(ruta_imagen, optimize=True, quality=60)  # Reducir calidad al 60%'''


# Cambiar nombres de los archivos y reducir tamaño
#for archivo_original, archivo_nuevo in zip(ruta_origin, ruta_modificada):
#    ruta_original = os.path.join(archivo_original)
#    ruta_nueva = os.path.join(archivo_nuevo)
    
    # Renombrar el archivo
#    os.rename(ruta_original, ruta_nueva)
    
'''    # Reducir el tamaño según el tipo de archivo
    extension = Path(ruta_nueva).suffix.lower()
    if extension == '.pdf':
        reducir_pdf(ruta_nueva)
    elif extension in ['.jpg', '.jpeg', '.png', '.tiff']:
        reducir_imagen(ruta_nueva)'''

# Confirmar cambios
conn.close()
