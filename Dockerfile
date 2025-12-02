# Usa una imagen base de Python (por ejemplo, la versión 3.11 slim)
FROM python:3.11-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos del repositorio al contenedor
# Esto incluye tus scripts Python
COPY . /app

# Instala cualquier dependencia (si tienes un archivo requirements.txt, úsalo aquí)
# Si no tienes dependencias externas, puedes omitir este paso,
# pero para entornos reales casi siempre es necesario.

# EXPOSE 8080 (O el puerto que use tu aplicación, si aplica)

# Define el comando por defecto para correr tu script principal
# (Ajusta 'importar_a_mongo_final.py' si este no es el archivo que debe ejecutar el contenedor)
CMD ["python", "importar_a_mongo_final.py"]
