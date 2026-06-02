# Usamos una versión ligera de Python
FROM python:3.10-slim

# Le decimos a Docker que trabaje en la carpeta /app dentro del contenedor
WORKDIR /app

# Copiamos primero los requerimientos para instalarlos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto de tus archivos (tu main.py)
COPY . .

# Comando por defecto (aunque nosotros lo sobreescribiremos al ejecutar)
CMD ["python", "main.py", "uniforme"]