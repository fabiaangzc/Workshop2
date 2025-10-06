Carga del CSV `the_grammy_awards.csv` a MySQL (Workbench)

Requisitos:
- Tener MySQL (MySQL Workbench o servicio MySQL) ejecutándose y accesible.
- Python 3.8+ instalado.

Pasos:
1) Instalar dependencias (PowerShell):

```powershell
python -m pip install -r requirements.txt
```

2) Establecer variables de entorno (PowerShell):

```powershell
$env:MYSQL_HOST = 'localhost'
$env:MYSQL_PORT = '3306'
$env:MYSQL_DB = 'your_db'
$env:MYSQL_USER = 'your_user'
$env:MYSQL_PASSWORD = 'your_password'
```

3) Ejecutar la carga:

```powershell
python load_grammys_to_mysql.py --csv data/the_grammy_awards.csv --table grammys --if-exists replace
```

Notas:
- El script normaliza columnas, convierte `year` a entero (si aplica) y `winner` a booleano.
- Si prefieres no usar variables de entorno, puedes modificar el script para leer un archivo `.env` o pasar una URL de conexión directamente.
- MySQL Workbench: después de la carga, refresca el esquema y deberías ver la tabla `grammys`.

Problemas comunes:
- Error de autenticación: revisa usuario/contraseña/host/puerto.
- Falta de driver: asegúrate de instalar `mysql-connector-python`.
