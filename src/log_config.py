import logging
import os

from datetime import datetime
from logging.handlers import RotatingFileHandler

# Crear el directorio de logs si no existe
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Generar el nombre del archivo de log con fecha y hora actual
log_filename = os.path.join(log_dir,  f"Registro_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
file_handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=2)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)