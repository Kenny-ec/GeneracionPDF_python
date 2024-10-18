import os
import time

from src.auth import *
from src.GoogleDrive import *
from src.log_config import *


if __name__ == "__main__":
    id_folder_excel = os.getenv("ID_DRIVE_SHEETS")
    id_folder_pdf = os.getenv("ID_DRIVE_DESTINO")

    if os.path.exists(directorio_credenciales):
        logger.info("Archivo de credenciales encontrado. Iniciando el proceso.")
        start_time = time.time()
        #Generar certificados paralelo
        generar_certificados_paralelo(id_folder_excel, id_folder_pdf)
        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f"Tiempo de ejecución paralelo: {elapsed_time:.2f} segundos")
    else:
        logger.warning("No se encontró el archivo de credenciales. Generando uno nuevo...")
        auth()