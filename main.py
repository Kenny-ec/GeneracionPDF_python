from src.auth import *
from src.GoogleDrive import *
import os
import time

if __name__ == "__main__":
    id_folder_excel = os.getenv("ID_DRIVE_SHEETS")
    id_folder_pdf = os.getenv("ID_DRIVE_DESTINO")

    if os.path.exists(directorio_credenciales):
        start_time = time.time()
        #Generar certificados secuencial
        #generar_certificados_secuencial(id_folder_excel, id_folder_pdf)

        #Generar certificados paralelo
        generar_certificados_paralelo(id_folder_excel, id_folder_pdf)
        
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"Tiempo de ejecución secuencial: {elapsed_time:.2f} segundos")
    else:
        print(f"No hay archivo de credenciales, generando una nueva...")
        auth()