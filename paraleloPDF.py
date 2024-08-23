from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor, as_completed

import io
import tempfile
import os
import time
directorio_credenciales = 'credentials_module.json'


#INICIAR SESION
def login():
    try:
        GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = directorio_credenciales
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile(directorio_credenciales)
        
        if gauth.credentials is None:
            gauth.LocalWebserverAuth(port_numbers=[8092])
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()
            
        gauth.SaveCredentialsFile(directorio_credenciales)
        #credenciales = GoogleDrive(gauth)
        return GoogleDrive(gauth), gauth.credentials
            
    except Exception as e:
        print(f"Ocurrió un error al iniciar sesion: {e}")
    
def listar_archivos(id_folder, drive):
    try:
        # Consulta para obtener archivos en la carpeta especificada
        query = f"'{id_folder}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        archivos = drive.ListFile({'q': query}).GetList()
        
        if not archivos:
            print("No se encontraron archivos en la carpeta.")
        else:
            return archivos
        
    except Exception as e:
        print(f"Ocurrió un error al listar los archivos: {e}")

def convertir_pdf(archivo, driveDestino,service, drive):
    archivo_id = archivo['id']
    nombre_archivo = archivo['title']
    temp_file_path = None

    # Solicitar la exportación a PDF
    request = service.files().export_media(fileId=archivo_id, mimeType='application/pdf')

    # Crear un buffer para almacenar el PDF
    pdf_io = io.BytesIO()

    # Descargar el archivo PDF
    downloader = MediaIoBaseDownload(pdf_io, request)
    done = False
    try:
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Descargando {nombre_archivo}: {int(status.progress() * 100)}% completado.")
        
        # Guardar el archivo PDF en un archivo temporal local
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
            temp_file.write(pdf_io.getvalue())
            temp_file_path = temp_file.name

        # Subir el archivo PDF desde el archivo temporal a Google Drive
        archivo_pdf = drive.CreateFile({
            'title': f"{nombre_archivo}.pdf",
            'parents': [{"id": driveDestino}]
        })

        archivo_pdf.SetContentFile(temp_file_path)
        archivo_pdf.Upload()
        print(f"{nombre_archivo}.pdf guardado en la carpeta destino.")
        
    except Exception as e:
        print(f"Ocurrió un error al convertir pdf: {e}")
    finally:
        # Asegurarse de que el archivo temporal se elimine
        if temp_file_path and os.path.isfile(temp_file_path):
            os.remove(temp_file_path)

def generar_certificados(driveExcel, drivePDF):
    drive, creds = login()
    service = build('drive', 'v3', credentials=creds)

    try:
        archivos = listar_archivos(driveExcel, drive)
        if not archivos:
            print("No hay archivos para procesar.")
            return
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for archivo in archivos:
                #print(f"Nombre: {archivo['title']}, ID: {archivo['id']}")
                #convertir_pdf(archivo, drivePDF, service, drive)
                futures.append(executor.submit(convertir_pdf, archivo, drivePDF, service, drive))

            for future in as_completed(futures):
                future.result()  # Esto captura cualquier excepción ocurrida en el hilo

    except Exception as e:
        print(f"Ocurrió un error al generar certificado: {e}")


if __name__ == "__main__":

    id_folder_excel = os.getenv("ID_DRIVE_SHEETS")
    id_folder_pdf = os.getenv("ID_DRIVE_DESTINO")

    start_time = time.time()

    generar_certificados(id_folder_excel, id_folder_pdf)
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución secuencial: {elapsed_time:.2f} segundos")
    