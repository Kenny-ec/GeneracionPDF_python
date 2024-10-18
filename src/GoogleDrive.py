import io

from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.auth import login
from src.log_config import *
    
def listar_archivos(id_folder, drive):
    try:
        logger.info(f"Listando archivos en la carpeta con ID: {id_folder}")
        # Consulta para obtener archivos en la carpeta especificada
        query = f"'{id_folder}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        archivos = drive.ListFile({'q': query}).GetList()
        
        if not archivos:
            logger.warning("No se encontraron archivos en la carpeta.")
        else:
            return archivos
        
    except Exception as e:
        logger.error(f"Ocurrió un error al listar los archivos: {e}")

def convertir_pdf(archivo, driveDestino,service, drive):
    archivo_id = archivo['id']
    nombre_archivo = archivo['title']
    
    try:
        # Solicitar la exportación a PDF
        request = service.files().export_media(fileId=archivo_id, mimeType='application/pdf')

        # Crear un stream para almacenar el PDF
        pdf_stream = io.BytesIO()

        # Descargar el archivo PDF directamente al stream
        downloader = MediaIoBaseDownload(pdf_stream, request)
        done = False

        while done is False:
            status, done = downloader.next_chunk()

        pdf_stream.seek(0)    

        # Subir el archivo PDF directamente desde el stream a Google Drive
        #media = MediaIoBaseUpload(pdf_stream, mimetype='application/pdf')
        archivo_pdf = drive.CreateFile({
            'title': f"{nombre_archivo}.pdf",
            'parents': [{"id": driveDestino}]
        })
        archivo_pdf.content = pdf_stream
        archivo_pdf.Upload()
        
        logger.info(f"Se convirtió el archivo: {nombre_archivo} a PDF")

    except Exception as e:
        logger.error(f"Ocurrió un error al convertir archivo {nombre_archivo} a PDF: {e}")

def generar_certificados_paralelo(driveExcel, drivePDF):
    drive, creds = login()
    service = build('drive', 'v3', credentials=creds)

    try:
        archivos = listar_archivos(driveExcel, drive)
        if not archivos:
            logger.warning("No hay archivos para procesar.")
            return
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for archivo in archivos:
                futures.append(executor.submit(convertir_pdf, archivo, drivePDF, service, drive))

            for future in as_completed(futures):
                try:
                    future.result() #obtiene resultado de la tarea de cualquier hilo
                except Exception as e:
                    logger.error(f"Ocurrió un error en un hilo mientras se generaban certificados: {e}")

    except Exception as e:
        logger.error(f"Ocurrió un error al generar certificados paralelamente: {e}")