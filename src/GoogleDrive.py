import io

from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.auth import login
from src.log_config import *
    
def listar_archivos(id_folder, drive):
    try:
        logger.info(f"Listando archivos de la carpeta Spreadsheets")
        # Consulta para obtener archivos en la carpeta especificada
        query = f"'{id_folder}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        archivos = drive.ListFile({'q': query}).GetList()
        
        if not archivos:
            logger.warning("No se encontraron archivos en la carpeta.")
        else:
            return archivos
        
    except Exception as e:
        logger.error(f"Ocurrió un error al listar los archivos: {e}")

def convertir_pdf(drive, service_Drive, archivo, hoja, id_carpeta_destino):
    archivo_id = archivo['id']
    nombre_archivo = archivo['title']
    hoja_nombre = hoja['properties']['title']
    hoja_id = hoja['properties']['sheetId']

    try:
        # Construir la URL de exportación para la hoja específica
        pdfExportUrl = f"https://docs.google.com/spreadsheets/d/{archivo_id}/export?exportFormat=pdf&format=pdf&gid={hoja_id}"

        # Realizar la solicitud para exportar la hoja a PDF
        response, content = service_Drive._http.request(uri=pdfExportUrl)

        # Crear un stream para almacenar el PDF
        pdf_stream = io.BytesIO(content)

        pdf_stream.seek(0)  

       # Subir el archivo PDF directamente desde el stream a Google Drive
        archivo_pdf = drive.CreateFile({
            'title': f"{hoja_nombre}.pdf",
            'parents': [{"id": id_carpeta_destino}]
        })
        archivo_pdf.content = pdf_stream
        archivo_pdf.Upload()
        
        logger.info(f"Se convirtió la hoja: {hoja_nombre} del archivo: {nombre_archivo} a PDF")

    except Exception as e:
        logger.error(f"Ocurrió un error al convertir la hoja {nombre_archivo} a PDF del archivo {nombre_archivo}: {e}")

def procesar_spreadsheet(drive, service_sheets, service_Drive, archivo, idDrivePDF):
    archivo_id = archivo['id']
    nombre_archivo = archivo['title']
    logger.info(f"Procesando Spreadsheet: {nombre_archivo}")

    # Crear una carpeta en el destino con el nombre del Spreadsheet
    id_carpeta_destino = crear_carpeta(drive, nombre_archivo, idDrivePDF)

    # Obtener todas las hojas del Spreadsheet
    hojas = obtener_hojas(service_sheets, archivo_id)
    # Procesar las hojas en paralelo
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for hoja in hojas:
            futures.append(executor.submit(convertir_pdf, drive, service_Drive, archivo, hoja, id_carpeta_destino))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Ocurrió un error al usar hilos en la conversión de hojas a PDF: {e}")

def crear_carpeta(drive, nombre_carpeta, id_folder_destino):
    try:
        folder_metadata = {
            'title': nombre_carpeta,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [{'id': id_folder_destino}]
        }
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
        logger.info(f"Carpeta '{nombre_carpeta}' creada con ID: {folder['id']}")
        return folder['id']
    except Exception as e:
        logger.error(f"Ocurrió un error al crear la carpeta {nombre_carpeta}: {e}")

def obtener_hojas(service, spreadsheet_id):
    try:
        sheet = service.spreadsheets()
        spreadsheet = sheet.get(spreadsheetId=spreadsheet_id).execute()
        hojas = spreadsheet['sheets']
        logger.info(f"Obtenidas {len(hojas)} hojas del Spreadsheet ID: {spreadsheet_id}")
        return hojas
    except Exception as e:
        logger.error(f"Ocurrió un error al obtener las hojas del Spreadsheet {spreadsheet_id}: {e}")


def generar_certificados_paralelo(idDriveExcel, idDrivePDF):
    drive, creds = login()
    service_Drive = build('drive', 'v3', credentials=creds)
    service_sheets = build('sheets', 'v4', credentials=creds)

    try:
        archivos = listar_archivos(idDriveExcel, drive)
        if not archivos:
            logger.warning("No hay archivos para procesar.")
            return
        
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            for archivo in archivos:
                futures.append(executor.submit(procesar_spreadsheet, drive, service_sheets, service_Drive, archivo, idDrivePDF))

            for future in as_completed(futures):
                try:
                    future.result() #obtiene resultado de la tarea de cualquier hilo
                except Exception as e:
                    logger.error(f"Ocurrió un error en un hilo mientras se generaban certificados: {e}")

    except Exception as e:
        logger.error(f"Ocurrió un error al generar certificados paralelamente: {e}")