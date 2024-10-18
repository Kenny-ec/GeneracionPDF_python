from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.auth import login
import io
import tempfile
import os
    
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
        print(f"Ocurrió un error al convertir_pdf: {e}")
    finally:
        # Asegurarse de que el archivo temporal se elimine
        if temp_file_path:
            os.remove(temp_file_path)

def generar_certificados_secuencial(driveExcel, drivePDF):
    drive, creds = login()
    service = build('drive', 'v3', credentials=creds)

    try:
        archivos = listar_archivos(driveExcel, drive)
        for archivo in archivos:
            print(f"Nombre: {archivo['title']}, ID: {archivo['id']}")
            convertir_pdf(archivo, drivePDF, service, drive)
    
    except Exception as e:
        print(f"Ocurrió un error al generar certificado: {e}")

def generar_certificados_paralelo(driveExcel, drivePDF):
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
                futures.append(executor.submit(convertir_pdf, archivo, drivePDF, service, drive))

            for future in as_completed(futures):
                future.result()  # Esto captura cualquier excepción ocurrida en el hilo

    except Exception as e:
        print(f"Ocurrió un error al generar certificado: {e}")