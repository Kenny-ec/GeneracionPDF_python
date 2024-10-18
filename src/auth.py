import yaml
import os

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError
from pydrive2.auth import GoogleAuth
from src.log_config import *
# Ruta al archivo de configuración YAML
yaml_file = 'settings.yaml'
directorio_credenciales = os.getenv("DIRECTORIO_CREDENCIALES")

def auth():

    try:
        #Cargar la configuración desde el archivo YAML
        with open(yaml_file, 'r') as file:
            config = yaml.safe_load(file)
        
        gauth = GoogleAuth()
        # Configurar GoogleAuth con los valores del archivo YAML
        gauth.LocalWebserverAuth()
        logger.info("Autenticación exitosa")
    except Exception as e:
        logger.error(f"Falló la autenticación: {e}")

#INICIAR SESION
def login():
   
    try:
        logger.info("Iniciando sesión con las credenciales guardadas")
        GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = directorio_credenciales
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile(directorio_credenciales)
        
        if gauth.credentials is None:
            gauth.LocalWebserverAuth(port_numbers=[8092])
        elif gauth.access_token_expired:
            logger.info("El token de acceso ha expirado, refrescando")
            gauth.Refresh()
        else:
            logger.info("Autorizando con las credenciales actuales")
            gauth.Authorize()
            
        gauth.SaveCredentialsFile(directorio_credenciales)
        logger.info("Credenciales guardadas exitosamente")
        #credenciales = GoogleDrive(gauth)
        return GoogleDrive(gauth), gauth.credentials
            
    except Exception as e:
        logger.error(f"Ocurrió un error al iniciar sesión: {e}")

