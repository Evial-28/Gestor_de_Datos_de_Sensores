# Maneja la autenticación con Google APIs (OAuth 2.0) y crea el objeto de servicio.

import os
import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# --- Constantes Globales ---
TOKEN_DIR = 'token files' # Directorio para guardar tokens

def create_service(client_secret_file, api_name, api_version, *scopes):
    """
    Crea o refresca credenciales y construye el objeto de servicio para una Google API.

    Args:
        client_secret_file (str): Ruta al archivo JSON de credenciales del cliente.
        api_name (str): Nombre de la API (ej. 'gmail').
        api_version (str): Versión de la API (ej. 'v1').
        *scopes: Lista de scopes (permisos) requeridos.

    Returns:
        googleapiclient.discovery.Resource: Objeto de servicio de la API o None si falla.
    """
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]] # Asegurar que scopes sea una lista
    TOKEN_FILE = os.path.join(TOKEN_DIR, f'token_{api_name}_{api_version}.json')

    creds = None

    # --- Carga o Refresco de Token ---
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            print(f"❌ Error al cargar token desde {TOKEN_FILE}: {e}")

    # Si no hay credenciales válidas o expiraron (y se puede refrescar)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print(f"Refrescando token para {api_name}...")
                creds.refresh(Request())
            except Exception as e:
                print(f"❌ Error al refrescar token: {e}. Se requerirá nuevo login.")
                creds = None # Forzar nuevo flujo si el refresh falla
        # Si no hay token o no se pudo refrescar, iniciar flujo OAuth
        if not creds:
            try:
                print(f"Iniciando flujo de autenticación para {api_name}...")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                # Puerto 0 para que elija uno libre automáticamente (más robusto)
                creds = flow.run_local_server(port=0, prompt='consent')
            except Exception as e:
                print(f"❌ Error durante el flujo de autenticación: {e}")
                return None

        # --- Guardado de Token ---
        try:
            os.makedirs(TOKEN_DIR, exist_ok=True) # Crear directorio si no existe
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
                print(f"✅ Token guardado/actualizado en: {TOKEN_FILE}")
        except Exception as e:
            print(f"❌ Error al guardar token en {TOKEN_FILE}: {e}")
            # No retornar None aquí, intentar crear el servicio de todas formas

    # --- Creación del Servicio de API ---
    try:
        print(f"Creando servicio para {API_SERVICE_NAME} v{API_VERSION}...")
        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds)
        print(f"✅ Servicio {API_SERVICE_NAME} creado con éxito.")
        return service
    except Exception as e:
        print(f"❌ Error al crear el servicio {API_SERVICE_NAME}: {e}")
        return None

# Bloque para probar la conexión si se ejecuta este archivo directamente
if __name__ == "__main__":
    # Reemplaza con tus datos si es necesario probar
    CLIENT_SECRET_FILE = "credentials.json"
    API_NAME = "gmail"
    API_VERSION = "v1"
    SCOPES = ["https://mail.google.com/"] # Ejemplo para Gmail

    service = create_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
    if service:
        print("\nPrueba de conexión exitosa.")
    else:
        print("\nPrueba de conexión fallida.")