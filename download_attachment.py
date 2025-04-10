# Descarga archivos adjuntos específicos de correos en Gmail y los marca como leídos.

import os
import base64
from typing import List
import time
from google_apis import create_service # Importa la función para crear el servicio

# --- Clases de Excepción Personalizadas ---
class GmailException(Exception):
    """Excepción base para errores de Gmail."""
    pass

class NoEmailFound(GmailException):
    """Excepción para cuando no se encuentran correos."""
    pass

# --- Constantes ---
CLIENT_FILE = 'credentials.json'
API_NAME = 'gmail'
API_VERSION = 'v1'
SCOPES = ['https://mail.google.com/'] # Permisos necesarios para leer y modificar correos
# Directorio donde se guardarán los reportes descargados
SAVE_LOCATION = r"C:\Users\erika\Desktop\SERVICIO SOCIAL - BD\PYTHON\Reportes desde gmail"

# --- Funciones de Interacción con Gmail API ---

def search_emails(service, query_string: str, label_ids: List = None):
    """Busca correos en Gmail que coincidan con la consulta."""
    try:
        # print(f"Buscando correos: '{query_string}'") # Descomentar para depuración
        message_list_response = service.users().messages().list(
            userId='me', labelIds=label_ids, q=query_string
        ).execute()

        message_items = message_list_response.get('messages', [])
        next_page_token = message_list_response.get('nextPageToken')

        # Manejar paginación si hay muchos resultados
        while next_page_token:
            message_list_response = service.users().messages().list(
                userId='me', labelIds=label_ids, q=query_string, pageToken=next_page_token
            ).execute()
            message_items.extend(message_list_response.get('messages', []))
            next_page_token = message_list_response.get('nextPageToken')

        if not message_items:
            print("ℹ️ No se encontraron correos nuevos que coincidan.")
        # else: # Descomentar para depuración
            # print(f" Se encontraron {len(message_items)} correos.")
        return message_items
    except Exception as e:
        print(f"❌ Error buscando correos: {e}")
        return []

def get_file_data(service, message_id, attachment_id):
    """Obtiene los datos binarios de un archivo adjunto."""
    try:
        response = service.users().messages().attachments().get(
            userId='me', messageId=message_id, id=attachment_id
        ).execute()
        file_data = base64.urlsafe_b64decode(response.get('data').encode('UTF-8'))
        return file_data
    except Exception as e:
        print(f"❌ Error descargando adjunto ID {attachment_id} del mensaje {message_id}: {e}")
        return None

def get_message_detail(service, message_id, msg_format='metadata', metadata_headers: List = None):
    """Obtiene los detalles de un mensaje específico."""
    try:
        message_detail = service.users().messages().get(
            userId='me', id=message_id, format=msg_format, metadataHeaders=metadata_headers
        ).execute()
        return message_detail
    except Exception as e:
        print(f"❌ Error obteniendo detalles del mensaje {message_id}: {e}")
        return None

# --- Flujo Principal ---
def main():
    print("--- Iniciando Descarga de Reportes Gmail ---")
    service = create_service(CLIENT_FILE, API_NAME, API_VERSION, SCOPES)

    if service is None:
        print("❌ Falló la creación del servicio de Gmail. Abortando.")
        return

    # Consulta para buscar correos no leídos con adjuntos
    query_string = 'is:unread has:attachment subject:"Marina report"' # Más específico si es posible
    # Si los correos no tienen un asunto común, usa solo:
    # query_string = 'is:unread has:attachment'

    # Crear directorio de guardado si no existe
    if not os.path.exists(SAVE_LOCATION):
        try:
            os.makedirs(SAVE_LOCATION)
            print(f"Directorio creado: {SAVE_LOCATION}")
        except OSError as e:
            print(f"❌ Error creando directorio {SAVE_LOCATION}: {e}. Abortando.")
            return

    # Buscar correos
    email_messages = search_emails(service, query_string)

    files_downloaded_count = 0
    # Procesar cada correo encontrado
    for email_message in email_messages:
        msg_id = email_message['id']
        print(f"\nProcesando mensaje ID: {msg_id}")
        messageDetail = get_message_detail(service, msg_id, msg_format='full', metadata_headers=['parts'])

        if not messageDetail: continue # Saltar si no se obtienen detalles

        messageDetailPayload = messageDetail.get('payload')
        if not messageDetailPayload: continue

        # Iterar sobre las partes del mensaje para encontrar adjuntos
        if 'parts' in messageDetailPayload:
            for msgPayload in messageDetailPayload['parts']:
                file_name = msgPayload.get('filename')
                body = msgPayload.get('body')

                # Verificar si es un adjunto CSV de reporte
                if file_name and file_name.lower().endswith('.csv') and file_name.startswith('report-') and body and 'attachmentId' in body:
                    attachment_id = body['attachmentId']
                    print(f"  Encontrado adjunto: {file_name} (ID: {attachment_id})")

                    # Descargar el contenido del adjunto
                    attachment_content = get_file_data(service, msg_id, attachment_id)

                    if attachment_content:
                        # Guardar el archivo
                        file_path = os.path.join(SAVE_LOCATION, file_name)
                        try:
                            with open(file_path, 'wb') as f:
                                f.write(attachment_content)
                            print(f"  ✅ Archivo guardado: {file_path}")
                            files_downloaded_count += 1
                        except IOError as e:
                            print(f"  ❌ Error guardando archivo {file_name}: {e}")
                    else:
                         print(f"  ⚠️ No se pudo descargar el contenido de {file_name}.")

        # Marcar el correo como leído (quitar etiqueta UNREAD) después de procesar adjuntos
        try:
            service.users().messages().modify(
                userId='me', id=msg_id, body={'removeLabelIds': ['UNREAD']}
            ).execute()
            print(f"  Correo {msg_id} marcado como leído.")
        except Exception as e:
            print(f"  ⚠️ Error marcando correo {msg_id} como leído: {e}")

        time.sleep(0.2) # Pequeña pausa entre correos

    print(f"\n--- Descarga Finalizada: {files_downloaded_count} archivos descargados ---")

if __name__ == '__main__':
    main()