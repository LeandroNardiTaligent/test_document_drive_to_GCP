from __future__ import print_function
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import FailedPrecondition, RetryError
from typing import List, Sequence

import re
from google.cloud import documentai, storage

import io
import json
import requests

import pickle
import os
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
#from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
#from google.oauth2.credentials import Credentials

# TODO(developer): Uncomment these variables before running the sample.
# project_id = 'YOUR_PROJECT_ID'
# location = 'YOUR_PROCESSOR_LOCATION' # Format is 'us' or 'eu'
# processor_id = 'YOUR_PROCESSOR_ID' # Create processor before running sample
# gcs_input_uri = "YOUR_INPUT_URI" # Format: gs://bucket/directory/file.pdf
# input_mime_type = "application/pdf"
# gcs_output_bucket = "YOUR_OUTPUT_BUCKET_NAME" # Format: gs://bucket
# gcs_output_uri_prefix = "YOUR_OUTPUT_URI_PREFIX" # Format: directory/subdirectory/
# field_mask = "text,entities,pages.pageNumber"  # Optional. The fields to return in the Document object.


def batch_process_documents(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_input_uri: str,
    input_mime_type: str,
    gcs_output_bucket: str,
    gcs_output_uri_prefix: str,
    field_mask: str = None,
    timeout: int = 1000,
):

    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    gcs_document = documentai.GcsDocument(
        gcs_uri=gcs_input_uri, mime_type=input_mime_type
    )

    # Load GCS Input URI into a List of document files
    gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
    input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

    # NOTE: Alternatively, specify a GCS URI Prefix to process an entire directory
    #
    # gcs_input_uri = "gs://bucket/directory/"
    # gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_uri)
    # input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)
    #

    # Cloud Storage URI for the Output Directory
    destination_uri = f"{gcs_output_bucket}/{gcs_output_uri_prefix}/"

    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=destination_uri, field_mask=field_mask
    )

    # Where to write results
    output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

    # The full resource name of the processor, e.g.:
    # projects/project_id/locations/location/processor/processor_id
    name = client.processor_path(project_id, location, processor_id)

    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    # BatchProcess returns a Long Running Operation (LRO)
    operation = client.batch_process_documents(request)

    # Continually polls the operation until it is complete.
    # This could take some time for larger files
    # Format: projects/PROJECT_NUMBER/locations/LOCATION/operations/OPERATION_ID
    try:
        print(f"Waiting for operation {operation.operation.name} to complete...")
        operation.result(timeout=timeout)
    # Catch exception when operation doesn't finish before timeout
    except (RetryError) as e:
        print(e.message)

    # NOTE: Can also use callbacks for asynchronous processing
    #
    def my_callback(future):
      result = future.result()
    
    operation.add_done_callback(my_callback)

    # Once the operation is complete,
    # get output document information from operation metadata
    metadata = documentai.BatchProcessMetadata(operation.metadata)

    if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
        raise ValueError(f"Batch Process Failed: {metadata.state_message}")

    storage_client = storage.Client()

    print("Output files:")
    # One process per Input Document
    for process in metadata.individual_process_statuses:
        # output_gcs_destination format: gs://BUCKET/PREFIX/OPERATION_NUMBER/INPUT_FILE_NUMBER/
        # The Cloud Storage API requires the bucket name and URI prefix separately
        matches = re.match(r"gs://(.*?)/(.*)", process.output_gcs_destination)
        if not matches:
            print(
                "Could not parse output GCS destination:",
                process.output_gcs_destination,
            )
            continue

        output_bucket, output_prefix = matches.groups()

        # Get List of Document Objects from the Output Bucket
        output_blobs = storage_client.list_blobs(output_bucket, prefix=output_prefix)
        
        list_documents = []
        # Document AI may output multiple JSON files per source file
        for blob in output_blobs:
            # Document AI should only output JSON files to GCS
            if ".json" not in blob.name:
                print(
                    f"Skipping non-supported file: {blob.name} - Mimetype: {blob.content_type}"
                )
                continue

            # Download JSON File as bytes object and convert to Document Object
            print(f"Fetching {blob.name}")
            document = documentai.Document.from_json(
                blob.download_as_bytes(), ignore_unknown_fields=True
            )

            list_documents.append(document)
            
        return list_documents


# TODO(developer): Uncomment these variables before running the sample.
# project_id = 'YOUR_PROJECT_ID'
# location = 'YOUR_PROCESSOR_LOCATION' # Format is 'us' or 'eu'


def fetch_processor_types_sample(project_id: str, location: str):
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the location
    # e.g.: projects/project_id/locations/location
    parent = client.common_location_path(project_id, location)

    # Fetch all processor types
    response = client.fetch_processor_types(parent=parent)
    
    print("Processor types:")
    # Print the available processor types
    for processor_type in response.processor_types:
        if processor_type.allow_creation:
            print(processor_type.type_)


            
# TODO(developer): Uncomment these variables before running the sample.
# project_id = 'YOUR_PROJECT_ID'
# location = 'YOUR_PROCESSOR_LOCATION' # Format is 'us' or 'eu'
# processor_display_name = 'YOUR_PROCESSOR_DISPLAY_NAME' # Must be unique per project, 
#                                                          e.g.: 'My Processor'
# processor_type = 'YOUR_PROCESSOR_TYPE' # Use fetch_processor_types to get available 
#                                              processor types


def create_processor_sample(
    project_id: str, location: str, processor_display_name: str, processor_type: str
):
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the location
    # e.g.: projects/project_id/locations/location
    parent = client.common_location_path(project_id, location)

    # Create a processor
    processor = client.create_processor(
        parent=parent,
        processor=documentai.Processor(
            display_name=processor_display_name, type_=processor_type
        ),
    )

    # Print the processor information
    print(f"Processor Name: {processor.name}")
    print(f"Processor Display Name: {processor.display_name}")
    print(f"Processor Type: {processor.type_}")





def enable_processor_sample(project_id: str, location: str, processor_id: str):
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the location
    # e.g.: projects/project_id/locations/location/processors/processor_id
    processor_name = client.processor_path(project_id, location, processor_id)
    request = documentai.EnableProcessorRequest(name=processor_name)

    # Make EnableProcessor request
    try:
        operation = client.enable_processor(request=request)

        # Print operation name
        print(operation.operation.name)
        # Wait for operation to complete
        operation.result()
    # Cannot enable a processor that is already enabled
    except FailedPrecondition as e:
        print(e.message)


# TODO(developer): Uncomment these variables before running the sample.
# project_id = 'YOUR_PROJECT_ID'
# location = 'YOUR_PROCESSOR_LOCATION' # Format is 'us' or 'eu'
# processor_id = 'YOUR_PROCESSOR_ID'


def disable_processor_sample(project_id: str, location: str, processor_id: str):
    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the processor
    # e.g.: projects/project_id/locations/location/processors/processor_id
    processor_name = client.processor_path(project_id, location, processor_id)
    request = documentai.DisableProcessorRequest(name=processor_name)

    # Make DisableProcessor request
    try:
        operation = client.disable_processor(request=request)

        # Print operation name
        print(operation.operation.name)
        # Wait for operation to complete
        operation.result()
    # Cannot disable a processor that is already disabled
    except FailedPrecondition as e:
        print(e.message)

def online_process(
    project_id: str,
    location: str,
    processor_id: str,
    file_path: str,
    mime_type: str,
    gcs_input_uri: str,
    gcs_output_bucket: str,
    gcs_output_uri_prefix: str,
) -> documentai.Document:
    """
    Processes a document using the Document AI Online Processing API.
    """

    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}

    # Instantiates a client
    documentai_client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    resource_name = documentai_client.processor_path(project_id, location, processor_id)

    # Read the file into memory
    with open(file_path, "rb") as image:
        image_content = image.read()

        # Load Binary Data into Document AI RawDocument Object
        raw_document = documentai.RawDocument(
            content=image_content, mime_type=mime_type
        )

        # Configure the process request
        request = documentai.ProcessRequest(
            name=resource_name, raw_document=raw_document
        )
        try:
            # Use the Document AI client to process the sample form
            result = documentai_client.process_document(request=request)

            
            return result.document
        
        except:
            result = batch_process_documents(
                project_id=project_id,
                location=location,
                processor_id=processor_id,
                gcs_input_uri=gcs_input_uri,
                input_mime_type=mime_type,
                gcs_output_bucket=gcs_output_bucket,
                gcs_output_uri_prefix=gcs_output_uri_prefix,
            )
            
            return result

def get_table_data(
    rows: Sequence[documentai.Document.Page.Table.TableRow], text: str
) -> List[List[str]]:
    """
    Get Text data from table rows
    """
    all_values: List[List[str]] = []
    for row in rows:
        current_row_values: List[str] = []
        for cell in row.cells:
            current_row_values.append(
                text_anchor_to_text(cell.layout.text_anchor, text)
            )
        all_values.append(current_row_values)
    return all_values


def text_anchor_to_text(text_anchor: documentai.Document.TextAnchor, text: str) -> str:
    """
    Document AI identifies table data by their offsets in the entirety of the
    document's text. This function converts offsets to a string.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in text_anchor.text_segments:
        start_index = int(segment.start_index)
        end_index = int(segment.end_index)
        response += text[start_index:end_index]
    return response.strip().replace("\n", " ")


def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None
    

def drive_to_GCPStorage(file_id, bucket_name, master_folder, folder, drive_service):
    
    CLIENT_SECRET_FILE = "credencials.json"
    APY_NAME = "drive"
    API_VERSION = "v3"
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    
    # Especificar las credenciales de tu proyecto de Google Cloud
    creds = Credentials.from_service_account_file('un-analitica-avanzada-c681cb310169.json')

    # Crear un cliente de Google Drive
    #drive_service = Create_Service(CLIENT_SECRET_FILE, APY_NAME, API_VERSION, SCOPES)

    # Crear un cliente de Google Cloud Storage
    storage_service = build('storage', 'v1', credentials=creds)
    

    # # Especificar la ID del archivo que deseas migrar de Google Drive a Google Cloud Storage
    # file_id = 'your-file-id'

    # Obtener el archivo desde Google Drive
    file = drive_service.files().get(fileId=file_id, fields='name, mimeType').execute()

    # Especificar el nombre del archivo en Google Cloud Storage y el tipo MIME del archivo
    file_name = file['name']
    file_mime_type = file['mimeType']

    # Leer el contenido del archivo como una cadena de bytes
    file_content = drive_service.files().get_media(fileId=file_id).execute()

    fh = io.BytesIO(file_content)
    
    media_body = MediaIoBaseUpload(fh, mimetype=file_mime_type)
    
    # Crear un objeto de tipo "blob" en Google Cloud Storage
    blob = storage_service.objects().insert(
        bucket=bucket_name,
        name=f"{master_folder}/{folder}/{file_name}",
        body={'contentType': file_mime_type},
        media_body=media_body
    ).execute()

# Imprimir el URL del archivo en Google Cloud Storage
    print(blob['mediaLink'])

def search_file(folder_id, service):
    """Search file in drive location

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    CLIENT_SECRET_FILE = "credencials.json"
    APY_NAME = "drive"
    API_VERSION = "v3"
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    try:
        # create drive api client
        #service = Create_Service(CLIENT_SECRET_FILE, APY_NAME, API_VERSION, SCOPES)
        files = []
        page_token = None
        query = f"parents = '{folder_id}'"
        while True:
            # pylint: disable=maybe-no-member
            response = service.files().list(q=query).execute()
            for file in response.get('files', []):
                # Process change
                print(F'Found file: {file.get("name")}, {file.get("id")}')
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    except HttpError as error:
        print(F'An error occurred: {error}')
        files = None

    return files


def download_file(real_file_id, file_name):
    """Downloads a file
    Args:
        real_file_id: ID of the file to download
    Returns : IO object with location.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    
    CLIENT_SECRET_FILE = "credencials.json"
    APY_NAME = "drive"
    API_VERSION = "v3"
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    
    try:
        
        #credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET_FILE, SCOPES)
        
        # create drive api client
        service = Create_Service(CLIENT_SECRET_FILE, APY_NAME, API_VERSION, SCOPES)

        file_id = real_file_id

        # pylint: disable=maybe-no-member
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'Download {int(status.progress() * 100)}.')
        
        file.seek(0)
        
        with open(file_name,"wb") as f:
            f.write(file.read())
            f.close()

    except HttpError as error:
        print(F'An error occurred: {error}')
        file = None

    return file.getvalue()


def upload_blob(bucket_name, source_file_name, destination_blob_name, folder):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{destination_blob_name}")

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {blob}."
    )

    
def migrate_drive_to_gcp(folder_id, bucket_name,master_folder):

    CLIENT_SECRET_FILE = "credencials.json"
    APY_NAME = "drive"
    API_VERSION = "v3"
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    # Crea una instancia del servicio de Drive
    service = Create_Service(CLIENT_SECRET_FILE, APY_NAME, API_VERSION, SCOPES)

    # Crea una consulta para buscar solo carpetas que estén dentro de la carpeta principal
    query = "mimeType='application/vnd.google-apps.folder' and trashed = false and parents in '" + folder_id + "'"

    # Ejecuta la consulta y obtiene el resultado
    result = service.files().list(q=query,fields="nextPageToken, files(id, name)").execute()
    items = result.get("files", [])

    # Muestra los ID y los nombres de cada carpeta que se encontró
    for item in items:
        
        print(f'{item["name"]} ({item["id"]})')
        
        files_download = search_file(item["id"], service=service)

        for file_download in files_download:
            drive_to_GCPStorage(file_id=file_download["id"], 
                                bucket_name=bucket_name, 
                                master_folder=master_folder,
                                folder=item["name"],
                                drive_service=service)
    

def get_subfolders_names(folder_name, bucket_name):
    
  # Crea un cliente de Cloud Storage utilizando las credenciales del servicio
  client = storage.Client.from_service_account_json('un-analitica-avanzada-c681cb310169.json')
  
  # Especifica el nombre de la carpeta de la cual quieres obtener las subcarpetas
  #folder_name = 'nombre-de-la-carpeta'

  # Obtén una lista de todos los objetos en la carpeta especificada
  objects = client.get_bucket(bucket_name).list_blobs(prefix=folder_name)

  # Crea una lista para almacenar los nombres de las subcarpetas
  subfolders = []

  # Itera a través de la lista de objetos y agrega el nombre de cualquier subcarpeta a la lista
  for obj in objects:
    subfolders.append(os.path.basename(os.path.dirname(obj.name)))

  #Elimino duplicados
  subfolders = list(set(subfolders))
  
  # Imprime la lista de nombres de subcarpetas
  #print(subfolders)
  if folder_name in subfolders:
      subfolders.remove(folder_name)
  
  return subfolders


def get_files_names(bucket_name, master_folder, folder_name):
  """
  Obtiene el nombre de todos los archivos, junto con su extensión, en una carpeta de Google Cloud Storage.
  """

  # Crea un cliente de Cloud Storage
  client = storage.Client.from_service_account_json('un-analitica-avanzada-c681cb310169.json')

   # Obtén una lista de todos los blobs en la carpeta
  objects = client.get_bucket(bucket_name).list_blobs(prefix=f"{master_folder}/{folder_name}/")
  

  # Itera sobre la lista de archivos y obtiene el nombre de cada uno junto con su extensión
  file_names = []
  for obj in objects:
    #print(os.path.basename(obj.name))
    file_names.append(os.path.basename(obj.name))

  return file_names

def GCPStorage_to_drive(bucket_name, master_folder, folder_id_drive):
    
  CLIENT_SECRET_FILE = "credencials.json"
  APY_NAME = "drive"
  API_VERSION = "v3"
  SCOPES = ["https://www.googleapis.com/auth/drive"]

  # Crea una instancia del servicio de Drive
  drive_service = Create_Service(CLIENT_SECRET_FILE, APY_NAME, API_VERSION, SCOPES)
  
  # Especificar las credenciales de tu proyecto de Google Cloud
  creds = Credentials.from_service_account_file('un-analitica-avanzada-c681cb310169.json')

  # Crear un cliente de Google Cloud Storage
  storage_service = build('storage', 'v1', credentials=creds)  
  
  # Obtener la lista de subcarpetas en la carpeta maestra de Google Cloud Storage
  subfolders = get_subfolders_names(bucket_name=bucket_name, folder_name=master_folder)

  # Iterar sobre las subcarpetas y migrar los archivos de cada una a Google Drive
  for subfolder in subfolders:
      
    # Crear una carpeta en Google Drive para almacenar los archivos
    folder_metadata = {
      'name': subfolder,
      'mimeType': 'application/vnd.google-apps.folder',
      'parents': [folder_id_drive]
    }
    
    folder_drive = drive_service.files().create(body=folder_metadata, fields='id').execute()
    folder_id = folder_drive['id']
    print(F'Folder with ID: "{folder_id}" created in Google Drive')
    
    # Obtener la lista de archivos en la subcarpeta de Google Cloud Storage
    files = storage_service.objects().list(bucket="test_documentai_process", prefix=f"{master_folder}/{subfolder}").execute()['items']

    # Iterar sobre la lista de archivos y migrar cada uno a Google Drive
    for file in files:
      file_name = file['name'].split("/")[-1]
      
      # Obtener el contenido del archivo como una cadena de bytes
      file_content = storage_service.objects().get_media(bucket="test_documentai_process", object=f"{master_folder}/{subfolder}/{file_name}").execute()

      # Crear un nuevo archivo en Google Drive con el contenido del archivo de Google Cloud Storage
      file_metadata = {
          'name': file_name,
          'parents': [folder_id]
      }
      media = MediaIoBaseUpload(io.BytesIO(file_content), mimetype='application/octet-stream',
                                chunksize=1024*1024, resumable=True)
      
      file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
      print(F'File with ID: "{file.get("id")}" created in Google Drive')