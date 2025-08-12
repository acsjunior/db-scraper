import pandas as pd
from pathlib import Path
import logging
from typing import Optional
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from datetime import datetime

from . import paths
from . import config

logger = logging.getLogger(__name__)
_gdrive_instance = None


def get_gdrive_instance() -> Optional[GoogleDrive]:
    """
    Provides a singleton entry point for authenticating and accessing Google Drive via PyDrive2.

    This function manages the authentication flow for Google Drive using PyDrive2, ensuring that only one
    instance of the authenticated GoogleDrive client is created and reused throughout the application.
    It loads credentials from the configured files, refreshes or authorizes as needed, and saves updated
    credentials for future use. If authentication fails, it logs the error and returns None.

    Returns:
        Optional[GoogleDrive]: An authenticated GoogleDrive instance if successful, or None if authentication fails.

    Workflow:
        1. Checks if a GoogleDrive instance already exists and returns it if so (singleton pattern).
        2. Loads client configuration and credentials from the paths specified in the project config.
        3. If no credentials are found, initiates browser-based authentication.
        4. If credentials are expired, refreshes them; otherwise, authorizes directly.
        5. Saves credentials for future sessions.
        6. Returns the authenticated GoogleDrive instance, or None if authentication fails.

    Notes:
        - Uses the global _gdrive_instance variable to cache the authenticated client.
        - Relies on the paths.CLIENT_SECRETS_FILE and paths.TOKEN_FILE for configuration and credential storage.
        - Logs progress, warnings, and errors using the logger.
        - Intended to be used as the only entry point for Google Drive access in the application.
    """
    global _gdrive_instance
    if _gdrive_instance:
        return _gdrive_instance
    try:
        gauth = GoogleAuth()
        gauth.settings["client_config_file"] = str(paths.CLIENT_SECRETS_FILE)
        gauth.LoadCredentialsFile(str(paths.TOKEN_FILE))
        if gauth.credentials is None:
            logger.info(
                "Nenhuma credencial encontrada, iniciando autenticação via navegador..."
            )
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            logger.info("Credenciais expiraram, atualizando...")
            gauth.Refresh()
        else:
            gauth.Authorize()
        gauth.SaveCredentialsFile(str(paths.TOKEN_FILE))
        _gdrive_instance = GoogleDrive(gauth)
        logger.info("Autenticação com o Google Drive bem-sucedida.")
        return _gdrive_instance
    except Exception as e:
        logger.error(f"Falha na autenticação com o Google Drive: {e}")
        return None


def find_or_create_folder(
    drive: GoogleDrive, folder_name: str, parent_folder_id: str = "root"
) -> str:
    """
    Finds a folder in Google Drive by name and parent, or creates it if it does not exist.

    This function searches for a folder with the specified name under the given parent folder ID in Google Drive.
    If the folder exists, it returns its ID. If not, it creates the folder and returns the new folder's ID.

    Args:
        drive (GoogleDrive): An authenticated GoogleDrive instance from PyDrive2.
        folder_name (str): The name of the folder to find or create.
        parent_folder_id (str, optional): The ID of the parent folder in which to search or create. Defaults to "root".

    Returns:
        str: The ID of the found or newly created folder.

    Workflow:
        1. Escapes the folder name for use in the query.
        2. Searches for a folder with the given name and parent ID in Google Drive.
        3. If found, returns the folder's ID.
        4. If not found, creates the folder under the parent and returns the new folder's ID.

    Notes:
        - Uses the Google Drive API via PyDrive2 to search and create folders.
        - Logs folder creation events using the logger.
        - The search is case-sensitive and matches the exact folder name.
        - Intended for organizing uploads into structured directories in Google Drive.
    """
    folder_name_escaped = folder_name.replace("'", "\\'")
    query = f"title = '{folder_name_escaped}' and mimeType = 'application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed = false"
    folder_list = drive.ListFile({"q": query}).GetList()
    if folder_list:
        return folder_list[0]["id"]
    else:
        logger.info(f"Criando pasta '{folder_name}' no Google Drive...")
        folder_metadata = {
            "title": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [{"id": parent_folder_id}],
        }
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
        return folder["id"]


def upload_audios_from_csv(csv_path: str, local_music_dir: str) -> None:
    """
    Reads a CSV audit file, uploads local MP3 files to Google Drive, and saves an updated audit CSV with Drive URLs.

    This function authenticates with Google Drive, iterates through the provided CSV file containing track metadata,
    uploads each corresponding MP3 file from the specified local directory to Google Drive (organizing by author),
    and updates the CSV with the resulting Google Drive URLs. If a file already exists in Drive, its URL is reused.
    The updated audit CSV is saved with a timestamp in the filename.

    Args:
        csv_path (str): Path to the audit CSV file containing track metadata and local file references.
        local_music_dir (str): Path to the base directory where local MP3 files are stored (organized by author subfolders).

    Returns:
        None

    Workflow:
        1. Authenticates with Google Drive using the singleton instance.
        2. Reads the audit CSV and ensures a 'gdrive_url' column exists.
        3. For each row, checks for the local MP3 file and uploads it to the appropriate Drive folder (by author).
        4. If the file already exists in Drive, retrieves and records its URL.
        5. Updates the DataFrame with the Drive URL for each uploaded or found file.
        6. Saves a new audit CSV with the updated URLs and a timestamp in the filename.

    Notes:
        - If authentication fails or the CSV cannot be read, the function logs an error and returns early.
        - Local files are expected to be organized in subfolders by author under the provided music directory.
        - The function logs progress, warnings, and errors using the logger.
        - The output audit CSV is saved in the same directory as the input, with '_upload_audit_<timestamp>' appended to the filename.
    """
    logger.info("--- Iniciando processo de upload para o Google Drive ---")
    drive = get_gdrive_instance()
    if not drive:
        logger.error("Não foi possível autenticar. O processo de upload foi cancelado.")
        return

    try:
        df = pd.read_csv(csv_path, dtype=config.COLUMN_DTYPES)
        if "gdrive_url" not in df.columns:
            df["gdrive_url"] = ""
    except FileNotFoundError:
        logger.error(f"Arquivo CSV não encontrado: {csv_path}")
        return

    main_drive_folder_id = find_or_create_folder(drive, "db_downloads")

    for index, row in df.iterrows():
        pasta_autor = str(row.get("pasta", ""))
        nome_arquivo = str(row.get("nome_arquivo", ""))

        if not pasta_autor or not nome_arquivo:
            continue

        local_file_path = Path(local_music_dir) / pasta_autor / nome_arquivo

        if local_file_path.exists():
            logger.info(f"Processando '{nome_arquivo}'...")
            author_drive_folder_id = find_or_create_folder(
                drive, pasta_autor, parent_folder_id=main_drive_folder_id
            )

            query = f"title = '{nome_arquivo}' and '{author_drive_folder_id}' in parents and trashed = false"
            existing_files = drive.ListFile({"q": query}).GetList()

            if existing_files:
                gdrive_url = existing_files[0]["alternateLink"]
                logger.info(
                    f"Arquivo '{nome_arquivo}' já existe no Google Drive. URL recuperada."
                )
                df.loc[index, "gdrive_url"] = gdrive_url
                continue

            try:
                gfile = drive.CreateFile(
                    {"title": nome_arquivo, "parents": [{"id": author_drive_folder_id}]}
                )
                gfile.SetContentFile(str(local_file_path))
                gfile.Upload()

                gdrive_url = gfile["alternateLink"]
                df.loc[index, "gdrive_url"] = gdrive_url
                logger.info(
                    f"-> Upload de '{nome_arquivo}' concluído. URL: {gdrive_url}"
                )
            except Exception as e:
                logger.error(f"Erro ao fazer o upload de '{nome_arquivo}': {e}")
        else:
            logger.warning(f"Arquivo local não encontrado: {local_file_path}. Pulando.")

    p = Path(csv_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{p.stem}_upload_audit_{timestamp}.csv"
    output_filepath = p.parent / output_filename

    df.to_csv(output_filepath, index=False, encoding="utf-8-sig")
    logger.info(
        f"--- Processo de upload concluído! Relatório atualizado salvo em: {output_filepath} ---"
    )
