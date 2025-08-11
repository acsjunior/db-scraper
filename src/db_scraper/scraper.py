import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import os
import pandas as pd
import re
import unicodedata
from datetime import datetime
from pathlib import Path
import logging

from . import config

logger = logging.getLogger(__name__)


def extract_playlist_data(playlist_id: str, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Extracts detailed information about all songs in a playlist.

    For each song, it retrieves the title, author, and other metadata from the initial HTML,
    and then makes a second request to the content API to obtain the audio URL.

    Args:
        playlist_id: The playlist ID from the Discografia Brasileira website.
        limit: The maximum number of tracks to extract (default: 500).

    Returns:
        A list of dictionaries, where each dictionary contains the
        information of a song. Returns an empty list if an error occurs.
    """
    # Build the tracklist URL using the template from config
    tracklist_url = config.API_TRACKLIST_URL_TEMPLATE.format(
        playlist_id=playlist_id, limit=limit
    )

    # Use the base headers directly from config, without the Referer
    headers = config.BASE_HEADERS

    all_songs_data = []

    logger.info(f"Buscando dados da playlist ID: {playlist_id}...")
    try:
        # --- Step 1: Get the list of tracks from the playlist ---
        response = requests.get(tracklist_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        tracks = soup.find_all("div", class_="track")
        logger.info(f"Encontradas {len(tracks)} faixas. Extraindo detalhes...")

        # --- Step 2: Iterate over each track to get details ---
        for track in tracks:
            data_id_tag = track.select_one(".play-bttn")
            data_id = data_id_tag.get("data-id") if data_id_tag else None

            titulo = track.select_one(".track-name a").text.strip().title()

            author_tags = track.select(".track-author a")
            autor = " / ".join([tag.text.strip() for tag in author_tags])

            interprete_tags = track.select(".track-performer a")
            interprete = " / ".join([tag.text.strip() for tag in interprete_tags])

            disco = track.select_one(".track-duration a").text.strip()
            ano_disco = track.select_one(".track-year").text.strip()

            gravacao_label = track.find(
                "div", class_="property-label", string="gravacao"
            )
            data_gravacao = (
                gravacao_label.find_next_sibling("div").text.strip()
                if gravacao_label
                else ""
            )

            lancamento_label = track.find(
                "div", class_="property-label", string="lancamento"
            )
            data_lancamento = (
                lancamento_label.find_next_sibling("div").text.strip()
                if lancamento_label
                else ""
            )

            # --- Step 3: Make a request to the content API to get the audio URL ---
            audio_url = ""
            if data_id:
                content_url = config.API_CONTENT_URL_TEMPLATE.format(data_id=data_id)
                try:
                    content_response = requests.get(content_url, headers=headers)
                    content_response.raise_for_status()
                    json_data = content_response.json()
                    audio_url = json_data["audio"][0]["contentUrl"][0]["@value"]
                except (requests.RequestException, KeyError, IndexError):
                    logger.warning(
                        f"  - Aviso: Não foi possível obter a URL do áudio para a faixa '{titulo}' (ID: {data_id})."
                    )
            else:
                logger.warning(
                    f"  - Aviso: Faixa '{titulo}' não possui data-id. URL do áudio não será buscada."
                )

            song_data = {
                "data_id": data_id,
                "titulo": titulo,
                "autor": autor,
                "interprete": interprete,
                "disco": disco,
                "ano_lancamento_disco": ano_disco,
                "data_gravacao": data_gravacao,
                "data_lancamento": data_lancamento,
                "audio_url": audio_url,
            }
            all_songs_data.append(song_data)

    except requests.RequestException as e:
        logger.error(f"Erro fatal ao buscar a lista de faixas: {e}")
        return []

    return all_songs_data


def _download_and_audit_dataframe(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    """
    Downloads audio files from URLs specified in the input DataFrame, organizes them into folders by author,
    and updates the DataFrame with audit information.
    This function processes each row in the DataFrame that contains a valid 'audio_url', downloads the corresponding
    audio file, and saves it in a directory named after the first listed author (or "Autor Desconhecido" if not available).
    The filename is generated from a slugified version of the title and the 'data_id'. The function also ensures that
    audit columns ('pasta', 'nome_arquivo', 'data_download') exist in the DataFrame, and updates them with the folder name,
    filename, and download date for each successfully downloaded file.
    Args:
        df (pd.DataFrame): Input DataFrame containing at least the columns 'audio_url', 'autor', 'titulo', and 'data_id'.
        output_dir (str): Path to the directory where audio files will be saved.
    Returns:
        pd.DataFrame: The updated DataFrame with audit information for each processed audio file.
    Notes:
        - If the audio file already exists, it is not downloaded again, but the audit information is updated.
        - If the download fails, the corresponding audit columns are not updated for that row.
        - The function prints progress and error messages to the console during execution.
    """
    # Ensure audit columns exist
    for col in ["pasta", "nome_arquivo", "data_download"]:
        if col not in df.columns:
            df[col] = pd.Series(dtype="object")

    df_com_audio = df[df["audio_url"].notna() & (df["audio_url"] != "")].copy()
    logger.info(f"\nEncontradas {len(df_com_audio)} músicas na lista para baixar.")

    for index, row in df_com_audio.iterrows():
        download_status = False
        nome_pasta = "Autor Desconhecido"
        nome_arquivo_final = ""
        autores = str(row["autor"])
        if pd.notna(autores) and autores:
            primeiro_autor = autores.split(" / ")[0].strip()
            nome_pasta = re.sub(r'[\\/*?:"<>|]', "", primeiro_autor)

        download_path = Path(output_dir) / nome_pasta
        os.makedirs(download_path, exist_ok=True)

        titulo = str(row["titulo"])
        data_id = row["data_id"]
        titulo_sem_acentos = (
            unicodedata.normalize("NFKD", titulo)
            .encode("ASCII", "ignore")
            .decode("utf-8")
        )
        titulo_limpo = re.sub(r"[^a-z0-9\s-]", "", titulo_sem_acentos.lower())
        slug_titulo = re.sub(r"[\s-]+", "-", titulo_limpo).strip("-")
        nome_arquivo_final = f"{slug_titulo}_{str(data_id)}.mp3"
        filepath = download_path / nome_arquivo_final

        if os.path.exists(filepath):
            logger.info(f"  - Já existe: '{titulo}'. Marcando como sucesso.")
            download_status = True
        else:
            logger.info(f"  - Baixando: '{titulo}'...")
            try:
                audio_response = requests.get(
                    str(row["audio_url"]),
                    headers=config.BASE_HEADERS,
                    stream=True,
                    timeout=20,
                )
                audio_response.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in audio_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info("    -> Sucesso ao baixar.")
                download_status = True
            except requests.RequestException as e:
                logger.error(f"    -> Erro ao baixar: {e}")
                download_status = False

        data_de_hoje = datetime.now().strftime("%d/%m/%Y")
        df.loc[index, "pasta"] = nome_pasta
        df.loc[index, "nome_arquivo"] = nome_arquivo_final
        if download_status:
            df.loc[index, "data_download"] = data_de_hoje

    return df


def save_playlist_to_csv(playlist_id: str, output_dir: str, limit: int = 500) -> None:
    """
    Extracts playlist metadata and saves the result to a CSV file.

    Args:
        playlist_id: The ID of the playlist to extract.
        filename: The output CSV filename (without the .csv extension).
        limit: The maximum number of tracks to extract (default: 500).
    """
    logger.info("--- Iniciando processo ---")
    dados_musicas = extract_playlist_data(playlist_id=playlist_id, limit=limit)

    if not dados_musicas:
        logger.warning("Nenhum dado foi extraído. O arquivo CSV não será gerado.")
        return

    df = pd.DataFrame(dados_musicas)
    df = df.reindex(columns=config.OUTPUT_COLUMNS)

    os.makedirs(output_dir, exist_ok=True)
    filename = f"playlist_{playlist_id}_metadata.csv"
    filepath = Path(output_dir) / filename
    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    logger.info("\n--- Extração concluída ---")
    logger.info(f"Os metadados foram salvos com sucesso em: {filepath}")


def download_from_csv(input_csv_path: str, output_dir: str) -> None:
    """
    Reads a CSV file (potentially edited by the user), downloads audio files, and saves a new audit CSV.

    This function loads track metadata from a CSV file, attempts to download the audio files for each track with a valid audio URL,
    and saves an updated CSV file with audit information (such as download status, folder, and filename) in the same directory as the input CSV.
    The audit CSV is timestamped to avoid overwriting the original.

    Args:
        input_csv_path (str): The path to the input CSV file containing track metadata and audio URLs.
        output_dir (str): The directory where audio files will be saved.

    Process:
        1. Reads the input CSV and validates its existence.
        2. Downloads audio files for tracks with valid audio URLs, organizing them into subfolders by author.
        3. Updates the DataFrame with audit information (folder, filename, download date).
        4. Saves a new audit CSV with a timestamp in the filename.

    Notes:
        - If the input CSV is not found, the function prints an error and returns.
        - Only tracks with a non-empty 'audio_url' are processed for download.
        - The function prints progress and status messages throughout the process.
    """
    logger.info("\n--- Iniciando Etapa 2: Download a partir de CSV ---")
    try:
        df = pd.read_csv(input_csv_path, dtype=config.COLUMN_DTYPES)
        logger.info(f"Lendo dados de: {input_csv_path}")
    except FileNotFoundError:
        logger.error(f"Erro: O arquivo CSV '{input_csv_path}' não foi encontrado.")
        return

    df_audit = _download_and_audit_dataframe(df, output_dir)

    p = Path(input_csv_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{p.stem}_{timestamp}{p.suffix}"
    new_filepath = p.parent / new_filename

    df_audit.to_csv(new_filepath, index=False, encoding="utf-8-sig")
    logger.info("\n--- Processo de Download Concluído ---")
    logger.info(f"O relatório de auditoria foi salvo em: {new_filepath}")


def download_from_playlist(playlist_id: str, output_dir: str, limit: int = 500) -> None:
    """
    Orchestrates the complete process of extracting playlist data, downloading audio files,
    and saving a final audited CSV report with download statuses.
    Args:
        playlist_id (str): The unique identifier of the playlist to process.
        output_dir (str): The directory where audio files and the final CSV report will be saved.
        limit (int, optional): The maximum number of tracks to process from the playlist. Defaults to 500.
    Process Overview:
        1. Extracts track data from the specified playlist.
        2. Downloads available audio files, organizing them into subfolders by author.
        3. Tracks the download status for each track.
        4. Saves a comprehensive CSV report with metadata and download results.
    The function prints progress and status messages throughout the process.
    """
    logger.info("--- Iniciando processo: Extração, Download e Auditoria ---")
    dados_musicas = extract_playlist_data(playlist_id=playlist_id, limit=limit)

    if not dados_musicas:
        logger.warning("Nenhuma música foi extraída. Encerrando o processo.")
        return

    df = pd.DataFrame(dados_musicas)
    df_audit = _download_and_audit_dataframe(df, output_dir)

    # Salva o CSV final auditado no diretório de saída principal
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_filename = f"playlist_{playlist_id}_completo_{timestamp}.csv"
    final_filepath = Path(output_dir) / final_filename

    df_audit = df_audit.reindex(columns=config.OUTPUT_COLUMNS)

    df_audit.to_csv(final_filepath, index=False, encoding="utf-8-sig")
    logger.info("\n--- Processo Completo Concluído ---")
    logger.info(f"O relatório final foi salvo em: {final_filepath}")
