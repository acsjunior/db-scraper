import requests
from bs4 import BeautifulSoup, Tag
from typing import List, Dict, Any
import os
import pandas as pd
import re
import unicodedata
from datetime import datetime
from pathlib import Path
import logging
from urllib.parse import quote_plus

from . import config

logger = logging.getLogger(__name__)


class DiscografiaScraper:
    """
    A class for extracting and downloading music data from the Discografia Brasileira website.

    This class provides a high-level interface for scraping playlists and author pages, downloading audio files,
    and saving metadata and audit reports as CSV files. It manages the output directory and HTTP headers,
    and centralizes logging for all operations.

    Attributes:
        output_dir (Path): The base directory where CSV and MP3 files will be saved.
        headers (dict): The HTTP headers used for all requests.

    Methods:
        __init__(output_dir: str):
            Initializes the scraper with the specified output directory and prepares the environment.
        # (Other methods should be listed here as you implement them, e.g., scrape_playlist, scrape_author, etc.)

    Notes:
        - All files are saved under the specified output directory.
        - Logging is used to track progress, warnings, and errors throughout the scraping and download process.
    """

    def __init__(self, output_dir: str):
        """
        Initializes the DiscografiaScraper with a specified output directory.

        This constructor sets up the base directory for saving all CSV and MP3 files, configures the HTTP headers
        for requests, ensures the output directory exists, and logs the initialization status.

        Args:
            output_dir (str): The base directory where all output files (CSV and MP3) will be saved.

        Attributes:
            output_dir (Path): The resolved output directory as a Path object.
            headers (dict): The HTTP headers used for all requests.

        Notes:
            - If the output directory does not exist, it will be created automatically.
            - A log message is generated to confirm the initialization and output path.
        """
        self.config = config
        self.output_dir = Path(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(
            f"Scraper inicializado. Os arquivos serão salvos em: {self.output_dir}"
        )

    def _parse_track_data(self, track: Tag) -> Dict[str, Any]:
        """
        Extracts metadata from a single BeautifulSoup 'track' element.

        This method parses the HTML structure of a track element to extract its unique ID, title, author(s), performer(s), album, year,
        recording and release dates, and attempts to retrieve the audio URL from the content API if a data-id is present. It is intended
        to be used as a helper for scraping track information from playlist or author pages.

        Args:
            track (Tag): A BeautifulSoup Tag object representing a single track element from the Discografia Brasileira website.

        Returns:
            Dict[str, Any]: A dictionary with the following keys:
                - data_id (str or None): The unique identifier of the track, or None if not found.
                - titulo (str): The title of the track, or "Título Desconhecido" if missing.
                - autor (str): The author(s) of the track, separated by ' / '.
                - interprete (str): The performer(s) of the track, separated by ' / '.
                - disco (str): The album or record name, or empty string if missing.
                - ano_lancamento_disco (str): The release year of the album, or empty string if missing.
                - data_gravacao (str): The recording date, or empty string if missing.
                - data_lancamento (str): The release date, or empty string if missing.
                - audio_url (str): The URL to the audio file, or empty string if not available.

        Notes:
            - If the track does not have a data-id, the audio_url will be an empty string.
            - If any field is missing in the HTML, a default value is used (e.g., empty string or "Título Desconhecido").
            - Warnings are logged if the audio URL cannot be retrieved from the content API.
        """
        data_id_tag = track.select_one(".play-bttn")
        data_id = data_id_tag.get("data-id") if data_id_tag else None

        titulo_tag = track.select_one(".track-name a")
        titulo = (
            titulo_tag.text.strip().title() if titulo_tag else "Título Desconhecido"
        )

        author_tags = track.select(".track-author a")
        autor = " / ".join([tag.text.strip() for tag in author_tags])

        interprete_tags = track.select(".track-performer a")
        interprete = " / ".join([tag.text.strip() for tag in interprete_tags])

        disco_tag = track.select_one(".track-duration a")
        disco = disco_tag.text.strip() if disco_tag else ""

        ano_disco_tag = track.select_one(".track-year")
        ano_disco = ano_disco_tag.text.strip() if ano_disco_tag else ""

        gravacao_label = track.find("div", class_="property-label", string="gravacao")
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

        fonte_url = titulo_tag.get("href", "") if titulo_tag else ""

        audio_url = ""
        if data_id:
            content_url = self.config.API_CONTENT_URL_TEMPLATE.format(data_id=data_id)
            try:
                content_response = requests.get(
                    content_url, headers=self.config.BASE_HEADERS, timeout=10
                )
                content_response.raise_for_status()
                json_data = content_response.json()
                audio_url = json_data["audio"][0]["contentUrl"][0]["@value"]
            except (requests.RequestException, KeyError, IndexError):
                logger.warning(
                    f"  - Não foi possível obter a URL do áudio para a faixa '{titulo}' (ID: {data_id})."
                )

        return {
            "data_id": data_id,
            "titulo": titulo,
            "autor": autor,
            "interprete": interprete,
            "disco": disco,
            "ano_lancamento_disco": ano_disco,
            "data_gravacao": data_gravacao,
            "data_lancamento": data_lancamento,
            "fonte_url": fonte_url,
            "audio_url": audio_url,
        }

    def _download_and_audit_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Downloads audio files from the URLs in the DataFrame and updates audit information for each track.

        This method processes each row in the DataFrame that contains a valid 'audio_url', downloads the corresponding audio file,
        and saves it in a subdirectory named after the first listed author (or "Autor Desconhecido" if not available) within the output directory.
        The filename is generated from a slugified version of the title and the 'data_id'. Audit columns ('pasta', 'nome_arquivo', 'data_download')
        are created if missing and updated for each successfully downloaded file.

        Args:
            df (pd.DataFrame): DataFrame containing at least the columns 'audio_url', 'autor', 'titulo', and 'data_id'.

        Returns:
            pd.DataFrame: The updated DataFrame with audit information for each processed audio file, including the columns:
                - pasta: The folder where the file was saved.
                - nome_arquivo: The name of the saved file.
                - data_download: The date the file was downloaded (dd/mm/yyyy), or empty if not downloaded.

        Notes:
            - If the audio file already exists, it is not downloaded again, but the audit information is updated.
            - If the download fails, the corresponding audit columns are not updated for that row.
            - Progress and error messages are logged using the logger.
            - Files are saved under the output directory specified when initializing the scraper instance.
        """
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

            download_path = Path(self.output_dir) / nome_pasta
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
                        headers=self.config.BASE_HEADERS,
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

    def extract_playlist_data(
        self, playlist_id: str, limit: int = 500
    ) -> List[Dict[str, Any]]:
        """
        Extracts detailed metadata for all tracks in a given playlist from the Discografia Brasileira website.

        This method fetches the playlist page, parses the HTML to extract track information for each song using the
        internal helper method `_parse_track_data`, and returns a list of dictionaries with all relevant metadata. For each track,
        it attempts to retrieve the audio URL by making an additional request to the content API if a data-id is present.

        Args:
            playlist_id (str): The unique identifier of the playlist on the Discografia Brasileira website.
            limit (int, optional): The maximum number of tracks to extract. Defaults to 500.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing metadata for a track. Returns an empty list if an error occurs.
                Each dictionary contains the following keys:
                    - data_id (str or None): The unique identifier of the track.
                    - titulo (str): The title of the track.
                    - autor (str): The author(s) of the track.
                    - interprete (str): The performer(s) of the track.
                    - disco (str): The album or record name.
                    - ano_lancamento_disco (str): The release year of the album.
                    - data_gravacao (str): The recording date.
                    - data_lancamento (str): The release date.
                    - audio_url (str): The URL to the audio file, if available.

        Notes:
            - Uses the `_parse_track_data` helper to centralize parsing logic.
            - If a track does not have a data-id, the audio_url will be an empty string.
            - Progress, warnings, and errors are logged using the logger.
            - This method is intended to be used as part of the scraping workflow for playlists.
        """
        tracklist_url = self.config.API_TRACKLIST_URL_TEMPLATE.format(
            playlist_id=playlist_id, limit=limit
        )

        logger.info(f"Buscando dados da playlist ID: {playlist_id}...")
        try:
            response = requests.get(tracklist_url, headers=self.config.BASE_HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            tracks = soup.find_all("div", class_="track")
            logger.info(f"Encontradas {len(tracks)} faixas. Extraindo detalhes...")

            all_songs_data = [self._parse_track_data(track) for track in tracks]

        except requests.RequestException as e:
            logger.error(f"Erro fatal ao buscar a lista de faixas: {e}")
            return []

        return all_songs_data

    def extract_author_data(self, author_name: str) -> List[Dict[str, Any]]:
        """
        Extracts metadata for all tracks by a given author, handling pagination, from the Discografia Brasileira website.

        This method fetches all pages of tracks associated with the specified author, parses the HTML to extract
        track information for each song using the internal helper method `_parse_track_data`, and returns a list of dictionaries
        with all relevant metadata. Pagination is handled automatically by following the "Next" page links until no more
        tracks are found.

        Args:
            author_name (str): The name of the author whose tracks should be extracted.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing metadata for a track by the author. Returns an empty list if no tracks are found or an error occurs.
                Each dictionary contains the following keys:
                    - data_id (str or None): The unique identifier of the track.
                    - titulo (str): The title of the track.
                    - autor (str): The author(s) of the track.
                    - interprete (str): The performer(s) of the track.
                    - disco (str): The album or record name.
                    - ano_lancamento_disco (str): The release year of the album.
                    - data_gravacao (str): The recording date.
                    - data_lancamento (str): The release date.
                    - audio_url (str): The URL to the audio file, if available.

        Notes:
            - Uses the `_parse_track_data` helper to centralize parsing logic.
            - If a track does not have a data-id, the audio_url will be an empty string.
            - Progress, warnings, and errors are logged using the logger.
            - Pagination is handled by following the "Next" page link until it is no longer present.
            - This method is intended to be used as part of the scraping workflow for authors.
        """
        # Format the author's name to be URL-safe (e.g., "Nilton Bastos" -> "Nilton%20Bastos")
        formatted_author = quote_plus(author_name)
        next_page_url = self.config.API_AUTHOR_URL_TEMPLATE.format(
            author_name=formatted_author
        )

        all_songs_data = []

        page_num = 1
        while next_page_url:
            logger.info(
                f"Buscando dados do autor '{author_name}', página {page_num}..."
            )
            try:
                response = requests.get(next_page_url, headers=self.config.BASE_HEADERS)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                tracks = soup.find_all("div", class_="track")
                if not tracks:
                    logger.info(
                        "Nenhuma faixa encontrada nesta página. Encerrando a busca do autor."
                    )
                    break

                logger.info(
                    f"Encontradas {len(tracks)} faixas na página {page_num}. Extraindo detalhes..."
                )

                for track in tracks:
                    all_songs_data.append(self._parse_track_data(track))

                next_page_tag = soup.select_one(
                    'span.pagination-item a[aria-label="Next"]'
                )
                if next_page_tag and next_page_tag.has_attr("href"):
                    next_page_url = next_page_tag["href"]
                    page_num += 1
                else:
                    next_page_url = None

            except requests.RequestException as e:
                logger.error(f"Erro fatal ao buscar a página do autor: {e}")
                break

        return all_songs_data

    def save_playlist_to_csv(self, playlist_id: str, limit: int = 500) -> None:
        """
        Extracts playlist metadata and saves it as a CSV file in the output directory.

        This method retrieves detailed metadata for all tracks in a specified playlist, including title, author, performer,
        album, year, recording and release dates, and audio URL. The extracted data is saved as a CSV file in the output directory
        defined for this scraper instance. The CSV columns and order are defined by the configuration. If no data is extracted, no file is created.

        Args:
            playlist_id (str): The unique identifier of the playlist to extract.
            limit (int, optional): The maximum number of tracks to extract. Defaults to 500.

        Returns:
            None

        Notes:
            - The output CSV file is named 'playlist_{playlist_id}_metadata.csv' and is saved in the output directory of the scraper instance.
            - If no tracks are found, the method logs a warning and does not create a file.
            - Progress and status messages are logged using the logger.
            - This method is intended to be used as part of the playlist scraping and export workflow.
        """
        logger.info("--- Iniciando processo ---")
        dados_musicas = self.extract_playlist_data(playlist_id=playlist_id, limit=limit)

        if not dados_musicas:
            logger.warning("Nenhum dado foi extraído. O arquivo CSV não será gerado.")
            return

        df = pd.DataFrame(dados_musicas)
        df = df.reindex(columns=self.config.OUTPUT_COLUMNS)

        os.makedirs(self.output_dir, exist_ok=True)
        filename = f"playlist_{playlist_id}_metadata.csv"
        filepath = Path(self.output_dir) / filename
        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        logger.info("\n--- Extração concluída ---")
        logger.info(f"Os metadados foram salvos com sucesso em: {filepath}")

    def save_author_to_csv(self, author_name: str) -> None:
        """
        Extracts metadata for all tracks by a given author and saves it as a CSV file in the output directory.

        This method retrieves detailed metadata for all tracks associated with the specified author, handling pagination as needed.
        The extracted data is saved as a CSV file in the output directory defined for this scraper instance. The CSV columns and order
        are defined by the configuration. If no data is extracted, no file is created.

        Args:
            author_name (str): The name of the author whose tracks should be extracted.

        Returns:
            None

        Notes:
            - The output CSV file is named 'autor_{author_name}_metadados.csv', with a sanitized author name, and is saved in the output directory of the scraper instance.
            - If no tracks are found, the method logs a warning and does not create a file.
            - Progress and status messages are logged using the logger.
            - This method is intended to be used as part of the author scraping and export workflow.
        """
        logger.info(
            f"--- Iniciando extração de metadados para o autor: {author_name} ---"
        )

        dados_musicas = self.extract_author_data(author_name)

        if not dados_musicas:
            logger.warning("Nenhum dado foi extraído. O arquivo CSV não será gerado.")
            return

        df = pd.DataFrame(dados_musicas)
        df = df.reindex(columns=self.config.OUTPUT_COLUMNS)

        os.makedirs(self.output_dir, exist_ok=True)
        safe_author_name = (
            re.sub(r'[\\/*?:"<>|]', "", author_name).replace(" ", "_").lower()
        )
        filename = f"autor_{safe_author_name}_metadados.csv"
        filepath = Path(self.output_dir) / filename
        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        logger.info("\n--- Extração Concluída ---")
        logger.info(
            f"Os metadados para '{author_name}' foram salvos com sucesso em: {filepath}"
        )

    def download_from_csv(self, input_csv_path: str) -> None:
        """
        Reads a CSV file (potentially edited by the user), downloads audio files, and saves a new audit CSV in the same directory.

        This method loads track metadata from a CSV file, attempts to download the audio files for each track with a valid audio URL,
        and saves an updated CSV file with audit information (such as download status, folder, and filename) in the same directory as the input CSV.
        The audit CSV is timestamped to avoid overwriting the original.

        Args:
            input_csv_path (str): The path to the input CSV file containing track metadata and audio URLs.

        Returns:
            None

        Process:
            1. Reads the input CSV and validates its existence.
            2. Downloads audio files for tracks with valid audio URLs, organizing them into subfolders by author (within the output directory of the scraper instance).
            3. Updates the DataFrame with audit information (folder, filename, download date).
            4. Saves a new audit CSV with a timestamp in the filename, in the same directory as the input CSV.

        Notes:
            - If the input CSV is not found, the method logs an error and returns.
            - Only tracks with a non-empty 'audio_url' are processed for download.
            - Progress and status messages are logged using the logger.
            - This method is intended to be used as part of the download and audit workflow from user-supplied or previously exported CSVs.
        """
        logger.info("\n--- Iniciando Etapa 2: Download a partir de CSV ---")
        try:
            df = pd.read_csv(input_csv_path, dtype=self.config.COLUMN_DTYPES)
            logger.info(f"Lendo dados de: {input_csv_path}")
        except FileNotFoundError:
            logger.error(f"Erro: O arquivo CSV '{input_csv_path}' não foi encontrado.")
            return

        df_audit = self._download_and_audit_dataframe(df)

        p = Path(input_csv_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{p.stem}_{timestamp}{p.suffix}"
        new_filepath = p.parent / new_filename

        df_audit.to_csv(new_filepath, index=False, encoding="utf-8-sig")
        logger.info("\n--- Processo de Download Concluído ---")
        logger.info(f"O relatório de auditoria foi salvo em: {new_filepath}")

    def download_from_playlist(
        self, playlist_id: str, limit: int = 500, save_report: bool = True, report_xlsx: bool = False
    ) -> None:
        """
        Executes the complete workflow for a playlist: extracts track metadata, downloads audio files, and optionally saves a final audit report (CSV or XLSX) in the output directory.

        This method automates the end-to-end process for a playlist by:
            1. Extracting all track metadata from the specified playlist.
            2. Downloading available audio files, organizing them into subfolders by author within the output directory.
            3. Tracking the download status for each track.
            4. Optionally saving a comprehensive report (CSV or XLSX) with metadata and download results, timestamped to avoid overwriting.

        Args:
            playlist_id (str): The unique identifier of the playlist to process.
            limit (int, optional): The maximum number of tracks to process from the playlist. Defaults to 500.
            save_report (bool, optional): If True, saves the final report (CSV or XLSX) with metadata and download results. If False, no report is saved. Defaults to True.
            report_xlsx (bool, optional): If True and save_report is True, saves the report as an XLSX file instead of CSV. Defaults to False.

        Returns:
            None

        Workflow:
            - Extracts track data from the specified playlist.
            - Downloads audio files for tracks with valid URLs, organizing them by author.
            - Updates audit information for each track (download status, folder, filename, date).
            - If `save_report` is True, saves a comprehensive report (CSV or XLSX) with metadata and download results, timestamped in the output directory.

        Notes:
            - If no tracks are found, the method logs a warning and does not create any files.
            - The final report is named 'playlist_{playlist_id}_completo_{timestamp}.csv' or '.xlsx' and is saved in the output directory if `save_report` is True.
            - Progress, warnings, and errors are logged using the logger.
            - This method is intended for use in the complete playlist download and audit workflow.
        """
        logger.info("--- Iniciando processo: Extração, Download e Auditoria ---")
        dados_musicas = self.extract_playlist_data(playlist_id=playlist_id, limit=limit)

        if not dados_musicas:
            logger.warning("Nenhuma música foi extraída. Encerrando o processo.")
            return

        df = pd.DataFrame(dados_musicas)
        df_audit = self._download_and_audit_dataframe(df)

        if save_report:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"playlist_{playlist_id}_completo_{timestamp}"
            filepath = Path(self.output_dir) / filename
            df_audit = df_audit.reindex(columns=self.config.OUTPUT_COLUMNS)

            if report_xlsx:
                df_audit.to_excel(filepath.with_suffix(".xlsx"), index=False)
            else:
                df_audit.to_csv(filepath.with_suffix(".csv"), index=False, encoding="utf-8-sig")

            logger.info("\n--- Processo de Download Concluído ---")
            logger.info(f"O relatório final foi salvo em: {filepath}")

        else:
            logger.info("\n--- Processo de Download Concluído ---")

    def download_from_author(self, author_name: str, save_report: bool = True, report_xlsx: bool = False) -> None:
        """
        Executes the complete workflow for an author: extracts track metadata, downloads audio files, and optionally saves a final audit report (CSV or XLSX) in the output directory.

        This method automates the end-to-end process for an author by:
            1. Extracting all track metadata for the specified author (with pagination).
            2. Downloading available audio files, organizing them into subfolders by author within the output directory.
            3. Tracking the download status for each track.
            4. Optionally saving a comprehensive report (CSV or XLSX) with metadata and download results, timestamped to avoid overwriting.

        Args:
            author_name (str): The name of the author whose tracks should be processed.
            save_report (bool, optional): If True, saves the final report (CSV or XLSX) with metadata and download results. If False, no report is saved. Defaults to True.
            report_xlsx (bool, optional): If True and save_report is True, saves the report as an XLSX file instead of CSV. Defaults to False.

        Returns:
            None

        Workflow:
            - Extracts track data for the specified author, handling pagination.
            - Downloads audio files for tracks with valid URLs, organizing them by author.
            - Updates audit information for each track (download status, folder, filename, date).
            - If `save_report` is True, saves a comprehensive report (CSV or XLSX) with metadata and download results, timestamped in the output directory.

        Notes:
            - If no tracks are found, the method logs a warning and does not create any files.
            - The final report is named 'autor_{author_name}_completo_{timestamp}.csv' or '.xlsx', with the author name sanitized, and is saved in the output directory if `save_report` is True.
            - Progress, warnings, and errors are logged using the logger.
            - This method is intended for use in the complete author download and audit workflow.
        """
        logger.info(f"--- Iniciando processo completo para o autor: {author_name} ---")

        dados_musicas = self.extract_author_data(author_name)

        if not dados_musicas:
            logger.warning("Nenhuma música foi extraída. Encerrando o processo.")
            return

        df = pd.DataFrame(dados_musicas)
        df_audit = self._download_and_audit_dataframe(df)

        if save_report:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_author_name = (
                re.sub(r'[\\/*?:"<>|]', "", author_name).replace(" ", "_").lower()
            )
            filename = f"autor_{safe_author_name}_completo_{timestamp}.csv"
            filepath = Path(self.output_dir) / filename
            df_audit = df_audit.reindex(columns=self.config.OUTPUT_COLUMNS)

            if report_xlsx:
                df_audit.to_excel(filepath.with_suffix(".xlsx"), index=False)
            else:
                df_audit.to_csv(filepath.with_suffix(".csv"), index=False, encoding="utf-8-sig")
            
            logger.info("\n--- Processo de Download Concluído ---")
            logger.info(
                f"O relatório final para o autor '{author_name}' foi salvo em: {filepath}"
            )
        else:
            logger.info("\n--- Processo de Download Concluído ---")
