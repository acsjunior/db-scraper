import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import os
import pandas as pd
import re
import unicodedata
from datetime import datetime
from pathlib import Path

from . import config
from . import paths


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

    print(f"Buscando dados da playlist ID: {playlist_id}...")
    try:
        # --- Step 1: Get the list of tracks from the playlist ---
        response = requests.get(tracklist_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        tracks = soup.find_all("div", class_="track")
        print(f"Encontradas {len(tracks)} faixas. Extraindo detalhes...")

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
                    print(
                        f"  - Aviso: Não foi possível obter a URL do áudio para a faixa '{titulo}' (ID: {data_id})."
                    )
            else:
                print(
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
        print(f"Erro fatal ao buscar a lista de faixas: {e}")
        return []

    return all_songs_data


def save_playlist_to_csv(playlist_id: str, output_dir: str, limit: int = 500):
    """
    Extracts playlist metadata and saves the result to a CSV file.

    Args:
        playlist_id: The ID of the playlist to extract.
        filename: The output CSV filename (without the .csv extension).
        limit: The maximum number of tracks to extract (default: 500).
    """
    print("--- Iniciando processo ---")
    dados_musicas = extract_playlist_data(playlist_id, limit)

    if not dados_musicas:
        print("Nenhum dado foi extraído. O arquivo CSV não será gerado.")
        return

    df = pd.DataFrame(dados_musicas)

    output_cols = [
        "data_id",
        "titulo",
        "interprete",
        "autor",
        "disco",
        "ano_lancamento_disco",
        "data_gravacao",
        "data_lancamento",
        "audio_url",
    ]
    df = df.reindex(columns=output_cols)

    os.makedirs(output_dir, exist_ok=True)
    filename = f"playlist_{playlist_id}_metadados.csv" 
    filepath = Path(output_dir) / filename
    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    print("\n--- Extração concluída ---")
    print(f"Os metadados foram salvos com sucesso em: {filepath}")

def process_playlist(playlist_id: str, output_dir: str, limit: int = 500) -> None:
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
    print("--- Iniciando processo: Extração, Download e Auditoria ---")
    dados_musicas = extract_playlist_data(playlist_id, limit=limit)

    if not dados_musicas:
        print("Nenhuma música foi extraída. Encerrando o processo.")
        return

    df = pd.DataFrame(dados_musicas)
    df['pasta_autor'] = ""
    df['nome_arquivo_mp3'] = ""
    df['download_sucesso'] = False
    df_com_audio = df[df['audio_url'].notna() & (df['audio_url'] != '')].copy()
    
    print(f"\nEncontradas {len(df_com_audio)} músicas com URL de áudio para baixar.")

    for index, row in df_com_audio.iterrows():
        # ... (lógica de download e atualização do df é a mesma da versão anterior)
        download_status = False
        nome_pasta_autor = "Autor Desconhecido"
        nome_arquivo_final = ""
        autores = str(row['autor'])
        if pd.notna(autores) and autores:
            primeiro_autor = autores.split(' / ')[0].strip()
            nome_pasta_autor = re.sub(r'[\\/*?:"<>|]', "", primeiro_autor)
        
        # O diretório de download dos MP3s será uma subpasta dentro do diretório de saída
        download_path = Path(output_dir) / nome_pasta_autor
        os.makedirs(download_path, exist_ok=True)
        
        titulo = str(row['titulo'])
        data_id = row['data_id']
        titulo_sem_acentos = unicodedata.normalize('NFKD', titulo).encode('ASCII', 'ignore').decode('utf-8')
        titulo_limpo = re.sub(r'[^a-z0-9\s-]', '', titulo_sem_acentos.lower())
        slug_titulo = re.sub(r'[\s-]+', '-', titulo_limpo).strip('-')
        nome_arquivo_final = f"{slug_titulo}_{str(data_id)}.mp3"
        filepath = download_path / nome_arquivo_final

        if os.path.exists(filepath):
            print(f"  - Já existe: '{titulo}'. Marcando como sucesso.")
            download_status = True
        else:
            print(f"  - Baixando: '{titulo}'...")
            try:
                audio_response = requests.get(str(row['audio_url']), headers=config.BASE_HEADERS, stream=True, timeout=20)
                audio_response.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in audio_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"    -> Sucesso.")
                download_status = True
            except requests.RequestException as e:
                print(f"    -> Erro ao baixar: {e}")
                download_status = False
        
        df.loc[index, 'pasta_autor'] = nome_pasta_autor
        df.loc[index, 'nome_arquivo_mp3'] = nome_arquivo_final
        df.loc[index, 'download_sucesso'] = download_status

    # Salva o CSV final auditado no diretório de saída principal
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    final_filename = f"playlist_{playlist_id}_completo_{timestamp}.csv"
    final_filepath = Path(output_dir) / final_filename

    colunas_finais = [
        'data_id', 'titulo', 'interprete', 'autor', 'disco', 'ano_lancamento_disco', 
        'data_gravacao', 'data_lancamento', 'audio_url', 'pasta_autor', 
        'nome_arquivo_mp3', 'download_sucesso'
    ]
    df = df.reindex(columns=colunas_finais)

    df.to_csv(final_filepath, index=False, encoding='utf-8-sig')
    print(f"\n--- Processo Completo Concluído ---")
    print(f"O relatório final foi salvo em: {final_filepath}")