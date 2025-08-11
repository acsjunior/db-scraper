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


def save_playlist_to_csv(playlist_id: str, filename: str, limit: int = 500):
    """
    Orchestrates the extraction of playlist data and saves the result to a CSV file.

    Args:
        playlist_id: The ID of the playlist to extract.
        filename: The name of the output CSV file (without the .csv extension).
        limit: The maximum number of tracks to extract (default: 500).
    """
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
    df = df[output_cols]

    output_dir = paths.MUSICS_DIR
    os.makedirs(output_dir, exist_ok=True)
    filepath = output_dir / f"{filename}.csv"
    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    print("\n--- Processo Concluído ---")
    print(f"Os dados foram salvos com sucesso em: {filepath}")


def download_audios_from_csv(csv_filepath: str):
    """
    Reads a CSV file of songs and downloads the available audio files,
    organizing them into folders based on the first author of each song.

    Args:
        csv_filepath: The full path to the CSV file.
    """
    print("\n--- Iniciando processo de download a partir do CSV ---")
    try:
        df = pd.read_csv(csv_filepath)
        print(f"Lendo dados de: {csv_filepath}")
    except FileNotFoundError:
        print(f"Erro: O arquivo CSV '{csv_filepath}' não foi encontrado.")
        return

    # 1. Initialize new audit columns
    df["pasta_autor"] = ""
    df["nome_arquivo_mp3"] = ""
    df["download_sucesso"] = False

    df_com_audio = df[df["audio_url"].notna() & (df["audio_url"] != "")].copy()

    if df_com_audio.empty:
        print("Nenhuma música com URL de áudio encontrada para download.")
        return

    print(f"Encontradas {len(df_com_audio)} músicas com URL de áudio para baixar.")
    headers = config.BASE_HEADERS

    for index, row in df_com_audio.iterrows():
        download_status = False
        nome_pasta_autor = "Autor Desconhecido"
        nome_arquivo_final = ""

        autores = str(row["autor"])
        if pd.notna(autores) and autores:
            primeiro_autor = autores.split(" / ")[0].strip()
            nome_pasta_autor = re.sub(r'[\\/*?:"<>|]', "", primeiro_autor)

        output_dir = paths.MUSICS_DIR / nome_pasta_autor
        os.makedirs(output_dir, exist_ok=True)

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
        filepath = output_dir / nome_arquivo_final

        if os.path.exists(filepath):
            print(f"  - Já existe: '{titulo}'. Marcando como sucesso.")
            download_status = True
        else:
            print(f"  - Baixando: '{titulo}'...")
            try:
                audio_response = requests.get(
                    str(row["audio_url"]), headers=headers, stream=True, timeout=20
                )
                audio_response.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in audio_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print("    -> Sucesso.")
                download_status = True
            except requests.RequestException as e:
                print(f"    -> Erro ao baixar: {e}")
                download_status = False

        # 2. Update the original DataFrame with the audit results
        df.loc[index, "pasta_autor"] = nome_pasta_autor
        df.loc[index, "nome_arquivo_mp3"] = nome_arquivo_final
        df.loc[index, "download_sucesso"] = download_status

    # 3. Save the new CSV with timestamp
    p = Path(csv_filepath)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{p.stem}_{timestamp}{p.suffix}"
    new_filepath = p.parent / new_filename

    df.to_csv(new_filepath, index=False, encoding="utf-8-sig")
    print("\n--- Processo de Auditoria Concluído ---")
    print(f"O DataFrame atualizado foi salvo em: {new_filepath}")


if __name__ == "__main__":
    playlist_id = "248904"
    # playlist_id ="247664"
    filename = f"playlist_{playlist_id}"
    limit = 300
    save_playlist_to_csv(playlist_id, filename)

    csv_path = paths.MUSICS_DIR / f"{filename}.csv"
    download_audios_from_csv(csv_path)
