import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from typing import List

from . import config

logger = logging.getLogger(__name__)


def merge_reports(file_paths: List[str], output_path: str) -> str:
    """
    Merges multiple CSV report files into a single unified file, removing duplicate tracks by 'data_id'.

    This function reads a list of CSV files containing track metadata, concatenates them into a single DataFrame,
    removes duplicate entries based on the 'data_id' column (keeping the first occurrence), and saves the resulting
    unified report as a new CSV file in the specified output directory. The output file is timestamped to avoid overwriting.

    Args:
        file_paths (List[str]): A list of file paths to the CSV report files to be merged.
        output_path (str): The directory where the unified CSV file will be saved.

    Returns:
        str: The full path to the generated unified CSV file, or an empty string if the operation fails.

    Workflow:
        1. Reads each CSV file in the provided list, skipping files that cannot be read.
        2. Concatenates all successfully read DataFrames into a single DataFrame.
        3. Removes duplicate rows based on the 'data_id' column, keeping only the first occurrence.
        4. Saves the deduplicated DataFrame as a new CSV file in the output directory, with a timestamp in the filename.
        5. Returns the path to the unified CSV file, or an empty string if no files could be merged.

    Notes:
        - If no file paths are provided or none of the files can be read, the function returns an empty string.
        - The output file is named 'relatorio_unificado_<timestamp>.csv'.
        - Progress, warnings, and errors are logged using the logger.
        - The function expects all input files to have a 'data_id' column for deduplication.
    """
    if not file_paths:
        logger.warning("Nenhuma lista de arquivos foi fornecida para a união.")
        return ""

    all_dfs = []
    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path, dtype=config.COLUMN_DTYPES)
            all_dfs.append(df)
            logger.info(f"Lido com sucesso o arquivo: {file_path}")
        except FileNotFoundError:
            logger.error(f"Arquivo não encontrado: {file_path}. Pulando.")
            continue
        except Exception as e:
            logger.error(f"Erro ao ler o arquivo {file_path}: {e}. Pulando.")
            continue

    if not all_dfs:
        logger.error("Nenhum arquivo CSV pôde ser lido. A união foi cancelada.")
        return ""

    merged_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Total de registros antes da remoção de duplicatas: {len(merged_df)}")

    deduplicated_df = merged_df.drop_duplicates(subset=["data_id"], keep="first")
    logger.info(
        f"Total de registros após a remoção de duplicatas: {len(deduplicated_df)}"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"relatorio_unificado_{timestamp}.csv"
    output_filepath = Path(output_path) / output_filename

    deduplicated_df.to_csv(output_filepath, index=False, encoding="utf-8-sig")
    logger.info(f"Relatório unificado salvo com sucesso em: {output_filepath}")

    return str(output_filepath)
