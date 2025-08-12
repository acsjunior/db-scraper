import streamlit as st
import re
from pathlib import Path
from db_scraper.scraper import DiscografiaScraper
from db_scraper import config


st.set_page_config(page_title="DB Downloader", page_icon="🎵", layout="centered")
st.title("🎵 DB Downloader")


# 1. Download type selection
st.header("1. Escolha o tipo de busca")
search_type = st.radio(
    "Você quer baixar músicas de uma Playlist ou de um Autor?",
    ("Playlist", "Autor"),
    label_visibility="collapsed",
)

# 2. Data input
st.header("2. Insira os dados")
if search_type == "Playlist":
    input_url = st.text_input(
        "URL da Playlist:",
        placeholder="Ex: https://discografiabrasileira.com.br/playlists/247664/samba-do-sindicatis",
    )
    input_id = None

else:
    input_id = st.text_input("Nome do Autor:", placeholder="Ex: Nilton Bastos")

# 3. Output directory definition
st.header("3. Local de salvamento")
try:
    default_download_folder = Path.home() / "Downloads"
    final_output_dir = default_download_folder / "db_downloads"
    st.info(f"Os downloads serão salvos em: `{final_output_dir}`")
    st.markdown(
        "📂 Dentro desta pasta, as músicas serão organizadas em subpastas com o nome do primeiro autor de cada faixa."
    )
except Exception as e:
    st.error(
        f"Não foi possível determinar a pasta de Downloads do seu sistema. Erro: {e}"
    )
    st.stop()

# 4. Options and Start button
st.header("4. Opções e Início")

save_report = st.checkbox(
    "Salvar relatório com os resultados",
    value=True,
    help="Se marcado, um arquivo com os metadados será salvo.",
)

report_format_option = st.selectbox(
    "Formato do relatório:", ("XLSX (Excel)", "CSV"), disabled=not save_report
)

if st.button("Baixar Músicas"):
    if search_type == "Playlist":
        if not input_url:
            st.warning("Por favor, insira a URL da Playlist.")
            st.stop()
        else:
            match = re.search(r"/playlists/(\d+)/", input_url)
            if match:
                input_id = match.group(1)
            else:
                st.error(
                    "URL da playlist inválida. Não foi possível encontrar o ID. Verifique o formato da URL."
                )
                st.stop()
    else:
        if not input_id:
            st.warning("Por favor, preencha o Nome do Autor.")
            st.stop()

    if input_id:
        scraper = DiscografiaScraper(output_dir=str(final_output_dir))

        save_as_xlsx = "XLSX" in report_format_option

        st.info("Iniciando o processo... Por favor, aguarde.")

        with st.spinner(
            "Extraindo metadados e baixando áudios... Este processo pode demorar."
        ):
            try:
                if search_type == "Playlist":
                    scraper.download_from_playlist(
                        playlist_id=input_id,
                        save_report=save_report,
                        report_xlsx=save_as_xlsx,
                        report_columns=config.UI_REPORT_COLUMNS,
                    )
                else:
                    scraper.download_from_author(
                        author_name=input_id,
                        save_report=save_report,
                        report_xlsx=save_as_xlsx,
                        report_columns=config.UI_REPORT_COLUMNS,
                    )

                st.success("Processo concluído com sucesso!")
                st.write(
                    f"Verifique a pasta `{final_output_dir}` para encontrar os arquivos de áudio."
                )

                if save_report:
                    report_type = "XLSX" if save_as_xlsx else "CSV"
                    st.write(
                        f"Um relatório em formato {report_type} também foi salvo na mesma pasta."
                    )

                st.balloons()

            except Exception as e:
                st.error(f"Ocorreu um erro inesperado durante o processo: {e}")
