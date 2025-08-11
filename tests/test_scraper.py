import pandas as pd
from src.db_scraper import scraper

# Mock data to be used in tests, representing the output of extract_playlist_data
MOCK_SONG_DATA = [
    {
        "data_id": "62582",
        "titulo": "É Mato",
        "autor": "Wilson Batista / Alvaiade",
        "interprete": "Odete Amaral",
        "disco": "Odeon 12071",
        "ano_lancamento_disco": "1941",
        "data_gravacao": "13 Outubro 1941",
        "data_lancamento": "Dezembro 1941",
        "audio_url": "http://fake.url/audio/62582.mp3",
    }
]


class TestWorkflowFunctions:
    """
    Tests the main workflow functions: save_playlist_to_csv and process_playlist.
    """

    def test_save_playlist_to_csv(self, mocker, tmp_path):
        """
        Tests the creation of the metadata-only CSV file in a temporary directory.
        """
        mocker.patch(
            "src.db_scraper.scraper.extract_playlist_data", return_value=MOCK_SONG_DATA
        )
        mocker.patch("src.db_scraper.paths.MUSICS_DIR", tmp_path)

        scraper.save_playlist_to_csv("fake_id", output_dir=tmp_path)

        expected_file = tmp_path / "playlist_fake_id_metadados.csv"
        assert expected_file.exists()

        df = pd.read_csv(expected_file)
        assert len(df) == 1
        assert df.iloc[0]["titulo"] == "É Mato"

    def test_process_playlist(self, mocker, tmp_path):
        """
        Tests the complete workflow: extract, download, and save the final audited CSV.
        """
        # 1. Arrange: Mock the extractor and the network download request
        mocker.patch(
            "src.db_scraper.scraper.extract_playlist_data", return_value=MOCK_SONG_DATA
        )
        mocker.patch(
            "requests.get",
            return_value=mocker.Mock(
                raise_for_status=mocker.Mock(),
                iter_content=lambda chunk_size: [b"fake_mp3_bytes"],
            ),
        )

        # 2. Act: Execute the main process function
        scraper.process_playlist("fake_id", output_dir=tmp_path)

        # 3. Assert: Check all expected outputs
        author_folder = tmp_path / "Wilson Batista"
        assert author_folder.is_dir()

        mp3_file = author_folder / "e-mato_62582.mp3"
        assert mp3_file.is_file()
        assert mp3_file.read_bytes() == b"fake_mp3_bytes"

        audit_files = list(tmp_path.glob("playlist_fake_id_completo_*.csv"))
        assert len(audit_files) == 1

        df_audit = pd.read_csv(audit_files[0])

        assert df_audit.iloc[0]["download_sucesso"]
        assert df_audit.iloc[0]["pasta_autor"] == "Wilson Batista"
        assert df_audit.iloc[0]["nome_arquivo_mp3"] == "e-mato_62582.mp3"
