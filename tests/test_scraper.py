import pandas as pd
from src.db_scraper.scraper import DiscografiaScraper

# Mock data to be used in tests
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
    Unit tests for the main workflow functions of the DiscografiaScraper class.
    """

    def test_save_playlist_to_csv(self, mocker, tmp_path):
        """
        Tests that the playlist metadata is correctly saved to a CSV file.
        """
        mocker.patch(
            "src.db_scraper.scraper.DiscografiaScraper.extract_playlist_data",
            return_value=MOCK_SONG_DATA,
        )
        scraper_instance = DiscografiaScraper(output_dir=str(tmp_path))
        scraper_instance.save_playlist_to_csv("fake_id")

        expected_file = tmp_path / "playlist_fake_id_metadata.csv"
        assert expected_file.exists()
        df = pd.read_csv(expected_file)
        assert len(df) == 1
        assert df.iloc[0]["titulo"] == "É Mato"

    def test_download_from_playlist(self, mocker, tmp_path):
        """
        Tests the complete workflow for a playlist: extract, download, and audit.
        """
        mocker.patch(
            "src.db_scraper.scraper.DiscografiaScraper.extract_playlist_data",
            return_value=MOCK_SONG_DATA,
        )
        mocker.patch(
            "requests.get",
            return_value=mocker.Mock(
                raise_for_status=mocker.Mock(),
                iter_content=lambda chunk_size: [b"fake_mp3_bytes"],
            ),
        )
        scraper_instance = DiscografiaScraper(output_dir=str(tmp_path))
        scraper_instance.download_from_playlist("fake_id")

        author_folder = tmp_path / "Wilson Batista"
        mp3_file = author_folder / "e-mato_62582.mp3"
        assert author_folder.is_dir()
        assert mp3_file.is_file()

        audit_files = list(tmp_path.glob("playlist_fake_id_completo_*.csv"))
        assert len(audit_files) == 1
        df_audit = pd.read_csv(audit_files[0])

        # Checks for the new 'data_download' column and verifies it's not empty.
        assert pd.notna(df_audit.iloc[0]["data_download"])
        assert df_audit.iloc[0]["data_download"] != ""

    def test_save_author_to_csv(self, mocker, tmp_path):
        """
        Tests that the author metadata is correctly saved to a CSV file.
        """
        mocker.patch(
            "src.db_scraper.scraper.DiscografiaScraper.extract_author_data",
            return_value=MOCK_SONG_DATA,
        )
        scraper_instance = DiscografiaScraper(output_dir=str(tmp_path))
        author_name = "Wilson Batista"
        scraper_instance.save_author_to_csv(author_name)

        expected_file = tmp_path / "autor_wilson_batista_metadados.csv"
        assert expected_file.exists()
        df = pd.read_csv(expected_file)
        assert len(df) == 1
        assert df.iloc[0]["autor"] == "Wilson Batista / Alvaiade"

    def test_download_from_author(self, mocker, tmp_path):
        """
        Tests the complete workflow for an author: extract, download, and audit.
        """
        mocker.patch(
            "src.db_scraper.scraper.DiscografiaScraper.extract_author_data",
            return_value=MOCK_SONG_DATA,
        )
        mocker.patch(
            "requests.get",
            return_value=mocker.Mock(
                raise_for_status=mocker.Mock(),
                iter_content=lambda chunk_size: [b"fake_mp3_bytes"],
            ),
        )
        scraper_instance = DiscografiaScraper(output_dir=str(tmp_path))
        author_name = "Wilson Batista"
        scraper_instance.download_from_author(author_name)

        author_folder = tmp_path / "Wilson Batista"
        mp3_file = author_folder / "e-mato_62582.mp3"
        assert author_folder.is_dir()
        assert mp3_file.is_file()

        audit_files = list(tmp_path.glob("autor_wilson_batista_completo_*.csv"))
        assert len(audit_files) == 1
        df_audit = pd.read_csv(audit_files[0])
        assert pd.notna(df_audit.iloc[0]["data_download"])
        assert df_audit.iloc[0]["data_download"] != ""
