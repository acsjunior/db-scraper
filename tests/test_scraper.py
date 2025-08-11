import pandas as pd
from src.db_scraper import scraper

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
    Unit tests for the main workflow functions of the scraper module.

    This test class covers the following scenarios:
        - Verifies that the playlist metadata can be correctly saved to a CSV file using the save_playlist_to_csv function.
        - Validates the complete workflow of extracting playlist data, downloading audio files, and saving the final audited CSV using the download_from_playlist function.

    The tests use mock data and patching to simulate file system operations and network requests, ensuring isolated and reliable test execution.
    """

    def test_save_playlist_to_csv(self, mocker, tmp_path):
        """
        Unit test for the save_playlist_to_csv function.

        This test verifies that the playlist metadata is correctly saved to a CSV file.
        It uses mock data to simulate the extraction process and patches the output directory to a temporary path.
        The test checks that the expected CSV file is created, contains the correct number of rows, and that the song title matches the mock data.

        Args:
            mocker: pytest-mock fixture for patching functions and objects.
            tmp_path: pytest fixture providing a temporary directory unique to the test invocation.
        """
        mocker.patch(
            "src.db_scraper.scraper.extract_playlist_data", return_value=MOCK_SONG_DATA
        )
        mocker.patch("src.db_scraper.paths.MUSICS_DIR", tmp_path)

        scraper.save_playlist_to_csv("fake_id", output_dir=tmp_path)

        expected_file = tmp_path / "playlist_fake_id_metadata.csv"

        assert expected_file.exists()
        df = pd.read_csv(expected_file)
        assert len(df) == 1
        assert df.iloc[0]["titulo"] == "É Mato"

    def test_download_from_playlist(self, mocker, tmp_path):
        """
        Unit test for the download_from_playlist function.

        This test validates the complete workflow of extracting playlist data, downloading audio files,
        and saving the final audited CSV report. It uses mock data to simulate the extraction process and
        patches the requests.get method to avoid real network calls. The test checks that the audio file
        is downloaded to the correct author folder, and that the final audit CSV is created with the expected naming pattern.

        Args:
            mocker: pytest-mock fixture for patching functions and objects.
            tmp_path: pytest fixture providing a temporary directory unique to the test invocation.
        """
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

        # --- CORREÇÃO 2: Usa o nome correto da função 'download_from_playlist' ---
        scraper.download_from_playlist("fake_id", output_dir=tmp_path)

        # Assertions
        author_folder = tmp_path / "Wilson Batista"
        assert author_folder.is_dir()

        mp3_file = author_folder / "e-mato_62582.mp3"
        assert mp3_file.is_file()

        audit_files = list(tmp_path.glob("playlist_fake_id_completo_*.csv"))
        assert len(audit_files) == 1
