import pandas as pd
from src.db_scraper import scraper

# Realistic mock data for tests
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


class TestFileOperations:
    """
    Tests for functions that perform file I/O, like saving CSVs and downloading.
    """

    def test_save_playlist_to_csv(self, mocker, tmp_path):
        """
        Tests the creation of the CSV file in a temporary directory.
        """
        mocker.patch(
            "src.db_scraper.scraper.extract_playlist_data", return_value=MOCK_SONG_DATA
        )
        mocker.patch("src.db_scraper.paths.MUSICS_DIR", tmp_path)

        # The function does not return anything, so we do not capture the result.
        scraper.save_playlist_to_csv("fake_id", "test_playlist")

        # Instead, we build the path we expect to be created.
        expected_file = tmp_path / "test_playlist.csv"

        # Now we check if the file exists and if the content is correct.
        assert expected_file.exists()
        df = pd.read_csv(expected_file)
        assert len(df) == 1
        assert df.iloc[0]["titulo"] == "É Mato"

    def test_download_audios_from_csv(self, mocker, tmp_path):
        """
        Tests the download and audit logic using a simulated file system and network.
        """
        # (This test function was already correct, but failed due to an error in the main function)
        input_csv_path = tmp_path / "input_playlist.csv"
        pd.DataFrame(MOCK_SONG_DATA).to_csv(input_csv_path, index=False)
        mocker.patch("src.db_scraper.paths.MUSICS_DIR", tmp_path)
        mocker.patch(
            "requests.get",
            return_value=mocker.Mock(
                raise_for_status=mocker.Mock(),
                iter_content=lambda chunk_size: [b"fake_mp3_bytes"],
            ),
        )

        scraper.download_audios_from_csv(str(input_csv_path))

        author_folder = tmp_path / "Wilson Batista"
        mp3_file = author_folder / "e-mato_62582.mp3"
        assert author_folder.is_dir()
        assert mp3_file.is_file()

        audit_files = list(tmp_path.glob("input_playlist_*.csv"))
        assert len(audit_files) == 1
