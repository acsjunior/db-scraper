"""
Centralizes the application settings, such as URLs,
file names, model parameters, and other constants.
"""

# Base headers that do not change
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

# API Templates
API_TRACKLIST_URL_TEMPLATE = "https://discografiabrasileira.com.br/fonograma/@relationById/{playlist_id}/@type/MusicRecording/@orderBy/@.@order/@orderDir/asc/@pp/{limit}/p/1?shiro_content=true"
API_CONTENT_URL_TEMPLATE = "https://discografiabrasileira.com.br/api/1.0/content/{data_id}?fields=_id,name,audio[contentUrl;duration],creator[_id;name],recordingOf[_id;name;author[_id;name]]"
API_AUTHOR_URL_TEMPLATE = "https://discografiabrasileira.com.br/fonograma/xAuthor/{author_name}/@property/audio/"

# Output columns for the final dataset
COLUMN_DTYPES = {
    'data_id': str,
    'titulo': str,
    'interprete': str,
    'autor': str,
    'disco': str,
    'ano_lancamento_disco': str,
    'data_gravacao': str,
    'data_lancamento': str,
    'fonte_url': str,
    'audio_url': str,
    'pasta': str,
    'nome_arquivo': str,
    'data_download': str
}
OUTPUT_COLUMNS = list(COLUMN_DTYPES.keys())

UI_REPORT_COLUMNS = [
    'titulo',
    'interprete',
    'autor',
    'disco',
    'ano_lancamento_disco',
    'pasta',
    'nome_arquivo',
    'fonte_url'
]
