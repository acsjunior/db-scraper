from . import paths

"""
Centralizes the application settings, such as URLs,
file names, model parameters, and other constants.
"""

# Base headers that do not change
BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}

# Template for the playlist view URL
BASE_PLAYLIST_URL_TEMPLATE = "https://discografiabrasileira.com.br/playlists/{playlist_id}"

# API Templates
API_TRACKLIST_URL_TEMPLATE = "https://discografiabrasileira.com.br/fonograma/@relationById/{playlist_id}/@type/MusicRecording/@orderBy/@.@order/@orderDir/asc/@pp/{limit}/p/1?shiro_content=true"
API_CONTENT_URL_TEMPLATE = "https://discografiabrasileira.com.br/api/1.0/content/{data_id}?fields=_id,name,audio[contentUrl;duration],creator[_id;name],recordingOf[_id;name;author[_id;name]]"