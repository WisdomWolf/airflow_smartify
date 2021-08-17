import json

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago

from spotiwise import Spotify
from spotiwise.oauth2 import SpotifyOAuth
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval="@hourly", start_date=days_ago(2), tags=['spotify'])
def smartify_playlist_sync():
    """### Weekly Spotify playlist sync
    This is a simple ETL data pipeline to keep Spotify Release Radar
    and Discover Weekly playlists in sync every week
    """

    @task()
    def initialize_spotiwise():

        SPOTIFY_APP_ID = Variable.get('SPOTIFY_APP_ID')
        SPOTIFY_APP_SECRET = Variable.get('SPOTIFY_APP_SECRET')
        SPOTIFY_REDIRECT_URI = 'http://127.0.0.1:5000/login/'
        ALL_SCOPES = [
            'playlist-read-private',
            'playlist-read-collaborative',
            'playlist-modify-public',
            'playlist-modify-private',
            'streaming',
            'ugc-image-upload',
            'user-follow-modify',
            'user-follow-read',
            'user-library-read',
            'user-library-modify',
            'user-read-private',
            'user-read-birthdate',
            'user-read-email',
            'user-top-read',
            'user-read-playback-state',
            'user-modify-playback-state',
            'user-read-currently-playing',
            'user-read-recently-played'
        ]
        username = 'wisdomwolf'
        redirect_url = 'http://127.0.0.1:5000/login/authorized'
        scopes = ' '.join(ALL_SCOPES)

        auth_manager = SpotifyOAuth(
            scope=scopes,
            client_id=SPOTIFY_APP_ID,
            client_secret=SPOTIFY_APP_SECRET,
            redirect_uri=redirect_url,
            cache_path='/opt/airflow/dags/.cache-wisdomwolf'
        )
        sp = Spotify(auth_manager=auth_manager)
        return sp

    @task()
    def display_spotify_user(sp):
        print(f'Current user: {sp.current_user()}')

    display_spotify_user(initialize_spotiwise())


smartify_playlist_sync = smartify_playlist_sync()
