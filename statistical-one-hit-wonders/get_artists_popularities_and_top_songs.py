import os
from dotenv import load_dotenv
import spotipy
from spotipy import SpotifyClientCredentials
from multiprocessing import Process
import time
import json

CLIENT = 2

def get_artists_popularities_and_top_songs(client: int):
    load_dotenv()

    if os.path.exists(".cache"):
        os.remove(".cache")

    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.getenv(f"SPOTIFY_CLIENT_ID_{client}"),
            client_secret=os.getenv(f"SPOTIFY_CLIENT_SECRET_{client}")
        ),
        requests_timeout=20
    )

    def get_artist_popularities_and_most_popular_song(artist_id: str):
        limit = 50

        # --- GET ARTIST'S ALBUMS ---
        print("    > Getting albums")
        offset = 0
        album_ids: set[str] = set()

        while True:
            res = sp.artist_albums(
                artist_id,
                include_groups="album,single",
                limit=limit,
                offset=offset
            )
            items = res.get("items", [])
            album_ids.update(album["id"] for album in items if album.get("id"))

            if len(items) < limit:
                break

            offset += limit
            time.sleep(0.02)

        # --- GET ARTIST'S TRACKS ---
        print(f"    > Getting tracks from {len(album_ids)} albums")
        seen_tracks: set[str] = set() # key of name + duration
        track_ids: set[str] = set()
        for album_id in album_ids:
            offset = 0

            while True:
                tracks = sp.album_tracks(album_id, limit=limit, offset=offset).get("items", [])
                for track in tracks:
                    track_id = track.get("id")
                    if artist_id in [track_artist["id"] for track_artist in track.get("artists", [])
                                    if track_id]:
                        key = track.get("name") + "_" + str(round(track.get("duration_ms", 0) / 1000))
                        
                        if track_id and key not in seen_tracks:
                            track_ids.add(track_id)
                            seen_tracks.add(key)

                if len(tracks) < limit:
                    break
                offset += limit
                time.sleep(0.02)

        # --- GET ARTIST'S TRACKS' POPULARITIES ---
        print(f"    > Getting tracks' popularities and most popular song from {len(track_ids)} tracks")
        popular_track_name = None
        max_popularity = -1
        track_popularities = []
        track_ids = list(track_ids)

        for i in range(0, len(track_ids), limit):
            chunk = track_ids[i:min(len(track_ids), i+limit)]
            tracks = sp.tracks(chunk).get("tracks", [])

            for track in tracks:
                popularity = int(track.get("popularity", -1) or -1)
                if popularity >= 0:
                    track_popularities.append(popularity)
                if popularity > max_popularity:
                    popular_track_name = track["name"]
                    max_popularity = popularity

            time.sleep(0.02)

        return track_popularities, popular_track_name

    offset = 0
    with open("data/popularities.ndjson", "r") as f:
        offset = sum(1 for line in f if len(line.strip()) > 0)

    print(f"First {offset} artists already processed. Skipping them.")

    with open("data/billboard_artists_2000_2025.tsv", "r") as f:
        n_artists = sum(1 for _ in f) - 1

    with open("data/billboard_artists_2000_2025.tsv", "r") as f:
        for i, line in enumerate(f):
            if (i == 0) or (i < offset + 1):
                continue

            parts = line.strip().split("\t")
            artist_name = parts[1]
            artist_id = parts[0]

            print(f"{artist_name} ({i}/{n_artists})")
            popularities, most_popular_track = get_artist_popularities_and_most_popular_song(artist_id)

            artist_json = {
                "id": artist_id,
                "name": artist_name,
                "popularities": popularities,
                "most_popular_track": most_popular_track
            }

            with open("data/popularities.ndjson", "a") as f:
                json.dump(artist_json, f)
                f.write("\n")

if __name__ == "__main__":
    N_CLIENTS = 3
    CLIENT = 0

    while True:
        p = Process(target=get_artists_popularities_and_top_songs, args=(CLIENT+6,))
        p.start()
        p.join(timeout=60)  # wait at most 60s

        if p.is_alive():
            print(f"--- Client {CLIENT + 6} timed out, switching to {(CLIENT + 1) % N_CLIENTS + 6} ---")
            p.terminate()  # kill the stuck process
            p.join()

        CLIENT = (CLIENT + 1) % N_CLIENTS