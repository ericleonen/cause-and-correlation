import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

TMDB_HEADERS = headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv('TMDB_API_KEY')}"
}

EXCLUDED_GENRES = set(["Animation", "Documentary"])

def sample_popular_movies_by_year(
    year: int, 
    max_movies_percentage: float,
    excluded_genres: set[str] = EXCLUDED_GENRES,
    min_actor_popularity: float = 2
):
    print(f"Samping at most {max_movies_percentage * 100}% of movies from {year}:")

    all_movies = []
    page = 0
    max_movies = None
    max_page = None

    while (max_movies is None or len(all_movies) < max_movies) and (max_page is None or page < max_page):
        page += 1

        print(f" > Searching page {page}:", end=" ")

        movies_added = 0

        movies_url = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page={page}&primary_release_date.gte={year}-01-01&primary_release_date.lte={year}-12-31&region=United%20States&sort_by=popularity.desc"
        movies_res = requests.get(movies_url, headers=TMDB_HEADERS)
        movies_res.raise_for_status()
        movies_json = movies_res.json()

        if max_page is None:
            max_page = movies_json["total_pages"]
        if max_movies is None:
            max_movies = int(movies_json["total_results"]*max_movies_percentage + 1)

        results = movies_json["results"]

        for result in results:
            if len(all_movies) == max_movies:
                break

            movie_id = result["id"]

            details_url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
            details_res = requests.get(details_url, headers=TMDB_HEADERS)
            details_res.raise_for_status()

            details = details_res.json()

            if details["budget"] > 0 and details["revenue"] > 0 and not any([genre["name"] in excluded_genres for genre in details["genres"]]):
                credits_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?language=en-US"
                credits_res = requests.get(credits_url, headers=TMDB_HEADERS)
                credits_res.raise_for_status()

                credits = credits_res.json()

                actors = [
                    person["name"] for person in credits["cast"] 
                    if person["known_for_department"] == "Acting" and "uncredited" not in person["character"].lower() and person["popularity"] >= min_actor_popularity
                ]

                if len(actors) == 0:
                    continue

                all_movies.append({
                    "title": details["title"],
                    "budget": details["budget"],
                    "revenue": details["revenue"],
                    "actors": actors
                })
                movies_added += 1

        print(f"{movies_added} movies found. ({len(all_movies)}/{max_movies})")

    return all_movies

if __name__ == "__main__":
    for year in range(2000, 2024 + 1):
        movies = sample_popular_movies_by_year(year, 0.001)

        with open(f"data/raw.ndjson", "a") as f:
            for movie in movies:
                json.dump(movie, f)
                f.write("\n")

        print(f"--- Added all {year} movies ---")