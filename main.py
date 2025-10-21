import asyncio
import aiohttp
import json
import csv
import os
from tqdm import tqdm
from dotenv import load_dotenv

async def fetch_movies(session, page_number, base_url, api_key, language):
    params = {"api_key": api_key, "language": language, "page": page_number}
    async with session.get(base_url, params=params) as response:
        if response.status == 200:
            data = await response.json()
            return data.get("results", [])
        return []

async def fetch_movies_data(base_url, api_key, language, total_pages):
    all_movies = []
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_movies(session, page, base_url, api_key, language) for page in range(1, total_pages + 1)]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching movies"):
            page_movies = await task
            all_movies.extend(page_movies)
            await asyncio.sleep(0.05)
    return all_movies

def save_as_json(movies_list, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(movies_list, f, indent=4, ensure_ascii=False)
    print(f"Saved JSON file -> {filename}")

def save_as_csv(movies_list, filename):
    if not movies_list:
        print("No movies to save.")
        return
    keys = movies_list[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(movies_list)
    print(f"Saved CSV file -> {filename}")

async def main():
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        api_key = input("Enter your TMDB API key: ").strip()

    total_pages = int(input("How many pages of movies do you want to fetch? ").strip())
    language = input("Enter language code (default: en-US): ").strip() or "en-US"
    output_format = input("Choose output format (json/csv): ").strip().lower()
    output_name = input("Enter the output file name (without extension): ").strip() or "movies"
    base_url = "https://api.themoviedb.org/3/movie/top_rated"

    print("\nFetching movies from TMDB...")
    movies = await fetch_movies_data(base_url, api_key, language, total_pages)
    print(f"Total movies fetched: {len(movies)}")

    if output_format == "csv":
        save_as_csv(movies, f"{output_name}.csv")
    else:
        save_as_json(movies, f"{output_name}.json")

if __name__ == "__main__":
    asyncio.run(main())
