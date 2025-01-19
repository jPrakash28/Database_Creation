import pandas as pd
from json import loads
import csv
import codecs


def to_file(name, data, header):
    with codecs.open(name, 'w', "utf-8") as f:
        writer = csv.writer(f, delimiter=',', lineterminator='\n')
        writer.writerow(header)
        for item in data:
            writer.writerow(item)


def main():
    # Load the CSV file
    df = pd.read_csv("/Users/josh/Documents/archive-2/tmdb_5000_movies.csv")
    df["genres"] = df["genres"].apply(loads)
    df["keywords"] = df["keywords"].apply(loads)

    genres = [(row["id"], genre["id"], genre["name"]) for _, row in df.iterrows() for genre in row["genres"]]

    keywords = [(row["id"], keyword["id"], keyword["name"]) for _, row in df.iterrows() for keyword in row["keywords"]]

    movies = [(row["id"], row["title"], row["popularity"], row["release_date"], row["vote_average"], row["vote_count"])
              for _, row in df.iterrows()]

    # Write data to CSV files
    to_file("genres.csv", genres, ('movieID', 'genreID', 'genre'))
    to_file("keywords.csv", keywords, ('movieID', 'keywordID', 'keywords'))
    to_file("movies.csv", movies, ('movieID', 'title', 'popularity', 'released', 'vote', 'vote_count'))


main()
