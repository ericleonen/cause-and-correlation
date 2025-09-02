import pandas as pd

if __name__ == "__main__":
    movies_df = pd.read_json("data/raw.ndjson", lines=True)

    movies_df["roi"] = movies_df["revenue"] / movies_df["budget"]
    movies_df.drop(columns=["revenue", "budget"], inplace=True)

    actor_dummies = movies_df["actors"].explode().str.get_dummies().groupby(level=0).max()
    movies_df = pd.concat([movies_df, actor_dummies], axis=1)
    movies_df.drop(columns=["actors"], inplace=True)

    movies_df = movies_df.set_index("title")

    movies_df_colsums = movies_df.sum(axis=0)
    single_movie_actors = movies_df_colsums[movies_df_colsums == 1].index.tolist()

    movies_df.drop(columns=single_movie_actors, inplace=True)

    movies_df.to_csv("data/processed.csv", sep="\t")