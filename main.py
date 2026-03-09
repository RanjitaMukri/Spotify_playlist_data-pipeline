import json
import pandas as pd

print("Starting Spotify Data Pipeline...")

# ---------------------------
# Load JSON Data
# ---------------------------
with open("playlist.json", "r", encoding="utf-8") as file:
    data = json.load(file)

playlist_name = data["playlist_name"]
tracks = data["tracks"]

df = pd.DataFrame(tracks)

df["playlist_name"] = playlist_name

# ---------------------------
# Save Raw Data
# ---------------------------
df.to_csv("playlist_raw.csv", index=False)

print("Raw data saved.")

# ---------------------------
# Transformations
# ---------------------------

# Convert duration ms → minutes
df["duration_minutes"] = df["duration_ms"] / 60000

# Extract release year
df["release_year"] = pd.to_datetime(df["release_date"]).dt.year

# Remove duplicates
df = df.drop_duplicates(subset="track_name")

# Handle missing values
df = df.fillna("Unknown")

# Popularity categories
def categorize(pop):
    if pop <= 40:
        return "Low"
    elif pop <= 70:
        return "Medium"
    else:
        return "High"

df["popularity_category"] = df["popularity"].apply(categorize)

# ---------------------------
# Analysis
# ---------------------------

top_tracks = df.sort_values(by="popularity", ascending=False).head(5)

avg_duration = df["duration_minutes"].mean()

tracks_per_artist = df["artist_name"].value_counts()

# ---------------------------
# Save Processed Data
# ---------------------------

df.to_csv("playlist_transformed.csv", index=False)

df.to_json("playlist_processed.json", orient="records", indent=4)

# ---------------------------
# Summary Report
# ---------------------------

with open("summary_report.txt", "w") as f:

    f.write("Spotify Playlist Data Pipeline Report\n\n")

    f.write("Playlist Name: " + playlist_name + "\n\n")

    f.write("Top 5 Tracks:\n")
    f.write(str(top_tracks[["track_name","artist_name","popularity"]]))

    f.write("\n\nAverage Duration (minutes): ")
    f.write(str(round(avg_duration,2)))

    f.write("\n\nTracks per Artist:\n")
    f.write(str(tracks_per_artist))

print("Pipeline completed successfully!")