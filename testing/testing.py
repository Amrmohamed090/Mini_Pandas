import pandas as pd

df = pd.read_csv("../Soccer Performance Data/Soccer Performance Data.csv")

# -----------------------------------------------------------------------
# Analysis
# -----------------------------------------------------------------------

print("=" * 60)
print("1. Mean maximum speed per player during Practice sessions")
print("=" * 60)
practice = df[df["Session_Type"] == "Practice"]
print(practice.groupby("Name")["Max_Speed"].mean().to_string())

print()
print("=" * 60)
print("2. Mean maximum speed per player during Game sessions")
print("=" * 60)
game = df[df["Session_Type"] == "Game"]
print(game.groupby("Name")["Max_Speed"].mean().to_string())

print()
print("=" * 60)
print("3. Mean, Median, and Mode of sleep quality per player")
print("=" * 60)
sleep_mean   = df.groupby("Name")["Sleep_Quality"].mean()
sleep_median = df.groupby("Name")["Sleep_Quality"].median()
sleep_mode   = df.groupby("Name")["Sleep_Quality"].agg(lambda x: x.mode().iloc[0])

sleep_stats = pd.DataFrame({
    "Mean":   sleep_mean,
    "Median": sleep_median,
    "Mode":   sleep_mode
})
print(sleep_stats.to_string())

# -----------------------------------------------------------------------
# Queries
# -----------------------------------------------------------------------

print()
print("=" * 60)
print("Query 1: Players with higher max speed in Game vs Practice")
print("=" * 60)
mean_practice = practice.groupby("Name")["Max_Speed"].mean()
mean_game     = game.groupby("Name")["Max_Speed"].mean()
comparison    = pd.DataFrame({"Practice": mean_practice, "Game": mean_game})
faster_in_game = comparison[comparison["Game"] > comparison["Practice"]]
print(faster_in_game.to_string())

print()
print("=" * 60)
print("Query 2: Players with the worst average sleep quality")
print("=" * 60)
worst_sleep = sleep_mean.sort_values()
min_val     = worst_sleep.min()
worst       = worst_sleep[worst_sleep == min_val]
print(worst.to_string())
