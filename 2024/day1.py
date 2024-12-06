import polars as pl

dfx = pl.read_csv("./2024/data/d1a.csv").select(pl.col("x")).sort("x")
dfy = pl.read_csv("./2024/data/d1a.csv").select(pl.col("y")).sort("y")

df = pl.DataFrame(
    {"x": dfx, "y": dfy,}
)

result = df.select(
    abs(pl.col("x") - pl.col("y"))
).sum()

print(result)


# part two
dfx = pl.read_csv("./2024/data/d1a.csv").select(pl.col("x")).group_by("x").len().sort("x")
dfy = pl.read_csv("./2024/data/d1a.csv").select(pl.col("y")).group_by("y").len().sort("y")

df = dfx.join(dfy, left_on="x", right_on="y", how="left")

result = df.fill_null(strategy="zero").select(
    pl.col("x") * pl.col("len") * pl.col("len_right")
).sum()


print(result)
