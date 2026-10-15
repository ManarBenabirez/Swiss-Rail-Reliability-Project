"""
Step 3 - Analysis: answer the Part 1 question with Spark SQL

Question: which stations have the most delay, weighted by their importance, and how
does punctuality change by hour, day of the week and canton?

Importance of a station:
  trains per day  = number of different trains that stop there each day
  lines           = number of different lines that stop there
  total delay     = average delay x trains per day (minutes of delay per day)
A train is on time if it arrives less than 3 minutes late. Delays come only from
arrivals that were really measured (see 02_clean.py).

Input:  data/parquet/stops_clean
Output: output/results/*.csv (tables), output/figures/*.png (charts for the report),
        data/parquet/baseline_station_hour (usual delay per station, weekday and hour, for Part 2)
"""
import matplotlib

matplotlib.use("Agg")                     # Docker has no screen
import matplotlib.pyplot as plt
import matplotlib.ticker
import pandas as pd

from spark_session import OUTPUT_DIR, PARQUET_DIR, get_spark

RESULTS = OUTPUT_DIR / "results"
FIGURES = OUTPUT_DIR / "figures"

# Small stations and lines give unreliable averages, so we only rank the bigger ones
MIN_TRAINS_PER_DAY = 20
MIN_MEASURED_STATION = 300
MIN_TRIPS_PER_DAY_LINE = 5
MIN_MEASURED_LINE = 500
TOP_N = 15

# Chart colours
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"


# Spark SQL queries (run on the table "stops")
Q_OVERALL = """
SELECT COUNT(*)                                        AS train_stops,
       COUNT(DISTINCT station_id)                      AS stations,
       COUNT(DISTINCT operating_day)                   AS days,
       COUNT(arrival_delay_min)                        AS measured_arrivals,
       ROUND(100 * AVG(CAST(is_punctual AS INT)), 2)   AS punctual_pct,
       ROUND(AVG(arrival_delay_min), 2)                AS avg_delay_min,
       ROUND(PERCENTILE(arrival_delay_min, 0.5), 2)    AS median_delay_min,
       ROUND(PERCENTILE(arrival_delay_min, 0.9), 2)    AS p90_delay_min,
       ROUND(100 * AVG(CAST(is_cancelled AS INT)), 2)  AS cancelled_pct
FROM stops
"""

# One row per station with the 3 importance measures and the delay
Q_STATIONS = """
WITH s AS (
  SELECT station_id,
         FIRST(COALESCE(station_name_official, station_name))    AS station,
         FIRST(canton)                                           AS canton,
         COUNT(DISTINCT operating_day, trip_id) / {days}         AS trains_per_day,
         COUNT(DISTINCT line_text)                               AS lines,
         COUNT(arrival_delay_min)                                AS measured,
         AVG(arrival_delay_min)                                  AS avg_delay_min,
         100 * AVG(CAST(is_punctual AS INT))                     AS punctual_pct,
         100 * AVG(CAST(is_cancelled AS INT))                    AS cancelled_pct
  FROM stops
  GROUP BY station_id
)
SELECT station_id, station, canton,
       ROUND(trains_per_day, 1)                    AS trains_per_day,
       lines, measured,
       ROUND(avg_delay_min, 2)                     AS avg_delay_min,
       ROUND(punctual_pct, 2)                      AS punctual_pct,
       ROUND(cancelled_pct, 2)                     AS cancelled_pct,
       ROUND(avg_delay_min * trains_per_day, 1)    AS total_delay_min_per_day
FROM s
WHERE trains_per_day >= {min_trains} AND measured >= {min_measured}
"""

Q_LINES = """
SELECT operator_abbr AS operator, line_text AS line,
       ROUND(COUNT(DISTINCT operating_day, trip_id) / {days}, 1)  AS trips_per_day,
       COUNT(DISTINCT station_id)                                 AS stations,
       COUNT(arrival_delay_min)                                   AS measured,
       ROUND(AVG(arrival_delay_min), 2)                           AS avg_delay_min,
       ROUND(100 * AVG(CAST(is_punctual AS INT)), 2)              AS punctual_pct
FROM stops
WHERE line_text IS NOT NULL
GROUP BY operator_abbr, line_text
HAVING COUNT(DISTINCT operating_day, trip_id) / {days} >= {min_trips}
   AND COUNT(arrival_delay_min) >= {min_measured}
"""

Q_HOUR = """
SELECT hour,
       ROUND(COUNT(DISTINCT operating_day, trip_id, station_id) / {days}) AS stops_per_day,
       ROUND(AVG(arrival_delay_min), 2)                            AS avg_delay_min,
       ROUND(100 * AVG(CAST(is_punctual AS INT)), 2)               AS punctual_pct
FROM stops GROUP BY hour ORDER BY hour
"""

Q_WEEKDAY = """
SELECT weekday, weekday_name,
       COUNT(DISTINCT operating_day)                    AS days,
       ROUND(COUNT(*) / COUNT(DISTINCT operating_day))  AS stops_per_day,
       ROUND(AVG(arrival_delay_min), 2)                 AS avg_delay_min,
       ROUND(100 * AVG(CAST(is_punctual AS INT)), 2)    AS punctual_pct
FROM stops GROUP BY weekday, weekday_name
ORDER BY (weekday + 5) % 7
"""

Q_CANTON = """
SELECT canton,
       COUNT(DISTINCT station_id)                       AS stations,
       COUNT(*)                                         AS train_stops,
       ROUND(AVG(arrival_delay_min), 2)                 AS avg_delay_min,
       ROUND(100 * AVG(CAST(is_punctual AS INT)), 2)    AS punctual_pct
FROM stops GROUP BY canton ORDER BY punctual_pct
"""

# Usual delay per station, weekday and hour (used in Part 2)
Q_BASELINE = """
SELECT station_id, weekday, hour,
       COUNT(arrival_delay_min)          AS measured,
       AVG(arrival_delay_min)            AS mean_delay_min,
       STDDEV(arrival_delay_min)         AS std_delay_min,
       PERCENTILE(arrival_delay_min, 0.9) AS p90_delay_min
FROM stops
WHERE arrival_delay_min IS NOT NULL
GROUP BY station_id, weekday, hour
"""


# Charts
def style(ax, title, xlabel=None, ylabel=None):
    ax.set_title(title, loc="left", fontsize=12, color=INK, fontweight="bold", pad=10)
    if xlabel:
        ax.set_xlabel(xlabel, color=INK2)
    if ylabel:
        ax.set_ylabel(ylabel, color=INK2)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2)
    ax.set_axisbelow(True)


def save(fig, name):
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIGURES / f"{name}.png", dpi=200, facecolor="white")
    plt.close(fig)
    print(f"  chart: {FIGURES / (name + '.png')}")


def chart_importance_vs_delay(st):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.grid(True, color=GRID, linewidth=0.6)
    ax.scatter(st["trains_per_day"], st["avg_delay_min"], s=18, color=BLUE,
               alpha=0.55, edgecolor="white", linewidth=0.4)
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.set_xticks([20, 50, 100, 200, 500, 1000, 2000])
    # Names of the 8 stations with the most total delay, stacked so they do not overlap
    top = st.nlargest(8, "total_delay_min_per_day").sort_values("trains_per_day")
    y_top = st["avg_delay_min"].max()
    step = (y_top - st["avg_delay_min"].min()) * 0.06
    for i, (_, r) in enumerate(top.iterrows()):
        x, y = r["trains_per_day"], r["avg_delay_min"]
        ax.scatter(x, y, s=40, color=ORANGE, edgecolor="white", linewidth=0.8, zorder=3)
        ax.annotate(r["station"], (x, y), xytext=(x, y_top - i * step),
                    ha="right" if i >= len(top) - 2 else "center", fontsize=8, color=INK,
                    arrowprops=dict(arrowstyle="-", color=INK2, linewidth=0.5, alpha=0.6))
    style(ax, "Station importance vs. delay  (orange: most total delay)",
          "Trains per day (log scale)", "Average arrival delay (min)")
    save(fig, "fig1_importance_vs_delay")


def chart_top_total_delay(st):
    top = st.nlargest(10, "total_delay_min_per_day").iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.grid(True, axis="x", color=GRID, linewidth=0.6)
    ax.barh(top["station"], top["total_delay_min_per_day"], color=BLUE, height=0.6)
    for i, (v, t) in enumerate(zip(top["total_delay_min_per_day"], top["trains_per_day"])):
        ax.text(v, i, f"  {v:,.0f} min  ({t:,.0f} trains/day)", va="center", fontsize=8, color=INK2)
    ax.set_xlim(0, top["total_delay_min_per_day"].max() * 1.45)
    style(ax, "Top 10 stations by total delay per day", "Minutes of delay per day (avg delay × trains)")
    save(fig, "fig2_top_total_delay")


def chart_by_hour(h):
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.grid(True, axis="y", color=GRID, linewidth=0.6)
    ax.plot(h["hour"], h["punctual_pct"], color=BLUE, linewidth=2, marker="o", markersize=4)
    worst = h.nsmallest(1, "punctual_pct").iloc[0]
    ax.annotate(f"{worst['punctual_pct']:.1f}% at {int(worst['hour'])}h",
                (worst["hour"], worst["punctual_pct"]), xytext=(0, -16),
                textcoords="offset points", ha="center", fontsize=8, color=INK)
    ax.set_xticks(range(0, 24, 2))
    ax.margins(y=0.18)
    style(ax, "Punctuality by hour of the day", "Hour (planned time)", "% arrivals < 3 min late")
    save(fig, "fig3_punctuality_by_hour")


def chart_by_weekday(w):
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.grid(True, axis="y", color=GRID, linewidth=0.6)
    x = range(len(w))
    ax.plot(x, w["punctual_pct"], color=GRID, linewidth=1, zorder=1)
    ax.scatter(x, w["punctual_pct"], s=60, color=BLUE, edgecolor="white", linewidth=1, zorder=2)
    for i, v in zip(x, w["punctual_pct"]):
        ax.annotate(f"{v:.1f}%", (i, v), xytext=(0, 8), textcoords="offset points",
                    ha="center", fontsize=8, color=INK2)
    ax.set_xticks(list(x))
    ax.set_xticklabels(w["weekday_name"].str[:3])
    ax.margins(y=0.25)
    style(ax, "Punctuality by day of the week", None, "% arrivals < 3 min late")
    save(fig, "fig4_punctuality_by_weekday")


def chart_by_canton(c):
    c = c[c["canton"].str.len() == 2].sort_values("punctual_pct")
    fig, ax = plt.subplots(figsize=(8, 6.5))
    ax.grid(True, axis="x", color=GRID, linewidth=0.6)
    y = range(len(c))
    ax.hlines(y, c["punctual_pct"].min() - 1, c["punctual_pct"], color=GRID, linewidth=1)
    ax.scatter(c["punctual_pct"], y, s=45, color=BLUE, edgecolor="white", linewidth=1, zorder=2)
    for i, v in zip(y, c["punctual_pct"]):
        ax.annotate(f"{v:.1f}", (v, i), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=7.5, color=INK2)
    ax.set_yticks(list(y))
    ax.set_yticklabels(c["canton"])
    ax.margins(x=0.08)
    style(ax, "Punctuality by canton", "% arrivals < 3 min late")
    save(fig, "fig5_punctuality_by_canton")


def to_csv(pdf, name, show=10):
    RESULTS.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(RESULTS / f"{name}.csv", index=False)
    print(f"\n=== {name} ===")
    print(pdf.head(show).to_string(index=False))


def main():
    spark = get_spark("03_analyze")
    spark.read.parquet(str(PARQUET_DIR / "stops_clean")).createOrReplaceTempView("stops")
    days = spark.sql("SELECT COUNT(DISTINCT operating_day) AS d FROM stops").first()["d"]
    params = dict(days=days, min_trains=MIN_TRAINS_PER_DAY, min_measured=MIN_MEASURED_STATION)

    to_csv(spark.sql(Q_OVERALL).toPandas(), "00_overall")

    st = spark.sql(Q_STATIONS.format(**params)).toPandas()
    print(f"\n{len(st)} stations with >= {MIN_TRAINS_PER_DAY} trains/day and "
          f">= {MIN_MEASURED_STATION} measured arrivals")
    to_csv(st.sort_values("trains_per_day", ascending=False), "01_stations_most_important", TOP_N)
    to_csv(st.sort_values("total_delay_min_per_day", ascending=False), "02_stations_most_total_delay", TOP_N)
    to_csv(st.sort_values("avg_delay_min", ascending=False), "03_stations_highest_avg_delay", TOP_N)
    to_csv(st.sort_values("punctual_pct"), "04_stations_lowest_punctuality", TOP_N)

    lines = spark.sql(Q_LINES.format(days=days, min_trips=MIN_TRIPS_PER_DAY_LINE,
                                     min_measured=MIN_MEASURED_LINE)).toPandas()
    to_csv(lines.sort_values("punctual_pct"), "05_lines_lowest_punctuality", TOP_N)

    hour = spark.sql(Q_HOUR.format(days=days)).toPandas()
    to_csv(hour, "06_by_hour", 24)
    weekday = spark.sql(Q_WEEKDAY).toPandas()
    to_csv(weekday, "07_by_weekday", 7)
    canton = spark.sql(Q_CANTON).toPandas()
    to_csv(canton, "08_by_canton", 30)

    # Is a busy station more delayed? (correlation)
    corr = st[["trains_per_day", "lines", "avg_delay_min", "punctual_pct"]].corr(method="spearman")
    to_csv(corr.reset_index().rename(columns={"index": "measure"}).round(3), "09_correlations")

    spark.sql(Q_BASELINE).write.mode("overwrite").parquet(str(PARQUET_DIR / "baseline_station_hour"))
    print(f"\n  baseline for Part 2: {PARQUET_DIR / 'baseline_station_hour'}")

    print("\nCharts:")
    chart_importance_vs_delay(st)
    chart_top_total_delay(st)
    chart_by_hour(hour)
    chart_by_weekday(weekday)
    chart_by_canton(canton)
    spark.stop()


if __name__ == "__main__":
    main()
