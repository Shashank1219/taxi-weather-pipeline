import matplotlib.pyplot as plt
import pandas as pd
from databricks import sql
import os

from dotenv import load_dotenv
load_dotenv()

conn = sql.connect(
    server_hostname=os.environ.get("DBT_DATABRICKS_HOST"),
    http_path=os.environ.get("DBT_DATABRICKS_HTTP_PATH"),
    access_token=os.environ.get("DBT_DATABRICKS_TOKEN"),
)

def query(sql_text):
    return pd.read_sql(sql_text, conn)

os.makedirs("outputs", exist_ok=True)

# Chart 1: hour x weekday heatmap
df1 = query("select * from taxi_case.gold.trips_by_hour_weekday")
pivot = df1.pivot(index="hour_of_day", columns="weekday", values="trip_count")
weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
pivot = pivot[weekday_order]
plt.figure(figsize=(10, 6))
plt.imshow(pivot, aspect="auto", cmap="YlOrRd")
plt.colorbar(label="Trip count")
plt.xticks(range(7), weekday_order, rotation=45)
plt.yticks(range(24), pivot.index)
plt.xlabel("Weekday"); plt.ylabel("Hour of day"); plt.title("Ridership by hour and weekday")
plt.tight_layout()
plt.savefig("outputs/hour_weekday_heatmap.png", dpi=120)
plt.close()

# Chart 2: daily trips vs precipitation
df2 = query("select * from taxi_case.gold.daily_weather_ridership order by trip_date")
fig, ax1 = plt.subplots(figsize=(12, 5))
ax1.plot(df2["trip_date"], df2["trip_count"], color="tab:blue", label="Trips")
ax1.set_ylabel("Trip count", color="tab:blue")
ax2 = ax1.twinx()
ax2.bar(df2["trip_date"], df2["total_precip_mm"], color="tab:gray", alpha=0.3, label="Precipitation (mm)")
ax2.set_ylabel("Precipitation (mm)", color="tab:gray")
plt.title("Daily ridership vs precipitation, Jan–May 2026")
fig.tight_layout()
plt.savefig("outputs/daily_trips_vs_precipitation.png", dpi=120)
plt.close()

# Chart 3: avg hourly ridership, dry vs precipitation
df3 = query("select * from taxi_case.gold.weather_impact")
grouped = df3.groupby("is_precipitation")["avg_trips_per_hour"].mean()
plt.figure(figsize=(6, 5))
plt.bar(["Dry", "Precipitation"], [grouped.get(False, 0), grouped.get(True, 0)], color=["tab:orange", "tab:blue"])
plt.ylabel("Avg trips per hour")
plt.title("Average hourly ridership: dry vs precipitation")
plt.tight_layout()
plt.savefig("outputs/dry_vs_precip_ridership.png", dpi=120)
plt.close()

print("Charts saved to outputs/")
print(grouped)

# Chart 4: demand by borough x hour (fleet placement insight)
df4 = query("select * from taxi_case.gold.demand_by_borough")
pivot4 = df4.pivot(index="hour_of_day", columns="pickup_borough", values="trip_count").fillna(0)
plt.figure(figsize=(10, 6))
for borough in pivot4.columns:
    plt.plot(pivot4.index, pivot4[borough], label=borough, marker="o", markersize=3)
plt.xlabel("Hour of day"); plt.ylabel("Trip count"); plt.title("Pickup demand by borough and hour")
plt.legend()
plt.tight_layout()
plt.savefig("outputs/demand_by_borough.png", dpi=120)
plt.close()

# Chart 5: DQ quarantine rate trend
total_rows_by_month = {
    "2026-01": 3_724_889, "2026-02": 3_399_866, "2026-03": 3_952_451,
    "2026-04": 3_831_240, "2026-05": 4_090_836,
}
df5 = query("select * from taxi_case.gold.dq_quarantine_trend")
monthly_total_q = df5.groupby("trip_month")["quarantined_count"].sum()
rate = {m: monthly_total_q.get(m, 0) / total_rows_by_month[m] * 100 for m in total_rows_by_month}

fig, ax1 = plt.subplots(figsize=(11, 5))
pivot5 = df5[df5["trip_month"] != "unknown_month"].pivot(
    index="trip_month", columns="rejection_reason", values="quarantined_count"
).fillna(0)
pivot5.plot(kind="bar", stacked=True, ax=ax1, colormap="tab20")
ax1.set_ylabel("Quarantined rows")
ax2 = ax1.twinx()
ax2.plot(range(len(rate)), list(rate.values()), color="black", marker="o", label="Quarantine rate %")
ax2.set_ylabel("Quarantine rate (%)")
plt.title("DQ quarantine trend by month")
fig.tight_layout()
plt.savefig("outputs/dq_quarantine_trend.png", dpi=120)
plt.close()
print("Monthly quarantine rate:", {k: round(v, 2) for k, v in rate.items()})

# Chart 6: ridership by temperature bucket, weekday vs weekend
df6 = query("select * from taxi_case.gold.weather_impact")
pivot6 = df6.groupby(["temp_bucket", "day_type"])["avg_trips_per_hour"].mean().unstack()
bucket_order = ["freezing", "cold", "mild", "warm"]
pivot6 = pivot6.reindex(bucket_order)
plt.figure(figsize=(8, 5))
pivot6.plot(kind="bar", ax=plt.gca())
plt.ylabel("Avg trips per hour"); plt.title("Ridership by temperature bucket: weekday vs weekend")
plt.tight_layout()
plt.savefig("outputs/ridership_by_temp_bucket.png", dpi=120)
plt.close()

# Fraud-signal stat which directly answers the brief's fraud-prevention bullet
outlier_df = query("""
    select count(*) as outlier_trips, count(*) * 100.0 / (select count(*) from taxi_case.silver.trips_input) as pct
    from taxi_case.silver.trips_input
    where trip_distance > 0 and total_amount / trip_distance > 100
""")

print("Potential fraud/anomaly signal — trips with fare > $100/mile:")
print(outlier_df)
print("Potential fraud/anomaly signal. Trips with fare > $100/mile:")
print(outlier_df)

print("Charts saved to outputs/")

# Diagnostic: what's driving May's quarantine spike?
# may_breakdown = query("""
#     select rejection_reason, quarantined_count
#     from taxi_case.gold.dq_quarantine_trend
#     where trip_month = '2026-05'
#     order by quarantined_count desc
# """)
# print("May 2026 quarantine breakdown:")
# print(may_breakdown)

# Concrete stat for the fleet-placement / borough finding
# borough_summary = query("""
#     select pickup_borough, sum(trip_count) as total_trips, round(sum(total_revenue), 0) as total_revenue
#     from taxi_case.gold.demand_by_borough
#     group by pickup_borough
#     order by total_trips desc
# """)
# print("Total trips and revenue by borough:")
# print(borough_summary)

# Chart 7: payment mix, reveals the TLC cash-tip data-capture quirk
df7 = query("select * from taxi_case.gold.payment_mix")
print("Payment mix:")
print(df7)
plt.figure(figsize=(8, 5))
plt.bar(df7["payment_type_label"], df7["avg_tip_pct"], color="tab:green")
plt.ylabel("Avg tip % of fare"); plt.title("Recorded tip % by payment type")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig("outputs/payment_mix_tip_pct.png", dpi=120)
plt.close()

# Chart 8: revenue efficiency by hour, fleet placement via efficiency, not just volume
df8 = query("select * from taxi_case.gold.revenue_efficiency_by_hour")
plt.figure(figsize=(10, 5))
plt.bar(df8["hour_of_day"], df8["avg_revenue_per_minute"], color="tab:purple")
plt.xlabel("Hour of day"); plt.ylabel("Avg revenue per minute ($)")
plt.title("Revenue efficiency by hour of day")
plt.tight_layout()
plt.savefig("outputs/revenue_efficiency_by_hour.png", dpi=120)
plt.close()

# Chart 9: weather's effect on trip speed, operational/supply-side, not demand-side
df9 = query("select * from taxi_case.gold.weather_speed_impact")
bucket_order = ["freezing", "cold", "mild", "warm"]
fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
for ax, day_type in zip(axes, ["weekday", "weekend"]):
    sub = df9[df9["day_type"] == day_type]
    pivot9 = sub.pivot(index="temp_bucket", columns="is_precipitation", values="avg_speed_mph").reindex(bucket_order)
    pivot9.plot(kind="bar", ax=ax)
    ax.set_title(day_type.capitalize())
    ax.set_xlabel("Temp bucket"); ax.set_ylabel("Avg speed (mph)")
plt.suptitle("Trip speed by weather condition: weekday vs weekend")
plt.tight_layout()
plt.savefig("outputs/weather_speed_impact.png", dpi=120)
plt.close()

print("Charts saved to outputs/")

null_check = query("""
    select payment_type, count(*) as cnt
    from taxi_case.silver.trips_input
    group by payment_type
    order by cnt desc
""")
print(null_check)