# visualize.py
# Vẽ chart từ Gold Layer CSV → lưu PNG vào charts/
# Commit PNG lên GitHub → hiển thị trong README

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

# Setup
GOLD_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gold_layer")
CHART_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
os.makedirs(CHART_DIR, exist_ok=True)

# Style chung
plt.rcParams.update({
    "figure.facecolor": "#0d1117",
    "axes.facecolor":   "#161b22",
    "axes.edgecolor":   "#30363d",
    "axes.labelcolor":  "#c9d1d9",
    "text.color":       "#c9d1d9",
    "xtick.color":      "#8b949e",
    "ytick.color":      "#8b949e",
    "grid.color":       "#21262d",
    "grid.linestyle":   "--",
    "grid.alpha":       0.6,
    "font.family":      "DejaVu Sans",
})

ACCENT   = "#58a6ff"   # xanh GitHub
DANGER   = "#f85149"   # đỏ
SUCCESS  = "#3fb950"   # xanh lá
WARNING  = "#d29922"   # vàng
PURPLE   = "#bc8cff"


# -------------------------------------------------------
# Chart 1: Trend đơn hàng + late rate theo tháng
# -------------------------------------------------------
def chart_monthly_trend():
    df = pd.read_csv(os.path.join(GOLD_DIR, "kpi_monthly_trend.csv"))

    # Chỉ lấy 2017-2018 có đủ data
    df = df[df["order_year"].isin([2017, 2018])].copy()
    df = df[df["total_orders"] > 100]  # bỏ tháng cuối dataset (chỉ vài đơn)
    df["month_label"] = df["order_year"].astype(str) + "-" + \
                        df["order_month"].astype(str).str.zfill(2)

    fig, ax1 = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor("#0d1117")

    # Bar: tổng đơn hàng
    bars = ax1.bar(
        df["month_label"], df["total_orders"],
        color=ACCENT, alpha=0.75, width=0.6, label="Tổng đơn hàng"
    )

    # Highlight tháng cao điểm
    peak_idx = df["total_orders"].idxmax()
    bars[df.index.get_loc(peak_idx)].set_color(WARNING)
    bars[df.index.get_loc(peak_idx)].set_alpha(1.0)

    ax1.set_ylabel("Số đơn hàng", color=ACCENT, fontsize=11)
    ax1.tick_params(axis="x", rotation=45)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

    # Line: late rate
    ax2 = ax1.twinx()
    ax2.plot(
        df["month_label"], df["late_rate_pct"],
        color=DANGER, linewidth=2.5, marker="o",
        markersize=5, label="Late rate (%)", zorder=5
    )
    ax2.set_ylabel("Tỷ lệ giao trễ (%)", color=DANGER, fontsize=11)
    ax2.tick_params(axis="y", colors=DANGER)
    ax2.set_ylim(0, df["late_rate_pct"].max() * 1.4)

    # Annotation tháng cao điểm
    peak_row = df.loc[peak_idx]
    ax1.annotate(
        f"Black Friday\n{int(peak_row['total_orders']):,} đơn",
        xy=(df.index.get_loc(peak_idx), peak_row["total_orders"]),
        xytext=(df.index.get_loc(peak_idx) - 2, peak_row["total_orders"] * 1.05),
        color=WARNING, fontsize=9,
        arrowprops=dict(arrowstyle="->", color=WARNING, lw=1.5)
    )

    # Legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc="upper left", facecolor="#161b22",
               edgecolor="#30363d", labelcolor="#c9d1d9")

    ax1.set_title(
        "Trend đơn hàng & Tỷ lệ giao trễ theo tháng (2017–2018)",
        fontsize=14, pad=16, color="#e6edf3", fontweight="bold"
    )
    ax1.grid(axis="y", alpha=0.4)
    fig.tight_layout()

    out = os.path.join(CHART_DIR, "chart_monthly_trend.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="#0d1117")
    plt.close()
    print(f"Đã lưu: {out}")


# -------------------------------------------------------
# Chart 2: Late rate theo State — horizontal bar
# -------------------------------------------------------
def chart_late_rate_by_state():
    df = pd.read_csv(os.path.join(GOLD_DIR, "kpi_delivery_by_state.csv"))
    df = df[df["total_orders"] >= 100]  # chỉ state có đủ data
    df = df.sort_values("late_rate_pct", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 9))
    fig.patch.set_facecolor("#0d1117")

    # Màu theo late rate
    colors = [
        DANGER  if x > 15 else
        WARNING if x > 8  else
        SUCCESS
        for x in df["late_rate_pct"]
    ]

    bars = ax.barh(df["customer_state"], df["late_rate_pct"],
                   color=colors, alpha=0.85, height=0.6)

    # Value labels
    for bar, val in zip(bars, df["late_rate_pct"]):
        ax.text(
            bar.get_width() + 0.2, bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}%", va="center", ha="left",
            color="#c9d1d9", fontsize=8.5
        )

    # Đường trung bình
    avg = df["late_rate_pct"].mean()
    ax.axvline(avg, color=ACCENT, linestyle="--", linewidth=1.5, alpha=0.8)
    ax.text(avg + 0.3, len(df) - 0.5, f"Avg: {avg:.1f}%",
            color=ACCENT, fontsize=9)

    ax.set_xlabel("Tỷ lệ giao trễ (%)", fontsize=11)
    ax.set_title(
        "Tỷ lệ giao trễ theo State\n(xanh <8% | vàng 8-15% | đỏ >15%)",
        fontsize=13, pad=14, color="#e6edf3", fontweight="bold"
    )
    ax.grid(axis="x", alpha=0.4)
    ax.set_xlim(0, df["late_rate_pct"].max() * 1.2)
    fig.tight_layout()

    out = os.path.join(CHART_DIR, "chart_late_rate_by_state.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="#0d1117")
    plt.close()
    print(f"Đã lưu: {out}")


# -------------------------------------------------------
# Chart 3: Delivery speed distribution — donut chart
# -------------------------------------------------------
def chart_delivery_speed():
    data = {
        "FAST\n(≤7 ngày)":      30725,
        "NORMAL\n(8-14 ngày)":  38008,
        "SLOW\n(15-30 ngày)":   23517,
        "VERY SLOW\n(>30 ngày)": 4298,
        "UNKNOWN":               2967,
    }

    colors  = [SUCCESS, ACCENT, WARNING, DANGER, "#8b949e"]
    labels  = list(data.keys())
    values  = list(data.values())
    total   = sum(values)

    fig, ax = plt.subplots(figsize=(9, 7))
    fig.patch.set_facecolor("#0d1117")

    wedges, texts, autotexts = ax.pie(
        values,
        labels=None,
        colors=colors,
        autopct=lambda p: f"{p:.1f}%\n({int(p*total/100):,})",
        pctdistance=0.78,
        startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "#0d1117", "linewidth": 2},
    )

    for at in autotexts:
        at.set_fontsize(8.5)
        at.set_color("#e6edf3")

    # Legend
    ax.legend(
        wedges, labels,
        loc="lower center", bbox_to_anchor=(0.5, -0.08),
        ncol=3, facecolor="#161b22",
        edgecolor="#30363d", labelcolor="#c9d1d9", fontsize=9
    )

    # Center text
    ax.text(0, 0, f"{total:,}\nđơn hàng",
            ha="center", va="center",
            fontsize=13, fontweight="bold", color="#e6edf3")

    ax.set_title(
        "Phân bố tốc độ giao hàng",
        fontsize=14, pad=20, color="#e6edf3", fontweight="bold"
    )

    out = os.path.join(CHART_DIR, "chart_delivery_speed.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="#0d1117")
    plt.close()
    print(f"Đã lưu: {out}")


# -------------------------------------------------------
# Main
# -------------------------------------------------------
if __name__ == "__main__":
    print("=" * 45)
    print("VISUALIZE — Masan Logistics KPI Charts")
    print("=" * 45)

    chart_monthly_trend()
    chart_late_rate_by_state()
    chart_delivery_speed()

    print("\nTất cả charts đã lưu vào charts/")