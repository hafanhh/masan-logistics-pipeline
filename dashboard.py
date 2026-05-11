# dashboard.py
# Streamlit dashboard — đọc Gold Layer CSV và hiển thị KPI
# Chạy local: streamlit run dashboard.py
# Deploy: streamlit.io/cloud (miễn phí)

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

# -------------------------------------------------------
# Config trang
# -------------------------------------------------------
st.set_page_config(
    page_title="Masan Logistics Dashboard",
    page_icon="📦", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS tùy chỉnh nhẹ
st.markdown("""
<style>
    .metric-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
    }
    .stMetric { 
        background: #161b22; 
        border-radius: 8px; 
        padding: 12px;
        color: white !important;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }
    .stMetric label, .stMetric .metric-label {
        color: white !important;
    }
    .stMetric .metric-value, .stMetric div[data-testid="stMetricValue"] {
        color: white !important;
    }
    .stMetric .metric-delta, .stMetric div[data-testid="stMetricDelta"] {
        color: #58a6ff !important;
    }
    .stMetric * {
        color: white !important;
    }
    /* Ensure equal column heights */
    [data-testid="column"] {
        display: flex;
        flex-direction: column;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------
# Load data
# -------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOLD_DIR = os.path.join(BASE_DIR, "gold_layer")

@st.cache_data
def load_data():
    summary  = pd.read_csv(os.path.join(GOLD_DIR, "executive_summary.csv"))
    state    = pd.read_csv(os.path.join(GOLD_DIR, "kpi_delivery_by_state.csv"))
    trend    = pd.read_csv(os.path.join(GOLD_DIR, "kpi_monthly_trend.csv"))
    late     = pd.read_csv(os.path.join(GOLD_DIR, "kpi_late_orders.csv"))
    return summary, state, trend, late

summary_df, state_df, trend_df, late_df = load_data()

# Helper: lấy value từ summary
def get_metric(metric_name):
    row = summary_df[summary_df["metric"] == metric_name]
    return row["value"].values[0] if len(row) > 0 else "N/A"


# -------------------------------------------------------
# Sidebar
# -------------------------------------------------------
with st.sidebar:
    st.image("https://via.placeholder.com/200x60/0d1117/58a6ff?text=MASAN+DE",
             use_container_width=True)
    st.markdown("### Masan Logistics Pipeline")
    st.markdown("""
    **Data Engineering Portfolio**  
    Simulating Palantir Foundry  
    
    **Stack:**
    - PySpark 4.1 (1M+ rows)
    - Medallion Architecture
    
    **Records:** 99,515 orders  
    """)

    st.divider()

    # Filter state
    all_states = sorted(state_df["customer_state"].unique().tolist())
    selected_states = st.multiselect(
        "Filter by State:",
        options=all_states,
        default=all_states[:5]
    )

    st.divider()
    st.markdown("[![GitHub](https://img.shields.io/badge/GitHub-View_Code-blue?logo=github)]"
                "(https://github.com/hafanhh/masan-logistics-pipeline)")


# -------------------------------------------------------
# Header
# -------------------------------------------------------
st.title("Masan Logistics — Data Pipeline Dashboard")
st.caption("Data Pipeline | PySpark 4.1")
st.divider()


# -------------------------------------------------------
# Row 1: Executive KPIs
# -------------------------------------------------------
st.subheader("Executive Summary")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Orders",
              get_metric("Tổng đơn hàng"))
with col2:
    st.metric("Success Rate",
              get_metric("Tỷ lệ giao thành công"),
              delta="97%+ = good")
with col3:
    st.metric("Avg Delivery Days",
              get_metric("Trung bình ngày giao"),
              delta_color="inverse")
with col4:
    st.metric("Late Rate",
              get_metric("Tỷ lệ giao trễ"),
              delta="-target <5%",
              delta_color="inverse")
with col5:
    st.metric("Fast Delivery ≤7 days",
              get_metric("Đơn giao nhanh (≤7 ngày)"))

st.divider()


# -------------------------------------------------------
# Row 2: Trend chart + Donut
# -------------------------------------------------------
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Order Trend & Late Rate by Month")

    df_trend = trend_df[
        trend_df["order_year"].isin([2017, 2018]) &
        (trend_df["total_orders"] > 100)
    ].copy()
    df_trend["month_label"] = (
        df_trend["order_year"].astype(str) + "-" +
        df_trend["order_month"].astype(str).str.zfill(2)
    )

    fig, ax1 = plt.subplots(figsize=(12, 4.5))
    fig.patch.set_facecolor("#0d1117")
    ax1.set_facecolor("#161b22")

    colors_bar = [
        "#d29922" if row["total_orders"] == df_trend["total_orders"].max()
        else "#58a6ff"
        for _, row in df_trend.iterrows()
    ]
    ax1.bar(df_trend["month_label"], df_trend["total_orders"],
            color=colors_bar, alpha=0.8, width=0.6)
    ax1.set_ylabel("Number of Orders", color="#58a6ff", fontsize=10)
    ax1.tick_params(axis="x", rotation=45, colors="#8b949e")
    ax1.tick_params(axis="y", colors="#58a6ff")
    ax1.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{int(x):,}")
    )
    ax1.set_facecolor("#161b22")
    for spine in ax1.spines.values():
        spine.set_edgecolor("#30363d")

    ax2 = ax1.twinx()
    ax2.plot(df_trend["month_label"], df_trend["late_rate_pct"],
             color="#f85149", linewidth=2.5, marker="o", markersize=5)
    ax2.set_ylabel("Late rate (%)", color="#f85149", fontsize=10)
    ax2.tick_params(axis="y", colors="#f85149")
    ax2.set_ylim(0, df_trend["late_rate_pct"].max() * 1.5)

    fig.tight_layout()
    st.pyplot(fig)
    plt.close()

with col_right:
    st.subheader("Delivery Speed")

    speed_data = {
        "FAST ≤7 days":    30725,
        "NORMAL 8-14":     38008,
        "SLOW 15-30":      23517,
        "VERY SLOW >30":    4298,
        "UNKNOWN":          2967,
    }
    colors_pie = ["#3fb950", "#58a6ff", "#d29922", "#f85149", "#8b949e"]

    fig2, ax_pie = plt.subplots(figsize=(5, 5))
    fig2.patch.set_facecolor("#0d1117")
    ax_pie.set_facecolor("#0d1117")

    wedges, _, autotexts = ax_pie.pie(
        list(speed_data.values()),
        colors=colors_pie,
        autopct="%1.1f%%",
        pctdistance=0.78,
        startangle=90,
        wedgeprops={"width": 0.55, "edgecolor": "#0d1117", "linewidth": 2},
    )
    for at in autotexts:
        at.set_color("#e6edf3")
        at.set_fontsize(8)

    ax_pie.legend(list(speed_data.keys()),
               loc="lower center", bbox_to_anchor=(0.5, -0.18),
               fontsize=8, facecolor="#161b22",
               edgecolor="#30363d", labelcolor="#c9d1d9", ncol=2)

    total = sum(speed_data.values())
    ax_pie.text(0, 0, f"{total:,}\norders", ha="center", va="center",
             fontsize=11, fontweight="bold", color="#e6edf3")

st.divider()


# -------------------------------------------------------
# Row 3: State performance table + Late rate chart
# -------------------------------------------------------
col3a, col3b = st.columns([1, 1])

with col3a:
    st.subheader("Performance by State")

    df_state_show = state_df.copy()
    if selected_states:
        df_state_show = df_state_show[
            df_state_show["customer_state"].isin(selected_states)
        ]

    df_state_show = df_state_show.sort_values(
        "avg_delivery_days", ascending=False
    )[[
        "customer_state", "total_orders",
        "avg_delivery_days", "late_rate_pct", "median_delivery_days"
    ]].rename(columns={
        "customer_state":       "State",
        "total_orders":         "Orders",
        "avg_delivery_days":    "Avg days",
        "late_rate_pct":        "Late %",
        "median_delivery_days": "Median days"
    })

    # Color late % by severity level
    def color_late(val):
        if val > 15:
            return "background-color: #3d1a1a; color: #f85149"
        elif val > 8:
            return "background-color: #2d2200; color: #d29922"
        return "background-color: #0d2318; color: #3fb950"

    st.dataframe(
        df_state_show.style.map(color_late, subset=["Late %"]),
        use_container_width=True,
        height=400
    )

with col3b:
    st.subheader("Late Order Classification")

    # Bar chart delay severity
    fig3, ax3 = plt.subplots(figsize=(6, 4.5))
    fig3.patch.set_facecolor("#0d1117")
    ax3.set_facecolor("#161b22")

    severity_colors = {
        "MINOR":    "#3fb950",
        "MODERATE": "#d29922",
        "SEVERE":   "#f0883e",
        "CRITICAL": "#f85149",
    }

    late_sorted = late_df.sort_values("avg_delay_days")
    bar_colors  = [severity_colors.get(s, "#8b949e")
                   for s in late_sorted["delay_severity"]]

    bars = ax3.barh(late_sorted["delay_severity"],
                    late_sorted["num_orders"],
                    color=bar_colors, alpha=0.85, height=0.5)

    for bar, val in zip(bars, late_sorted["num_orders"]):
        ax3.text(bar.get_width() + 15, bar.get_y() + bar.get_height()/2,
                 f"{val:,}", va="center", color="#c9d1d9", fontsize=10)

    ax3.set_xlabel("Number of Orders", color="#8b949e")
    ax3.tick_params(colors="#c9d1d9")
    ax3.set_xlim(0, late_sorted["num_orders"].max() * 1.2)
    for spine in ax3.spines.values():
        spine.set_edgecolor("#30363d")

    # Add avg delay label
    for i, row in late_sorted.iterrows():
        ax3.text(
            10, late_sorted.index.get_loc(i),
            f"avg {row['avg_delay_days']} days",
            va="center", color="#8b949e", fontsize=8
        )

    fig3.tight_layout()
    st.pyplot(fig3)
    plt.close()

    st.info(f"**Total Late Orders: {late_df['num_orders'].sum():,}**  \n"
            f"Most Critical (CRITICAL): **{late_df[late_df['delay_severity']=='CRITICAL']['num_orders'].values[0]:,} orders**  \n"
            f"Max Delay: **{late_df[late_df['delay_severity']=='CRITICAL']['max_delay_days'].values[0]} days**")


# -------------------------------------------------------
# Footer
# -------------------------------------------------------
st.divider()
st.caption(
    "Built by **hafanhh** | "
    "Stack: PySpark 4.1 · Pandas · Matplotlib · Streamlit | "
    "Architecture: Palantir Foundry Medallion"
)