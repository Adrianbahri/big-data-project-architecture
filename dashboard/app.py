import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

import json
import streamlit as st
import pandas as pd
import altair as alt
import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# ─── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Tip — Dashboard",
    page_icon="🚖",
    layout="wide"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');

/* Force dark theme */
html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {
    background-color: #171717 !important;
    color: #ededed !important;
    font-family: 'Inter', 'Helvetica Neue', Helvetica, Arial, sans-serif;
}
[data-testid="stSidebar"], [data-testid="stHeader"] {
    background-color: #171717 !important;
}
[data-testid="stAppViewBlockContainer"] {
    background-color: #171717 !important;
}

/* Metric cards — flat, 1px hairline, 0px radius (siku-siku) */
.card {
    background: #1c1c1c;
    border: 1px solid #2e2e2e;
    border-radius: 0px !important;
    padding: 24px 28px;
    text-align: center;
    box-shadow: none;
}
.card-value {
    font-family: 'Inter', sans-serif;
    font-size: 1.8rem;
    font-weight: 500;
    margin: 0;
    line-height: 1.3;
    color: #ffffff;
    letter-spacing: -0.42px;
}
.card-label {
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    font-weight: 400;
    color: #9a9a9a;
    margin: 6px 0 0 0;
    line-height: 1.45;
}

/* Color utilities */
.c-green  { color: #3ecf8e !important; }
.c-yellow { color: #ffdb13 !important; }
.c-red    { color: #ff4a4a !important; }
.c-ink    { color: #ffffff !important; }
.c-mute   { color: #9a9a9a !important; }

/* Section heading — heading-md */
.section-heading {
    font-family: 'Inter', sans-serif;
    font-size: 18px;
    font-weight: 500;
    color: #ffffff;
    margin: 28px 0 16px 0;
    line-height: 1.4;
}

/* Kappa badge — pill-tag style, 0px radius */
.kappa-badge {
    display: inline-block;
    padding: 6px 16px;
    border-radius: 0px !important;
    font-weight: 500;
    font-size: 14px;
    font-family: 'Inter', sans-serif;
}
.badge-green  { background: #0f3724; color: #3ecf8e; border: 1px solid #1c553a; }
.badge-yellow { background: #3e3200; color: #ffdb13; border: 1px solid #5c4b00; }
.badge-red    { background: #3d0a00; color: #ff4a4a; border: 1px solid #631200; }

/* Divider — hairline */
.divider {
    border: none;
    border-top: 1px solid #2e2e2e;
    margin: 28px 0;
}

/* Tables — clean, hairline borders */
.styled-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'Inter', sans-serif;
    font-size: 14px;
}
.styled-table th {
    background: #1c1c1c;
    padding: 10px 14px;
    text-align: left;
    font-weight: 500;
    color: #9a9a9a;
    font-size: 13px;
    border-bottom: 1px solid #2e2e2e;
}
.styled-table td {
    padding: 10px 14px;
    border-bottom: 1px solid #2e2e2e;
    color: #ededed;
}
.styled-table tr:hover td {
    background: #202020;
}
.styled-table .highlight {
    font-weight: 500;
}

/* Override Streamlit tab styling */
[data-testid="stTab"] {
    font-family: 'Inter', sans-serif !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    color: #9a9a9a !important;
}
[data-testid="stTab"][aria-selected="true"] {
    color: #ffffff !important;
}

/* Override text colors */
.stMarkdown p:not(.card-value):not(.c-green):not(.c-yellow):not(.c-red):not(.c-ink):not(.c-mute), 
.stMarkdown li, .stCaption, label {
    color: #ededed !important;
}
.stCaption p {
    color: #9a9a9a !important;
}

/* Progress bar green */
[data-testid="stProgress"] > div > div {
    background-color: #3ecf8e !important;
}

/* Enforce sharp corners globally for widgets */
div[data-baseweb="select"], div[data-baseweb="input"], input, button, select, 
[data-testid="stBaseButton-primary"], [data-testid="stBaseButton-secondary"],
[data-testid="stExpander"] {
    border-radius: 0px !important;
}

/* Button — primary green, 0px radius */
[data-testid="stBaseButton-primary"] {
    background-color: #3ecf8e !important;
    color: #171717 !important;
    border-radius: 0px !important;
    border: none !important;
    font-weight: 500 !important;
}
[data-testid="stBaseButton-primary"]:hover {
    background-color: #24b47e !important;
}
</style>

""", unsafe_allow_html=True)

# ─── Spark & Models ──────────────────────────────────────────
@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("TaxiDashboard") \
        .master("local[*]") \
        .getOrCreate()

@st.cache_resource
def load_models():
    reg_model   = RandomForestRegressionModel.load("../models/taxi_reg_model")
    class_model = RandomForestClassificationModel.load("../models/taxi_class_model")
    return reg_model, class_model

@st.cache_data(ttl=60)
def load_metrics():
    path = "../models/evaluation_metrics.json"
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None

@st.cache_data(ttl=10)
def get_prediction_samples():
    try:
        client = clickhouse_connect.get_client(
            host="localhost", port=8123,
            username="mahasiswa", password="bigdata123",
            database="taxi_db"
        )
        df_ch = client.query_df("""
            SELECT tripDistance, passenger_count AS passengerCount, 
                   CAST(puLocationId AS Float64) AS pickup_zone, 
                   CAST(doLocationId AS Float64) AS dropoff_zone, 
                   toHour(lpepPickupDatetime) AS pickup_hour,
                   toDayOfWeek(lpepPickupDatetime) AS pickup_day,
                   tip_amount
            FROM green_taxi 
            ORDER BY lpepPickupDatetime DESC 
            LIMIT 100
        """)
        if df_ch.empty:
            return None, None
        
        from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
        schema = StructType([
            StructField("tripDistance",   DoubleType()),
            StructField("passengerCount", IntegerType()),
            StructField("pickup_zone",    DoubleType()),
            StructField("dropoff_zone",   DoubleType()),
            StructField("pickup_hour",    IntegerType()),
            StructField("pickup_day",     IntegerType()),
            StructField("tip_amount",     DoubleType())
        ])
        
        data = [
            (
                float(row["tripDistance"]),
                int(row["passengerCount"]),
                float(row["pickup_zone"]),
                float(row["dropoff_zone"]),
                int(row["pickup_hour"]),
                int(row["pickup_day"]),
                float(row["tip_amount"])
            )
            for _, row in df_ch.iterrows()
        ]
        spark_df = spark.createDataFrame(data, schema)
        
        FEATURE_COLS = ["tripDistance", "passengerCount", "pickup_zone", "dropoff_zone", "pickup_hour", "pickup_day"]
        assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
        spark_df_features = assembler.transform(spark_df)
        
        pred_reg = model_reg.transform(spark_df_features).select("tip_amount", "prediction").collect()
        pred_clf = model_class.transform(spark_df_features).select("tip_amount", "prediction").collect()
        
        reg_plot_df = pd.DataFrame([
            {"Aktual": r[0], "Prediksi": r[1]} for r in pred_reg
        ])
        
        def get_cat(tip):
            if tip <= 2.0: return "Rendah ($0–$2)"
            elif tip <= 5.0: return "Menengah ($2–$5)"
            else: return "Tinggi (>$5)"
            
        label_map = {0.0: "Rendah ($0–$2)", 1.0: "Menengah ($2–$5)", 2.0: "Tinggi (>$5)"}
        
        clf_plot_df = pd.DataFrame([
            {
                "Aktual": get_cat(r[0]),
                "Prediksi": label_map.get(r[1], "N/A")
            } for r in pred_clf
        ])
        
        return reg_plot_df, clf_plot_df
    except Exception as e:
        return None, None

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")
model_reg, model_class = load_models()

# ─── Header ───────────────────────────────────────────────────
st.markdown("## NYC Green Taxi Tip Prediction")
st.caption("Kappa Architecture  ·  Spark MLlib  ·  Random Forest")
st.markdown('<hr class="divider">', unsafe_allow_html=True)

# ─── Tabs ─────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["Prediksi", "Evaluasi Model", "Analisis ClickHouse", "Panduan"])

# ══════════════════════════════════════════════════════════════
# TAB 1 — PREDIKSI
# ══════════════════════════════════════════════════════════════
with tab1:
    st.markdown('<p class="section-heading">Input Data Perjalanan</p>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        dist = st.number_input("Jarak (miles)", min_value=0.1, max_value=100.0, value=3.2, step=0.1)
        pax  = st.slider("Penumpang", 1, 6, 1)
        pu   = st.number_input("Zona Pickup (1–263)", min_value=1, max_value=263, value=140)
    with col2:
        do   = st.number_input("Zona Dropoff (1–263)", min_value=1, max_value=263, value=236)
        hour = st.slider("Jam Pickup (0–23)", 0, 23, 9)
        day  = st.selectbox("Hari", ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"], index=1)

    day_map = {"Senin": 1, "Selasa": 2, "Rabu": 3, "Kamis": 4, "Jumat": 5, "Sabtu": 6, "Minggu": 7}

    st.markdown("")
    if st.button("Prediksi Tip", use_container_width=True, type="primary"):
        FEATURE_COLS = ["tripDistance", "passengerCount", "pickup_zone",
                        "dropoff_zone", "pickup_hour", "pickup_day"]
        schema = StructType([
            StructField("tripDistance",   DoubleType()),
            StructField("passengerCount", IntegerType()),
            StructField("pickup_zone",    DoubleType()),
            StructField("dropoff_zone",   DoubleType()),
            StructField("pickup_hour",    IntegerType()),
            StructField("pickup_day",     IntegerType()),
        ])
        data = [(float(dist), int(pax), float(pu), float(do), int(hour), int(day_map[day]))]
        df   = spark.createDataFrame(data, schema)

        assembler   = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
        df_features = assembler.transform(df)

        pred_nominal = model_reg.transform(df_features).select("prediction").collect()[0][0]
        pred_cat     = model_class.transform(df_features).select("prediction").collect()[0][0]

        label_map = {0.0: "Rendah ($0–$2)", 1.0: "Menengah ($2–$5)", 2.0: "Tinggi (>$5)"}

        st.markdown('<hr class="divider">', unsafe_allow_html=True)
        m1, m2 = st.columns(2)
        with m1:
            st.markdown(f"""<div class="card">
                <p class="card-value c-green">${pred_nominal:.2f}</p>
                <p class="card-label">Estimasi Tip</p>
            </div>""", unsafe_allow_html=True)
        with m2:
            st.markdown(f"""<div class="card">
                <p class="card-value c-ink">{label_map.get(pred_cat, "N/A")}</p>
                <p class="card-label">Kategori</p>
            </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# TAB 2 — EVALUASI MODEL
# ══════════════════════════════════════════════════════════════
with tab2:
    metrics = load_metrics()
    reg_df, clf_df = get_prediction_samples()

    if metrics is None:
        st.info("Jalankan `cd ml && python evaluate_model.py` untuk menghasilkan data evaluasi.")
    else:
        ts = metrics.get("timestamp", "")
        if ts:
            st.caption(f"Terakhir dievaluasi: {ts[:19].replace('T', ' ')}")

        reg = metrics["regression"]
        clf = metrics["classification"]
        kappa_val = clf["kappa"]

        # ── Regresi ───────────────────────────────────────────
        st.markdown('<p class="section-heading">Regresi — Prediksi Nilai Tip</p>', unsafe_allow_html=True)

        def color_class(val, thresholds, invert=False):
            good, ok = thresholds
            if invert:
                return "c-green" if val < good else ("c-yellow" if val < ok else "c-red")
            return "c-green" if val > good else ("c-yellow" if val > ok else "c-red")

        c1, c2, c3 = st.columns(3)
        with c1:
            cc = color_class(reg["rmse"], (1.0, 2.5), invert=True)
            st.markdown(f'<div class="card"><p class="card-value {cc}">${reg["rmse"]:.4f}</p><p class="card-label">RMSE</p></div>', unsafe_allow_html=True)
        with c2:
            cc = color_class(reg["mae"], (0.8, 1.5), invert=True)
            st.markdown(f'<div class="card"><p class="card-value {cc}">${reg["mae"]:.4f}</p><p class="card-label">MAE</p></div>', unsafe_allow_html=True)
        with c3:
            cc = color_class(reg["r2"], (0.5, 0.2))
            st.markdown(f'<div class="card"><p class="card-value {cc}">{reg["r2"]:.4f}</p><p class="card-label">R\u00b2</p></div>', unsafe_allow_html=True)

        st.markdown("")
        r2_pct = max(0.0, min(1.0, reg["r2"]))
        st.progress(r2_pct, text=f"R\u00b2 = {reg['r2']:.4f}")

        if reg_df is not None and not reg_df.empty:
            st.markdown('<p class="section-heading">Grafik Prediksi vs Aktual (Sampel Regresi)</p>', unsafe_allow_html=True)
            max_val_reg = float(max(reg_df["Aktual"].max(), reg_df["Prediksi"].max()))
            line_df = pd.DataFrame({"x": [0.0, max_val_reg], "y": [0.0, max_val_reg]})
            
            line = alt.Chart(line_df).mark_line(color="#ffdb13", strokeDash=[4, 4]).encode(
                x="x:Q",
                y="y:Q"
            )
            
            points = alt.Chart(reg_df).mark_point(
                color="#3ecf8e",
                opacity=0.6,
                size=40,
                shape="square"
            ).encode(
                x=alt.X("Aktual:Q", title="Nilai Aktual Tip ($)", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                y=alt.Y("Prediksi:Q", title="Nilai Prediksi Tip ($)", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                tooltip=["Aktual:Q", "Prediksi:Q"]
            )
            
            reg_chart = (points + line).properties(
                height=250
            ).configure_view(
                strokeWidth=0
            ).configure_axis(
                titleColor="#9a9a9a", titleFont="Inter", domainColor="#2e2e2e"
            )
            st.altair_chart(reg_chart, use_container_width=True)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        # ── Klasifikasi ───────────────────────────────────────
        st.markdown('<p class="section-heading">Klasifikasi — Prediksi Kategori Tip</p>', unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            cc = color_class(clf["accuracy"], (0.8, 0.6))
            st.markdown(f'<div class="card"><p class="card-value {cc}">{clf["accuracy"]*100:.2f}%</p><p class="card-label">Akurasi</p></div>', unsafe_allow_html=True)
        with c2:
            cc = color_class(clf["f1"], (0.8, 0.6))
            st.markdown(f'<div class="card"><p class="card-value {cc}">{clf["f1"]:.4f}</p><p class="card-label">F1-Score</p></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="card"><p class="card-value c-ink">{kappa_val:.4f}</p><p class="card-label">Cohen\'s Kappa</p></div>', unsafe_allow_html=True)

        st.markdown("")
        if kappa_val > 0.6:
            badge_cls = "badge-green"
        elif kappa_val > 0.4:
            badge_cls = "badge-yellow"
        else:
            badge_cls = "badge-red"
        st.markdown(f'<span class="kappa-badge {badge_cls}">\u03ba = {kappa_val:.4f} \u2014 {clf["kappa_interpretation"]}</span>', unsafe_allow_html=True)

        if clf_df is not None and not clf_df.empty:
            st.markdown('<p class="section-heading">Grafik Distribusi Kategori Aktual vs Prediksi (Sampel Klasifikasi)</p>', unsafe_allow_html=True)
            act_counts = clf_df["Aktual"].value_counts().reset_index()
            act_counts.columns = ["Kategori", "Jumlah"]
            act_counts["Tipe"] = "Aktual"
            
            pred_counts = clf_df["Prediksi"].value_counts().reset_index()
            pred_counts.columns = ["Kategori", "Jumlah"]
            pred_counts["Tipe"] = "Prediksi"
            
            dist_df = pd.concat([act_counts, pred_counts])
            
            clf_chart = alt.Chart(dist_df).mark_bar(
                cornerRadiusEnd=0
            ).encode(
                x=alt.X("Tipe:N", title=None, axis=alt.Axis(labels=True, labelColor="#ededed")),
                y=alt.Y("Jumlah:Q", title="Jumlah Trip", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                color=alt.Color("Tipe:N", scale=alt.Scale(domain=["Aktual", "Prediksi"], range=["#3ecf8e", "#ffdb13"]), title=None),
                column=alt.Column("Kategori:N", title="Kategori Tip", header=alt.Header(labelColor="#ededed", titleColor="#9a9a9a"))
            ).properties(
                width=180,
                height=220
            ).configure_view(
                strokeWidth=0
            ).configure_axis(
                titleColor="#9a9a9a", titleFont="Inter", domainColor="#2e2e2e"
            )
            st.altair_chart(clf_chart)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        # ── Feature Importance ────────────────────────────────
        st.markdown('<p class="section-heading">Feature Importance</p>', unsafe_allow_html=True)

        fi = metrics["feature_importance"]
        fi_df = pd.DataFrame([
            {"Fitur": k, "Importance": v}
            for k, v in sorted(fi.items(), key=lambda x: x[1], reverse=True)
        ])
        fi_df["Persen"] = (fi_df["Importance"] * 100).round(2)

        chart = alt.Chart(fi_df).mark_bar(
            cornerRadiusEnd=0,
            color="#3ecf8e",
        ).encode(
            x=alt.X("Importance:Q",
                     title="Score",
                     axis=alt.Axis(format=".2f", grid=True, gridColor="#2e2e2e")),
            y=alt.Y("Fitur:N", sort="-x", title=None,
                     axis=alt.Axis(labelColor="#ededed", labelFontSize=13, labelFont="Inter")),
            tooltip=[
                alt.Tooltip("Fitur:N"),
                alt.Tooltip("Importance:Q", format=".4f"),
                alt.Tooltip("Persen:Q", title="%", format=".2f"),
            ]
        ).properties(
            height=220,
        ).configure_view(
            strokeWidth=0,
        ).configure_axis(
            titleColor="#9a9a9a",
            titleFont="Inter",
            gridColor="#2e2e2e",
            domainColor="#2e2e2e",
        )
        st.altair_chart(chart, use_container_width=True)

        # Table
        fi_table = '<table class="styled-table"><thead><tr><th>Fitur</th><th>Score</th><th>Kontribusi</th></tr></thead><tbody>'
        for _, row in fi_df.iterrows():
            bar_w = row["Importance"] / fi_df["Importance"].max() * 100
            fi_table += f"""<tr>
                <td class="highlight">{row['Fitur']}</td>
                <td>{row['Importance']:.4f}</td>
                <td><div style="background:#3ecf8e;height:6px;border-radius:0px;width:{bar_w:.1f}%"></div></td>
            </tr>"""
        fi_table += "</tbody></table>"
        with st.expander("Detail"):
            st.markdown(fi_table, unsafe_allow_html=True)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        # ── Confusion Matrix ──────────────────────────────────
        st.markdown('<p class="section-heading">Confusion Matrix</p>', unsafe_allow_html=True)

        cm = metrics["confusion_matrix"]
        labels = metrics["confusion_matrix_labels"]

        cm_long = []
        for i, actual in enumerate(labels):
            for j, predicted in enumerate(labels):
                cm_long.append({"Aktual": actual, "Prediksi": predicted, "Jumlah": cm[i][j]})
        cm_long_df = pd.DataFrame(cm_long)
        max_val = cm_long_df["Jumlah"].max()

        heatmap = alt.Chart(cm_long_df).mark_rect(cornerRadius=0).encode(
            x=alt.X("Prediksi:N", title="Prediksi", sort=labels,
                     axis=alt.Axis(labelAngle=0, labelColor="#ededed", labelFont="Inter")),
            y=alt.Y("Aktual:N", title="Aktual", sort=labels,
                     axis=alt.Axis(labelColor="#ededed", labelFont="Inter")),
            color=alt.Color("Jumlah:Q",
                            scale=alt.Scale(scheme="greens"),
                            legend=alt.Legend(title="Jumlah", titleColor="#9a9a9a",
                                             labelColor="#9a9a9a")),
            tooltip=[alt.Tooltip("Aktual:N"), alt.Tooltip("Prediksi:N"), alt.Tooltip("Jumlah:Q")]
        ).properties(height=260, width=400)

        text = alt.Chart(cm_long_df).mark_text(fontSize=14, fontWeight=500, font="Inter").encode(
            x=alt.X("Prediksi:N", sort=labels),
            y=alt.Y("Aktual:N", sort=labels),
            text=alt.Text("Jumlah:Q", format=","),
            color=alt.condition(
                alt.datum.Jumlah > max_val * 0.5,
                alt.value("white"), alt.value("#111827")
            )
        )

        cm_chart = (heatmap + text).configure_view(strokeWidth=0).configure_axis(
            titleColor="#9a9a9a", titleFont="Inter", domainColor="#2e2e2e",
        )
        st.altair_chart(cm_chart, use_container_width=True)
        st.caption("Diagonal = prediksi benar.")

        # Table
        with st.expander("Detail"):
            cm_table = '<table class="styled-table"><thead><tr><th>Aktual / Prediksi</th>'
            for l in labels:
                cm_table += f"<th>{l}</th>"
            cm_table += "</tr></thead><tbody>"
            for i, actual in enumerate(labels):
                cm_table += f"<tr><td class='highlight'>{actual}</td>"
                for j in range(len(labels)):
                    val = cm[i][j]
                    cls = "highlight c-green" if i == j else ""
                    cm_table += f'<td class="{cls}">{val:,}</td>'
                cm_table += "</tr>"
            cm_table += "</tbody></table>"
            st.markdown(cm_table, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# TAB 3 — ANALISIS CLICKHOUSE
# ══════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<p class="section-heading">Analisis Real-time ClickHouse</p>', unsafe_allow_html=True)
    
    @st.cache_data(ttl=5)
    def fetch_clickhouse_data():
        try:
            client = clickhouse_connect.get_client(
                host="localhost", port=8123,
                username="mahasiswa", password="bigdata123",
                database="taxi_db"
            )
            res = client.query("SELECT count(), avg(tip_amount), avg(fareAmount) FROM green_taxi").result_rows[0]
            total_rows = res[0]
            avg_tip = res[1] if res[1] is not None else 0.0
            avg_fare = res[2] if res[2] is not None else 0.0

            trend_df = client.query_df("""
                SELECT 
                    toStartOfMinute(lpepPickupDatetime) AS waktu, 
                    count() AS total_trip, 
                    avg(tip_amount) AS rata_tip 
                FROM green_taxi 
                GROUP BY waktu 
                ORDER BY waktu DESC 
                LIMIT 30
            """)
            
            scatter_df = client.query_df("""
                SELECT tripDistance, tip_amount, fareAmount 
                FROM green_taxi 
                ORDER BY lpepPickupDatetime DESC 
                LIMIT 200
            """)
            return total_rows, avg_tip, avg_fare, trend_df, scatter_df
        except Exception as e:
            return None, None, None, None, None

    total_rows, avg_tip, avg_fare, trend_df, scatter_df = fetch_clickhouse_data()

    if total_rows is None:
        st.warning("Gagal menghubungkan ke ClickHouse. Pastikan container ClickHouse aktif dan ingest pipeline berjalan.")
    else:
        col_ch1, col_ch2, col_ch3 = st.columns(3)
        with col_ch1:
            st.markdown(f"""<div class="card">
                <p class="card-value c-ink">{total_rows:,}</p>
                <p class="card-label">Total Trip Ingested</p>
            </div>""", unsafe_allow_html=True)
        with col_ch2:
            st.markdown(f"""<div class="card">
                <p class="card-value c-green">${avg_tip:.2f}</p>
                <p class="card-label">Rata-rata Tip</p>
            </div>""", unsafe_allow_html=True)
        with col_ch3:
            st.markdown(f"""<div class="card">
                <p class="card-value c-yellow">${avg_fare:.2f}</p>
                <p class="card-label">Rata-rata Tarif</p>
            </div>""", unsafe_allow_html=True)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        if trend_df is not None and not trend_df.empty:
            st.markdown('<p class="section-heading">Tren Rata-rata Tip per Menit</p>', unsafe_allow_html=True)
            
            line_chart = alt.Chart(trend_df).mark_line(
                color="#3ecf8e",
                strokeWidth=2
            ).encode(
                x=alt.X("waktu:T", title="Waktu", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                y=alt.Y("rata_tip:Q", title="Rata-rata Tip ($)", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                tooltip=["waktu:T", "total_trip:Q", "rata_tip:Q"]
            ).properties(
                height=250
            ).configure_view(
                strokeWidth=0
            ).configure_axis(
                titleColor="#9a9a9a", titleFont="Inter", domainColor="#2e2e2e"
            )
            st.altair_chart(line_chart, use_container_width=True)

        st.markdown('<hr class="divider">', unsafe_allow_html=True)

        if scatter_df is not None and not scatter_df.empty:
            st.markdown('<p class="section-heading">Korelasi Jarak Perjalanan vs Tip Amount</p>', unsafe_allow_html=True)
            
            scatter_chart = alt.Chart(scatter_df).mark_point(
                color="#3ecf8e",
                opacity=0.6,
                size=40,
                shape="square"
            ).encode(
                x=alt.X("tripDistance:Q", title="Jarak Trip (miles)", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                y=alt.Y("tip_amount:Q", title="Tip ($)", axis=alt.Axis(grid=True, gridColor="#2e2e2e", labelColor="#ededed")),
                color=alt.Color("fareAmount:Q", scale=alt.Scale(scheme="greens"), title="Tarif ($)"),
                tooltip=["tripDistance:Q", "tip_amount:Q", "fareAmount:Q"]
            ).properties(
                height=300
            ).configure_view(
                strokeWidth=0
            ).configure_axis(
                titleColor="#9a9a9a", titleFont="Inter", domainColor="#2e2e2e"
            )
            st.altair_chart(scatter_chart, use_container_width=True)

# ══════════════════════════════════════════════════════════════
# TAB 4 — PANDUAN
# ══════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<p class="section-heading">Menjalankan Simulasi Real-time</p>', unsafe_allow_html=True)
    st.markdown("Pastikan Docker aktif dan semua container berjalan.")
    st.code("docker compose up -d", language="bash")

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    st.markdown('<p class="section-heading">Pipeline — 3 Terminal</p>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Terminal 1 — Producer**")
        st.code("cd stream\npython data_generator.py", language="bash")
    with c2:
        st.markdown("**Terminal 2 — Ingest**")
        st.code("cd stream\npython kappa_stream_ingest.py", language="bash")
    with c3:
        st.markdown("**Terminal 3 — Prediction**")
        st.code("cd stream\npython realtime_predictor.py", language="bash")

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    st.markdown('<p class="section-heading">Evaluasi Model</p>', unsafe_allow_html=True)
    st.code("cd ml\npython evaluate_model.py", language="bash")
    st.caption("Evaluasi model tanpa training ulang. Hasil diekspor ke models/evaluation_metrics.json.")

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    st.markdown('<p class="section-heading">Grafana</p>', unsafe_allow_html=True)
    grafana_html = """
    <table class="styled-table">
        <thead><tr><th>Setting</th><th>Value</th></tr></thead>
        <tbody>
            <tr><td>URL</td><td class="highlight">http://localhost:3000</td></tr>
            <tr><td>Login</td><td>admin / admin123</td></tr>
            <tr><td>Data Source</td><td>ClickHouse</td></tr>
            <tr><td>Server</td><td>localhost:9000</td></tr>
            <tr><td>Database</td><td>taxi_db</td></tr>
            <tr><td>Credentials</td><td>mahasiswa / bigdata123</td></tr>
        </tbody>
    </table>
    """
    st.markdown(grafana_html, unsafe_allow_html=True)