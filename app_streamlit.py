# app_cityflow_dashboard.py
import streamlit as st
import pandas as pd
import requests
import plotly.express as px

st.set_page_config(page_title="🚦 Dashboard Trafic & 🚲 Vélo", layout="wide")
st.title("🌍 CityFlow — Trafic 🚗 & Vélo 🚲 Analytics (DynamoDB via API)")

API_TRAFFIC = "https://oeagxmsmhl.execute-api.eu-west-3.amazonaws.com/stage/stats-trafic"
API_BIKE    = "https://oeagxmsmhl.execute-api.eu-west-3.amazonaws.com/stage/stats-velos"

# --------------------------
# 🔧 Utils
# --------------------------
@st.cache_data(ttl=300)
def call_api(url, params=None):
    """Appel API robuste : supporte dict(items=[]) ou liste brute."""
    try:
        r = requests.get(url, params=params or {}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "items" in data:
            data = data["items"]
        elif isinstance(data, dict):
            data = [data]
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Erreur API {url}: {e}")
        return pd.DataFrame()

def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def clean_cols(df):
    df.columns = [c.strip() for c in df.columns]
    return df

# --------------------------
# 🎛️ Filtres
# --------------------------
st.sidebar.header("Filtres")
# Quelques dates connues de ton jeu (tu peux en ajouter)
known_dates = ["2025-11-04", "2025-11-03", "2025-10-17", "2025-09-03", "2025-09-02", "2025-09-01"]
dates = st.sidebar.multiselect("📅 Sélectionne une ou plusieurs dates", options=known_dates, default=["2025-11-04"])

departement_filter = st.sidebar.text_input("Département (optionnel)", "")
niveau_filter = st.sidebar.multiselect("Niveau de congestion (optionnel)", options=["Faible","Modérée","Forte"], default=[])
rue_filter = st.sidebar.text_input("Nom de rue (contient, optionnel)", "")
bike_loc_filter = st.sidebar.text_input("Emplacement vélo (contient, optionnel)", "")

# --------------------------
# 📦 Chargement des données
# --------------------------
# Trafic : concat sur plusieurs dates
df_traffic = pd.concat([call_api(API_TRAFFIC, {"date": d}) for d in dates], ignore_index=True)
df_traffic = clean_cols(df_traffic)

# Vélo : concat sur plusieurs dates
df_bike = pd.concat([call_api(API_BIKE, {"date": d}) for d in dates], ignore_index=True)
df_bike = clean_cols(df_bike)

st.sidebar.success(f"Trafic: {len(df_traffic)} lignes | Vélo: {len(df_bike)} lignes")

# ============================================================
# 🚗 TRAFIC
# ============================================================
st.header("🚗 Trafic — Indicateurs & Analyses")

if df_traffic.empty:
    st.info("Aucune donnée trafic pour ces critères.")
else:
    # Colonnes attendues (d’après ton DynamoDB)
    # id, date, departement, heure_de_pointe, niveau_congestion, nom_rue,
    # taux_congestion_pct, temps_trajet_total_s, vitesse_heure_pointe_kmh, vitesse_moyenne_kmh
    num_cols = ["taux_congestion_pct", "temps_trajet_total_s", "vitesse_heure_pointe_kmh", "vitesse_moyenne_kmh"]
    df_traffic = coerce_numeric(df_traffic, num_cols)

    # Filtres côté app (en plus du filtre date côté API)
    if departement_filter and "departement" in df_traffic.columns:
        df_traffic = df_traffic[df_traffic["departement"].astype(str).str.contains(departement_filter, case=False, na=False)]
    if niveau_filter and "niveau_congestion" in df_traffic.columns:
        df_traffic = df_traffic[df_traffic["niveau_congestion"].isin(niveau_filter)]
    if rue_filter and "nom_rue" in df_traffic.columns:
        df_traffic = df_traffic[df_traffic["nom_rue"].astype(str).str.contains(rue_filter, case=False, na=False)]

    if df_traffic.empty:
        st.warning("Aucune donnée trafic après application des filtres.")
    else:
        # KPI
        c1, c2, c3, c4 = st.columns(4)
        vit_moy = df_traffic["vitesse_moyenne_kmh"].mean() if "vitesse_moyenne_kmh" in df_traffic.columns else None
        cong_moy = df_traffic["taux_congestion_pct"].mean() if "taux_congestion_pct" in df_traffic.columns else None
        nb_rues = df_traffic["nom_rue"].nunique() if "nom_rue" in df_traffic.columns else 0
        # heure la plus critique selon la congestion moyenne
        heure_crit = "-"
        if "heure_de_pointe" in df_traffic.columns and "taux_congestion_pct" in df_traffic.columns:
            tmp = df_traffic.groupby("heure_de_pointe")["taux_congestion_pct"].mean().sort_values(ascending=False)
            if not tmp.empty:
                heure_crit = tmp.index[0]

        c1.metric("📊 Nb relevés", len(df_traffic))
        c2.metric("⚡ Vitesse moyenne (km/h)", f"{vit_moy:.1f}" if vit_moy is not None else "N/A")
        c3.metric("🔥 Congestion moyenne (%)", f"{cong_moy:.1f}" if cong_moy is not None else "N/A")
        c4.metric("🛣️ Rues uniques", nb_rues)

        # Tableau brut (échantillon)
        with st.expander("Aperçu des données trafic"):
            st.dataframe(df_traffic)

        # Top 10 rues par congestion moyenne
        if "nom_rue" in df_traffic.columns and "taux_congestion_pct" in df_traffic.columns:
            st.subheader("🏆 Top 10 rues les plus congestionnées (moyenne %)")
            top_cong = (
                df_traffic.groupby("nom_rue")["taux_congestion_pct"]
                .mean()
                .sort_values(ascending=False)
                .head(10)
                .reset_index()
            )
            fig_top = px.bar(
                top_cong, x="nom_rue", y="taux_congestion_pct", text="taux_congestion_pct",
                labels={"nom_rue":"Rue","taux_congestion_pct":"Congestion (%)"},
                title="Top 10 rues par congestion moyenne"
            )
            fig_top.update_traces(textposition="outside")
            st.plotly_chart(fig_top, use_container_width=True)

        # Heatmap Rue × Heure (moyenne % congestion)
        if {"nom_rue","heure_de_pointe","taux_congestion_pct"}.issubset(df_traffic.columns):
            st.subheader("🧱 Heatmap — Congestion moyenne par Rue × Heure")
            # Tri des heures par ordre numérique (extrait '7' de '7h00')
            def hour_key(h):
                try:
                    return int("".join([ch for ch in str(h) if ch.isdigit()]) or 0)
                except:
                    return 0
            heat = df_traffic.pivot_table(
                index="nom_rue", columns="heure_de_pointe",
                values="taux_congestion_pct", aggfunc="mean"
            )
            # Réordonne colonnes d'heure
            heat = heat[sorted(heat.columns, key=hour_key)]
            fig_heat = px.imshow(
                heat,
                color_continuous_scale="RdYlGn_r",
                labels={"color":"Congestion (%)"},
                aspect="auto"
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        # Vitesse moyenne par heure
        if {"heure_de_pointe","vitesse_moyenne_kmh"}.issubset(df_traffic.columns):
            st.subheader("📈 Vitesse moyenne par heure de pointe")
            # heuristique tri heures
            df_traffic["_heure_num"] = df_traffic["heure_de_pointe"].astype(str).str.extract(r"(\d+)").astype(float)
            speed_hour = df_traffic.groupby("_heure_num")["vitesse_moyenne_kmh"].mean().reset_index().sort_values("_heure_num")
            fig_hour = px.line(
                speed_hour, x="_heure_num", y="vitesse_moyenne_kmh",
                markers=True, labels={"_heure_num":"Heure","vitesse_moyenne_kmh":"Vitesse (km/h)"},
                title="Vitesse moyenne par heure"
            )
            st.plotly_chart(fig_hour, use_container_width=True)

        # Répartition des niveaux de congestion
        if "niveau_congestion" in df_traffic.columns:
            st.subheader("📊 Répartition des niveaux de congestion")
            fig_pie = px.pie(df_traffic, names="niveau_congestion", title="Niveaux de congestion")
            st.plotly_chart(fig_pie, use_container_width=True)

        # Histogramme des vitesses
        if "vitesse_moyenne_kmh" in df_traffic.columns:
            st.subheader("📉 Distribution des vitesses (km/h)")
            fig_hist = px.histogram(df_traffic, x="vitesse_moyenne_kmh", nbins=25, title="Histogramme des vitesses")
            st.plotly_chart(fig_hist, use_container_width=True)

# ============================================================
# 🚲 VÉLO
# ============================================================
st.header("🚲 Vélo — Indicateurs & Tendance")

if df_bike.empty:
    st.info("Aucune donnée vélo pour ces critères.")
else:
    # Attendu : Location_Name, Date, avg_counts, total_counts
    # Mise au propre
    renames = {}
    for c in df_bike.columns:
        if c.lower() == "location_name": renames[c] = "Location_Name"
        if c.lower() == "date": renames[c] = "Date"
    if renames:
        df_bike = df_bike.rename(columns=renames)

    df_bike = coerce_numeric(df_bike, ["avg_counts","total_counts"])

    # Filtres
    if bike_loc_filter and "Location_Name" in df_bike.columns:
        df_bike = df_bike[df_bike["Location_Name"].astype(str).str.contains(bike_loc_filter, case=False, na=False)]

    if df_bike.empty:
        st.warning("Aucune donnée vélo après filtres.")
    else:
        # KPI
        total_passages = int(df_bike["total_counts"].sum()) if "total_counts" in df_bike.columns else 0
        avg_passages = float(df_bike["avg_counts"].mean()) if "avg_counts" in df_bike.columns else 0.0
        nb_sites = df_bike["Location_Name"].nunique() if "Location_Name" in df_bike.columns else 0

        c1, c2, c3 = st.columns(3)
        c1.metric("🚴 Total passages (somme)", f"{total_passages}")
        c2.metric("📈 Moyenne (avg_counts)", f"{avg_passages:.1f}")
        c3.metric("📍 Emplacements uniques", nb_sites)

        with st.expander("Aperçu des données vélo"):
            st.dataframe(df_bike)

        # Top 10 emplacements par total_counts
        if {"Location_Name","total_counts"}.issubset(df_bike.columns):
            st.subheader("🏆 Top 10 emplacements vélo (total_counts)")
            top_bike = df_bike.groupby("Location_Name")["total_counts"].sum().nlargest(10).reset_index()
            fig_bike = px.bar(
                top_bike, x="Location_Name", y="total_counts", text="total_counts",
                title="Top 10 emplacements (somme des passages)"
            )
            fig_bike.update_traces(textposition="outside")
            st.plotly_chart(fig_bike, use_container_width=True)

        # Série temporelle (si plusieurs dates sélectionnées côté filtre)
        if {"Date","total_counts"}.issubset(df_bike.columns):
            st.subheader("📆 Volume vélo par date (somme)")
            by_date = df_bike.groupby("Date")["total_counts"].sum().reset_index().sort_values("Date")
            fig_time = px.line(by_date, x="Date", y="total_counts", markers=True, title="Total passages vélo par date")
            st.plotly_chart(fig_time, use_container_width=True)

        # Nuage avg vs total (par site)
        if {"avg_counts","total_counts","Location_Name"}.issubset(df_bike.columns):
            st.subheader("🔎 avg_counts vs total_counts (par emplacement)")
            fig_sc = px.scatter(
                df_bike, x="avg_counts", y="total_counts", color="Location_Name",
                title="Relation entre moyenne et total (vélo)"
            )
            st.plotly_chart(fig_sc, use_container_width=True)

# ============================================================
# 🚦🚲 Comparatif Trafic ↔ Vélo (par date)
# ============================================================
if not df_traffic.empty and not df_bike.empty:
    st.header("🔗 Comparaison Trafic ↔ Vélo (par date)")

    # Agréger trafic par date (moyenne congestion), vélo par date (somme)
    if "date" in df_traffic.columns:
        traffic_by_date = (
            df_traffic.groupby("date")["taux_congestion_pct"]
            .mean()
            .reset_index()
            .rename(columns={"date":"Date","taux_congestion_pct":"congestion_moy_pct"})
        )
        if {"Date","total_counts"}.issubset(df_bike.columns):
            bike_by_date = df_bike.groupby("Date")["total_counts"].sum().reset_index()
            comp = pd.merge(traffic_by_date, bike_by_date, on="Date", how="inner")
            if not comp.empty:
                c1, c2 = st.columns(2)
                with c1:
                    fig_cmp1 = px.bar(comp, x="Date", y="congestion_moy_pct", title="Congestion moyenne (%) par date")
                    st.plotly_chart(fig_cmp1, use_container_width=True)
                with c2:
                    fig_cmp2 = px.bar(comp, x="Date", y="total_counts", title="Total passages vélo par date")
                    st.plotly_chart(fig_cmp2, use_container_width=True)

                st.subheader("📈 Corrélation (date agrégée) — Congestion vs Vélo")
                fig_corr = px.scatter(comp, x="congestion_moy_pct", y="total_counts", text="Date",
                                      title="Congestion moyenne (%) vs Total vélo (par date)")
                fig_corr.update_traces(textposition="top center")
                st.plotly_chart(fig_corr, use_container_width=True)

st.markdown("---")
st.caption("CityFlow • AWS Lambda + API Gateway + DynamoDB + Streamlit • KPIs, heatmaps, comparatifs 🚀")
