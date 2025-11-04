import boto3
import pandas as pd
import io
from datetime import datetime
import logging

# ----------------------------
# CONFIGURATION
# ----------------------------
RAW_BUCKET = "cityflow-raw0"           # Bucket S3 où se trouvent les CSV bruts
RAW_PREFIX = "etat-trafic"             # Dossier dans S3
DDB_TABLE = "traffic_metrics"          # Nom de la table DynamoDB
REGION = "eu-west-3"                   # Région AWS (Paris)

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")


# ----------------------------
# FONCTIONS UTILITAIRES
# ----------------------------

def read_csv_from_s3(bucket, prefix):
    """Lit tous les CSV du dossier du jour depuis S3 et les concatène."""
    today = datetime.utcnow().date()
    path = f"{prefix}/{today.year}/{today.month:02d}/{today.day:02d}/"
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=path)

    if "Contents" not in objs:
        logger.warning(f"Aucun fichier trouvé sur S3 pour aujourd'hui : {path}")
        return pd.DataFrame()

    dfs = []
    for obj in objs["Contents"]:
        if obj["Key"].endswith(".csv"):
            logger.info(f"Lecture de {obj['Key']}")
            file_obj = s3.get_object(Bucket=bucket, Key=obj["Key"])
            df = pd.read_csv(io.BytesIO(file_obj["Body"].read()))
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def clean_and_prepare(df):
    """Nettoie et prépare les données avant calcul des agrégats."""
    if df.empty:
        return df

    # Convertir datetime
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    # Convertir les colonnes numériques
    for col in ["vitesse_maxi", "traveltime", "averagevehiclespeed", "vehicleprobemeasurement"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Supprimer les lignes sans tronçon
    df = df.dropna(subset=["id_rva_troncon_fcd_v1_1"])

    # Calcul des champs dérivés
    df["date"] = df["datetime"].dt.date
    df["hour"] = df["datetime"].dt.hour
    df["speed_ratio"] = (df["averagevehiclespeed"] / df["vitesse_maxi"]).clip(0, 1)
    df["lost_time_sec"] = df["traveltime"] * (1 - df["speed_ratio"])
    df["is_congested"] = (df["averagevehiclespeed"] < 0.4 * df["vitesse_maxi"]) | (df["lost_time_sec"] > 60)

    return df


def aggregate_data(df):
    """Calcule les agrégats horaires et journaliers."""
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    hourly = (
        df.groupby(["date", "hour", "id_rva_troncon_fcd_v1_1"], dropna=False)
        .agg(
            vehicles_total=("vehicleprobemeasurement", "sum"),
            avg_speed_kmh=("averagevehiclespeed", "mean"),
            avg_traveltime_s=("traveltime", "mean"),
            lost_time_s=("lost_time_sec", "sum"),
            vitesse_maxi_kmh=("vitesse_maxi", "max"),
            congested_ratio=("is_congested", "mean"),
        )
        .reset_index()
    )
    hourly["is_congested"] = hourly["congested_ratio"] >= 0.5

    daily = (
        hourly.groupby(["date", "id_rva_troncon_fcd_v1_1"], dropna=False)
        .agg(
            vehicles_total=("vehicles_total", "sum"),
            avg_speed_kmh=("avg_speed_kmh", "mean"),
            avg_traveltime_s=("avg_traveltime_s", "mean"),
            lost_time_s=("lost_time_s", "sum"),
            vitesse_maxi_kmh=("vitesse_maxi_kmh", "max"),
            congested_ratio=("is_congested", "mean"),
        )
        .reset_index()
    )
    daily["is_congested"] = daily["congested_ratio"] >= 0.3
    return hourly, daily


def store_in_dynamodb(daily_df):
    """Stocke les agrégats journaliers dans DynamoDB."""
    if daily_df.empty:
        logger.info("Aucun agrégat à insérer dans DynamoDB.")
        return

    with table.batch_writer() as batch:
        for _, row in daily_df.iterrows():
            item = {
                "pk": f"TRONCON#{int(row['id_rva_troncon_fcd_v1_1'])}",
                "sk": f"DATE#{str(row['date'])}",
                "date": str(row["date"]),
                "troncon_id": int(row["id_rva_troncon_fcd_v1_1"]),
                "vehicles_total": float(row["vehicles_total"]),
                "avg_speed_kmh": float(row["avg_speed_kmh"]),
                "lost_time_s": float(row["lost_time_s"]),
                "congested_ratio": float(row["congested_ratio"]),
                "is_congested": bool(row["is_congested"]),
            }
            batch.put_item(Item=item)
    logger.info(f"{len(daily_df)} agrégats journaliers insérés dans DynamoDB.")


if __name__ == "__main__":
    logger.info("🚀 Lancement du traitement quotidien sur EC2...")

    df = read_csv_from_s3(RAW_BUCKET, RAW_PREFIX)
    if df.empty:
        logger.warning("Aucune donnée brute trouvée. Fin du script.")
    else:
        df = clean_and_prepare(df)
        hourly, daily = aggregate_data(df)
        store_in_dynamodb(daily)
        logger.info(f"✅ Terminé : {len(df)} lignes traitées, {len(daily)} agrégats insérés.")
