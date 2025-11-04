import pandas as pd
import requests
from io import StringIO
import os
import boto3
from datetime import datetime, timezone



DATA_URL = "https://data.rennesmetropole.fr/explore/dataset/eco-counter-data/download/?format=csv&timezone=Europe/Paris&use_labels_for_header=true"

S3_BUCKET_NAME = "cityflow-raw0"

S3_PREFIX = "bike/"

LOCAL_REFERENCE_FILE = "cleaned_data.csv"

START_DATE = datetime(2025, 9, 1, tzinfo=timezone.utc)




def download_data(url: str):
    """Télécharge les données depuis l'URL source"""
    print("🚀 Téléchargement des données...")
    response = requests.get(url)
    if response.status_code == 200:
        print("✅ Téléchargement réussi.")
        return response.content.decode("utf-8")
    print(f"❌ Erreur de téléchargement : {response.status_code}")
    return None


def load_data(data: str):
    """Charge le CSV en DataFrame pandas"""
    df = pd.read_csv(StringIO(data), delimiter=";")
    print(f"📥 Données chargées : {len(df)} lignes.")
    print("Colonnes détectées :", df.columns.tolist())
    return df


def clean_data(df: pd.DataFrame):
    """Nettoie et formate les données brutes"""
    print("🧹 Nettoyage des données...")

    df.rename(columns={
        "date": "Date",
        "isodate": "ISO_Date",
        "counts": "Counts",
        "status": "Status",
        "id": "Sensor_ID",
        "name": "Location_Name",
        "geo": "Coordinates",
        "sens": "Direction"
    }, inplace=True, errors="ignore")

    # Conversion des dates et valeurs
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df["Counts"] = pd.to_numeric(df["Counts"], errors="coerce")
    df.dropna(subset=["Date", "Counts"], inplace=True)

    # Filtrer à partir de la date définie
    df = df[df["Date"] >= START_DATE]

    print(f"✅ {len(df)} lignes après nettoyage (depuis {START_DATE.date()})")
    return df



def get_latest_date_from_s3(bucket_name, prefix="bike/"):
    """Récupère la dernière date de données présente sur S3"""
    s3 = boto3.client("s3")
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in objects:
            print("📭 Aucun fichier trouvé dans S3.")
            return None

        latest_file = max(objects["Contents"], key=lambda x: x["LastModified"])["Key"]
        print(f"📦 Dernier fichier S3 détecté : {latest_file}")

        csv_obj = s3.get_object(Bucket=bucket_name, Key=latest_file)
        df_last = pd.read_csv(csv_obj["Body"])
        if "Date" in df_last.columns:
            last_date = pd.to_datetime(df_last["Date"], utc=True).max()
            print(f"🕓 Dernière date trouvée dans S3 : {last_date}")
            return last_date
        return None
    except Exception as e:
        print("⚠️ Erreur S3 :", e)
        return None


def upload_to_s3(df: pd.DataFrame, bucket_name: str, file_name: str):
    """Charge un fichier CSV sur S3"""
    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    print(f"✅ Fichier envoyé : s3://{bucket_name}/{file_name}")



def main():
    print("============== 🌆 DÉBUT DU BATCH CITYFLOW ==============")

    # Étape 1 — Télécharger les données
    data = download_data(DATA_URL)
    if not data:
        print("❌ Téléchargement échoué, arrêt du batch.")
        return

    # Étape 2 — Charger et nettoyer
    df = load_data(data)
    df_cleaned = clean_data(df)

    # Étape 3 — Déterminer la dernière date
    latest_date = None
    if os.path.exists(LOCAL_REFERENCE_FILE):
        existing = pd.read_csv(LOCAL_REFERENCE_FILE)
        latest_date = pd.to_datetime(existing["Date"], utc=True).max()
        print(f"🕓 Dernière date locale connue : {latest_date}")
    else:
        latest_date = get_latest_date_from_s3(S3_BUCKET_NAME, prefix=S3_PREFIX)

    # Étape 4 — Filtrer les nouvelles données
    if latest_date is not None:
        new_data = df_cleaned[df_cleaned["Date"] > latest_date]
    else:
        new_data = df_cleaned

    if new_data.empty:
        print("ℹ️ Aucune nouvelle donnée à charger.")
        print("============== ✅ FIN DU BATCH (aucune mise à jour) ==============")
        return

    # Étape 5 — Mettre à jour le fichier local
    if os.path.exists(LOCAL_REFERENCE_FILE):
        combined = pd.concat([existing, new_data]).drop_duplicates(subset=["Date", "Sensor_ID"])
    else:
        combined = new_data
    combined.to_csv(LOCAL_REFERENCE_FILE, index=False)
    print(f"💾 Fichier local mis à jour : {LOCAL_REFERENCE_FILE}")

    # Étape 6 — Envoi sur S3
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    s3_key = f"{S3_PREFIX}cleaned_data_delta_{timestamp}.csv"
    upload_to_s3(new_data, S3_BUCKET_NAME, s3_key)

    print(f"📈 {len(new_data)} nouvelles lignes envoyées.")
    print("============== ✅ FIN DU BATCH CITYFLOW ==============")



if __name__ == "__main__":
    main()
