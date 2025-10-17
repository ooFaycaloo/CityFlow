import requests
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO
import time

# -------------------------
# 🔧 Configuration
# -------------------------
URL = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-du-trafic-en-temps-reel&rows=100"
BUCKET_NAME = "cityflow-raw0"
S3_FOLDER = "etat-trafic/"  # 🔹 le dossier cible sur S3

# Crée un client S3 (assure-toi que les credentials AWS sont configurés sur ton EC2)
s3 = boto3.client("s3")

déjà_vus = set()

print("🚀 Démarrage de l’ingestion Rennes Métropole...")

while True:
    try:
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            records = data.get("records", [])

            if records:
                flat_records = []

                for record in records:
                    record_id = record.get("recordid")
                    if record_id and record_id not in déjà_vus:
                        déjà_vus.add(record_id)
                        fields = record.get("fields", {})
                        fields["recordid"] = record_id
                        flat_records.append(fields)

                if flat_records:
                    pandas_df = pd.DataFrame(flat_records)

                    # Génération du chemin S3
                    now = datetime.now()
                    filename = f"{now.strftime('%H%M%S')}.csv"
                    s3_key = f"{S3_FOLDER}{now.year}/{now.month:02d}/{now.day:02d}/{filename}"

                    # Conversion en CSV en mémoire
                    csv_buffer = StringIO()
                    pandas_df.to_csv(csv_buffer, index=False)

                    # Upload direct vers S3
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=s3_key,
                        Body=csv_buffer.getvalue()
                    )

                    print(f"[{datetime.now()}] ☁️  Fichier uploadé sur S3 : s3://{BUCKET_NAME}/{s3_key}")
                else:
                    print(f"[{datetime.now()}] Aucun nouvel enregistrement.")
            else:
                print(f"[{datetime.now()}] Aucun record reçu de l’API.")
        else:
            print(f"❌ Erreur API : {response.status_code}")

    except Exception as e:
        print(f"⚠️ Erreur lors de l’appel API : {str(e)}")

    # Attente avant la prochaine requête (30 secondes)
    time.sleep(30)
