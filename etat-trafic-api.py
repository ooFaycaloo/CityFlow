import requests

import pandas as pd

import os

import shutil

from datetime import datetime

from pathlib import Path

import boto3
 
# -------------------------

# 🔧 Configuration

# -------------------------

URL = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-du-trafic-en-temps-reel&ro…

BASE_DIR = "data_rennes"

BUCKET_NAME = "cityflow-rennes-raw"
 
# Crée un client S3 (assure-toi que les credentials AWS sont configurés sur ton EC2)

s3 = boto3.client("s3")
 
# Crée le dossier local si non existant

os.makedirs(BASE_DIR, exist_ok=True)
 
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
 
                    # Chemin d’enregistrement local par date

                    now = datetime.now()

                    save_dir = os.path.join(BASE_DIR, f"{now.year}/{now.month:02d}/{now.day:02d}")

                    os.makedirs(save_dir, exist_ok=True)
 
                    filename = f"{now.strftime('%H%M%S')}.csv"

                    temp_dir = os.path.join(save_dir, "tmp_" + filename.replace(".csv", ""))

                    os.makedirs(temp_dir, exist_ok=True)
 
                    temp_path = os.path.join(temp_dir, filename)

                    pandas_df.to_csv(temp_path, index=False)
 
                    final_path = os.path.join(save_dir, filename)

                    shutil.move(temp_path, final_path)

                    shutil.rmtree(temp_dir)
 
                    print(f"[{datetime.now()}] ✅ Fichier écrit : {final_path}")
 
                    # Upload vers S3

                    s3_key = f"{now.year}/{now.month:02d}/{now.day:02d}/{filename}"

                    s3.upload_file(final_path, BUCKET_NAME, s3_key)

                    print(f"☁️  Fichier uploadé sur S3 : s3://{BUCKET_NAME}/{s3_key}")
 
                else:

                    print(f"[{datetime.now()}] Aucun nouvel enregistrement.")

            else:

                print(f"[{datetime.now()}] Aucun record reçu de l’API.")

        else:

            print(f"❌ Erreur API : {response.status_code}")
 
    except Exception as e:

        print(f"⚠️ Erreur lors de l’appel API : {str(e)}")
 
    # Attente avant la prochaine requête (3 minutes)

    import time

    time.sleep(30)

 