import pandas as pd
import requests
from io import StringIO
import os
import boto3

# URL des données
DATA_URL = "https://data.rennesmetropole.fr/explore/dataset/eco-counter-data/download/?format=csv"

# Configuration AWS
S3_BUCKET_NAME = "cityflow-processed"  # Remplacez par le nom de votre bucket

# Fonction pour télécharger les données
def download_data(url):
    print("Téléchargement des données...")
    response = requests.get(url)
    if response.status_code == 200:
        print("Téléchargement réussi.")
        return response.content.decode("utf-8")
    else:
        print(f"Erreur lors du téléchargement : {response.status_code}")
        return None

# Fonction pour charger les données dans un DataFrame
def load_data(data):
    print("Chargement des données dans un DataFrame...")
    df = pd.read_csv(StringIO(data), delimiter=";")
    print("Colonnes disponibles :", df.columns.tolist())  # Affiche les colonnes disponibles
    print("Chargement terminé.")
    return df

# Fonction pour nettoyer les données
def clean_data(df):
    print("Nettoyage des données...")
    # Renommer les colonnes pour plus de clarté
    df.rename(columns={
        "date": "Date",
        "isoDate": "ISO_Date",
        "counts": "Counts",
        "status": "Status",
        "ID": "Sensor_ID",
        "name": "Location_Name",
        "geo": "Coordinates",
        "sens": "Direction"
    }, inplace=True)

    # Convertir les colonnes de date en format datetime
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    if "ISO_Date" in df.columns:
        df["ISO_Date"] = pd.to_datetime(df["ISO_Date"], errors="coerce")
    else:
        print("Colonne 'ISO_Date' absente, utilisation de 'Date' uniquement.")

    # Supprimer les lignes avec des valeurs manquantes
    df.dropna(subset=["Date", "Counts"], inplace=True)

    # Convertir les colonnes numériques
    df["Counts"] = pd.to_numeric(df["Counts"], errors="coerce")
    if "Status" in df.columns:
        df["Status"] = pd.to_numeric(df["Status"], errors="coerce")

    print("Nettoyage terminé.")
    return df

# Fonction pour vérifier si un fichier existe déjà dans S3
def file_exists_in_s3(bucket_name, s3_key):
    try:
        s3 = boto3.client("s3")
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise

# Fonction pour envoyer les données nettoyées vers S3
def upload_to_s3(df, bucket_name, file_name):
    print(f"Envoi des données vers le bucket S3 : {bucket_name}...")
    s3_key = file_name  # Utiliser le nom du fichier comme clé dans S3

    # Vérifier si le fichier existe déjà dans S3
    if file_exists_in_s3(bucket_name, s3_key):
        print(f"⚠️  Fichier déjà présent dans S3 : s3://{bucket_name}/{s3_key}")
        return

    # Si le fichier n'existe pas, l'uploader
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "eu-west-1")  # Région par défaut si non spécifiée
    )
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"✅ Données envoyées avec succès dans le fichier : {s3_key} du bucket {bucket_name}.")

# Instructions pour EC2
# 1. Configurez les variables d'environnement sur votre instance EC2 :
#    export AWS_ACCESS_KEY_ID=VotreAccessKey
#    export AWS_SECRET_ACCESS_KEY=VotreSecretKey
#    export AWS_REGION=VotreRegion
# 2. Assurez-vous que boto3 est installé sur l'instance EC2 :
#    pip install boto3
# 3. Exécutez ce script directement sur l'instance EC2.

# Fonction principale
def main():
    # Étape 1 : Télécharger les données
    data = download_data(DATA_URL)
    if data is None:
        return

    # Étape 2 : Charger les données dans un DataFrame
    df = load_data(data)

    # Étape 3 : Nettoyer les données
    df_cleaned = clean_data(df)

    # Étape 4 : Sauvegarder les données nettoyées en mode incrémental
    output_file = "cleaned_data.csv"

    if os.path.exists(output_file):
        # Charger les données existantes
        existing_data = pd.read_csv(output_file)

        # Trouver la dernière date dans les données existantes
        if "Date" in existing_data.columns:
            latest_date = pd.to_datetime(existing_data["Date"], errors="coerce").max()
            # Filtrer les nouvelles données uniquement
            new_data = df_cleaned[df_cleaned["Date"] > latest_date]
        else:
            print("Colonne 'Date' absente dans les données existantes, ajout de toutes les nouvelles données.")
            new_data = df_cleaned

        # Ajouter uniquement les nouvelles données
        if not new_data.empty:
            combined_data = pd.concat([existing_data, new_data]).drop_duplicates()
            combined_data.to_csv(output_file, index=False)
            print(f"Données incrémentales ajoutées et sauvegardées dans le fichier : {output_file}")
        else:
            print("Aucune nouvelle donnée à ajouter.")
    else:
        # Sauvegarder directement si le fichier n'existe pas
        df_cleaned.to_csv(output_file, index=False)
        print(f"Fichier créé et données sauvegardées dans : {output_file}")

    # Étape 5 : Envoyer les données nettoyées vers S3
    upload_to_s3(df_cleaned, S3_BUCKET_NAME, "cleaned_data_s3.csv")

    # Aperçu des données nettoyées
    print(df_cleaned.head())

if __name__ == "__main__":
    main()