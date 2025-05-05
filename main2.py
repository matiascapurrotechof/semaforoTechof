import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import time

# ------------------------- CONFIGURACIÓN -------------------------

# Facebook Ads App Credentials
my_app_id = '9567000766756455'
my_app_secret = '9333d5c7b0a1d9979e1cbb349fbe79af'
my_access_token = 'EACH9IvJEtmcBO3CCUmcwnA2KZB3HRpEZAbZChoPwoAIDyAZAfg1FCjH0gve1C3TcYXmKIZBMiK1IeUSZAMZA8CGRNY1Kmu16wotFb3hAvAz5d5DQMFVNVtiA08XUgmyZBGUJoFhrOQrO2ZBnfIF02244d7a7HDg1eKufloGsER9bsqIAvn3m8HcBl2jVTstmEXwC3'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# Business Manager ID
business_id = '107833143845430'

# CSV en Drive
csv_file = 'ads_data.csv'
service_account_file = 'autosemaforo-f3745e16ca62.json'
drive_folder_id = '1nK0FsepCIJv39ZIuFOkg9X7glwSsfSuW'

# ------------------------- AUTENTICACIÓN GOOGLE DRIVE API -------------------------

scopes = ['https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file(service_account_file, scopes=scopes)
drive_service = build('drive', 'v3', credentials=credentials)

# Descargar CSV si existe en Drive
query = f"name='{csv_file}' and '{drive_folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
items = results.get('files', [])
file_id = None
if items:
    file_id = items[0]['id']
    request = drive_service.files().get_media(fileId=file_id)
    with open(csv_file, 'wb') as f:
        f.write(request.execute())
    df_existing = pd.read_csv(csv_file)
    print("☁️ CSV descargado desde Google Drive.")
else:
    df_existing = pd.DataFrame()
    print("📁 No se encontró el archivo en Google Drive. Se creará uno nuevo.")

# ------------------------- CÁLCULO DE BLOQUE HORARIO -------------------------

if df_existing.empty or 'hour_block' not in df_existing.columns:
    next_block_start = datetime(2025, 5, 1, 0, 0)
else:
    df_existing['hour_block'] = pd.to_datetime(df_existing['hour_block'], format='mixed', errors='coerce')
    last_block = df_existing['hour_block'].max()
    next_block_start = last_block + timedelta(hours=1)

now = datetime.now()
if next_block_start + timedelta(hours=1) > now:
    print(f"⏱️ Aún no terminó el bloque de {next_block_start.strftime('%Y-%m-%d %H:%M')}, se canceló la ejecución.")
    exit()

fecha_desde = next_block_start.strftime('%Y-%m-%d')
fecha_hasta = next_block_start.strftime('%Y-%m-%d')
print(f"📅 Extrayendo bloque horario: {fecha_desde} {next_block_start.strftime('%H:%M')} → {(next_block_start + timedelta(hours=1)).strftime('%H:%M')}")

# ------------------------- FUNCIONES AUXILIARES -------------------------

def obtener_compras(actions):
    if not isinstance(actions, list):
        return 0
    return sum(float(a.get('value', 0)) for a in actions if a.get('action_type') in ['purchase', 'offsite_conversion.purchase'])

def obtener_conversion_value(action_values):
    if not isinstance(action_values, list):
        return 0
    return sum(float(a.get('value', 0)) for a in action_values if a.get('action_type') in ['purchase', 'offsite_conversion.purchase'])

# ------------------------- EXTRACCIÓN DE DATOS -------------------------

business = Business(business_id)
accounts = business.get_client_ad_accounts(fields=['name', 'account_id'])

df_total = pd.DataFrame()

for acc in accounts:
    ad_account_id = f"act_{acc['account_id']}"
    nombre_cuenta = acc['name']
    print(f"\n🔍 Procesando cuenta: {nombre_cuenta} ({ad_account_id})")

    for attempt in range(3):
        try:
            ad_account = AdAccount(ad_account_id)
            params = {
                'time_range': {'since': fecha_desde, 'until': fecha_hasta},
                'level': 'ad',
                'fields': [
                    'ad_id', 'ad_name', 'campaign_name',
                    'impressions', 'clicks', 'spend',
                    'actions', 'action_values',
                    'date_start', 'date_stop'
                ],
                'limit': 5000
            }

            insights = list(ad_account.get_insights(params=params))
            if not insights:
                print("⚠️ No se encontraron datos de anuncios con actividad en este bloque.")
                break

            df = pd.DataFrame(insights)
            if 'actions' not in df.columns:
                df['actions'] = [{}] * len(df)
            if 'action_values' not in df.columns:
                df['action_values'] = [{}] * len(df)

            ad_ids = df['ad_id'].tolist()
            ads_info = ad_account.get_ads(fields=['id', 'effective_status'], params={'filtering': [{'field': 'id', 'operator': 'IN', 'value': ad_ids}]})
            status_dict = {ad['id']: ad['effective_status'] for ad in ads_info}

            df['compras'] = df['actions'].apply(obtener_compras)
            df['conversion_value'] = df['action_values'].apply(obtener_conversion_value)
            df['compras'] = pd.to_numeric(df['compras'], errors='coerce').fillna(0)
            df['conversion_value'] = pd.to_numeric(df['conversion_value'], errors='coerce').fillna(0)

            df['effective_status'] = df['ad_id'].map(lambda x: status_dict.get(x, 'N/A'))
            df['ad_account_id'] = ad_account_id
            df['hour_block'] = next_block_start.strftime('%Y-%m-%d %H:%M')

            for col in ['clicks', 'impressions', 'spend']:
                if col not in df.columns:
                    df[col] = 0

            df_final = df[[
                'ad_account_id', 'ad_name', 'campaign_name', 'clicks', 'date_start', 'date_stop',
                'impressions', 'spend', 'compras', 'conversion_value', 'effective_status', 'hour_block'
            ]]

            df_total = pd.concat([df_total, df_final], ignore_index=True)
            break

        except Exception as e:
            print(f"❌ Error procesando {ad_account_id}, intento {attempt + 1}: {e}")
            time.sleep(5)

# ------------------------- GUARDADO Y ACTUALIZACIÓN EN DRIVE -------------------------

if df_total.empty:
    print("\n⚠️ No se encontraron datos en ninguna cuenta.")
else:
    df_total = df_total.replace([np.inf, -np.inf], np.nan).fillna(0)
    df_final = pd.concat([df_existing, df_total], ignore_index=True)
    df_final.to_csv(csv_file, index=False)

    media = MediaFileUpload(csv_file, mimetype='text/csv', resumable=True)
    if file_id:
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        file_metadata = {'name': csv_file, 'parents': [drive_folder_id]}
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    print(f"✅ Datos actualizados en Google Drive: {csv_file}")