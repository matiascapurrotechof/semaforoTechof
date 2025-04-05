import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import gspread
from google.oauth2.service_account import Credentials

# -------------------------
# Parte 1: Extracción de Datos de Facebook Ads
# -------------------------
my_app_id = '1194537789046420'
my_app_secret = '0f03d14ea2f2e4dfe6423732838723d5'
my_access_token = 'EAAQZBbQCWPpQBOZCsK71H6mXbig7Kh4DZAfyeuIduKK0ebaSedZCrFDW7fBWkKWuh9FCI8hU9G3LDM3whGs4SgpITAO2UCbSQWNZBPJV3CFZBXrEDZAC53HRtJDmgNzidE1NNGix9jw43r4eLdSknys7fhRiZBHLZC32ULCCDaxBFWRcGmGw59gkb20cTtoZAJ0Se3'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

ad_account_ids = ['act_66066177']

# Define el rango de fechas para el último mes (30 días hasta ayer)
today = datetime.today()
start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')

todos_los_datos = []

for account_id in ad_account_ids:
    print(f"Consultando insights para la cuenta publicitaria: {account_id}")
    ad_account = AdAccount(account_id)
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'ad',  # Puedes cambiar a 'adset' o 'campaign'
        'fields': ['campaign_name', 'ad_name', 'impressions', 'clicks', 'spend', 'actions', 'action_values'],
        'limit': 5000
    }
    try:
        insights = list(ad_account.get_insights(params=params))
        if not insights:
            print(f"No se encontraron datos para la cuenta {account_id} en el rango {start_date} a {end_date}.")
        else:
            print(f"Se encontraron {len(insights)} registros para la cuenta {account_id}.")
        for insight in insights:
            insight['ad_account_id'] = account_id
            todos_los_datos.append(insight)
    except Exception as e:
        print(f"Error al obtener insights para {account_id}: {e}")

df_insights = pd.DataFrame(todos_los_datos)
print("\nDataFrame final:")
print(df_insights.head())

# -------------------------
# Parte 2: Cálculo de Métricas Adicionales (ej. Compras y Conversion Value)
# -------------------------
def obtener_compras(actions):
    if not isinstance(actions, list):
        return 0
    # Sumar valores de acciones que sean 'purchase' o 'offsite_conversion.purchase'
    compras = [float(a.get('value', 0)) for a in actions if a.get('action_type') in ['offsite_conversion.purchase', 'purchase']]
    return sum(compras) if compras else 0

def obtener_conversion_value(action_values):
    if not isinstance(action_values, list):
        return 0
    valores = [float(a.get('value', 0)) for a in action_values if a.get('action_type') in ['offsite_conversion.purchase', 'purchase']]
    return sum(valores) if valores else 0

if not df_insights.empty:
    df_insights['compras'] = df_insights['actions'].apply(obtener_compras)
    df_insights['conversion_value'] = df_insights['action_values'].apply(obtener_conversion_value)
    print("\nDataFrame con columnas 'compras' y 'conversion_value':")
    print(df_insights[['campaign_name', 'ad_name', 'clicks', 'impressions', 'spend', 'compras', 'conversion_value']].head())
else:
    print("La consulta se realizó correctamente, pero no se encontraron datos de insights.")

# -------------------------
# Parte 3: Actualización de Google Sheets
# -------------------------

# Opcional: elimina las columnas "actions" y "action_values" para evitar problemas en el Sheet
for col in ['actions', 'action_values']:
    if col in df_insights.columns:
        df_insights = df_insights.drop(columns=[col])

# Sanitiza el DataFrame para evitar valores no JSON compliant
df_insights = df_insights.replace([np.inf, -np.inf], np.nan).fillna(0)

# Convertir DataFrame a lista de listas (incluyendo encabezados)
data = [df_insights.columns.tolist()] + df_insights.values.tolist()

# Ruta al archivo JSON de credenciales (asegúrate de que el nombre y la ruta sean correctos)
json_file = 'autosemaforo-cd96ce9df97b.json'
scopes = ['https://www.googleapis.com/auth/spreadsheets']
credentials = Credentials.from_service_account_file(json_file, scopes=scopes)

# Autoriza con gspread
gc = gspread.authorize(credentials)

# Reemplaza 'TU_GOOGLE_SHEET_ID' con el ID de tu Google Sheet (parte de la URL)
sheet_id = '1geoIvO9qNXxVAPUaI8TFJGlv-xEOHzjOsCSRTwhwjpQ'
worksheet = gc.open_by_key(sheet_id).sheet1

# Actualiza la hoja a partir de la celda A1
worksheet.update(data, range_name='A1')
print("Google Sheet actualizada correctamente.")
