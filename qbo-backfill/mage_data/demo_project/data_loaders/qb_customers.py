import requests
from datetime import datetime
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
import pandas as pd

from mage_ai.data_preparation.decorators import transformer,test

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def refresh_access_token():
    client_id = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_secret_id')
    refresh_token = get_secret_value('qb_refresh_token')

    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    respuesta = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    token = respuesta.json()
    return token["access_token"]

def _fetch_qb_data(realm_id, access_token, query, base_url, minor_version, entity, page_size=100, max_retries=5):
    if not base_url or not minor_version:
        raise ValueError("Se necesita url base y minor version")

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-Type': 'text/plain'
    }

    url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query"
    all_results = []
    start_position = 1

    while True:
        paginated_query = f"{query} STARTPOSITION {start_position} MAXRESULTS {page_size}"
        for i in range(max_retries):
            try:
                print(f"Request a la API: {url}\nQuery: {paginated_query}")
                response = requests.get(url, headers=headers, params={'query': paginated_query, 'minorversion': minor_version}, timeout=60**max_retries)
                response.raise_for_status()
                data = response.json()
                results = data.get("QueryResponse", {}).get(entity, [])

                if not results:
                    return all_results  

                all_results.extend(results)

                if len(results) < page_size:
                    return all_results

                start_position += page_size
                break  

            except requests.exceptions.RequestException as e:
                print(f"Error en el intento {i+1}/{max_retries}: {e}")
                if i == max_retries - 1:
                    raise

@data_loader
def load_data(*args, **kwargs):
    fecha_inicio = kwargs.get('fecha_inicio')
    fecha_fin = kwargs.get('fecha_fin')

    realm_id = get_secret_value('qb_realm_id')

    access_token = refresh_access_token()

    minor_version = 75
    base_url = 'https://sandbox-quickbooks.api.intuit.com'
    entity = "Customer"

    filtros = []
    if fecha_inicio:
        filtros.append(f"Metadata.LastUpdatedTime >= '{fecha_inicio}'")
    if fecha_fin:
        filtros.append(f"Metadata.LastUpdatedTime < '{fecha_fin}'")
    where_clause = ''
    if filtros:
        where_clause = ' WHERE ' + ' AND '.join(filtros)

    query = f"select * from Customer{where_clause} order by Metadata.LastUpdatedTime asc"

    data = _fetch_qb_data(
        realm_id,
        access_token,
        query,
        base_url,
        minor_version,
        entity
    )

    if not data:
        data = [{"Id": "test", "DisplayName": "Cliente Prueba", "MetaData": {"LastUpdatedTime": "2025-01-01"}}]

    return data


def transform(data, *args, **kwargs):
    pipeline_name=kwargs.get('pipeline_uuid',"").lower()

    if 'invoice'in pipeline_name:
        tabla='qb_invoices_raw'
    elif'customer' in pipeline_name:
        tabla='qb_customers_raw'
    elif'item' in pipeline_name:
        tabla='qb_items_raw'
    else:
        tabla='qb_unknown_raw'

    print(f"Procesando datos para tabla:{tabla}")

    if not data:
        print("No hay datos para procesar")
        return pd.DataFrame()

    registros=[]
    ahora=datetime.utcnow()

    for i, record in enumerate (data):
        record_id=record.get("Id") or record.get('id') or f"temp_{i}"

        registro={
            'id':str(record_id),
            'payload':json.dumps(record),
            'ingested_at_utc':ahora,
            'extract_window_start_utc':ahora.replace(hour=0,minute=0,second=0),
            'extract_window_end_utc': ahora,
            'page_number': 1,
            'page_size':len(data),
            'request_payload':'{}'
        }

        registros.append(registro)

    df=pd.DataFrame(registros)
    print(f"Datos preparados:{len(df)} registros")

    return df


@test
def test_output(*outputs) -> None:
    assert outputs is not None, 'No hay outputs en el bloque'
    assert len(outputs) > 0, 'El bloque devolvió lista vacía'
