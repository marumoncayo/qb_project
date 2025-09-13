import json
import pandas as pd
from datetime import datetime
from mage_ai.data_preparation.decorators import transformer, test


@transformer
def transform(data, *args, **kwargs):
    """
    Transformer para generar tablas raw de QuickBooks.
    Convierte los registros en DataFrame con metadatos obligatorios.
    """
    pipeline = kwargs.get('pipeline_uuid', '').lower()
    if 'invoice' in pipeline:
        tabla = 'qb_invoices_raw'
    elif 'customer' in pipeline:
        tabla = 'qb_customers_raw'
    elif 'item' in pipeline:
        tabla = 'qb_items_raw'
    else:
        tabla = 'qb_unknown_raw'

    print(f"Creando registros para tabla: {tabla}")

    if not data:
        return pd.DataFrame(columns=[
            'id', 'payload', 'ingested_at_utc',
            'extract_window_start_utc', 'extract_window_end_utc',
            'page_number', 'page_size', 'request_payload'
        ])

    ahora = datetime.utcnow()
    registros = []

    for i, record in enumerate(data):
        record_id = record.get('Id') or record.get('id') or f"temp_{i}"

        registros.append({
            'id': str(record_id),
            'payload': json.dumps(record),
            'ingested_at_utc': ahora,
            'extract_window_start_utc': ahora.replace(hour=0, minute=0, second=0, microsecond=0),
            'extract_window_end_utc': ahora,
            'page_number': 1,
            'page_size': len(data),
            'request_payload': json.dumps({})
        })

    return pd.DataFrame(registros)


@test
def test_output(output, *args) -> None:
    """
    Test vacío, solo para cumplir con la estructura.
    """
    pass
