if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import json
import pandas as pd
from datetime import datetime
from mage_ai.data_preparation.decorators import transformer,test

@transformer
def transform(data, *args, **kwargs):

    pipeline_name=kwargs.get('pipeline_uuid',"").lower()

    if 'invoice'in pipeline_name:
        tabla='qb_invoices_raw'
    elif'customer' in pipeline_name:
        tabla='qb_customers-raw'
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
    print(f"Datos preparados:{len(df)}registros")

    return df


    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
