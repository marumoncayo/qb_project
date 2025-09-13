import psycopg2
import json
from datetime import datetime, timezone
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_invoices(data, *args, **kwargs):
    """
    Inserta registros en la tabla raw.qb_invoices en Postgres con UPSERT (idempotente).
    Maneja rows que pueden venir como dict o string JSON (para triggers).
    """
    print("Conectando a Postgres...")

    conn = psycopg2.connect(
        host=get_secret_value('pg_host'),
        port=get_secret_value('pg_port'),
        dbname=get_secret_value('pg_db'),
        user=get_secret_value('pg_user'),
        password=get_secret_value('pg_password')
    )
    cursor = conn.cursor()

    print("Conexión establecida")

    # Crear esquema y tabla si no existen
    cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.qb_invoices (
        id TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        ingested_at_utc TIMESTAMPTZ NOT NULL,
        extract_window_start_utc TIMESTAMPTZ NOT NULL,
        extract_window_end_utc TIMESTAMPTZ NOT NULL,
        page_number INT NOT NULL,
        page_size INT NOT NULL,
        request_payload JSONB
    );
    """)

    # Convertir DataFrame a lista de registros
    if hasattr(data, "to_dict"):
        rows = data.to_dict(orient="records")
    else:
        rows = data

    if not rows:
        print("No hay registros para insertar, saliendo...")
        return

    insertados = 0
    for i, row in enumerate(rows):
        # Parsear row si es string JSON
        if isinstance(row, str):
            row = json.loads(row)

        payload = row.get("payload") or row
        record_id = row.get("id") or row.get("Id") or f"temp_{i}"

        ahora = datetime.now(timezone.utc)
        ingested_at = row.get("ingested_at_utc") or ahora
        start_utc = row.get("extract_window_start_utc") or ahora.replace(hour=0, minute=0, second=0, microsecond=0)
        end_utc = row.get("extract_window_end_utc") or ahora
        page_number = row.get("page_number", 1)
        page_size = row.get("page_size", len(rows))
        request_payload = row.get("request_payload", {})

        cursor.execute("""
            INSERT INTO raw.qb_invoices (
                id, payload, ingested_at_utc, extract_window_start_utc,
                extract_window_end_utc, page_number, page_size, request_payload
            )
            VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                payload = EXCLUDED.payload,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                extract_window_start_utc = EXCLUDED.extract_window_start_utc,
                extract_window_end_utc = EXCLUDED.extract_window_end_utc,
                page_number = EXCLUDED.page_number,
                page_size = EXCLUDED.page_size,
                request_payload = EXCLUDED.request_payload;
        """, (
            record_id,
            json.dumps(payload),
            ingested_at,
            start_utc,
            end_utc,
            page_number,
            page_size,
            json.dumps(request_payload),
        ))

        insertados += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Exportación terminada: {insertados} registros insertados/actualizados en raw.qb_invoices")
