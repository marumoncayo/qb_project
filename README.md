
# Proyecto QBO Backfill - Mage + Postgres

## Descripción
Este proyecto implementa pipelines de backfill de QuickBooks Online (QBO) usando **Mage** para la extracción, transformación y carga (ETL) de datos hacia **Postgres**.  
Permite procesar datos de las entidades:

- `Invoice`
- `Customer`
- `Item`

Los pipelines están diseñados para soportar:

- Segmentación por rango de fechas (`fecha_inicio`, `fecha_fin` en UTC).
- Idempotencia: reejecución de un tramo no genera duplicados.
- Paginación y manejo de rate limits de la API de QBO.
- Registro de metadatos y volumetría.

---

## Arquitectura
```
+----------------+       +-----------------+       +-------------------+
| Mage Pipeline  | --->  | Postgres DB     | <---  | Mage Secrets       |
| qb_invoices    |       | raw schema      |       | (tokens, cred)    |
| qb_customers   |       | qb_<entidad>_raw|       +-------------------+
| qb_items       |       +-----------------+
+----------------+
El proyecto está diseñado para extraer datos de QuickBooks Online mediante pipelines específicos para facturas, clientes y artículos. Cada pipeline se conecta a la API de QuickBooks utilizando credenciales seguras almacenadas en Mage Secrets y obtiene los datos en tramos definidos por rangos de fecha. Una vez obtenidos, los datos se transforman dentro de Mage, donde se les añade un conjunto de metadatos esenciales como identificadores únicos, fecha de ingestión y detalles del tramo de extracción, además del payload completo de cada registro en formato JSON. Posteriormente, estos datos se cargan en una base de datos PostgreSQL dentro de un esquema crudo, donde cada entidad tiene su propia tabla. La inserción se realiza de forma idempotente, asegurando que si un tramo se vuelve a ejecutar no se generen duplicados.




PASOS PARA LEVANTAR LOS CONTENEDORES


1. Instalar dependencias necesarias, es decir Docker desktop
2. Crear la carpeta del proyecto (yo lo hice desde powershell) cd qbo-backfill
3. Crear archivo .env
Dentro de la carpeta del proyecto, cree un archivo .env con las variables necesarias para Postgres y PgAdmin:
POSTGRES_USER=pguser
POSTGRES_PASSWORD=pgpass123
POSTGRES_DB=analytics
PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=pgadminpass
4. Cree los volúmenes para persistencia de datos con la sentencia:
docker volume create postgres_data

5. Verifique el  docker-compose.yml
Me aseguré de que el archivo docker-compose.yml contenga los servicios postgres, pgadmin y mage con sus respectivos puertos y volúmenes. Luce de esta manera:

version: '3.8'
services:
  postgres:
    image: postgres:15
    container_name: postgres
    restart: always
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  pgadmin:
    image: dpage/pgadmin4
    container_name: pgadmin
    restart: always
    environment:
      PGADMIN_DEFAULT_EMAIL: ${PGADMIN_DEFAULT_EMAIL}
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_DEFAULT_PASSWORD}
    ports:
      - "5050:80"

  mage:
    image: mage_ai_image
    container_name: mage
    ports:
      - "6789:6789"

volumes:
  postgres_data:
6. Levanté los contenedores con este comando:

docker-compose up -d
7. Verifique que esten corriendo
8. Accedí a los servicios de pgadmin, mage y postgres
9. Accedí los secrets de Quickbooks y postgres en el mage secrets
```



GESTION DE SECRETOS


| Secreto            | Propósito             
| ------------------ | ---------------------- 
| `qb_client_id`     | Cliente QBO           
| `qb_secret_id`     | Secreto QBO           
| `qb_refresh_token` | Token refresco QBO     
| `pg_host`          | Host Postgres          
| `pg_port`          | Puerto Postgres        
| `pg_user`          | Usuario Postgres       
| `pg_password`      | Password Postgres      
| `pg_db`            | Base de datos Postgres 




DETALLES DE LOS PIPELINES

Los pipelines qb_invoices_backfill, qb_customers_backfill y qb_items_backfill están diseñados para extraer datos de QuickBooks Online dentro de rangos de fechas definidos por los parámetros fecha_inicio y fecha_fin, ambos en UTC. Para manejar grandes volúmenes de información, cada pipeline puede ejecutarse en tramos temporales, garantizando que si un tramo se vuelve a ejecutar, no se generen duplicados gracias a la idempotencia implementada.
Durante la extracción, los pipelines respetan los límites de la API de QuickBooks y cuentan con reintentos automáticos en caso de fallos. Para la operación y monitoreo, se dispone de un runbook que indica cómo revisar los logs de ejecución en Mage, validar que las tablas raw.qb_invoices, raw.qb_customers y raw.qb_items contengan registros, y reejecutar los tramos que hayan fallado.

##Triggers one time
El trigger one-time se configura para cada uno de los pipelines qb_invoices_backfill, qb_customers_backfill y qb_items_backfill. Su fecha y hora de inicio se definen en UTC y se documenta también su equivalente en la zona horaria de America/Guayaquil. Una vez que la ejecución del trigger concluye correctamente, este se deshabilita o se marca como completado para evitar que se vuelva a ejecutar de manera accidental. En casos donde se necesite procesar rangos de datos muy grandes, es posible planificar múltiples triggers encadenados, ejecutando cada tramo de manera secuencial.
```

Esquema raw (Postgres)
+---------------------+---------------------------------------------------------------+
| Tabla               | Campos obligatorios                                           |
+---------------------+---------------------------------------------------------------+
| qb_invoices_raw     | id (PK), payload (JSONB), ingested_at_utc,                  |
|                     | extract_window_start_utc, extract_window_end_utc,           |
|                     | page_number, page_size, request_payload                      |
+---------------------+---------------------------------------------------------------+
| qb_customers_raw    | id (PK), payload (JSONB), ingested_at_utc,                  |
|                     | extract_window_start_utc, extract_window_end_utc,           |
|                     | page_number, page_size, request_payload                      |
+---------------------+---------------------------------------------------------------+
| qb_items_raw        | id (PK), payload (JSONB), ingested_at_utc,                  |
|                     | extract_window_start_utc, extract_window_end_utc,           |
|                     | page_number, page_size, request_payload                      |
+---------------------+---------------------------------------------------------------+


```
VALIDACIONES Y VOLUMETRIA 

Se ejecutaron querys en postgres para validar que los registros se cargaron. Se ejecutaron los siguientes querys:
```
SELECT COUNT(*) FROM raw.qb_invoices_raw;
SELECT COUNT(*) FROM raw.qb_customers_raw;
SELECT COUNT(*) FROM raw.qb_items_raw;
```
Tmabien se hizo como verificacion una comparación con la API de QuickBooks Online (QBO), ya que se verifica que el número de registros en Postgres coincida con la cantidad reportada por QBO en el mismo rango de fechas, asegurando que todos los tramos fueron procesados.

Ademas se hace una revisión de metadatos:

ingested_at_utc: indica la fecha y hora en que cada registro fue insertado o actualizado en Postgres, lo que permite detectar reingestiones y validar la idempotencia.

extract_window_start_utc y extract_window_end_utc: muestran el rango de fechas de cada tramo del pipeline, ayudando a identificar gaps o superposiciones en la carga de datos.

TROUBLESHOOTING
Durante la ejecución de los pipelines qb_invoices_backfill, qb_customers_backfill y qb_items_backfill pueden surgir distintos problemas relacionados con autenticación, paginación, límites de API, zonas horarias, almacenamiento y permisos.

En cuanto a la autenticación, es muy importante que las credenciales de QuickBooks Online y de PostgreSQL estén configuradas correctamente en Mage Secrets. Si un token de acceso expira o se introduce incorrectamente, los pipelines fallarán inmediatamente. Para solucionarlo, se debe regenerar el token en QBO, actualizarlo en Mage Secrets y volver a ejecutar el pipeline. Siempre se debe validar que los nombres de los secretos coincidan con los utilizados en los scripts.

La paginación es crítica para manejar grandes volúmenes de datos provenientes de la API de QBO. Cada pipeline procesa los registros en tramos o páginas, de manera que si no se implementa correctamente, se podrían omitir registros o generar duplicados. Es importante verificar que los parámetros page_number y page_size se actualicen de manera consistente y que la lógica de reingestión respete la idempotencia.

Los límites de la API de QBO pueden provocar errores cuando se excede la cantidad de solicitudes permitidas por minuto. Para esto, los pipelines incluyen manejos de rate limits y reintentos automáticos con backoff exponencial. 

En cuanto a las zonas horarias, todos los pipelines trabajan internamente en UTC para la ingestión.  La correcta interpretación de extract_window_start_utc y extract_window_end_utc es crucial para garantizar que los tramos temporales sean precisos y no se generen huecos o solapamientos.

El almacenamiento en PostgreSQL debe ser suficiente para los volúmenes esperados. Si las tablas raw crecen demasiado, pueden presentarse problemas de rendimiento en inserciones o consultas. Es recomendable revisar periódicamente el tamaño de las tablas, purgar registros antiguos si es necesario y asegurarse de que los índices estén creados correctamente.

Finalmente, los permisos en la base de datos deben permitir a Mage insertar, actualizar y leer registros en el esquema raw. Problemas de permisos pueden manifestarse como errores de NOT NULL o permission denied. 

CHECKLIST DE ACEPTACION 
si-Mage y Postgres se comunican por nombre de servicio.
si-Todos los secretos (QBO y Postgres) están en Mage Secrets; no hay secretos en el
repo/entorno expuesto.
si- Pipelines qb_<entidad>_backfill acepta fecha_inicio y fecha_fin (UTC) y
segmenta el rango.
si- Trigger one-time configurado, ejecutado y luego deshabilitado/marcado como
completado.
si- Esquema raw con tablas por entidad, payload completo y metadatos obligatorios.
si- Idempotencia verificada: reejecución de un tramo no genera duplicados.
si- Paginación y rate limits manejados y documentados.
si- Volumetría y validaciones mínimas registradas y archivadas como evidencia.
si- Runbook de reanudación y reintentos disponible y seguido.




