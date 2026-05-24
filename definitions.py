from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest
)
import assets
import checks
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

pipeline_job = define_asset_job(
    name="pipeline_completo_job",
    selection=AssetSelection.all()
)

@sensor(job=pipeline_job)
def sensor_datos(context):
    """
    Vigila los ficheros de datos en la carpeta data.
    Si alguno cambia, lanza el pipeline completo.
    """
    ficheros = [
        'rentamedia-sc-3.csv',
        'distribucion-renta-ingresos.csv',
        'ocupacion-sc-3.csv',
        'actividad-sc-3.csv'
    ]

    last_mtime = context.cursor or "0"
    max_mtime = last_mtime

    for fichero in ficheros:
        ruta = os.path.join(DATA_DIR, fichero)
        if os.path.exists(ruta):
            curr_mtime = str(os.path.getmtime(ruta))
            if curr_mtime > max_mtime:
                max_mtime = curr_mtime

    if max_mtime != last_mtime:
        context.update_cursor(max_mtime)
        yield RunRequest(run_key=max_mtime)


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([checks]),
    jobs=[pipeline_job],
    sensors=[sensor_datos]
)