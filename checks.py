import os
from dagster import asset_check, AssetCheckResult, MetadataValue
import assets


# ============================================================
# CHECKS DE CARGA
# ============================================================

@asset_check(asset=assets.raw_renta_media)
def check_nulos_renta(raw_renta_media):
    """
    Verifica que los nulos en renta media no superan el 5%.
    Protege viz_mapa_renta_2023 y viz_evolucion_renta_zonas.
    """
    total = len(raw_renta_media)
    nulos = int(raw_renta_media['OBS_VALUE'].isna().sum())
    pct_nulos = nulos / total * 100
    return AssetCheckResult(
        passed=bool(pct_nulos < 5),
        metadata={
            "total_filas": MetadataValue.int(total),
            "nulos": MetadataValue.int(nulos),
            "pct_nulos": MetadataValue.float(round(pct_nulos, 2))
        }
    )


@asset_check(asset=assets.raw_distribucion_ingresos)
def check_rango_porcentajes(raw_distribucion_ingresos):
    """
    Verifica que los porcentajes están entre 0 y 100.
    Protege viz_distribucion_zonas_2023 y viz_distribucion_ingresos_2023.
    """
    df_limpio = raw_distribucion_ingresos['OBS_VALUE'].dropna()
    fuera_rango = int(((df_limpio < 0) | (df_limpio > 100)).sum())
    return AssetCheckResult(
        passed=bool(fuera_rango == 0),
        metadata={
            "valores_fuera_rango": MetadataValue.int(fuera_rango),
            "valor_min": MetadataValue.float(float(df_limpio.min())),
            "valor_max": MetadataValue.float(float(df_limpio.max()))
        }
    )


@asset_check(asset=assets.raw_renta_media)
def check_anos_completos_renta(raw_renta_media):
    """
    Verifica que los tres años (2021-2023) están presentes.
    Protege viz_evolucion_renta_zonas.
    """
    anos_esperados = {2021, 2022, 2023}
    anos_presentes = set(raw_renta_media['año'].unique())
    faltantes = sorted(anos_esperados - anos_presentes)
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        metadata={
            "anos_esperados": MetadataValue.text(str(sorted(anos_esperados))),
            "anos_presentes": MetadataValue.text(str(sorted(anos_presentes))),
            "anos_faltantes": MetadataValue.text(str(faltantes))
        }
    )


@asset_check(asset=assets.raw_distribucion_ingresos)
def check_anos_completos_distribucion(raw_distribucion_ingresos):
    """
    Verifica que los tres años (2021-2023) están presentes.
    Protege viz_distribucion_zonas_2023.
    """
    anos_esperados = {2021, 2022, 2023}
    anos_presentes = set(raw_distribucion_ingresos['año'].unique())
    faltantes = sorted(anos_esperados - anos_presentes)
    return AssetCheckResult(
        passed=len(faltantes) == 0,
        metadata={
            "anos_esperados": MetadataValue.text(str(sorted(anos_esperados))),
            "anos_presentes": MetadataValue.text(str(sorted(anos_presentes))),
            "anos_faltantes": MetadataValue.text(str(faltantes))
        }
    )


@asset_check(asset=assets.raw_geojson)
def check_n_secciones_geojson(raw_geojson):
    """
    Verifica que cada GeoJSON contiene entre 580 y 610 secciones de Tenerife.
    Protege todos los mapas coropléticos activos.
    """
    resultados = {}
    passed = True
    for año, gdf in raw_geojson.items():
        n = len(gdf)
        resultados[año] = n
        if n < 580 or n > 610:
            passed = False

    return AssetCheckResult(
        passed=passed,
        metadata={
            f"secciones_{año}": MetadataValue.int(cant) for año, cant in resultados.items()
        }
    )


# ============================================================
# CHECKS DE TRANSFORMACIÓN
# ============================================================

@asset_check(asset=assets.distribucion_ingresos_procesada)
def check_suma_porcentajes(distribucion_ingresos_procesada):
    """
    Verifica que los porcentajes por sección suman aproximadamente 100%.
    Protege viz_distribucion_zonas_2023 y viz_distribucion_ingresos_2023.
    """
    df = distribucion_ingresos_procesada.dropna(subset=['porcentaje'])
    sumas = df.groupby(['TERRITORIO_CODE', 'año'])['porcentaje'].sum()
    incorrectas = sumas[(sumas < 98) | (sumas > 102)]
    return AssetCheckResult(
        passed=bool(len(incorrectas) == 0),
        metadata={
            "secciones_incorrectas": MetadataValue.int(len(incorrectas)),
            "suma_min": MetadataValue.float(float(sumas.min())),
            "suma_max": MetadataValue.float(float(sumas.max()))
        }
    )


@asset_check(asset=assets.renta_media_procesada)
def check_n_secciones(renta_media_procesada):
    """
    Verifica que el número de secciones por año está entre 670 y 690.
    Protege el cruce de datos temporales agregados.
    """
    secciones_por_ano = renta_media_procesada.groupby('año')['TERRITORIO_CODE'].nunique()
    fuera_rango = secciones_por_ano[(secciones_por_ano < 670) | (secciones_por_ano > 690)]
    return AssetCheckResult(
        passed=bool(len(fuera_rango) == 0),
        metadata={"secciones_por_ano": MetadataValue.text(str(secciones_por_ano.to_dict()))}
    )


@asset_check(asset=assets.datos_mapa)
def check_join_mapa(datos_mapa):
    """
    Verifica que el merge con el GeoJSON no pierde secciones.
    Protege viz_mapa_renta_2023.
    """
    resultados = {}
    passed = True
    for año, gdf in datos_mapa.items():
        n_secciones = len(gdf)
        n_con_geometria = int(gdf.geometry.notna().sum())
        n_sin_renta = int(gdf['renta_media'].isna().sum())
        resultados[año] = {
            "total": n_secciones,
            "con_geometria": n_con_geometria,
            "sin_renta": n_sin_renta
        }
        if n_con_geometria < n_secciones:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={"resumen": MetadataValue.text(str(resultados))}
    )


@asset_check(asset=assets.datos_ocupaciones_elementales)
def check_ocupaciones_elementales(datos_ocupaciones_elementales):
    """
    Verifica que los porcentajes calculados están entre 0 y 100.
    Protege viz_mapa_ocupaciones_elementales y viz_mapa_ocup_zonas_2023.
    """
    passed = True
    resultados = {}
    for año, datos in datos_ocupaciones_elementales.items():
        gdf = datos['secciones']
        n_con_dato = int(gdf['pct_elementales'].notna().sum())
        pct_max = float(gdf['pct_elementales'].max()) if n_con_dato > 0 else 0
        pct_min = float(gdf['pct_elementales'].min()) if n_con_dato > 0 else 0
        if pct_max > 100 or pct_min < 0:
            passed = False
        resultados[año] = {
            "secciones": len(gdf),
            "con_dato": n_con_dato,
            "pct_min": round(pct_min, 2),
            "pct_max": round(pct_max, 2)
        }
    return AssetCheckResult(
        passed=passed,
        metadata={"resumen": MetadataValue.text(str(resultados))}
    )


@asset_check(asset=assets.zonas_tenerife)
def check_zonas_tenerife(zonas_tenerife):
    """
    Verifica que se generan exactamente 3 zonas con geometría válida.
    Protege todos los mapas que superponen los límites de zona.
    """
    zonas_esperadas = {'Norte', 'Metropolitana', 'Sur'}
    zonas_presentes = set(zonas_tenerife['zona'].tolist())
    faltantes = zonas_esperadas - zonas_presentes
    geometrias_vacias = int(zonas_tenerife.geometry.is_empty.sum())
    return AssetCheckResult(
        passed=bool(len(faltantes) == 0 and geometrias_vacias == 0),
        metadata={
            "zonas_esperadas": MetadataValue.text(str(sorted(zonas_esperadas))),
            "zonas_presentes": MetadataValue.text(str(sorted(zonas_presentes))),
            "zonas_faltantes": MetadataValue.text(str(sorted(faltantes))),
            "geometrias_vacias": MetadataValue.int(geometrias_vacias)
        }
    )


@asset_check(asset=assets.datos_zonas)
def check_datos_zonas(datos_zonas):
    """
    Verifica que el dataset de zonas tiene datos para los tres años y las tres zonas.
    Protege viz_evolucion_renta_zonas y viz_mapa_renta_zonas_2023.
    """
    df_renta = datos_zonas['renta_por_zona']
    anos_presentes = set(df_renta['año'].unique())
    zonas_presentes = set(df_renta['zona'].unique())
    anos_esperados = {2021, 2022, 2023}
    zonas_esperadas = {'Norte', 'Metropolitana', 'Sur'}
    anos_faltantes = sorted(anos_esperados - anos_presentes)
    zonas_faltantes = sorted(zonas_esperadas - zonas_presentes)
    return AssetCheckResult(
        passed=bool(len(anos_faltantes) == 0 and len(zonas_faltantes) == 0),
        metadata={
            "anos_faltantes": MetadataValue.text(str(anos_faltantes)),
            "zonas_faltantes": MetadataValue.text(str(zonas_faltantes)),
            "combinaciones": MetadataValue.int(len(df_renta))
        }
    )


# ============================================================
# CHECKS DE VISUALIZACIÓN
# ============================================================

def _check_png(ruta):
    existe = os.path.exists(ruta)
    tamano = os.path.getsize(ruta) if existe else 0
    return AssetCheckResult(
        passed=bool(existe and tamano > 0),
        metadata={
            "archivo": MetadataValue.text(str(ruta)),
            "existe": MetadataValue.text(str(existe)),
            "tamano_bytes": MetadataValue.int(int(tamano))
        }
    )


@asset_check(asset=assets.viz_mapa_renta_zonas_2023)
def check_png_mapa_renta_zonas_2023(viz_mapa_renta_zonas_2023):
    return _check_png(viz_mapa_renta_zonas_2023)


@asset_check(asset=assets.viz_mapa_ocup_zonas_2023)
def check_png_mapa_ocup_zonas_2023(viz_mapa_ocup_zonas_2023):
    return _check_png(viz_mapa_ocup_zonas_2023)


@asset_check(asset=assets.viz_distribucion_zonas_2023)
def check_png_distribucion_zonas(viz_distribucion_zonas_2023):
    return _check_png(viz_distribucion_zonas_2023)


@asset_check(asset=assets.viz_mapa_renta_2023)
def check_png_mapa_renta_2023(viz_mapa_renta_2023):
    return _check_png(viz_mapa_renta_2023)


@asset_check(asset=assets.viz_mapa_ocupaciones_elementales)
def check_png_mapa_ocupaciones(viz_mapa_ocupaciones_elementales):
    return _check_png(viz_mapa_ocupaciones_elementales)


@asset_check(asset=assets.viz_distribucion_ingresos_2023)
def check_png_distribucion_ingresos(viz_distribucion_ingresos_2023):
    return _check_png(viz_distribucion_ingresos_2023)


@asset_check(asset=assets.viz_evolucion_renta_zonas)
def check_png_evolucion_renta_zonas(viz_evolucion_renta_zonas):
    return _check_png(viz_evolucion_renta_zonas)