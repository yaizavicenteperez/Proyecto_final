import os
import pandas as pd
import geopandas as gpd
from dagster import asset, Output, MetadataValue

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'outputs')

ZONA_MAP = {
    'Buenavista del Norte': 'Norte', 'Los Silos': 'Norte',
    'El Tanque': 'Norte', 'Garachico': 'Norte',
    'Icod de los Vinos': 'Norte', 'La Guancha': 'Norte',
    'San Juan de la Rambla': 'Norte', 'Los Realejos': 'Norte',
    'Puerto de La Cruz': 'Norte', 'La Orotava': 'Norte',
    'La Matanza de Acentejo': 'Norte', 'La Victoria de Acentejo': 'Norte',
    'Santa Úrsula': 'Norte', 'El Sauzal': 'Norte', 'Tacoronte': 'Norte',
    'San Cristóbal de La Laguna': 'Metropolitana', 'Tegueste': 'Metropolitana',
    'Santa Cruz de Tenerife': 'Metropolitana', 'El Rosario': 'Metropolitana',
    'Candelaria': 'Sur', 'Arafo': 'Sur', 'Güímar': 'Sur',
    'Fasnia': 'Sur', 'Arico': 'Sur', 'Granadilla de Abona': 'Sur',
    'San Miguel de Abona': 'Sur', 'Vilaflor de Chasna': 'Sur',
    'Santiago del Teide': 'Sur', 'Guía de Isora': 'Sur',
    'Adeje': 'Sur', 'Arona': 'Sur'
}

ORDEN_MUNICIPIOS_CON_SEP = [
    'Buenavista del Norte', 'Los Silos', 'El Tanque', 'Garachico',
    'Icod de los Vinos', 'La Guancha', 'San Juan de la Rambla',
    'Los Realejos', 'Puerto de La Cruz', 'La Orotava',
    'La Matanza de Acentejo', 'La Victoria de Acentejo',
    'Santa Úrsula', 'El Sauzal', 'Tacoronte',
    '__sep1__',
    'San Cristóbal de La Laguna', 'Tegueste',
    'Santa Cruz de Tenerife', 'El Rosario',
    '__sep2__',
    'Candelaria', 'Arafo', 'Güímar', 'Fasnia', 'Arico',
    'Granadilla de Abona', 'San Miguel de Abona',
    'Vilaflor de Chasna', 'Santiago del Teide',
    'Guía de Isora', 'Adeje', 'Arona'
]

ORDEN_APILADO_INGRESOS = [
    'Otras prestaciones', 'Otros ingresos', 'Pensiones',
    'Prestaciones por desempleo', 'Sueldos y salarios'
]

ORDEN_LABELS_INGRESOS = [
    'Sueldos y salarios', 'Prestaciones por desempleo', 'Pensiones',
    'Otros ingresos', 'Otras prestaciones'
]

COLORES_ZONA = {
    'Norte': '#2980b9',
    'Metropolitana': '#f39c12',
    'Sur': '#c0392b'
}


# ============================================================
# ASSETS DE CARGA
# ============================================================

@asset
def raw_renta_media():
    df = pd.read_csv(os.path.join(DATA_DIR, 'rentamedia-sc-3.csv'))
    return Output(value=df, metadata={
        "filas": MetadataValue.int(len(df)),
        "anos": MetadataValue.text(str(sorted(df['año'].unique().tolist()))),
        "secciones": MetadataValue.int(df['TERRITORIO_CODE'].nunique()),
        "nulos_obs_value": MetadataValue.int(int(df['OBS_VALUE'].isna().sum()))
    })


@asset
def raw_distribucion_ingresos():
    df = pd.read_csv(os.path.join(DATA_DIR, 'distribucion-renta-ingresos.csv'))
    df['OBS_VALUE'] = df['OBS_VALUE'].str.replace(',', '.').astype(float)
    return Output(value=df, metadata={
        "filas": MetadataValue.int(len(df)),
        "anos": MetadataValue.text(str(sorted(df['año'].unique().tolist()))),
        "tipos_ingreso": MetadataValue.text(str(df['MEDIDAS#es'].unique().tolist())),
        "nulos_obs_value": MetadataValue.int(int(df['OBS_VALUE'].isna().sum()))
    })


@asset
def raw_ocupacion():
    df = pd.read_csv(os.path.join(DATA_DIR, 'ocupacion-sc-3.csv'))
    return Output(value=df, metadata={
        "filas": MetadataValue.int(len(df)),
        "anos": MetadataValue.text(str(sorted(df['año'].unique().tolist()))),
        "sectores": MetadataValue.text(str(df['ocupacion'].unique().tolist())),
        "nulos": MetadataValue.int(int(df.isnull().sum().sum()))
    })


@asset
def raw_geojson():
    gdfs = {}
    for año in [2021, 2022, 2023, 2024]:
        ruta = os.path.join(DATA_DIR, f'secciones_{año}0101_tenerife.json')
        gdf = gpd.read_file(ruta)
        gdf = gdf[gdf['gcd_isla'] == 'ES709'].copy()
        gdfs[año] = gdf
    return Output(value=gdfs, metadata={
        "secciones_por_ano": MetadataValue.text(str({año: len(gdf) for año, gdf in gdfs.items()}))
    })


# ============================================================
# ASSETS DE TRANSFORMACIÓN
# ============================================================

@asset
def renta_media_procesada(raw_renta_media):
    df = raw_renta_media.copy()
    df = df[df['MEDIDAS#es'] == 'Renta bruta media por hogar'].copy()
    df = df[['año', 'TERRITORIO_CODE', 'municipio', 'distrito', 'seccion', 'OBS_VALUE']].copy()
    df = df.rename(columns={'OBS_VALUE': 'renta_media'})
    df['municipio'] = df['municipio'].str.strip()
    return Output(value=df, metadata={
        "filas": MetadataValue.int(len(df)),
        "secciones": MetadataValue.int(df['TERRITORIO_CODE'].nunique()),
        "renta_min": MetadataValue.float(float(df['renta_media'].min())),
        "renta_max": MetadataValue.float(float(df['renta_media'].max())),
        "nulos": MetadataValue.int(int(df['renta_media'].isna().sum()))
    })


@asset
def distribucion_ingresos_procesada(raw_distribucion_ingresos):
    df = raw_distribucion_ingresos.copy()
    df['municipio'] = df['municipio'].str.strip()
    df = df.rename(columns={'MEDIDAS#es': 'tipo_ingreso'})
    df = df[['año', 'TERRITORIO_CODE', 'municipio', 'tipo_ingreso', 'OBS_VALUE']].copy()
    df = df.rename(columns={'OBS_VALUE': 'porcentaje'})
    return Output(value=df, metadata={
        "filas": MetadataValue.int(len(df)),
        "tipos_ingreso": MetadataValue.text(str(df['tipo_ingreso'].unique().tolist())),
        "secciones": MetadataValue.int(df['TERRITORIO_CODE'].nunique()),
        "nulos": MetadataValue.int(int(df['porcentaje'].isna().sum()))
    })


@asset
def ocupacion_procesada(raw_ocupacion):
    df = raw_ocupacion.copy()
    df = df.groupby(['año', 'geocode', 'municipio', 'ocupacion'], as_index=False)['num_casos'].sum()
    df['municipio'] = df['municipio'].str.strip()
    df['geocode'] = df.apply(
        lambda r: r['geocode'].replace(str(r['año']) + '0101', str(r['año'] + 1) + '0101'), axis=1
    )
    return Output(value=df, metadata={
        "filas": MetadataValue.int(len(df)),
        "secciones": MetadataValue.int(df['geocode'].nunique()),
        "ocupaciones": MetadataValue.text(str(df['ocupacion'].unique().tolist()))
    })


@asset
def municipios_tenerife(raw_geojson):
    gdf = raw_geojson[2024].copy()
    gdf['nombre_mun'] = gdf['etiqueta'].str.split(' - ').str[-1].str.strip()
    municipios = sorted(gdf['nombre_mun'].unique().tolist())
    return Output(value=municipios, metadata={
        "n_municipios": MetadataValue.int(len(municipios)),
        "municipios": MetadataValue.text(str(municipios))
    })


@asset
def zonas_tenerife(raw_geojson):
    gdf = raw_geojson[2024].copy()
    gdf['municipio'] = gdf['etiqueta'].str.split(' - ').str[-1].str.strip()
    gdf['zona'] = gdf['municipio'].map(ZONA_MAP)
    gdf_zonas = gdf[gdf['zona'].notna()].dissolve(by='zona').reset_index()
    return Output(value=gdf_zonas, metadata={
        "n_zonas": MetadataValue.int(len(gdf_zonas)),
        "zonas": MetadataValue.text(str(sorted(gdf_zonas['zona'].tolist())))
    })


@asset
def datos_mapa(renta_media_procesada, distribucion_ingresos_procesada, raw_geojson):
    """Une renta e ingreso dominante con el GeoJSON para cada año"""
    mapas = {}
    for año_datos in [2021, 2022, 2023]:
        año_geo = año_datos + 1
        gdf = raw_geojson[año_geo].copy()

        renta = renta_media_procesada[
            renta_media_procesada['año'] == año_datos
        ][['TERRITORIO_CODE', 'renta_media', 'municipio']]

        dist = distribucion_ingresos_procesada[
            (distribucion_ingresos_procesada['año'] == año_datos) &
            (distribucion_ingresos_procesada['porcentaje'].notna())
        ]
        ingreso_dominante = dist.loc[
            dist.groupby('TERRITORIO_CODE')['porcentaje'].idxmax()
        ][['TERRITORIO_CODE', 'tipo_ingreso']].rename(
            columns={'tipo_ingreso': 'ingreso_dominante'}
        )

        gdf = gdf.merge(renta, left_on='geocode', right_on='TERRITORIO_CODE', how='left')
        gdf = gdf.merge(ingreso_dominante, left_on='geocode', right_on='TERRITORIO_CODE', how='left')
        gdf['año'] = año_datos
        mapas[año_datos] = gdf

    return Output(value=mapas, metadata={
        "secciones_2021": MetadataValue.int(len(mapas[2021])),
        "secciones_2022": MetadataValue.int(len(mapas[2022])),
        "secciones_2023": MetadataValue.int(len(mapas[2023])),
        "columnas": MetadataValue.text(str(list(mapas[2023].columns)))
    })


@asset
def datos_ocupaciones_elementales(raw_ocupacion, raw_geojson):
    resultados = {}
    for año_datos in [2021, 2022, 2023]:
        año_geo = año_datos + 1
        gdf = raw_geojson[año_geo].copy()
        gdf_municipios = gdf.dissolve(by='gcd_municipio').reset_index()

        df = raw_ocupacion[raw_ocupacion['año'] == año_datos].copy()
        df['geocode'] = df['geocode'].str.replace(
            str(año_datos) + '0101', str(año_geo) + '0101'
        )

        df_agg = df.groupby(['geocode', 'ocupacion'])['num_casos'].sum().reset_index()
        df_total = df_agg.groupby('geocode')['num_casos'].sum().reset_index()
        df_total.columns = ['geocode', 'total']

        df_elem = df_agg[df_agg['ocupacion'] == 'Ocupaciones elementales'].copy()
        df_elem = df_elem.merge(df_total, on='geocode')
        df_elem['pct_elementales'] = df_elem['num_casos'] / df_elem['total'] * 100

        gdf_merged = gdf.merge(df_elem[['geocode', 'pct_elementales']], on='geocode', how='left')
        resultados[año_datos] = {'secciones': gdf_merged, 'municipios': gdf_municipios}

    return Output(value=resultados, metadata={
        "anos": MetadataValue.text(str(list(resultados.keys())))
    })


@asset
def datos_zonas(raw_geojson, renta_media_procesada, raw_ocupacion):
    municipios_tenerife = list(ZONA_MAP.keys())

    df_renta = renta_media_procesada.copy()
    df_renta = df_renta[df_renta['municipio'].isin(municipios_tenerife)].copy()
    df_renta['zona'] = df_renta['municipio'].map(ZONA_MAP)
    df_zona_renta = df_renta.groupby(['año', 'zona'], as_index=False)['renta_media'].mean()
    df_zona_renta['etiqueta'] = df_zona_renta['renta_media'].round(0).astype(int).astype(str) + '€'

    df_ocup = raw_ocupacion[raw_ocupacion['año'] == 2023].copy()
    df_ocup['municipio'] = df_ocup['municipio'].str.strip()
    df_ocup['zona'] = df_ocup['municipio'].map(ZONA_MAP)
    df_ocup = df_ocup[df_ocup['zona'].notna()].copy()
    df_ocup_agg = df_ocup.groupby(['zona', 'ocupacion'])['num_casos'].sum().reset_index()
    df_ocup_total = df_ocup_agg.groupby('zona')['num_casos'].sum().reset_index()
    df_ocup_total.columns = ['zona', 'total']
    df_elem = df_ocup_agg[df_ocup_agg['ocupacion'] == 'Ocupaciones elementales'].copy()
    df_elem = df_elem.merge(df_ocup_total, on='zona')
    df_elem['pct_elementales'] = df_elem['num_casos'] / df_elem['total'] * 100
    df_elem['etiqueta'] = df_elem['pct_elementales'].round(1).astype(str) + '%'

    gdf = raw_geojson[2024].copy()
    gdf['municipio'] = gdf['etiqueta'].str.split(' - ').str[-1].str.strip()
    gdf['zona'] = gdf['municipio'].map(ZONA_MAP)
    gdf_zonas = gdf[gdf['zona'].notna()].dissolve(by='zona').reset_index()

    return Output(
        value={
            'renta_por_zona': df_zona_renta,
            'ocup_por_zona': df_elem,
            'gdf_zonas': gdf_zonas
        },
        metadata={
            "anos_renta": MetadataValue.text(str(sorted(df_zona_renta['año'].unique().tolist()))),
            "zonas": MetadataValue.text(str(sorted(gdf_zonas['zona'].tolist())))
        }
    )


# ============================================================
# ASSETS DE VISUALIZACIÓN
# ============================================================

from plotnine import (
    ggplot, aes, geom_map, geom_col, geom_line, geom_text, geom_point,
    scale_fill_gradient, scale_fill_manual, scale_color_manual,
    scale_x_continuous, scale_x_discrete, scale_y_continuous,
    labs, theme_void, theme_minimal, theme, element_text
)

PALETA_INGRESOS = {
    'Sueldos y salarios': '#2ecc71',
    'Pensiones': '#3498db',
    'Prestaciones por desempleo': '#e74c3c',
    'Otras prestaciones': '#e67e22',
    'Otros ingresos': '#9b59b6'
}

COLOR_MUNICIPIOS = '#444444'

THEME_MAPA = theme(
    figure_size=(16, 12),
    plot_title=element_text(size=16, weight='bold', ha='center'),
    plot_subtitle=element_text(size=11, ha='center'),
    plot_caption=element_text(size=9),
    legend_title=element_text(size=11),
    legend_text=element_text(size=10),
    legend_position='right',
    plot_margin=0.05
)


def _scale_renta(name='Renta bruta\nmedia (€)'):
    return scale_fill_gradient(
        low='#c7e9c0', high='#1a7d34',
        na_value='#cccccc', name=name
    )


def _scale_ocup(name='% ocupaciones\nelementales'):
    return scale_fill_gradient(
        low='#f2e6f9', high='#6a1b9a',
        na_value='#cccccc', name=name
    )


def _geom_limites_municipios(gdf_municipios):
    return geom_map(gdf_municipios, fill='none', color=COLOR_MUNICIPIOS, size=0.3)


def _geom_limites_zonas(gdf_zonas):
    return geom_map(gdf_zonas, aes(color='zona'), fill='none', size=2.0)


def _scale_color_zonas():
    return scale_color_manual(values=COLORES_ZONA, name='Zona')


def _preparar_distribucion(df, municipios_tenerife):
    df = df[
        (df['año'] == 2023) &
        (df['municipio'].isin(municipios_tenerife))
    ].copy()
    df['porcentaje'] = df['porcentaje'].fillna(0)
    df_mun = df.groupby(['municipio', 'tipo_ingreso'], as_index=False)['porcentaje'].mean()
    df_mun['zona'] = df_mun['municipio'].map(ZONA_MAP)
    return df_mun


@asset
def viz_mapa_renta_zonas_2023(datos_zonas):
    df_zona_renta = datos_zonas['renta_por_zona']
    gdf_zonas = datos_zonas['gdf_zonas']

    gdf_mapa = gdf_zonas.merge(
        df_zona_renta[df_zona_renta['año'] == 2023][['zona', 'renta_media', 'etiqueta']],
        on='zona', how='left'
    )
    gdf_mapa['zona_color'] = gdf_mapa['zona']

    centroides = pd.DataFrame({
        'zona': gdf_mapa['zona'].values,
        'etiqueta': gdf_mapa['etiqueta_y'].values,
        'x': gdf_mapa.geometry.centroid.x.values,
        'y': gdf_mapa.geometry.centroid.y.values
    })

    grafico = (
        ggplot()
        + geom_map(gdf_mapa, aes(fill='renta_media'), color='white', size=0.5)
        + geom_map(gdf_mapa, aes(color='zona_color'), fill='none', size=2.0)
        + geom_text(centroides, aes(x='x', y='y', label='etiqueta'), size=11, color='black')
        + _scale_renta()
        + scale_color_manual(values=COLORES_ZONA, name='Zona')
        + labs(
            title='Renta bruta media por zona geográfica en Tenerife (2023)',
            subtitle='Media de los municipios de cada zona.',
            caption='Fuente: ISTAC'
        )
        + theme_void()
        + THEME_MAPA
    )

    ruta = os.path.join(OUTPUT_DIR, 'mapa_renta_zonas_2023.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})



@asset
def viz_mapa_ocup_zonas_2023(datos_zonas):
    df_elem = datos_zonas['ocup_por_zona']
    gdf_zonas = datos_zonas['gdf_zonas']

    gdf_mapa = gdf_zonas.merge(
        df_elem[['zona', 'pct_elementales', 'etiqueta']],
        on='zona', how='left'
    )
    gdf_mapa['zona_color'] = gdf_mapa['zona']

    centroides = pd.DataFrame({
        'zona': gdf_mapa['zona'].values,
        'etiqueta': gdf_mapa['etiqueta_y'].values,
        'x': gdf_mapa.geometry.centroid.x.values,
        'y': gdf_mapa.geometry.centroid.y.values
    })

    grafico = (
        ggplot()
        + geom_map(gdf_mapa, aes(fill='pct_elementales'), color='white', size=0.5)
        + geom_map(gdf_mapa, aes(color='zona_color'), fill='none', size=2.0)
        + geom_text(centroides, aes(x='x', y='y', label='etiqueta'), size=11, color='black')
        + _scale_ocup()
        + scale_color_manual(values=COLORES_ZONA, name='Zona')
        + labs(
            title='Porcentaje de ocupaciones elementales por zona geográfica (2023)',
            subtitle='Porcentaje sobre el total de ocupados de cada zona.',
            caption='Fuente: INE'
        )
        + theme_void()
        + THEME_MAPA
    )

    ruta = os.path.join(OUTPUT_DIR, 'mapa_ocup_zonas_2023.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})


@asset
def viz_distribucion_zonas_2023(distribucion_ingresos_procesada, municipios_tenerife):
    df_mun = _preparar_distribucion(distribucion_ingresos_procesada, municipios_tenerife)

    df_zona = df_mun[df_mun['zona'].notna()].groupby(
        ['zona', 'tipo_ingreso'], as_index=False
    )['porcentaje'].mean()

    df_zona_labels = df_zona.copy()
    df_zona_labels['tipo_ingreso'] = pd.Categorical(
        df_zona_labels['tipo_ingreso'], categories=ORDEN_LABELS_INGRESOS, ordered=True
    )
    df_zona_labels = df_zona_labels.sort_values(['zona', 'tipo_ingreso'])
    df_zona_labels['cumsum'] = df_zona_labels.groupby('zona')['porcentaje'].cumsum()
    df_zona_labels['pos'] = df_zona_labels['cumsum'] - df_zona_labels['porcentaje'] / 2
    df_zona_labels['etiqueta'] = df_zona_labels['porcentaje'].round(1).astype(str) + '%'

    df_zona['tipo_ingreso'] = pd.Categorical(
        df_zona['tipo_ingreso'], categories=ORDEN_APILADO_INGRESOS, ordered=True
    )
    df_zona['zona'] = pd.Categorical(
        df_zona['zona'], categories=['Norte', 'Metropolitana', 'Sur'], ordered=True
    )
    df_zona_labels['zona'] = pd.Categorical(
        df_zona_labels['zona'], categories=['Norte', 'Metropolitana', 'Sur'], ordered=True
    )

    grafico = (
        ggplot(df_zona, aes(x='zona', y='porcentaje', fill='tipo_ingreso'))
        + geom_col(position='stack')
        + geom_text(df_zona_labels, aes(x='zona', y='pos', label='etiqueta'), size=8, color='white')
        + scale_fill_manual(values=PALETA_INGRESOS, name='Fuente de ingreso')
        + scale_y_continuous(limits=(0, 101))
        + labs(
            title='Distribución media de fuentes de ingreso por zona en Tenerife (2023)',
            subtitle='Media de los municipios de cada zona geográfica.',
            x='Zona', y='Porcentaje (%)', caption='Fuente: ISTAC'
        )
        + theme_minimal()
        + theme(
            figure_size=(10, 7),
            plot_title=element_text(size=14, weight='bold', ha='center'),
            plot_subtitle=element_text(size=10, ha='center'),
            axis_text_x=element_text(size=11, weight='bold'),
            legend_position='bottom'
        )
    )

    ruta = os.path.join(OUTPUT_DIR, 'distribucion_zonas_2023.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})




@asset
def viz_mapa_renta_2023(datos_mapa, raw_geojson, zonas_tenerife):
    gdf = datos_mapa[2023].copy()
    gdf_municipios = raw_geojson[2024].dissolve(by='gcd_municipio').reset_index()

    grafico = (
        ggplot()
        + geom_map(gdf, aes(fill='renta_media'), color='white', size=0.05)
        + _geom_limites_municipios(gdf_municipios)
        + _geom_limites_zonas(zonas_tenerife)
        + _scale_color_zonas()
        + _scale_renta()
        + labs(
            title='Renta bruta media por hogar en Tenerife (2023)',
            subtitle='Por sección censal. Líneas finas: municipios. Líneas gruesas: zonas.',
            caption='Fuente: ISTAC'
        )
        + theme_void()
        + THEME_MAPA
    )

    ruta = os.path.join(OUTPUT_DIR, 'mapa_renta_2023.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})


@asset
def viz_mapa_ocupaciones_elementales(datos_ocupaciones_elementales, zonas_tenerife):
    datos_2023 = datos_ocupaciones_elementales[2023]
    gdf = datos_2023['secciones']
    gdf_municipios = datos_2023['municipios']

    grafico = (
        ggplot()
        + geom_map(gdf, aes(fill='pct_elementales'), color='white', size=0.05)
        + _geom_limites_municipios(gdf_municipios)
        + _geom_limites_zonas(zonas_tenerife)
        + _scale_color_zonas()
        + _scale_ocup()
        + labs(
            title='Porcentaje de ocupaciones elementales por sección censal (2023)',
            subtitle='Líneas finas: municipios. Líneas gruesas: zonas.',
            caption='Fuente: INE'
        )
        + theme_void()
        + THEME_MAPA
    )

    ruta = os.path.join(OUTPUT_DIR, 'mapa_ocupaciones_elementales_2023.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})


@asset
def viz_distribucion_ingresos_2023(distribucion_ingresos_procesada, municipios_tenerife):
    df_mun = _preparar_distribucion(distribucion_ingresos_procesada, municipios_tenerife)

    df_mun_sep = df_mun.copy()
    for sep in ['__sep1__', '__sep2__']:
        for tipo in df_mun_sep['tipo_ingreso'].unique():
            df_mun_sep = pd.concat([df_mun_sep, pd.DataFrame({
                'municipio': [sep], 'tipo_ingreso': [tipo],
                'porcentaje': [0], 'zona': [None]
            })], ignore_index=True)

    df_mun_sep['municipio'] = pd.Categorical(
        df_mun_sep['municipio'], categories=ORDEN_MUNICIPIOS_CON_SEP, ordered=True
    )

    grafico = (
        ggplot(df_mun_sep, aes(x='municipio', y='porcentaje', fill='tipo_ingreso'))
        + geom_col(position='stack')
        + scale_fill_manual(values=PALETA_INGRESOS, name='Fuente de ingreso')
        + scale_x_discrete(
            labels=lambda x: ['' if v.startswith('__sep') else v for v in x]
        )
        + scale_y_continuous(limits=(0, 101))
        + labs(
            title='Distribución de fuentes de ingreso por municipio en Tenerife (2023)',
            subtitle='Ordenado por zona geográfica: Norte → Metropolitana → Sur',
            x='Municipio', y='Porcentaje (%)', caption='Fuente: ISTAC'
        )
        + theme_minimal()
        + theme(
            figure_size=(16, 8),
            plot_title=element_text(size=14, weight='bold', ha='center'),
            plot_subtitle=element_text(size=10, ha='center'),
            axis_text_x=element_text(rotation=45, hjust=1, size=9),
            legend_position='bottom'
        )
    )

    ruta = os.path.join(OUTPUT_DIR, 'distribucion_ingresos_2023.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})



@asset
def viz_evolucion_renta_zonas(datos_zonas):
    df_zona_renta = datos_zonas['renta_por_zona'].copy()
    df_zona_renta['zona'] = pd.Categorical(
        df_zona_renta['zona'],
        categories=['Norte', 'Metropolitana', 'Sur'],
        ordered=True
    )

    grafico = (
        ggplot(df_zona_renta, aes(x='año', y='renta_media', color='zona', group='zona'))
        + geom_line(size=1.2)
        + geom_point(size=4)
        + geom_text(aes(label='etiqueta'), nudge_y=500, size=8)
        + scale_color_manual(values=COLORES_ZONA, name='Zona')
        + scale_x_continuous(breaks=[2021, 2022, 2023])
        + labs(
            title='Evolución de la renta media por zona geográfica en Tenerife (2021-2023)',
            subtitle='Media de la renta bruta media por hogar de los municipios de cada zona.',
            x='Año', y='Renta bruta media por hogar (€)', caption='Fuente: ISTAC'
        )
        + theme_minimal()
        + theme(
            figure_size=(10, 6),
            plot_title=element_text(size=14, weight='bold', ha='center'),
            plot_subtitle=element_text(size=10, ha='center'),
            legend_position='right'
        )
    )

    ruta = os.path.join(OUTPUT_DIR, 'evolucion_renta_zonas.png')
    grafico.save(ruta, dpi=200)
    return Output(value=ruta, metadata={"ruta": MetadataValue.text(ruta)})