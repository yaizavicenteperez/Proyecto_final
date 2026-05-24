# Trabajo final visualización

## Descripción
Se implementa un pipeline de datos en Dagster que carga la renta bruta media por hogar del ISTAC, la distribución de fuentes de ingreso de la población y el nivel de ocupación por sección censal en la isla de Tenerife. Se realiza una transformación y cruce de estos datos con geometrías oficiales (GeoJSON) para generar 7 visualizaciones que analizan la brecha socioeconómica y su evolución temporal a nivel municipal, de sección y por zonas geográficas.

## Estructura del proyecto
Programas:
- assets.py: pipeline de carga, transformación y generación de gráficos en Dagster.
- checks.py: validaciones de calidad de datos (Data Quality Checks) para proteger el pipeline y las visualizaciones.
- definitions:  definitions.py: punto de entrada de Dagster que agrupa y expone los assets, los checks y los jobs del proyecto para la interfaz web.

Datos:
- rentamedia-sc-3.csv: datos de renta bruta media por hogar (ISTAC).
- distribucion-renta-ingresos.csv: datos de distribución de fuentes de ingreso (ISTAC).
- ocupacion-sc-3.csv: datos de ocupación y sectores económicos (INE).
- secciones_[AÑO]0101_tenerife.json: archivos GeoJSON con las delimitaciones de las secciones censales para los años 2021, 2022, 2023 y 2024.

## Visualizaciones generadas
1. Mapa coroplético de zona: 'Renta bruta media por zona geográfica en Tenerife (2023)'
2. Mapa coroplético de zona: 'Porcentaje de ocupaciones elementales por zona geográfica (2023)'
3. Gráfico de barras apiladas de zona: 'Distribución media de fuentes de ingreso por zona en Tenerife (2023)'
4. Mapa coroplético de detalle: 'Renta bruta media por hogar en Tenerife (2023)' (por sección censal)
5. Mapa coroplético de detalle: 'Porcentaje de ocupaciones elementales por sección censal (2023)'
6. Gráfico de barras apiladas municipal: 'Distribución de fuentes de ingreso por municipio en Tenerife (2023)' (agrupado por zonas)
7. Gráfico de líneas temporales: 'Evolución de la renta media por zona geográfica en Tenerife (2021-2023)'

## Paquetes utilizados
- Dagster
- Pandas
- Geopandas
- Plotnine
