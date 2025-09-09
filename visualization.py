# %% 
# 1. Импорт библиотек
import polars as pl
import glob

# %%
# 2. Загрузка данных
data_files = glob.glob("data/*.csv")
print(*data_files, sep="\n")
# %% 3. Обработка данных
df = pl.concat([
    pl.read_csv(file).select([
        pl.col('id').cast(pl.Int64),
        pl.col('date').cast(pl.String),
        pl.col('value').cast(pl.Float64)
    ])
    for file in data_files
])
unique_ids = df.select("id").unique().sort("id")["id"].to_list()
print(*unique_ids, sep="\n")

# Получаем min/max дат и значений из collected.csv
date_min, date_max, value_min, value_max = pl.read_csv("data/raw/collected.csv").select(
    pl.col('collected').min().alias('min_date'), 
    pl.col('collected').max().alias('max_date'),
    pl.col('property_value').min().alias('min_value'),
    pl.col('property_value').max().alias('max_value')
).row(0)

# Вычисляем зум по умолчанию на 1/3 диапазона по X
from datetime import datetime

# Конвертируем даты в datetime для вычислений
date_min_dt = datetime.fromisoformat(date_min.replace('T', ' ').replace('Z', ''))
date_max_dt = datetime.fromisoformat(date_max.replace('T', ' ').replace('Z', ''))
date_range = date_max_dt - date_min_dt
zoom_range = date_range / 3
zoom_start = date_min_dt + date_range / 4  # начинаем с 1/4 диапазона  
zoom_end = zoom_start + zoom_range

# Конвертируем обратно в строки для altair
zoom_start_str = zoom_start.isoformat()
zoom_end_str = zoom_end.isoformat()

# Загружаем collected.csv для второго слоя
collected_df = pl.read_csv("data/raw/collected.csv").select([
    pl.col('item_id').alias('id'),
    pl.col('collected').alias('date'), 
    pl.col('property_value').cast(pl.Float64).alias('value')  # приведение к Float64
]).with_columns([
    pl.col('id').map_elements(lambda x: unique_ids.index(x) if x in unique_ids else -1, return_dtype=pl.Int64).alias('id_index')
]).filter(pl.col('id_index') >= 0)
# %%
# 4. Интерактивная визуализация
import altair as alt
alt.data_transformers.disable_max_rows()

# Создаем маппинг ID -> имя файла
import os
id_to_filename = {}
for file_path in data_files:
    filename = os.path.basename(file_path)  # получаем только имя файла
    # Извлекаем ID из имени файла (series_159782957_filled.csv -> 159782957)
    import re
    id_match = re.search(r'series_(\d+)', filename)
    if id_match:
        id_from_filename = int(id_match.group(1))
    id_to_filename[id_from_filename] = filename

# Объединяем данные с метками типов
combined_data = pl.concat([
    df.with_columns([
        pl.col('id').map_elements(lambda x: unique_ids.index(x), return_dtype=pl.Int64).alias('id_index'),
        pl.col('id').map_elements(lambda x: id_to_filename.get(x, f'series_{x}.csv'), return_dtype=pl.String).alias('layer')
    ]),
    collected_df.with_columns(pl.lit('Collected данные').alias('layer'))
])

# Слайдер для переключения между ID
slider = alt.selection_point(
    fields=['id_index'], 
    bind=alt.binding_range(min=0, max=len(unique_ids)-1, step=1, name="ID: "), 
    value=[{'id_index': 0}]
)

base_chart = alt.Chart(combined_data).add_params(slider)

common_encoding = {
    'x': alt.X('date:T', scale=alt.Scale(domain=[zoom_start_str, zoom_end_str])),
    'y': alt.Y('value:Q', scale=alt.Scale(domain=[value_min, value_max])),
    'color': alt.Color(
        'layer:N', 
        scale=alt.Scale(scheme='set2'),  # colorblind-safe palette
        legend=alt.Legend(
            title="Тип данных",
            orient='none',
            legendX=1000,  # скорректировано под новую ширину
            legendY=50,    # отступ от верха
            direction='vertical',
            fillColor='white',
            strokeColor='gray',
            padding=10
        )
    ),
    'tooltip': ['value:Q', 'id:N', alt.Tooltip('date:T', format='%d.%m.%Y %H:%M', title='Дата и время')]
}

# Series данные (линии с точками)
series_layer = (base_chart
                .mark_line(
                    point=alt.OverlayMarkDef(size=100), 
                    strokeWidth=2, 
                    opacity=0.5
                )
                .encode(**common_encoding)
                .transform_filter(slider)
                .transform_filter(alt.datum.layer != 'Collected данные'))

# Collected данные (точки)
collected_layer = (base_chart
                   .mark_point(size=30, opacity=1, filled=True)
                   .encode(**common_encoding)
                   .transform_filter(slider)
                   .transform_filter(alt.datum.layer == 'Collected данные'))

chart = (series_layer + collected_layer).properties(
    width=1200, 
    height=600, 
    title="Временные ряды: Series (линии) + Collected (точки)"
).interactive()

chart
# %%