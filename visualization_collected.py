# %%
# Новая визуализация collected данных с сортировкой по количеству заполненных дней
import polars as pl
import altair as alt

alt.data_transformers.disable_max_rows()

# %%
# 1. Загрузка и анализ данных
print("Загружаем и анализируем collected.csv...")

# Загружаем collected данные
df = pl.read_csv("data/raw/collected.csv").select([
    pl.col('item_id').alias('id'),
    pl.col('collected').alias('date'),  # оставляем как datetime
    pl.col('property_value').cast(pl.Float64).alias('value')
])

# %%
# 1.1. Загрузка дополнительных серий из normalized данных
print("Загружаем дополнительные серии из data/normalized/without_unit/...")

import os
from pathlib import Path

additional_series_data = []
normalized_dir = Path("data/normalized/with_unit/")

for csv_file in normalized_dir.glob("*.csv"):
    print(f"Загружаем {csv_file.name}...")
    series_df = pl.read_csv(str(csv_file)).select([
        'id', 'date', 'value'  # игнорируем колонку unit
    ]).with_columns([
        pl.lit("additional").alias("series_type")  # помечаем как дополнительные серии
    ])
    additional_series_data.append(series_df)

# Объединяем все дополнительные серии
if additional_series_data:
    additional_df = pl.concat(additional_series_data)
    print(f"Загружено {len(additional_series_data)} дополнительных серий, всего строк: {additional_df.height}")
    
    # Получаем список уникальных ID дополнительных серий
    additional_series_ids = additional_df['id'].unique().sort().to_list()
    print(f"Дополнительные серии: {additional_series_ids}")
else:
    additional_df = pl.DataFrame(schema={"id": pl.Utf8, "date": pl.Utf8, "value": pl.Float64, "series_type": pl.Utf8})
    additional_series_ids = []

# Подсчитываем количество уникальных дней для каждого ID
# Создаем колонку только с датой (без времени)
df = df.with_columns([
    pl.col('date').str.slice(0, 10).alias('date_only')  # берем только YYYY-MM-DD
])

days_per_id = df.group_by('id').agg([
    pl.col('date_only').n_unique().alias('unique_days'),
    pl.col('date').count().alias('total_points')
]).sort('unique_days', descending=True)

print(f"Всего уникальных ID: {days_per_id.height}")
print(f"Максимум дней у одного ID: {days_per_id['unique_days'].max()}")
print(f"Минимум дней у одного ID: {days_per_id['unique_days'].min()}")

# %%
# 2. Фильтрация и сортировка
# Хардкод фильтр: только ID с >20 заполненными днями (для производительности)
MIN_DAYS = 20
filtered_ids = days_per_id.filter(pl.col('unique_days') > MIN_DAYS)

print(f"ID с >{MIN_DAYS} днями: {filtered_ids.height}")

# Получаем отсортированный список ID
sorted_id_list = filtered_ids['id'].to_list()

# Фильтруем основные данные
filtered_data = df.filter(pl.col('id').is_in(sorted_id_list))

# Добавляем информацию о количестве дней
filtered_data = filtered_data.join(
    filtered_ids.select(['id', 'unique_days']), 
    on='id', 
    how='left'
)

print(f"Отфильтрованных строк данных: {filtered_data.height}")

# %%
# 3. Создание групп по 10 ID
# Создаем маппинг ID -> номер группы
id_to_group = {}
for i, id_val in enumerate(sorted_id_list):
    group_num = i // 10  # 10 ID на группу (для производительности)
    id_to_group[id_val] = group_num

# Добавляем номер группы к данным
final_data = filtered_data.with_columns([
    pl.col('id').map_elements(lambda x: id_to_group.get(x, 0), return_dtype=pl.Int64).alias('group_number')
])

total_groups = max(id_to_group.values()) + 1 if id_to_group else 1
print(f"Всего групп: {total_groups}")

# %%
# 4. Интерактивная визуализация

# Параметр для переключения между группами
group_param = alt.param(
    value=0,
    bind=alt.binding_range(
        min=0, 
        max=total_groups-1, 
        step=1, 
        name=f"Группа (0-{total_groups-1}): "
    )
)

# Параметр для включения/отключения линий
connect_lines = alt.param(
    value=False,
    bind=alt.binding_checkbox(name="Соединять точки линиями: ")
)

# Параметр для выбора единиц измерения
unit_multiplier = alt.param(
    value=1,
    bind=alt.binding_radio(
        options=[1, 1024, 1024**2, 1024**3],
        labels=["GB", "MB", "KB", "bytes"],
        name="Единицы измерения: "
    )
)

# Создание чекбоксов для дополнительных серий
additional_series_params = {}
if additional_series_ids:
    print(f"Создаем чекбоксы для {len(additional_series_ids)} дополнительных серий...")
    for series_id in additional_series_ids:
        # Создаем параметр-чекбокс для каждой серии
        param_name = f"{series_id}_checkbox"
        additional_series_params[series_id] = alt.param(
            value=False,
            bind=alt.binding_checkbox(name=f"Показать {series_id}: ")
        )

# Подготавливаем дополнительные серии для объединения
if additional_series_ids:
    # Добавляем колонку date_only для дополнительных данных
    additional_df_with_date = additional_df.with_columns([
        pl.col('date').str.slice(0, 10).alias('date_only')  # берем только YYYY-MM-DD
    ])
    
    # Конвертируем основные данные в строковые ID для совместимости
    final_data_str = final_data.with_columns([
        pl.col('id').cast(pl.Utf8)
    ])
    
    # Вычисляем реальные значения unique_days для дополнительных серий
    additional_days_per_id = additional_df_with_date.group_by('id').agg([
        pl.col('date_only').n_unique().alias('unique_days')
    ])
    
    print(f"Подсчитанные дни для дополнительных серий:")
    for row in additional_days_per_id.iter_rows(named=True):
        print(f"  {row['id']}: {row['unique_days']} дней")
    
    # Объединяем дополнительные данные с подсчитанными днями
    additional_df_with_days = additional_df_with_date.join(
        additional_days_per_id, 
        on='id', 
        how='left'
    )
    
    # Добавляем колонки для совместимости с основными данными
    prepared_additional = additional_df_with_days.with_columns([
        pl.lit(-1).cast(pl.Int64).alias('group_number'),  # специальная группа для дополнительных серий
    ]).drop('series_type').select([
        'id', 'date', 'value', 'date_only', 'unique_days', 'group_number'  # тот же порядок что и в основных данных
    ])
    
    # Проверяем схемы данных
    print("Схема основных данных:", final_data_str.schema)
    print("Схема дополнительных данных:", prepared_additional.schema)
    
    # Объединяем все данные (основные + дополнительные)
    combined_data = pl.concat([final_data_str, prepared_additional])
else:
    # Если нет дополнительных серий, также конвертируем основные данные в строковые ID
    combined_data = final_data.with_columns([pl.col('id').cast(pl.Utf8)])

# Базовый чарт с параметрами
all_params = [group_param, connect_lines, unit_multiplier] + list(additional_series_params.values())
base_chart = alt.Chart(combined_data).add_params(*all_params)

# Основная визуализация точек
points = base_chart.mark_point(
    size=50,  # увеличенные точки
    opacity=0.8
).transform_calculate(
    converted_value=f'datum.group_number == -1 ? datum.value * {unit_multiplier.name} : datum.value'
).encode(
    x=alt.X('date:T', title='Дата'),
    y=alt.Y('converted_value:Q', title='Значение'),
    color=alt.Color(
        'id:N', 
        scale=alt.Scale(scheme='category20'),
        legend=alt.Legend(
            title="ID",
            orient='right',
            columns=1,
            symbolLimit=10,  # обновлено для 10 ID
            titleLimit=100
        )
    ),
    tooltip=[
        alt.Tooltip('date:T', format='%d.%m.%Y %H:%M:%S', title='Дата и время'),
        alt.Tooltip('converted_value:Q', format='.0f', title='Значение'), 
        alt.Tooltip('id:N', title='ID'),
        alt.Tooltip('unique_days:Q', title='Заполненных дней')
    ]
).transform_filter(
    # Показываем выбранную группу ИЛИ выбранные дополнительные серии
    f"datum.group_number == {group_param.name}" +
    ("" if not additional_series_params else 
     " || (" + " || ".join([
         f"(datum.id == '{series_id}' && {param.name})" 
         for series_id, param in additional_series_params.items()
     ]) + ")")
)

# Слой линий для соединения точек одного ID
lines = base_chart.mark_line(
    strokeWidth=1
).transform_calculate(
    converted_value=f'datum.group_number == -1 ? datum.value * {unit_multiplier.name} : datum.value'
).encode(
    x=alt.X('date:T', sort='ascending'),  # сортировка по времени
    y=alt.Y('converted_value:Q'),
    color=alt.Color('id:N', scale=alt.Scale(scheme='category20'), legend=None),  # без легенды для линий
    opacity=alt.condition(connect_lines, alt.value(0.6), alt.value(0)),  # видимость через checkbox
    detail='id:N'  # группировка по ID для отдельных линий
).transform_filter(
    # Показываем выбранную группу ИЛИ выбранные дополнительные серии
    f"datum.group_number == {group_param.name}" +
    ("" if not additional_series_params else 
     " || (" + " || ".join([
         f"(datum.id == '{series_id}' && {param.name})" 
         for series_id, param in additional_series_params.items()
     ]) + ")")
)

# Объединяем слои точек и линий
chart = alt.layer(points, lines).properties(
    width=1200, 
    height=700, 
    title=alt.Title(
        "Collected данные: отсортировано по количеству заполненных дней",
        subtitle=f"Фильтр: >{MIN_DAYS} дней. Группы по 10 ID. Всего {filtered_ids.height} ID в {total_groups} группах"
    )
).interactive()

# Отображаем график
chart
# %%