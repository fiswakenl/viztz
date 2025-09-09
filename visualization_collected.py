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

# Базовый чарт с параметрами
base_chart = alt.Chart(final_data).add_params(group_param)

# Основная визуализация точек
points = base_chart.mark_point(
    size=50,  # увеличенные точки
    opacity=0.8
).encode(
    x=alt.X('date:T', title='Дата'),
    y=alt.Y('value:Q', title='Значение'),
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
        alt.Tooltip('value:Q', format='.0f', title='Значение'), 
        alt.Tooltip('id:N', title='ID'),
        alt.Tooltip('unique_days:Q', title='Заполненных дней')
    ]
).transform_filter(
    # Показываем только выбранную группу
    alt.datum.group_number == group_param
).properties(
    width=1200, 
    height=700, 
    title=alt.Title(
        "Collected данные: отсортировано по количеству заполненных дней",
        subtitle=f"Фильтр: >{MIN_DAYS} дней. Группы по 20 ID. Всего {filtered_ids.height} ID в {total_groups} группах"
    )
).interactive()

# Отображаем график
points
# %%