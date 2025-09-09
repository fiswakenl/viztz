# %%
# Нормализация данных мониторинга загруженности БД под формат series_*.csv
import polars as pl
import glob
import os
import re
from pathlib import Path

# %%
# Функции для обработки данных

def parse_decimal_comma(value_str):
    """Конвертирует запятые в точки для десятичных чисел"""
    if isinstance(value_str, str):
        return float(value_str.replace(',', '.'))
    return float(value_str) if value_str is not None else None

def extract_numeric_value(value_str):
    """Извлекает числовое значение из строк с единицами измерения"""
    if value_str is None or value_str == '':
        return None
    
    value_str = str(value_str).strip()
    
    # Обрабатываем единицы измерения
    if 'TB' in value_str:
        numeric = re.search(r'(\d+(?:[.,]\d+)?)', value_str)
        if numeric:
            return parse_decimal_comma(numeric.group(1)) * 1000  # TB -> GB
    elif 'GB' in value_str:
        numeric = re.search(r'(\d+(?:[.,]\d+)?)', value_str)
        if numeric:
            return parse_decimal_comma(numeric.group(1))
    elif 'MB' in value_str:
        numeric = re.search(r'(\d+(?:[.,]\d+)?)', value_str)
        if numeric:
            return parse_decimal_comma(numeric.group(1)) / 1000  # MB -> GB
    else:
        # Обычное число с запятой как разделителем
        return parse_decimal_comma(value_str)

def normalize_file(file_path, output_dir):
    """Нормализует один файл в формат id,date,value"""
    filename = os.path.basename(file_path)
    print(f"Обрабатываем: {filename}")
    
    try:
        # Читаем файл с обработкой ошибок
        if 'errrors.csv' in filename:
            df = pl.read_csv(file_path, ignore_errors=True, infer_schema_length=10000)
        else:
            df = pl.read_csv(file_path, infer_schema_length=10000)
        
        normalized_dfs = []
        
        if filename.startswith('err') and filename.endswith('.csv'):
            # err1.csv, err2.csv, err3.csv, err4.csv, err5.csv
            err_num = re.search(r'err(\d+)', filename)
            if err_num:
                series_id = 1000 + int(err_num.group(1))  # err1=1001, err2=1002, etc.
            else:
                series_id = 1000  # fallback для errrors.csv
            
            if 'errrors.csv' in filename:
                # Фильтруем пустые строки и обрабатываем единицы
                df_clean = df.filter(
                    pl.any_horizontal([pl.col(c).is_not_null() for c in df.columns if c != 'created_at'])
                )
                
                if df_clean.height > 0:
                    # Используем count как основную метрику
                    normalized_df = df_clean.select([
                        pl.lit(series_id).alias('id'),
                        pl.col('created_at').alias('date'),
                        pl.col('count').map_elements(extract_numeric_value, return_dtype=pl.Float64).alias('value')
                    ]).filter(pl.col('value').is_not_null())
                    
                    if normalized_df.height > 0:
                        normalized_dfs.append(normalized_df)
                        
            elif 'errrors1.csv' in filename:
                # Обрабатываем как count с единицами измерения
                df_with_units = df.with_columns([
                    pl.concat_str([pl.col('count'), pl.col('')], separator=' ').alias('value_with_unit')
                ])
                
                normalized_df = df_with_units.select([
                    pl.lit(series_id).alias('id'),
                    pl.col('created_at').alias('date'),
                    pl.col('value_with_unit').map_elements(extract_numeric_value, return_dtype=pl.Float64).alias('value')
                ]).filter(pl.col('value').is_not_null())
                
                if normalized_df.height > 0:
                    normalized_dfs.append(normalized_df)
                    
            else:
                # err1-5.csv: простая конвертация запятых
                normalized_df = df.select([
                    pl.lit(series_id).alias('id'),
                    pl.col('created_at').alias('date'),
                    pl.col('count').map_elements(parse_decimal_comma, return_dtype=pl.Float64).alias('value')
                ]).filter(pl.col('value').is_not_null())
                
                if normalized_df.height > 0:
                    normalized_dfs.append(normalized_df)
        
        elif filename == 'PG02.csv':
            # PostgreSQL метрики: создаем два отдельных ряда
            # odm_std_08 (ID=3001) и postgres (ID=3002)
            for col_name, series_id in [('odm_std_08', 3001), ('postgres', 3002)]:
                if col_name in df.columns:
                    normalized_df = df.select([
                        pl.lit(series_id).alias('id'),
                        pl.col('Time').alias('date'),
                        pl.col(col_name).map_elements(extract_numeric_value, return_dtype=pl.Float64).alias('value')
                    ]).filter(pl.col('value').is_not_null())
                    
                    if normalized_df.height > 0:
                        normalized_dfs.append(normalized_df)
        
        elif filename == 'wd.csv':
            # Working Directory метрики
            normalized_df = df.select([
                pl.lit(4001).alias('id'),
                pl.col('created_at').alias('date'),
                pl.col('count').map_elements(parse_decimal_comma, return_dtype=pl.Float64).alias('value')
            ]).filter(pl.col('value').is_not_null())
            
            if normalized_df.height > 0:
                normalized_dfs.append(normalized_df)
        
        elif filename == 'wd_time_range.csv':
            # Предполагаем что большие числа это байты, конвертируем в GB
            col_names = df.columns
            normalized_df = df.select([
                pl.lit(4002).alias('id'),
                pl.col(col_names[0]).alias('date'),  # первый столбец - дата
                (pl.col(col_names[1]) / 1_000_000_000).alias('value')  # второй столбец - значение в байтах -> GB
            ]).filter(pl.col('value').is_not_null())
            
            if normalized_df.height > 0:
                normalized_dfs.append(normalized_df)
        
        # Сохраняем результаты
        for i, norm_df in enumerate(normalized_dfs):
            if norm_df.height > 0:
                series_id = norm_df['id'][0]
                output_filename = f"series_{series_id}.csv"
                if i > 0:  # Если несколько серий из одного файла
                    output_filename = f"series_{series_id}_{i}.csv"
                
                output_path = os.path.join(output_dir, output_filename)
                norm_df.write_csv(output_path)
                print(f"  -> Сохранено: {output_filename} ({norm_df.height} строк)")
        
    except Exception as e:
        print(f"  -> Ошибка обработки {filename}: {e}")

# %%
# Основная обработка

# Создаем директорию для результатов
output_dir = "data/normalized"
Path(output_dir).mkdir(parents=True, exist_ok=True)

# Обрабатываем все CSV файлы в data/new/
input_files = glob.glob("data/new/*.csv")
print(f"Найдено {len(input_files)} файлов для обработки:\n")

for file_path in sorted(input_files):
    normalize_file(file_path, output_dir)

print(f"\nОбработка завершена. Результаты в директории: {output_dir}")

# %%
# Проверяем результаты
print("\n=== РЕЗУЛЬТАТЫ НОРМАЛИЗАЦИИ ===")
result_files = glob.glob(f"{output_dir}/*.csv")

for result_file in sorted(result_files):
    try:
        df = pl.read_csv(result_file)
        filename = os.path.basename(result_file)
        print(f"{filename}: {df.shape[0]} строк, ID={df['id'][0]}, период {df['date'].min()} - {df['date'].max()}")
    except Exception as e:
        print(f"Ошибка чтения {result_file}: {e}")

# %%