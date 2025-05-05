import pandas as pd

def transform_data(filepath: str) -> pd.DataFrame:
    print(f"[DEBUG] Чтение файла {filepath} с автоопределением разделителя")

    try:
        # Читаем первые строки с двумя вариантами разделителя
        df_comma = pd.read_csv(filepath, sep=',', nrows=5)
        df_semicolon = pd.read_csv(filepath, sep=';', nrows=5)

        # Логгируем количество колонок и названия
        print(f"[DEBUG] ',' -> {len(df_comma.columns)} колонок: {list(df_comma.columns)}")
        print(f"[DEBUG] ';' -> {len(df_semicolon.columns)} колонок: {list(df_semicolon.columns)}")

        # Выбираем вариант с большим числом колонок
        chosen_sep = ';' if len(df_semicolon.columns) > len(df_comma.columns) else ','
        print(f"[DEBUG] Выбран разделитель: '{chosen_sep}'")

        # Чтение всего файла с нужным разделителем
        return pd.read_csv(filepath, sep=chosen_sep)

    except Exception as e:
        print(f"[ERROR] Ошибка при чтении файла {filepath}: {e}")
        raise
