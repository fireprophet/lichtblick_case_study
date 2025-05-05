import os
import json
import difflib
import pandas as pd
from etl.schemas import EXPECTED_COLUMNS

def transform_data(file_path: str, entity: str) -> pd.DataFrame:
    print(f"[TRANSFORM] Чтение файла: {file_path}")
    matching_dir = os.getenv("MATCHING_DIR", "/app/matchings")
    os.makedirs(matching_dir, exist_ok=True)
    matching_file = os.path.join(matching_dir, f"matching_{os.path.basename(file_path).replace('.csv', '.json')}")

    # ✅ Используем сохранённый delimiter, если есть
    if os.path.exists(matching_file):
        try:
            with open(matching_file) as f:
                match_data = json.load(f)
                delimiter = match_data.get("__delimiter__")
                if delimiter:
                    print(f"[TRANSFORM] 📎 Используем сохранённый разделитель: '{delimiter}'")
                    df = pd.read_csv(file_path, sep=delimiter)
                    df.columns = [col.strip().replace('\ufeff', '') for col in df.columns]
                    print(f"[DEBUG] 📋 Колонки после чтения: {list(df.columns)}")
                    if check_column_match(df, entity, file_path, delimiter):
                        print(f"[TRANSFORM] ✅ Успешно прочитано с сохранённым разделителем '{delimiter}'")
                        return df
                    else:
                        raise ValueError("[TRANSFORM] ⚠️ Структура не соответствует, несмотря на сохранённый разделитель.")
        except Exception as e:
            print(f"[TRANSFORM] ⚠ Ошибка при чтении matching-файла: {e}")

        print(f"[TRANSFORM] 🛑 Matching-файл уже существует — пропускаем подбор лучшего разделителя.")
        raise ValueError(f"[TRANSFORM] Не удалось прочитать файл {file_path} с корректной структурой.")

    # 🧠 Подбор лучшего разделителя и создание нового matching-файла
    best_result, best_missing_count, best_df, best_sep = None, float("inf"), None, None

    for sep in [";", ","]:
        try:
            df = pd.read_csv(file_path, sep=sep)
            df.columns = [col.strip().replace('\ufeff', '') for col in df.columns]
            missing = get_missing_columns(df, entity)
            print(f"[TRANSFORM] Попытка с разделителем '{sep}', пропущено полей: {len(missing)}")
            if len(missing) < best_missing_count:
                best_result, best_df, best_sep, best_missing_count = missing, df, sep, len(missing)
        except Exception as e:
            print(f"[TRANSFORM] ⚠ Ошибка чтения с разделителем '{sep}': {e}")

    if best_df is not None:
        match_data = {"__delimiter__": best_sep}
        for miss in best_result:
            close = difflib.get_close_matches(miss, set(best_df.columns), n=1, cutoff=0.6)
            match_data[miss] = {
                "suggested": close[0] if close else None,
                "user_submitted": 0,
                "pass_as_null": 0
            }
        with open(matching_file, "w") as f:
            json.dump(match_data, f, indent=2, ensure_ascii=False)
        print(f"[TRANSFORM] 💡 Записан лучший разделитель '{best_sep}' в matching-файл")

        if check_column_match(best_df, entity, file_path, best_sep):
            print(f"[TRANSFORM] ✅ Успешно прочитано с выбранным разделителем '{best_sep}'")
            return best_df

    raise ValueError(f"[TRANSFORM] Не удалось прочитать файл {file_path} с корректной структурой.")


def get_missing_columns(df: pd.DataFrame, entity: str):
    expected = EXPECTED_COLUMNS.get(entity, set())
    return expected - set(df.columns)


def check_column_match(df: pd.DataFrame, entity: str, file_path: str, sep: str) -> bool:
    expected = EXPECTED_COLUMNS.get(entity, set())
    actual = set(df.columns)
    missing = expected - actual
    if not missing:
        return True

    matching_dir = os.getenv("MATCHING_DIR", "/app/matchings")
    matching_file = os.path.join(matching_dir, f"matching_{os.path.basename(file_path).replace('.csv', '.json')}")

    confirmed, fill_as_null = {}, set()

    try:
        with open(matching_file) as f:
            match_data = json.load(f)
            print(f"[VALIDATION] 📄 Содержимое matching-файла:")
            print(json.dumps(match_data, indent=2, ensure_ascii=False))

            for target, rule in match_data.items():
                if target.startswith("__"):
                    continue
                if rule.get("user_submitted") == 1 and rule.get("suggested") in df.columns:
                    confirmed[target] = rule["suggested"]
                elif rule.get("pass_as_null") == 1:
                    fill_as_null.add(target)

        if confirmed:
            print(f"[VALIDATION] ✅ Подтверждённые сопоставления: {confirmed}")
            df.rename(columns={v: k for k, v in confirmed.items()}, inplace=True)
            print(f"[DEBUG] 🧾 Колонки после переименования: {list(df.columns)}")

        for col in fill_as_null:
            if col not in df.columns:
                df[col] = "no_data"

        still_missing = expected - set(df.columns)
        if not still_missing:
            return True
        print(f"[VALIDATION] ❌ Не все поля покрыты: {still_missing}")
    except Exception as e:
        print(f"[VALIDATION] ⚠ Ошибка при чтении matching-файла: {e}")

    return False
