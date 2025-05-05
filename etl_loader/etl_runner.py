from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess

app = FastAPI()

class ETLRequest(BaseModel):
    entity: str
    start_month: str
    end_month: str
    load_id: str  # <-- добавляем

@app.post("/run")
def run_etl(request: ETLRequest):
    try:
        result = subprocess.run(
            [
                "python", "etl_main.py",
                "--entity", request.entity,
                "--start", request.start_month,
                "--end", request.end_month,
                "--load_id", request.load_id  # <-- добавлено
            ],
            check=True,
            capture_output=True,
            text=True
        )
        # 👇 Явный вывод stdout и stderr в консоль контейнера
        print("✅ STDOUT:\n", result.stdout)
        print("⚠️ STDERR:\n", result.stderr)

        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        print("❌ Ошибка при запуске ETL:")
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)
        raise HTTPException(status_code=500, detail=e.stderr)


