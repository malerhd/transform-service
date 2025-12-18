# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from transformer import run_transform

app = FastAPI()


class TransformRequest(BaseModel):
    client_key: str
    platform: str
    gcs_path: str
    project_id: str


@app.post("/transform")
def transform(req: TransformRequest):
    try:
        rows = run_transform(
            gcs_path=req.gcs_path,
            client_key=req.client_key,
            platform=req.platform,
            project_id=req.project_id,
        )
        return {
            "status": "ok",
            "rows_upserted": rows,
            "client_key": req.client_key,
            "platform": req.platform,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
