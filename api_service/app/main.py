from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from models import Dg_ak_stock_zh_a_hist_daily_bfq
from schemas import Dg_ak_stock_zh_a_hist_daily_bfqCreate, Dg_ak_stock_zh_a_hist_daily_bfqRead
from utils.db import db
from utils.logger import logger

app = FastAPI()

def get_db():
    db_session = db.get_session()
    try:
        yield db_session
    finally:
        db_session.close()

@app.post("/items/", response_model=Dg_ak_stock_zh_a_hist_daily_bfqRead)
def create_item(item: Dg_ak_stock_zh_a_hist_daily_bfqCreate, db: Session = Depends(get_db)):
    db_item = Dg_ak_stock_zh_a_hist_daily_bfq(**item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

@app.get("/items/{s_code}", response_model=Dg_ak_stock_zh_a_hist_daily_bfqRead)
def read_item(s_code: str, db: Session = Depends(get_db)):
    item = db.query(Dg_ak_stock_zh_a_hist_daily_bfq).filter(Dg_ak_stock_zh_a_hist_daily_bfq.s_code == s_code).first()
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
