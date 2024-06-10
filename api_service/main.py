from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from models import Da_ak_tracing_stock_price_hl
from schemas import Da_ak_tracing_stock_price_hlCreate, Da_ak_tracing_stock_price_hlRead
from utils.db import db
from utils.logger import logger

app = FastAPI()

def get_db():
    db_session = db.get_session()
    try:
        yield db_session
    finally:
        db_session.close()

@app.post("/items/", response_model=Da_ak_tracing_stock_price_hlRead)
def create_item(item: Da_ak_tracing_stock_price_hlCreate, db: Session = Depends(get_db)):
    db_item = Da_ak_tracing_stock_price_hl(**item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

@app.get("/items/{s_code}", response_model=Da_ak_tracing_stock_price_hlRead)
def read_item(s_code: str, db: Session = Depends(get_db)):
    item = db.query(Da_ak_tracing_stock_price_hl).filter(Da_ak_tracing_stock_price_hl.s_code == s_code).first()
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
