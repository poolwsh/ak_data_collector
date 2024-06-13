from fastapi import FastAPI, Depends, HTTPException, APIRouter
from sqlalchemy.orm import Session
from typing import Type, List
from fastapi.openapi.utils import get_openapi
from utils.db import db  
from utils.logger import logger
import models  
import schemas 
import inspect

app = FastAPI()

def get_db():
    db_session = db.get_session()
    try:
        yield db_session
    finally:
        db_session.close()

def create_crud_router(model: Type[models.Base], create_schema: Type[schemas.BaseModel], read_schema: Type[schemas.BaseModel]) -> APIRouter:
    router = APIRouter()

    @router.post("/", response_model=read_schema)
    def create_item(item: create_schema, db: Session = Depends(get_db)):
        db_item = model(**item.dict())
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item

    @router.get("/{item_id}", response_model=read_schema)
    def read_item(item_id: int, db: Session = Depends(get_db)):
        item = db.query(model).get(item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return item

    @router.get("/", response_model=List[read_schema])
    def read_items(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
        items = db.query(model).offset(skip).limit(limit).all()
        return items

    return router

def get_all_models_and_schemas():
    for name, obj in inspect.getmembers(models):
        if inspect.isclass(obj) and issubclass(obj, models.Base):
            model_name = obj.__name__
            create_schema = getattr(schemas, f"{model_name}Create", None)
            read_schema = getattr(schemas, f"{model_name}Read", None)
            if create_schema and read_schema:
                yield obj, create_schema, read_schema

@app.get("/")
def read_root():
    return {"message": "Welcome to the API service"}

for model, create_schema, read_schema in get_all_models_and_schemas():
    model_name = model.__tablename__
    router = create_crud_router(model, create_schema, read_schema)
    app.include_router(router, prefix=f"/{model_name}", tags=[model_name])

@app.get("/openapi.json")
def get_openapi_schema():
    return get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
    )
