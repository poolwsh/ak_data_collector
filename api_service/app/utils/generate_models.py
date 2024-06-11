#!/usr/bin/env python

import os
import re
import sys
import warnings
from sqlalchemy import (
    Column, Integer, Float, String, Date, DECIMAL, BigInteger, Numeric, TIMESTAMP, DateTime
)        
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from logger import logger

# Get the absolute path of the current script file
current_path = os.path.abspath(os.path.dirname(__file__))
# Construct the absolute path to the DAGS directory
DAGS_DIR = os.path.abspath(os.path.join(current_path, '..', '..', '..', 'dags'))
# Construct the absolute path to the models.py file
MODEL_PATH = os.path.abspath(os.path.join(current_path, '..', 'models.py'))
# Construct the absolute path to the schemas.py file
SCHEMA_PATH = os.path.abspath(os.path.join(current_path, '..', 'schemas.py'))

logger.info('Script started: Generating SQLAlchemy models and Pydantic schemas from SQL files')

# Log the key information
logger.debug(f'DAGS_DIR: {DAGS_DIR}')
logger.debug(f'MODEL_PATH: {MODEL_PATH}')
logger.debug(f'SCHEMA_PATH: {SCHEMA_PATH}')

# Ensure DAGS_DIR exists
if not os.path.isdir(DAGS_DIR):
    logger.error(f'Directory not found: {DAGS_DIR}')
    sys.exit(1)

# Show all deprecation warnings
warnings.filterwarnings('always', category=DeprecationWarning)
warnings.filterwarnings('always', category=PendingDeprecationWarning)

Base = declarative_base()

# Map SQL types to SQLAlchemy and Pydantic types
type_mapping = {
    "INT": Integer,
    "INTEGER": Integer,
    "FLOAT": Float,
    "VARCHAR": String,
    "DATE": Date,
    "DECIMAL": DECIMAL,
    "BIGINT": BigInteger,
    "NUMERIC": Numeric,
    "TIMESTAMP": TIMESTAMP,
    "CHAR": String,
    "TEXT": String,
}

pydantic_type_mapping = {
    "Integer": "int",
    "Float": "float",
    "String": "str",
    "Date": "date",
    "DECIMAL": "Decimal",
    "BigInteger": "int",
    "Numeric": "Decimal",
    "TIMESTAMP": "datetime",
}

# Parse a single table definition from the SQL file
def parse_table(sql):
    table_name_match = re.search(r'CREATE TABLE (\w+)', sql)
    if table_name_match:
        table_name = table_name_match.group(1)
        logger.debug(f'Parsing table: {table_name}')
        columns = []
        pydantic_fields = []
        for column_def in re.findall(r'(\w+ [A-Z]+(?:\(\d+(?:,\s*\d+)?\))?)', sql):
            parts = column_def.split()
            if len(parts) < 2:
                logger.warning(f'Invalid column definition: {column_def}')
                continue
            name = parts[0]
            col_type = parts[1]
            col_type = re.sub(r'\(\d+(?:,\s*\d+)?\)', '', col_type)
            col_type_mapped = type_mapping.get(col_type.upper())
            if col_type_mapped is None:
                logger.warning(f'Unknown column type: {col_type.upper()} in column: {name}')
                continue
            pydantic_type_mapped = pydantic_type_mapping.get(col_type_mapped.__name__)
            columns.append(Column(name, col_type_mapped))
            pydantic_fields.append((name, pydantic_type_mapped))
        primary_keys = re.findall(r'PRIMARY KEY \(([^)]+)\)', sql)
        if primary_keys:
            for pk in primary_keys[0].split(','):
                pk = pk.strip()
                for col in columns:
                    if col.name == pk:
                        col.primary_key = True
                        break
        else:
            # If no primary key is found, log a warning and skip the table
            logger.warning(f'No primary key found for table: {table_name}')
            return None, None, None
        return table_name, columns, pydantic_fields
    else:
        logger.warning(f'No table name found in SQL: {sql}')
        return None, None, None

# Generate model class for the table
def generate_model_class(table_name, columns):
    logger.debug(f'Generating model class for table: {table_name}')
    attrs = {'__tablename__': table_name}
    attrs.update({col.name: col for col in columns})
    model = type(table_name.capitalize(), (Base,), attrs)
    return model


def generate_pydantic_schema_class(table_name, pydantic_fields):
    logger.debug(f'Generating Pydantic schema class for table: {table_name}')
    base_class = f"{table_name.capitalize()}Base"
    create_class = f"{table_name.capitalize()}Create"
    read_class = f"{table_name.capitalize()}Read"

    # Defining the context for eval function
    eval_context = {
        "int": int,
        "float": float,
        "str": str,
        "date": Date,
        "Decimal": DECIMAL,
        "datetime": DateTime
    }

    base_attrs = {name: (eval(p_type, eval_context), ...) for name, p_type in pydantic_fields}
    base_attrs['__annotations__'] = {name: eval(p_type, eval_context) for name, p_type in pydantic_fields}

    # Using ConfigDict for Pydantic v2.0
    config_dict = {"arbitrary_types_allowed": True}

    base_schema = type(base_class, (BaseModel,), {**base_attrs, 'model_config': config_dict})
    create_schema = type(create_class, (base_schema,), {})
    read_schema = type(read_class, (base_schema,), {"model_config": {"from_attributes": True, "arbitrary_types_allowed": True}})

    return base_schema, create_schema, read_schema






# Process all SQL files in the directory
def process_sql_files(directory):
    models = []
    pydantic_schemas = []
    logger.info(f'Processing SQL files in directory: {directory}')
    # Walk through the directory and its subdirectories
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.sql'):
                filepath = os.path.join(root, filename)
                logger.info(f'Processing file: {filepath}')
                if not os.path.isfile(filepath):
                    logger.warning(f'File not found: {filepath}')
                    continue
                with open(filepath) as file:
                    sql = file.read()
                    logger.debug(f'File content from {filepath}:\n{sql}')
                    # Ignore create_hypertable statements, only process CREATE TABLE statements
                    sql = re.sub(r'CREATE HYPERTABLE.*?;', '', sql, flags=re.S)
                    table_definitions = re.findall(r'(CREATE TABLE .*?;)', sql, re.S)
                    logger.debug(f'Found {len(table_definitions)} table definitions in {filepath}')
                    for table_def in table_definitions:
                        logger.debug(f'Table definition:\n{table_def}')
                        table_name, columns, pydantic_fields = parse_table(table_def)
                        if table_name and columns and pydantic_fields:
                            model = generate_model_class(table_name, columns)
                            models.append(model)
                            pydantic_schemas.append(generate_pydantic_schema_class(table_name, pydantic_fields))
    return models, pydantic_schemas

def main():
    models, pydantic_schemas = process_sql_files(DAGS_DIR)
    
    # Writing models to models.py file
    with open(MODEL_PATH, 'w') as f:
        logger.info(f'Writing models to file: {MODEL_PATH}')
        f.write("from sqlalchemy import Column, Integer, Float, String, Date, DECIMAL, BigInteger, Numeric, TIMESTAMP\n")
        f.write("from sqlalchemy.orm import declarative_base\n")
        f.write("from db import db\n\n")
        f.write("Base = declarative_base()\n\n")

        for model in models:
            logger.debug(f'Writing model: {model.__name__}')
            f.write(f"class {model.__name__}(Base):\n")
            f.write(f"    __tablename__ = '{model.__tablename__}'\n")
            for column in model.__table__.columns:
                col_type = column.type.__class__.__name__
                col_args = [col_type]
                if column.primary_key:
                    col_args.append("primary_key=True")
                f.write(f"    {column.name} = Column({', '.join(col_args)})\n")
            f.write("\n")
        
        f.write("Base.metadata.create_all(bind=db.engine)\n")

    # Writing Pydantic schemas to schemas.py file
    with open(SCHEMA_PATH, 'w') as f:
        logger.info(f'Writing schemas to file: {SCHEMA_PATH}')
        f.write("from pydantic import BaseModel\n")
        f.write("from datetime import Date, DateTime\n")
        f.write("from decimal import DECIMAL\n\n")
        for base_schema, create_schema, read_schema in pydantic_schemas:
            logger.debug(f'Writing Pydantic schema: {base_schema.__name__}')
            f.write(f"class {base_schema.__name__}(BaseModel):\n")
            for name, p_type in base_schema.__annotations__.items():
                f.write(f"    {name}: {p_type.__name__}\n")
            f.write("\n")
            
            logger.debug(f'Writing Pydantic schema: {create_schema.__name__}')
            f.write(f"class {create_schema.__name__}({base_schema.__name__}):\n")
            f.write("    pass\n")
            f.write("\n")
            
            logger.debug(f'Writing Pydantic schema: {read_schema.__name__}')
            f.write(f"class {read_schema.__name__}({base_schema.__name__}):\n")
            f.write("    class Config:\n")
            f.write("        from_attributes = True\n")
            f.write("\n")

if __name__ == '__main__':
    main()
