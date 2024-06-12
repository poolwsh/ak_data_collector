FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11

WORKDIR /app

# COPY ./api_service/app/ /app/

COPY ./api_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

