FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11

WORKDIR /app

# Copy application files
COPY ./app /app

# Install dependencies
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
