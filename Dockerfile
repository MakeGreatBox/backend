FROM python:3.12-slim

WORKDIR /

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY gateway.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
