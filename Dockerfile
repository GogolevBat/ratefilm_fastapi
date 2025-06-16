FROM python:3.12

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["uvicorn", "app.main:app", "--port", "8051", "--host", "0.0.0.0", "--reload", "--timeout-keep-alive", "60"]