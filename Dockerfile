FROM python:3.8

WORKDIR /app

COPY . .

RUN make requirements

CMD ["python", "collection/load_data.py", "/app"]
