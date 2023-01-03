FROM python:3.10

WORKDIR /etl
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . .
CMD python main.py