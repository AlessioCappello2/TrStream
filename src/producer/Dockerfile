FROM python:3.10
COPY produce.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "produce.py"]