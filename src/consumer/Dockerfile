FROM python:3.10
COPY consume.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "consume.py"]