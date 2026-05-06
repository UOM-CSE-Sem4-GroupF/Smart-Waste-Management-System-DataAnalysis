FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

RUN useradd --create-home --shell /bin/bash routeopt && chown -R routeopt:routeopt /app
USER routeopt

EXPOSE 8083
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8083"]