FROM tiangolo/uvicorn-gunicorn:python3.8

LABEL maintainer="Sebastian Ramirez <tiangolo@gmail.com>"

COPY ./app /app
RUN python -m pip install --upgrade pip && pip install -r requirements.txt