FROM python:3-slim

ARG PYPI_MIRROR

WORKDIR /code

COPY requirements.txt .
RUN set -ex; \
  if [ -n "${PYPI_MIRROR}" ]; then \
    pip3 install -r requirements.txt -i ${PYPI_MIRROR}; \
  else \
    pip3 install -r requirements.txt; \
  fi;

COPY . .
CMD python3 main.py
