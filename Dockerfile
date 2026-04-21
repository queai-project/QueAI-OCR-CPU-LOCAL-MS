ARG OCR_BASE_IMAGE=ghcr.io/queai-project/queai-ocr-base:sha-bb7bf33
FROM ${OCR_BASE_IMAGE}

ENV TESSDATA_PREFIX=/data

WORKDIR /app

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY app ./app
COPY frontend_dist ./frontend_dist
COPY .env.example .

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]