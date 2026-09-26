# Hugging Face Docker Space. Build from the repository root:
#   docker build -f space/Dockerfile .
FROM python:3.12-slim

WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

EXPOSE 7860
CMD ["handoffs", "demo", "--host", "0.0.0.0", "--port", "7860", "--public-demo", "--speed", "2"]
