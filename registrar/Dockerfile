# Connector registrar sidecar: Connect REST API 로 커넥터를 선언형으로 동기화
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY registrar ./registrar
USER 65534
ENTRYPOINT ["python", "-m", "registrar"]
CMD ["reconcile", "--loop"]
