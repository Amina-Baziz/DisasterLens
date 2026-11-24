# Image for the Streamlit dashboard. Build context is the project folder.

FROM python:3.11-slim

WORKDIR /app
COPY dashboard/requirements.txt dashboard/requirements.txt
RUN pip install --no-cache-dir -r dashboard/requirements.txt

# The app reads sql/queries.sql and the theme in .streamlit/config.toml
COPY .streamlit .streamlit
COPY sql sql
COPY dashboard dashboard

EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
