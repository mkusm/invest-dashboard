FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY ./code .
COPY ./dev_data.csv .


EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "streamlit_app.py"]
