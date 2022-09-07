make: build run

build:
	docker build -t invest .

run:
	docker run -p 8501:8501 --env-file .env invest
