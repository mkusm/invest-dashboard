make: build run debug

build:
	docker build -t stonks-image ./stonks-app/

run:
	docker compose up -d

debug: 
	docker attach stonks-app-container

deploy:
	gcloud run deploy stonks --allow-unauthenticated --region europe-central2 --source ./stonks-app/
