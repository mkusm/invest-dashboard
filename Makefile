make: build run

build:
	docker build -t stonks-image .

run:
	docker rm stonks-container; docker run -p 8080:8080 --env-file .env --name stonks-container stonks-image

deploy:
	gcloud run deploy stonks --allow-unauthenticated --region europe-central2 --source .
