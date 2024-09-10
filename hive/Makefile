network:
	docker network create custom-network
	
build:
	docker compose -f docker-compose.yml build
	
up:
	docker compose -f docker-compose.yml up -d 
	
down:
	docker compose -f docker-compose.yml down 

stop:
	docker compose stop

	
first:
	@make network
	@make build
	@make up
	
restart:
	@make down 
	@make up
	
