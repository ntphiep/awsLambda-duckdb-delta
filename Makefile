docker-build:
	docker build \
		--build-arg AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
		--build-arg AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
		--platform linux/amd64 \
		-t ducklamb

docker-login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.us-east-1.amazonaws.com

docker-tag:
	docker tag ducklamb:latest $(AWS_ACCOUNT_ID).dkr.ecr.us-east-1.amazonaws.com/ntphiep/ducklamb:latest

docker-push:
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.us-east-1.amazonaws.com/ntphiep/ducklamb:latest