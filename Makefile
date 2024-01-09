run-dev:
	kubectl apply -f deploy/namespace.yaml

	# Just chosing a random, (hopefully) free port
	nix run -f. tilt -- up --port 5500 --namespace trino-lb

stop-dev:
	nix run -f. tilt -- down
