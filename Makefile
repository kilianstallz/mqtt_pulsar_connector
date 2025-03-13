

publish: 
	export KO_DOCKER_REPO=ghcr.io/kilianstallz/mqtt_pulsar_connector && \
		ko build .
