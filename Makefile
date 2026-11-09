.PHONY: test test-java test-scala up down smoke load replay logs

test: test-java test-scala

test-java:
	cd command-service && mvn -B -q verify
	cd query-service && mvn -B -q verify

test-scala:
	cd ledger-processor && sbt -batch test

up:
	./scripts/run-local.sh

down:
	docker compose down

smoke:
	./scripts/smoke-test.sh

load:
	python3 scripts/load_gen.py

replay:
	./scripts/replay-projections.sh

logs:
	docker compose logs -f --tail=100
