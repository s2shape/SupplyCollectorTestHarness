#/bin/sh
docker build --no-cache -t "s2shape/s2shape-test-harness:latest" .
docker push s2shape/s2shape-test-harness:latest
