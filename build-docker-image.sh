#/bin/sh
docker build --no-cache -t "docker.pkg.github.com/s2shape/docker-images/s2shape-test-harness:latest" .
docker push docker.pkg.github.com/s2shape/docker-images/s2shape-test-harness:latest
