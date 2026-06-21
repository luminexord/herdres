# Container for the herdres update e2e (tests/e2e/restart_in_container.sh).
# Plain Ubuntu + python3 + git; no privileges, no real systemd needed — the test
# uses a process-backed systemctl shim. Build context is a `git archive` of the
# repo (see run_container_e2e.sh).
FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN git config --system init.defaultBranch main
WORKDIR /work
COPY . /work/
CMD ["bash", "tests/e2e/restart_in_container.sh"]
