#!bin/bash
docker run --name ipdatabase-postgres \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=ipdb-dev \
  -p 5545:5432 \
  -d postgres:16-alpine