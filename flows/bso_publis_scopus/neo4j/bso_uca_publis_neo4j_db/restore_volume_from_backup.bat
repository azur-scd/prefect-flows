@ECHO OFF
docker volume create neo4j-bso-publis-db-volume
docker run --rm -v bso-publis-neo4j-db-volume:/backup -v C:/Users/geoffroy/Docker/automated_scripts/bso_uca_publis_neo4j_db/backups:/backup-test alpine:3.9 sh -c "cd /backup && apk add --no-cache unzip && unzip /backup-test/bso-publis-neo4j-db-volume_backup.zip"
ECHO Volume neo4j-bso-publis-db-volume created
PAUSE