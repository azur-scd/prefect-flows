@ECHO OFF
docker run --rm --mount source=bso-publis-neo4j-db-volume,destination=/backup -v C:/Users/geoffroy/Docker/automated_scripts/bso_uca_publis_neo4j_db/backups:/backup-test alpine:3.9 sh -c "apk add --no-cache zip && zip -r /backup-test/bso-publis-neo4j-db-volume_backup.zip /backup"
ECHO .zip backup successfully created in C:/Users/geoffroy/Docker/automated_scripts/bso_uca_publis_neo4j_db/backups
PAUSE