@ECHO OFF
docker run --name bso-publis-neo4j-db -p 7474:7474 -p 7687:7687 -d --env NEO4J_AUTH=neo4j/admin --env NEO4JLABS_PLUGINS=["apoc"] --env NEO4J_dbms_connector_https_advertised__address="localhost:7473" --env NEO4J_dbms_connector_http_advertised__address="localhost:7474" --env NEO4J_dbms_connector_bolt_advertised__address="localhost:7687" neo4j:latest
ECHO Launched container:bso-publis-neo4j-db
docker cp C:/Users/geoffroy/Docker/automated_scripts/bso_uca_publis_neo4j_db/backups/neo4j_db_data bso-publis-neo4j-db:/data
ECHO neo4j db data fiels copied
PAUSE