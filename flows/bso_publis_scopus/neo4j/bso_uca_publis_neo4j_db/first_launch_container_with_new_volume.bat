@ECHO OFF
docker volume create bso-publis-neo4j-db-volume
ECHO Volume bso-publis-neo4j-db-volume created
docker run --name bso-publis-neo4j-db -p 7474:7474 -p 7687:7687 -d -v bso-publis-neo4j-db-volume,target=/data --env NEO4J_AUTH=neo4j/admin --env NEO4JLABS_PLUGINS=["apoc"] --env NEO4J_dbms_connector_https_advertised__address="localhost:7473" --env NEO4J_dbms_connector_http_advertised__address="localhost:7474" --env NEO4J_dbms_connector_bolt_advertised__address="localhost:7687" neo4j:latest
ECHO Launched container:bso-publis-neo4j-db with volume:bso-publis-neo4j-db-volume
ECHO Go to C:/Users/geoffroy/Docker/prefect-flows
PAUSE