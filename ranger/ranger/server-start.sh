#!/bin/bash
#server-start.sh
cd /usr/hdp/current/ranger-admin
if [ "$DB_TYPE" == "POSTGRES" ]; then
  JAR=postgresql-jdbc3.jar
else
  JAR=mssql-jdbc-7.2.2.jre8.jar
fi

if [ -z "$HOSTNAME" ]; then
  HOSTNAME=$(hostname -f)
fi

cat << EOF > install.properties
setup_mode=SeparateDBA
PYTHON_COMMAND_INVOKER=python
DB_FLAVOR=$DB_TYPE
SQL_CONNECTOR_JAR=/usr/share/java/$JAR
db_host=$DB_HOST
db_name=$DB_NAME
db_user=$DB_USER
db_password=$DB_PASSWORD
rangerAdmin_password=$ADMINPASSWORD
rangerTagsync_password=$ADMINPASSWORD
rangerUsersync_password=$ADMINPASSWORD
keyadmin_password=$ADMINPASSWORD
audit_store=solr
audit_solr_urls=http://localhost:6083/solr/ranger_audits
audit_solr_collection_name=ranger_audits
policymgr_external_url=https://$HOSTNAME:6182
policymgr_http_enabled=false
policymgr_https_keystore_file=/security/server.jks
policymgr_https_keystore_keyalias=gateway-identity
policymgr_https_keystore_password=$KEYPASSWORD
policymgr_supportedcomponents=kafka
unix_user=ranger
unix_user_pwd=ranger
unix_group=ranger
javax_net_ssl_trustStore=/security/truststore.jks
javax_net_ssl_trustStorePassword=$KEYPASSWORD
RANGER_ADMIN_LOG_DIR=$PWD
RANGER_PID_DIR_PATH=/var/run/ranger
XAPOLICYMGR_DIR=$PWD
app_home=$PWD/ews/webapp
TMPFILE=$PWD/.fi_tmp
LOGFILE=$PWD/logfile
LOGFILES="$LOGFILE"
JAVA_BIN='java'
JAVA_VERSION_REQUIRED='1.8'
JAVA_ORACLE='Java(TM) SE Runtime Environment'
hadoop_conf=/etc/hadoop/conf
ranger_admin_max_heap_size=1g
PATCH_RETRY_INTERVAL=120
STALE_PATCH_ENTRY_HOLD_TIME=10
mysql_core_file=db/mysql/optimized/current/ranger_core_db_mysql.sql
mysql_audit_file=db/mysql/xa_audit_db.sql
oracle_core_file=db/oracle/optimized/current/ranger_core_db_oracle.sql
oracle_audit_file=db/oracle/xa_audit_db_oracle.sql
postgres_core_file=db/postgres/optimized/current/ranger_core_db_postgres.sql
postgres_audit_file=db/postgres/xa_audit_db_postgres.sql
sqlserver_core_file=db/sqlserver/optimized/current/ranger_core_db_sqlserver.sql
sqlserver_audit_file=db/sqlserver/xa_audit_db_sqlserver.sql
sqlanywhere_core_file=db/sqlanywhere/optimized/current/ranger_core_db_sqlanywhere.sql
sqlanywhere_audit_file=db/sqlanywhere/xa_audit_db_sqlanywhere.sql
cred_keystore_filename=$app_home/WEB-INF/classes/conf/.jceks/rangeradmin.jceks
EOF


if [ "$DB_HOST" == "postgres" ]; then
  curl -s postgres:5432; until [ $? ==  "52" ]; do sleep 5; done
fi

./setup.sh
./set_globals.sh

if [ ! -f /security/server.jks ]; then
  keytool -genkey -noprompt -alias gateway-identity -keyalg RSA -dname "CN=$HOSTNAME, OU=Ranger, O=Apache, L=Toronto, S=ON, C=CA" -keystore /security/server.jks -storepass "$KEYPASSWORD"  -keypass "$KEYPASSWORD"
  chmod 755 /security/server.jks
fi
/usr/bin/ranger-admin start
/opt/solr/ranger_audit_server/scripts/start_solr.sh
cd ews/logs
ls | grep -vi gc | xargs tail -f
