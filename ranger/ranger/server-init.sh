#!/bin/bash
#server-init.sh
curl -s http://public-repo-1.hortonworks.com/HDP/centos7/3.x/updates/3.1.0.0/hdp.repo -o /etc/yum.repos.d/hdp.repo
yum -y install ranger-admin java-1.8.0-openjdk.x86_64 postgresql-jdbc wget unzip
curl http://central.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/7.2.2.jre8/mssql-jdbc-7.2.2.jre8.jar > /usr/share/java/mssql-jdbc-7.2.2.jre8.jar
ln -sf /usr/hdp/current/ranger-admin/ews/start-ranger-admin.sh /usr/bin/ranger-admin-start
ln -sf /usr/hdp/current/ranger-admin/ews/stop-ranger-admin.sh /usr/bin/ranger-admin-stop
#ln -sf /usr/hdp/current/ranger-admin/ews/ranger-admin-initd /usr/bin/ranger-admin
mkdir /security
chmod 755 /security
cd /usr/hdp/current/ranger-admin/
cd contrib/solr_for_audit_setup/
cat << EOF > install.properties
#!/bin/bash
JAVA_HOME=/etc/alternatives/jre
SOLR_USER=solr
SOLR_GROUP=solr
MAX_AUDIT_RETENTION_DAYS=90
SOLR_INSTALL=true
SOLR_DOWNLOAD_URL=http://archive.apache.org/dist/lucene/solr/5.2.1/solr-5.2.1.tgz
SOLR_INSTALL_FOLDER=/opt/solr
SOLR_RANGER_HOME=/opt/solr/ranger_audit_server
SOLR_RANGER_PORT=6083
SOLR_DEPLOYMENT=standalone
SOLR_RANGER_DATA_FOLDER=/opt/solr/data
SOLR_ZK=
SOLR_HOST_URL=http://localhost:${SOLR_RANGER_PORT}
SOLR_SHARDS=1
SOLR_REPLICATION=1
SOLR_LOG_FOLDER=/var/log/solr/ranger_audits
SOLR_RANGER_COLLECTION=ranger_audits
SOLR_MAX_MEM=2g
EOF
./setup.sh
