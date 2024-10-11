#!/bin/bash
yum -y install postgresql postgresql-server
su - postgres -c "/usr/bin/initdb -D /var/lib/pgsql/data/"
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" /var/lib/pgsql/data/postgresql.conf
echo "host    all             all             0.0.0.0/0            md5" >> /var/lib/pgsql/data/pg_hba.conf
