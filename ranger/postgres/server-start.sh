#!/bin/bash
#server-start
su - postgres -c "/usr/bin/pg_ctl start -D /var/lib/pgsql/data -l /tmp/postgres.log"

until su - postgres -c "echo SELECT \* FROM pg_user\; | psql"; do
  sleep 5
done

if ! su - postgres -c "echo SELECT \* FROM pg_user\; | psql" | grep $DB_USER; then
  su - postgres -c "echo create role $DB_USER with login password \'$DB_PASSWORD\'\; | psql "
fi

if ! su -  postgres -c  "echo select datname from pg_database\; | psql" | grep $DB_NAME; then
  su - postgres -c "echo create database $DB_NAME with owner $DB_USER\; | psql"
fi

tail -f /tmp/postgres.log
