#!/bin/sh
export LIB1=/opt/solr/dist/solrj-lib
export LIB2=/opt/solr/server/solr-webapp/webapp/WEB-INF/lib
export LIB3=/opt/solr/server/lib/ext/

for i in /opt/solr/dist/solrj-lib/*.jar
do
   CLASSPATH=${CLASSPATH}:${i}
done

for i in /opt/solr/server/solr-webapp/webapp/WEB-INF/lib/*.jar
do
   CLASSPATH=${CLASSPATH}:${i}
done

for i in /opt/solr/server/lib/ext/*.jar
do
   CLASSPATH=${CLASSPATH}:${i}
done

CLASSPATH=.:$CLASSPATH:.
# echo $CLASSPATH
javac -cp $CLASSPATH *.java

java -cp $CLASSPATH SolrJLoadTest