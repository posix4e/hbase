#!/bin/bash
set -ex

i=0
tests='echo starting'
for test in "mvn test -PskipServerTests" \
"mvn -pl hbase-server test -PrunVerySlowRegionServerTests" \
"mvn -pl hbase-server test -PrunVerySlowMapReduceTests" \
"mvn -pl hbase-server test -PrunRegionServerTests" \
"mvn -pl hbase-server test -PrunMapreduceTests" \
"mvn -pl hbase-server test -PrunMiscTests" \
"mvn -pl hbase-server test -PrunClientTests" \
"mvn -pl hbase-server test -PrunMasterTests" \
"mvn -pl hbase-server test -PrunCoprocessorTests" \
"mvn -pl hbase-server test -PrunFilterTests" \
"mvn -pl hbase-server test -PrunIOTests" \
"mvn -pl hbase-server test -PrunRPCTests" \
"mvn -pl hbase-server test -PrunRestTests" \
"mvn -pl hbase-server test -PrunReplicationTests" \
"mvn -pl hbase-server test -PrunSecurityTests" \

do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    tests+=&&${test}
  fi
  ((i++))|| ps ax
done

${tests}
