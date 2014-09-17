#!/bin/bash
set -ex

#"mvn -DreuseForks=true -pl hbase-server test -PrunVerySlowRegionServerTests" \
#"mvn -DreuseForks=true -pl hbase-server test -PrunVerySlowMapReduceTests" \
#"mvn -DreuseForks=true -pl hbase-server test -PrunMapreduceTests" \
#flakey
i=0
tests='echo starting'
for test in "mvn -DreuseForks=true test -PskipServerTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunRegionServerTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunMiscTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunClientTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunMasterTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunCoprocessorTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunFilterTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunIOTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunRPCTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunRestTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunReplicationTests" \
"mvn -DreuseForks=true -pl hbase-server test -PrunSecurityTests" \

do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    tests+=&&${test}
  fi
  ((i++))|| ps ax
done

${tests}
