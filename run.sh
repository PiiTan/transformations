#!/bin/bash

set -e

SCRIPT_DIR=$(cd $(dirname $0) ; pwd -P)

TASK=$1

#Tasks
help_load="Copy and load data to hdfs"
task_load() {
  docker cp src/test/resources/data hadoop-master:/root/data
  docker exec -it hadoop-master hdfs dfs -mkdir -p /user/root/src/test/resources/data
  docker exec -it hadoop-master hdfs dfs -copyFromLocal /root/data/* /user/root/src/test/resources/data/
}

help_wordcount="Copy and submit wordcount to hadoop-master"
task_wordcount() {
  docker cp target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar hadoop-master:/root/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
  docker exec -it hadoop-master spark-submit \
      --deploy-mode cluster \
      --num-executors 3 \
      --class thoughtworks.wordcount.WordCount \
       tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
}

help_transform="Copy and submit transform to hadoop-master"
task_transform() {
  docker cp target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar hadoop-master:/root/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
  docker exec -it hadoop-master spark-submit \
    --deploy-mode cluster \
    --num-executors 3 \
    --class thoughtworks.ingest.DailyDriver \
     tw-pipeline_2.11-0.1.0-SNAPSHOT.jar hdfs://hadoop-master:9000/user/root/src/test/resources/data hdfs://hadoop-master:9000/user/root/src/test/resources/transformed
}

help_citibike="Copy and submit citibike to hadoop-master"
task_citibike() {
  docker cp target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar hadoop-master:/root/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
  docker exec -it hadoop-master spark-submit \
    --deploy-mode cluster \
    --num-executors 3 \
    --class thoughtworks.citibike.CitibikeTransformer \
     tw-pipeline_2.11-0.1.0-SNAPSHOT.jar hdfs://hadoop-master:9000/user/root/src/test/resources/transformed hdfs://hadoop-master:9000/user/root/src/test/resources/computed
}

## main

list_all_helps() {
  compgen -v | egrep "^help_.*"
}

NEW_LINE=$'\n'
if type -t "task_$TASK" &>/dev/null; then
  task_$TASK "${@:2}"
else
  echo "usage: $0 <task> [<..args>]"
  echo "task:"

  HELPS=""
  for help in $(list_all_helps)
  do

    HELPS="$HELPS    ${help/help_/} |-- ${!help}$NEW_LINE"
  done

  echo "$HELPS" | column -t -s "|"
  exit 1
fi