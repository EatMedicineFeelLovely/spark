#! /bin/sh

#初始化配置
set -x -e
export LANG=en_US.UTF-8
PROJ_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
jobParams=$1
#第三方jar
MAVEN_PATH=$PROJ_HOME/lib
for i in ${MAVEN_PATH}/*.jar; do
  SPARK_CLASSPATH=$i,$SPARK_CLASSPATH
done
jar_path=$PROJ_HOME/lib/sparkpj-core-v1.0.0.jar
test_properties=$PROJ_HOME/resources/test.properties
#联合建模-模型训练
spark2-submit \
--master yarn	\
--name test	\
--deploy-mode cluster	\
--class com.lmq.spark.Test	\
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"	\
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"	\
--files "${test_properties}" \
--jars $SPARK_CLASSPATH \
${jar_path} \
"--params" $jobParams