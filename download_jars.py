"""Download the Spark Kafka connector jars from Maven Central into jars/.

Needed only on Windows, where spark.jars.packages does not work without winutils.exe.
"""
import os
import urllib.request

import pyspark

MAVEN = "https://repo1.maven.org/maven2"
SPARK_VERSION = pyspark.__version__

ARTIFACTS = [
    f"org/apache/spark/spark-sql-kafka-0-10_2.13/{SPARK_VERSION}/spark-sql-kafka-0-10_2.13-{SPARK_VERSION}.jar",
    f"org/apache/spark/spark-token-provider-kafka-0-10_2.13/{SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-{SPARK_VERSION}.jar",
    "org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar",
    "org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar",
]

os.makedirs("jars", exist_ok=True)

for artifact in ARTIFACTS:
    target = os.path.join("jars", os.path.basename(artifact))
    if os.path.exists(target):
        print(f"exists: {target}")
        continue
    print(f"downloading: {target}")
    urllib.request.urlretrieve(f"{MAVEN}/{artifact}", target)
