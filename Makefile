# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

all: build

build:
	./mvnw clean package -pl :wayang-assembly -Pdistribution
	cp wayang-assembly/target/apache-wayang-assembly-1.0.0-SNAPSHOT-incubating-dist.tar.gz install
	cd install && rm -rf wayang-1.0.0-SNAPSHOT && tar xvf apache-wayang-assembly-1.0.0-SNAPSHOT-incubating-dist.tar.gz
.PHONY: build

build-bench:
	./mvnw clean package -pl :wayang-benchmark -DskipTests
	cp wayang-benchmark/target/apache-wayang-benchmark-1.0.0-SNAPSHOT-incubating.jar ./install/wayang-1.0.0-SNAPSHOT/jars/wayang-benchmark-1.0.0-SNAPSHOT.jar

build-api-scala-java:
	./mvnw clean package install -pl :wayang-api-scala-java -DskipTests
	cp wayang-api/wayang-api-scala-java/target/apache-wayang-api-scala-java-1.0.0-SNAPSHOT-incubating.jar ./install/wayang-1.0.0-SNAPSHOT/jars/wayang-api-scala-java-1.0.0-SNAPSHOT.jar

build-spark:
	./mvnw clean package install -pl :wayang-spark -DskipTests
	cp wayang-platforms/wayang-spark/target/apache-wayang-spark-1.0.0-SNAPSHOT-incubating.jar install/wayang-1.0.0-SNAPSHOT/jars/wayang-spark-1.0.0-SNAPSHOT.jar
