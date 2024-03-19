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