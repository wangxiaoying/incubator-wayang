all: build

build:
	./mvnw clean package -pl :wayang-assembly -Pdistribution
	cp wayang-assembly/target/apache-wayang-assembly-1.0.0-SNAPSHOT-incubating-dist.tar.gz install
	cd install && rm -rf wayang-1.0.0-SNAPSHOT && tar xvf apache-wayang-assembly-1.0.0-SNAPSHOT-incubating-dist.tar.gz

build-bench:
	./mvnw clean package -pl :wayang-benchmark -DskipTests
	cp wayang-benchmark/target/apache-wayang-benchmark-1.0.0-SNAPSHOT-incubating.jar ./install/wayang-1.0.0-SNAPSHOT/jars/wayang-benchmark-1.0.0-SNAPSHOT.jar