mvn clean install
targetdir=/Users/jruaux/dev/confluent-6.1.0/share/java/redis-enterprise-kafka
mkdir -p $targetdir
cp target/redis-enterprise-kafka-1.0.0-SNAPSHOT.jar $targetdir

