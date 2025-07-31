## Building from Source

To build the Redis Kafka Connector locally:

[source,console]
```
gradlew.bat clean build -x test
```

Run the above command in a Windows CMD or PowerShell terminal from the root directory of the cloned repository.

After a successful build, the connector's complete JAR (including all dependencies) will be located at:

[source,console]
```
build/libs/redis-kafka-connect-{project-version}-all.jar
```

Replace `{project-version}` with the current version number, e.g., `0.9.4`.

You can use this `*-all.jar` file to deploy the connector manually or include it in your Kafka Connect plugins directory.

---

## Using the Complete JAR

Copy the `redis-kafka-connect-{project-version}-all.jar` file to your Kafka Connect `plugins` folder or load it directly as required by your deployment environment.

---

# Redis Kafka Connector (Source and Sink) by Redis
:linkattrs:
:project-owner:   redis-field-engineering
:project-name:    redis-kafka-connect
:project-group:   com.redis
:project-version: 0.9.1
:project-url:     https://github.com/{project-owner}/{project-name}
:doc-url:         https://{project-owner}.github.io/{project-name}


image:https://github.com/{project-owner}/{project-name}/actions/workflows/early-access.yml/badge.svg["Build Status", link#"{project-url}/actions"]
image:https://codecov.io/gh/{project-owner}/{project-name}/branch/master/graph/badge.svg?token#MTMRRGEWBD["Coverage", link#"https://codecov.io/gh/{project-owner}/{project-name}"]

Redis Kafka Connector (Source and Sink) by Redis.

## Documentation

Documentation is available at link:{doc-url}[{doc-url}].

## Downloading

The connector is published on https://www.confluent.io/hub/redis/redis-kafka-connect[Confluent Hub] and {project-url}/releases/latest[Github].

## Support

Contact us on the https://forum.redis.com/[Redis Forum] or create an issue on {project-url}/issues[Github] where we provide support on a good faith effort basis.

## Docker Example

Clone this repository, execute `run.sh` and follow prompts:

[source,console,subs#"verbatim,attributes"]
----
git clone {project-url}
cd {project-name}
./run.sh
----

