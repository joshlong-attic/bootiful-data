# Getting Started..

- Start the `server`, then `shell`.
- `mvn clean install` both the `stream101` and `batch101`.
- register the task: `module register --type task --name batch101 --uri maven://com.example:batch101:0.0.1-SNAPSHOT`.
- create an instance of the task (possibly with unique parameters): `task create --name batchnow --definition "batch101 --foo=bar`
- launch that instance: `task launch --name batchnow`. You will see logs being emitted in the server console. tail those logs. eventually, `task execution list` will work, too.
- register the time and log modules (they're no longer registered by default).
- `module register --name log --type sink --uri maven://org.springframework.cloud.stream.app:log-sink-rabbit:1.0.0.BUILD-SNAPSHOT`
- `module register --name time --type source --uri maven://org.springframework.cloud.stream.app:time-source-rabbit:1.0.0.BUILD-SNAPSHOT`
- then register the `stream101` processor: `module register --type processor --name streamnow --uri maven://com.example:stream101:0.0.1-SNAPSHOT`
- then create a stream out of them all: ` stream create --name s1 --definition "time | streamnow | log " `
- then deploy the stream: `stream deploy --name s1 `. You will see logs being emitted in the server console. tail those logs.

## For more
check out Spring Cloud Task [lead Michael Minella's webinar materials](https://github.com/mminella/spring-cloud-task-webinar)
