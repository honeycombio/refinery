# Smoke Testing

⚠️ All configuration in this directory is for development and testing purposes.
This is not an example of a production-ready Refinery deployment.

## What Is Under Test

Refinery sharing one Redis backend that requires **TLS and AUTH**. The unencrypted, unauthenticated path is well-covered by the Go test suite.

Multiple Refinery hosts, so we can assert the primary usage of a Redis backend: peers discovering each other.

## How Do I Even?

From the root of the project repo:

```shell
> make smoke
```

That builds the local image and runs the [bats](https://bats-core.readthedocs.io/) suite in this directory.
Bats brings the services up and runs the assertions.

The services stay up afterwards, pass or fail, so you can poke at it or use it as a local two-node Refinery to develop against.

To tear it down:

```shell
> make unsmoke
```

To do a re-run from a clean slate:

```shell
> make resmoke
```

Congratulations! You have applied power and [the magic smoke was not released](https://en.wikipedia.org/wiki/Smoke_testing_(software)#Etymology)!

### Poking At It By Hand

```shell
> make local_image
> cd smoke-test
> docker compose up
```


|             | node 1                                                         | node 2                                                         |
| ----------- | -------------------------------------------------------------- | -------------------------------------------------------------- |
| HTTP ingest | [http://localhost:8080](http://localhost:8080)                 | [http://localhost:8081](http://localhost:8081)                 |
| gRPC ingest | [http://localhost:9090](http://localhost:9090)                 | [http://localhost:9091](http://localhost:9091)                 |
| metrics     | [http://localhost:2112/metrics](http://localhost:2112/metrics) | [http://localhost:2113/metrics](http://localhost:2113/metrics) |


## Shooting Trouble

### Refinery warning: failed to upload metrics

#### Problem

The logs for the Refinery node contains:

```plain
failed to upload metrics: failed to send metrics to <A URL>: 401 Unauthorized
```

This message on its own is not a Refinery *failure*.
The service is likely operating, but unable to send the telemetry concerning its internal operations on to the configured endpoint.

#### Solution

Double-check the `LegacyMetrics` and `OTelMetrics` sections of `config.yaml` are set to send telemetry to the destination you expect.
Confirm that the API key provided there or in environment variables is correct for the intended destination.

### Docker Error: No such image

#### Problem

The command `docker compose up` returns the following error:

```plain
Error response from daemon: No such image: ko.local/refinery:latest
```

#### Solution

The local image needs to be built. Run `make local_image` at the root of the repo.

### Redis Error: SSL routines::wrong version number

#### Problem

The services for Redis and Refinery start, but the Redis log contains numerous entries like:

```plain
redis-1      | 1:M 19 Aug 2024 17:23:52.114 # Error accepting a client connection: error:0A00010B:SSL routines::wrong version number (addr=172.25.0.3:37484 laddr=172.25.0.2:6379)
```

This is a sign that Refinery is not using TLS to connect to Redis which *is* using TLS.

#### Solution

Check the config.yaml used by the Refinery container.

- Is `UseTLS` set to true?
- Is `UseTLSInsecure` set to true? (because we're self-signed locally)
- Do we have a bug with TLS connections?

