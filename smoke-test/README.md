# Smoke Testing

⚠️ All configuration in this directory is for development and testing purposes.
This is not an example of a production-ready Refinery deployment.

## How Do I Even?

From the root of the project repo:

```shell
> make local_image
```

Then change to this directory and run docker compose:

```shell
> cd smoke-test
> docker compose up
```

Observe the log output of the services.
Refinery ought to have connected to Redis to report and then find itself in the peer list.

Congratulations! You have applied power and [the magic smoke was not released](https://en.wikipedia.org/wiki/Smoke_testing_(software)#Etymology)!

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

The local image needs to be built. Run `make local_target` at the root of the repo.

### Redis Error: SSL routines::wrong version number

#### Problem

The services for Redis and Refinery start, but the Redis log contains numerous entries like:

```plain
redis-1      | 1:M 19 Aug 2024 17:23:52.114 # Error accepting a client connection: error:0A00010B:SSL routines::wrong version number (addr=172.25.0.3:37484 laddr=172.25.0.2:6379)
```

This is a sign that Refinery is not using TLS to connect to Redis which *is* using TLS.

#### Solution

Check the config.yaml used by the Refinery container.

* Is `UseTLS` set to true?
* Is `UseTLSInsecure` set to true? (because we're self-signed locally)
* Do we have a bug with TLS connections?
