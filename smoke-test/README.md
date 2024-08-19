# How Do I Even?

From the root of the project repo:

```shell
> make local_image
```

Then change to this directory and run docker compose:

```shell
> cd smoke-test
> docker compose up
```

## Shooting Trouble

### Docker Error: No such image

The command `docker compose up` returns the following error:

```plain
Error response from daemon: No such image: ko.local/refinery:latest
```

The local image needs to be built. Run `make local_target` at the root of the repo.

### Redis Error: SSL routines::wrong version number

The services for Redis and Refinery start, but the Redis log contains numerous entries like:

```plain
redis-1      | 1:M 19 Aug 2024 17:23:52.114 # Error accepting a client connection: error:0A00010B:SSL routines::wrong version number (addr=172.25.0.3:37484 laddr=172.25.0.2:6379)
```

This is a sign that Refinery is not using TLS to connect to Redis which _is_ using TLS. Check the config.yaml used by the Refinery container.

* Is `UseTLS` set to true?
* Is `UseTLSInsecure` set to true? (because we're self-signed locally)
* Do we have a bug with TLS connections?
