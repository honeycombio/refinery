# Testing Refinery in a Local Cluster

## Run and test a single instance

### Run
You can run a single instance of refinery pretty easily. Something like this will probably do it:

```sh
go run cmd/refinery/main.go --config=tools/loadtest/config.toml --rules_config=tools/loadtest/rules.toml
```

### Send a single span using Honeycomb events

```sh
curl -X POST -H "Content-Type: application/json" --header "X-Honeycomb-Team:<your API key>" --data '{"test":0, "foo":17.5, "bar":124.4}' "https://api-dogfood.honeycomb.io/1/events/YOUR_DATASET"
```

But that only gets you so far. We'd like to send a cluster significant quantities of traffic that we control. That's going to take a few steps

To make it easier, we're going to use Locust, which is a load testing tool that uses Python scripts to generate traffic, and can run many copies of that script to generate lots of traffic.

This document assumes you're developing on a Mac.

## Run and test a cluster

We want to run a local Kubernetes cluster. To do that we will use `kind` to create a local cluster, and configure it to allow us to send local traffic to it.

### Set up kind:

```sh
brew install kind
```

### Create a cluster config
The configuration maps a local port to a container port, and we’re using the Kubernetes NodePort feature to do this. NodePort, by default, limits port numbers to a range between 30000 and 32767, and we’re going to use 31333.

```yaml
skind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 31333
    hostPort: 31333
```

### more steps need to go here!
For now, at Honeycomb, search Quip for "Testing refinery in a local cluster".

## Setting up Locust

```sh
brew install locust
```

Locust needs `locustfile.py`, which is in this folder. This file loads `loadtest_config.json` to get configuration data. There's a `.example` file in this folder as well.

To run it, open a new terminal, since locust runs in the foreground, then run `locust` (in this directory). It will open a web port on `localhost:8089`; you can change the port with `locust --web-port 9797`.

Then open that page in your browser. If you hit `start test` you can specify how many "users" you want, which is locust's way of talking about tasks. If you want to stress refinery, that number will need to be in the hundreds or low thousands. If you just want some traffic, make it like 10-20.

