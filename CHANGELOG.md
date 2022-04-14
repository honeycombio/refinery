# Refinery Changelog

## 1.13.0 2022-04-08

### Enhancements

- Add parsing for nested json fields in the rules sampler (#418) | [@ecobrien29](https://github.com/ecobrien29)

### Maintenance

- Update husky to v0.10.3 (#431) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
- Bump google.golang.org/grpc from 1.43.0 to 1.45.0 (#428)
- Bump github.com/klauspost/compress from 1.13.6 to 1.15.1 (#427)
- Bump github.com/stretchr/testify from 1.7.0 to 1.7.1 (#426)
- Bump github.com/prometheus/client_golang from 1.11.0 to 1.12.1 (#390)

## 1.12.1 2022-03-28

### Fixes

- fix: error log event metadata (#422) | [@vreynolds](https://github.com/vreynolds)

### Maintenance

- Create checksums when building binaries (#423) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
- Cache google ko deps between workflows (#424) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 1.12.0 2022-02-24

### Enhancements

- feat: add support for env name from auth (#410) | [@JamieDanielson](https://github.com/JamieDanielson)

### Maintenance

- update aws-client orb to latest (#409) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 1.11.0 2022-02-17

### Enhancements

**Note: Environment & Services Support requires v1.12.0 and higher**

Do **not** use this version with Environment & Services.

- Add Environment & Services support (#403) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Maintenance

- docs: add helm charts step to releasing (#400) | [@vreynolds](https://github.com/vreynolds)

## 1.10.0 2022-02-10

### Enhancements

- added username in config for redis auth (#397) | [@ecobrien29](https://github.com/ecobrien29)
- build: add ARM64 (aarch64) RPM artifact (#395) | [@jharley](https://github.com/jharley)

### Fixes

- fix: deadlock when reloading configs (#398) | [@vreynolds](https://github.com/vreynolds)
- Fixed "honeeycomb" typo in log output when reloading config (#394) | [@looneym](https://github.com/looneym)

## 1.9.0 2022-02-01

### Enhancements

- Honor env. variable to set gRPC listener address (#386) | [@seh](https://github.com/seh)
- Add retries when connecting to redis during init (#382) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Fixes

- Properly set meta.refinery.local_hostname field (#387) | [@jharley](https://github.com/jharley)

### Maintenance

- docs: update rules example (#378) | [@vreynolds](https://github.com/vreynolds)
- Bump github.com/gomodule/redigo from 1.8.5 to 1.8.8 (#374)
- Bump github.com/spf13/viper from 1.9.0 to 1.10.1 (#375)
- Bump google.golang.org/grpc from 1.42.0 to 1.43.0 (#372)

## 1.8.1 2022-01-06

### Maintenance

- Add re-triage workflow (#368) | [@vreynolds](https://github.com/vreynolds)
- Bump libhoney & golang (#373) | [@lizthegrey](https://github.com/lizthegrey)
- Bump github.com/honeycombio/husky from 0.5.0 to 0.6.0 (#370)
- Bump github.com/prometheus/client_golang from 0.9.4 to 1.11.0 (#357)

## 1.8.0 2021-12-08

### Enhancements

- Make MaxBatchSize configurable (#365) | [@JamieDanielson](https://github.com/JamieDanielson)

### Maintenance

- Bump husky to v0.5.0 (#366) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)
- Bump husky to v0.4.0 (#361) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 1.7.0 2021-11-29

### Enhancements

- Replace internal duplicated code with Husky (#341) [@MikeGoldsmith](https://github.com/MikeGoldsmith)
  - Also fixes segfaults caused by nil appearing in OTLP data as described in (#358)
- Improves histogram buckets over the default set (#355) [@bdarfler](https://github.com/bdarfler)

### Maintenance

- Update dependabot to monthly (#356) [@vreynolds](https://github.com/vreynolds)

## 1.6.1 2021-11-10

- Revert "Use alpine as base image (#343)" (#352)

## 1.6.0 2021-11-04

- Add an --interface-names flag (#342) | [@ismith](https://github.com/ismith)

### Fixes

- bump libhoney-go to v1.15.6
- empower apply-labels action to apply labels (#344)
- Bump github.com/honeycombio/libhoney-go from 1.15.4 to 1.15.5 (#327)
- Re-add missing docker login when publishing (#338)

## 1.5.2 2021-10-13

### Fixes

- Build multi-arch docker images during publish CI step (#336) [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 1.5.1

### Fixes

- Fix for race condition in prometheus metrics (#324) [@estheruary](https://github.com/estheruary)
- Update race condition fix to use RWLock instead of Lock (#331) [@MikeGoldsmith](https://github.com/MikeGoldsmith) & [@robbkidd](https://github.com/robbkidd)

### Maintenance

- Build docker images on all builds and publish only on tag (#328) [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 1.5.0

### Enhancements

- Add dynamic sampler support to rules based samplers (#317) [@puckpuck](https://github.com/puckpuck)
- Publish arm64 Docker images (#323) [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Maintenance

- Adds Stalebot (#321) [@JamieDanielson](https://github.com/JamieDanielson)
- Switch licecycle terminology to maintained (#315) [cartermp](https://github.com/cartermp)
- Add NOTICE (#314) [cartermp](https://github.com/cartermp)
- Add issue and PR templates (#307) [@vreynolds](https://github.com/vreynolds)
- Add OSS lifecycle badge (#304) [@vreynolds](https://github.com/vreynolds)
- Add community health files (#303) [@vreynolds](https://github.com/vreynolds)
- Bump github.com/spf13/viper from 1.8.1 to 1.9.0 (#320) [dependabot[bot]]
- Bump github.com/json-iterator/go from 1.1.11 to 1.1.12 (#316) [dependabot[bot]]
- Bump github.com/klauspost/compress from 1.13.4 to 1.13.6 (#319) [dependabot[bot]]
- Bump github.com/fsnotify/fsnotify from 1.5.0 to 1.5.1 (#311) [dependabot[bot]]
- Bump google.golang.org/grpc from 1.39.1 to 1.40.0 (#305) [dependabot[bot]]
- Bump github.com/fsnotify/fsnotify from 1.4.9 to 1.5.0 (#308) [dependabot[bot]]
- Bump github.com/klauspost/compress from 1.13.3 to 1.13.4 (#306) [dependabot[bot]]

## 1.4.1

### Fixes

- Add span.kind when ingesting OTLP (#299)

### Maintenance

- Bump google.golang.org/grpc from 1.39.0 to 1.39.1 (#300)
- Bump github.com/klauspost/compress from 1.13.2 to 1.13.3 (#301)
- Bump github.com/honeycombio/libhoney-go from 1.12.4 to 1.15.4 (#295)
- Bump github.com/klauspost/compress from 1.10.3 to 1.13.2 (#297)

## 1.4.0

### Added

- Add support for OTLP over HTTP/protobuf [#279](https://github.com/honeycombio/refinery/pull/279) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

### Maintenance

- Bump github.com/sirupsen/logrus from 1.2.0 to 1.8.1 (#290)
- Bump google.golang.org/grpc from 1.37.1 to 1.39.0 (#288)
- Bump github.com/gomodule/redigo from 1.8.4 to 1.8.5 (#287)
- Bump github.com/spf13/viper from 1.7.0 to 1.8.1 (#274)
- Bump github.com/gogo/protobuf from 1.3.1 to 1.3.2 (#242)
- Bump github.com/golang/protobuf from 1.4.3 to 1.5.2 (#252)
- Bump github.com/grpc-ecosystem/grpc-gateway from 1.12.1 to 1.16.0 (#233)

## 1.3.0

### Added

- Add support to "does-not-contain" operator on RulesBasedSampler [#267](https://github.com/honeycombio/refinery/pull/267) | [@tr-fteixeira](https://github.com/tr-fteixeira)

### Fixes

- Ensure span links and events generate events and get resource attrs [#264](https://github.com/honeycombio/refinery/pull/264) | [@MikeGoldsmith](https://github.com/MikeGoldsmith)

## 1.2.1

### Fixes

- OTLP span events are now supported, they were being dropped on the floor previously (#261) | [@dstrelau](https://github.com/dstrelau)

## 1.2.0

### Added

- Add `UseTLSInsecure` config option to skip TLS verification with Redis (#254) | [@beanieboi](https://github.com/beanieboi)
- Add `AddHostMetadataToTrace` config option to add Refinery hostname information to spans (#250) | [@jharley](https://github.com/jharley)
- Additional config validation: verify that sample rate trace field key is specified, if needed (#248) | [@paulosman](https://github.com/paulosman)

### Changed

- Remove redundant peer/api suffix from response error metrics (#247) | [@vreynolds](https://github.com/vreynolds)
    - `api_response_errors_api`, `api_response_errors_peer`, `peer_response_errors_api`, `peer_response_errors_peer`
    - replaced by `api_response_errors`, `peer_response_errors`
- Fix rules sampler to emit correct metric (#236) | [@isnotajoke](https://github.com/isnotajoke)
    - Previously `dynsampler_num_dropped` was emitted, now `rulessampler_num_dropped` will be emitted

### Maintenance

- Update README content (#239) | [@jjziv](https://github.com/jjziv)
- Move from garyburd Redigo to supported redigo (#249) | [@verajohne](https://github.com/verajohne)
- Bump google.golang.org/grpc from 1.32.0 to 1.37.1 (#253)
- Bump github.com/prometheus/client_golang from 0.9.3 to 0.9.4 (#240)
- Bump github.com/pkg/errors from 0.8.1 to 0.9.1 (#232)
- Bump github.com/stretchr/testify from 1.5.1 to 1.7.0 (#231)
- Bump github.com/jessevdk/go-flags from 1.4.0 to 1.5.0 (#230)
- Bump github.com/hashicorp/golang-lru from 0.5.1 to 0.5.4 (#229)

## 1.1.1

### Fixes

- Refinery startup issues in v1.1.0

## 1.1.0

### Improvements

- Add support environment variables for API keys (#221)
- Removes whitelist terminology (#222)
- Log sampler config and validation errors (#228)

### Fixes

- Pass along upstream and peer metrics configs to libhoney (#227)
- Guard against nil pointer dereference when processing OTLP span.Status (#223)
- Fix YAML config parsing (#220)

### Maintenance

- Add test for OTLP handler, including spans with no status (#225)

## 1.0.0

Initial GA release of Refinery
