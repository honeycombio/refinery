# Refinery Changelog

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
