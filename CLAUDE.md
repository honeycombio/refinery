# Refinery Code Guidelines

## Build Commands
- Build: `make local_image`
- Test all: `make test`
- Test specific: `go test -v ./PATH/TO/PACKAGE -run TestName`
- Test with race detection: `make test_with_race`
- Smoke test: `make smoke`
- Clean up: `make clean`

## Code Style Guidelines
- **Imports**: Group standard library first, third-party packages second, internal packages last; alphabetize within groups
- **Error handling**: Check errors immediately; use structured logging via `Logger.Error().Logf()`
- **Naming**: CamelCase for public items, camelCase for private items; descriptive names, acronyms capitalized
- **Types**: Interfaces defined in package-named files; implementations in separate files; use struct embedding for DI
- **Metrics**: Always track operation metrics; use proper naming; register in app startup
- **Comments**: Document public methods with clear purpose descriptions
- **Formatting**: Use `gofmt` standard formatting; use tabs for indentation
- **Dependencies**: Use dependency injection via struct tags; maintain clear separation of interfaces/implementations
- **Testing**: Write thorough tests covering edge cases; use mocks judiciously

## Package Organization
- Keep packages small with clear responsibility boundaries
- Use interfaces to decouple implementation details