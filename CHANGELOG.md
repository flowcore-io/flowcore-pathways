# Changelog

## [0.12.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.11.0...v0.12.0) (2025-04-15)

### Features

- **pathways:** :sparkles: add SessionUserResolver for session-specific user resolvers
  ([87be987](https://github.com/flowcore-io/flowcore-pathways/commit/87be987825e6c1b465950644d8a3b9d6ed170a7d))

### Bug Fixes

- :rotating_light: fixed linting errors
  ([a4ed698](https://github.com/flowcore-io/flowcore-pathways/commit/a4ed69892ff604e8ef57e9eafae411c61ad235bb))
- **pathways:** :art: update SessionUser store to support unknown timeout types
  ([7330cbf](https://github.com/flowcore-io/flowcore-pathways/commit/7330cbfef35001bbb91b2072e6a35345549c7383))
- **pathways:** :bug: update session user resolvers to use enableSessionUserResolvers flag
  ([13623de](https://github.com/flowcore-io/flowcore-pathways/commit/13623de687db252c9ba89499d5ce3171783014ef))

## [0.11.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.10.0...v0.11.0) (2025-04-14)

### Features

- batch support
  ([32b641e](https://github.com/flowcore-io/flowcore-pathways/commit/32b641e4f181d9ff747a1f0b2bab85a9aea48504))
- batch support
  ([4ccf19b](https://github.com/flowcore-io/flowcore-pathways/commit/4ccf19b73419589ddea56dfa13533c4e73947287))

## [0.10.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.9.1...v0.10.0) (2025-04-09)

### Features

- allow to chain handle, subscribe, onError and onAnyError
  ([a560142](https://github.com/flowcore-io/flowcore-pathways/commit/a560142b940c61626a69d41b6c64dcad7947c957))
- allow to chain handle, subscribe, onError and onAnyError
  ([4ac4fdb](https://github.com/flowcore-io/flowcore-pathways/commit/4ac4fdb245ede9ef6c7bcf7e79918a9744bec611))

### Bug Fixes

- only create table if it doesn't exist
  ([828d584](https://github.com/flowcore-io/flowcore-pathways/commit/828d58499067d6a46fa6e293b0c4eac67be840c7))

## [0.9.1](https://github.com/flowcore-io/flowcore-pathways/compare/v0.9.0...v0.9.1) (2025-04-08)

### Bug Fixes

- force bun-sqlite-key-value only as optional dependency
  ([c8a4ce5](https://github.com/flowcore-io/flowcore-pathways/commit/c8a4ce523def84e33c8986da04b758ab0b45ac17))

## [0.9.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.8.0...v0.9.0) (2025-03-18)

### Features

- **pathways:** :sparkles: Introduce Session Pathways for session management
  ([5ad1f1d](https://github.com/flowcore-io/flowcore-pathways/commit/5ad1f1d706dcfb4ba517c38a2748274d43762cb6))

### Bug Fixes

- **pathways:** :art: Change import of KvAdapter to type import
  ([732da2d](https://github.com/flowcore-io/flowcore-pathways/commit/732da2d191b87781b2be3adf9efa150e2eaab3ef))

## [0.8.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.7.0...v0.8.0) (2025-03-18)

### Features

- **pathways:** :sparkles: Add session-specific user resolver functionality
  ([0e0c074](https://github.com/flowcore-io/flowcore-pathways/commit/0e0c074b451b15d3a83ea49f0d838ed559306290))

## [0.7.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.6.0...v0.7.0) (2025-03-18)

### Features

- **pathways:** :sparkles: Add cloning functionality to PathwaysBuilder
  ([674c72a](https://github.com/flowcore-io/flowcore-pathways/commit/674c72a085c9e8805d127ebd2ae70c0279d990e4))

## [0.6.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.5.0...v0.6.0) (2025-03-18)

### Features

- **pathways:** :sparkles: Add auditMode option to PathwayWriteOptions
  ([f2db7f3](https://github.com/flowcore-io/flowcore-pathways/commit/f2db7f34382ccb618c24d54af74294b3d005dca3))
- **pathways:** :sparkles: Add user ID resolver configuration to PathwaysBuilder
  ([9faef35](https://github.com/flowcore-io/flowcore-pathways/commit/9faef35a76764b9419938ac8d372fbbaa4438a23))

### Bug Fixes

- **pathways:** :art: Refactor audit handler to use user resolver method
  ([a63e673](https://github.com/flowcore-io/flowcore-pathways/commit/a63e673fb3c0718ce9eb1f3491b41d800d85dee3))

## [0.5.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.4.0...v0.5.0) (2025-03-17)

### Features

- **pathways:** :sparkles: Add event payload validation and improve handler typings
  ([228e262](https://github.com/flowcore-io/flowcore-pathways/commit/228e262527a9e1d26aaa68f959480b3a3d2ef70e))

## [0.4.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.3.0...v0.4.0) (2025-03-15)

### Features

- **postgres:** :sparkles: Add support for connection string and individual parameters in PostgreSQL configuration
  ([881fba2](https://github.com/flowcore-io/flowcore-pathways/commit/881fba26485349a3d4253cdb112928b9a9c31996))

## [0.3.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.4...v0.3.0) (2025-03-15)

### Features

- **postgres:** :sparkles: Enhance PostgreSQL configuration with connection string support
  ([9b51155](https://github.com/flowcore-io/flowcore-pathways/commit/9b51155eaa8f11c5ca3e8f6422d2b4268c4f27ea))

## [0.2.4](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.3...v0.2.4) (2025-03-15)

### Bug Fixes

- **logger:** :art: Update error method signatures and improve JSDoc documentation
  ([07ab772](https://github.com/flowcore-io/flowcore-pathways/commit/07ab77237c7aa88f6bc5999ff95148873c2e45d0))

## [0.2.3](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.2...v0.2.3) (2025-03-15)

### Bug Fixes

- **dependencies:** :art: Update deno.lock with new package versions and metadata
  ([62d85f5](https://github.com/flowcore-io/flowcore-pathways/commit/62d85f56a05c49fdc43c66781901197868e0dc29))

## [0.2.2](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.1...v0.2.2) (2025-03-15)

### Bug Fixes

- **deno.json:** :memo: Update library description to specify TypeScript
  ([014f1fc](https://github.com/flowcore-io/flowcore-pathways/commit/014f1fc88d5e5545dd92ff78fa0aae86a9aceebb))
- **mod:** :memo: Add module declaration comments to pathways and mod files
  ([275f41a](https://github.com/flowcore-io/flowcore-pathways/commit/275f41ac9d75b2f2509ddf78ddbd31fc8cd3914a))
- **types:** :memo: Add module declaration comments and improve JSDoc formatting
  ([21781f2](https://github.com/flowcore-io/flowcore-pathways/commit/21781f2ab8b8064935d831dfc9f2965f703af343))

## [0.2.1](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.0...v0.2.1) (2025-03-15)

### Bug Fixes

- **dependencies:** :art: Update package versions in deno.lock and import statements
  ([e47298e](https://github.com/flowcore-io/flowcore-pathways/commit/e47298e565743ebc7381d7d9683ddc7f33b98bf0))

## [0.2.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.1.0...v0.2.0) (2025-03-15)

### Features

- :sparkles: Add support for file-based pathways and multi-event writes
  ([e9d9bcc](https://github.com/flowcore-io/flowcore-pathways/commit/e9d9bcc8d4f6ff8959f99760c736671ba422e4a5))
- :sparkles: Allow non-async pathway handlers in PathwaysBuilder
  ([4313e63](https://github.com/flowcore-io/flowcore-pathways/commit/4313e635c04a6bd9a395f9df782482a0c18d0f37))
- :sparkles: Enhance PathwaysBuilder with improved type safety and writable pathway support
  ([a2c1c36](https://github.com/flowcore-io/flowcore-pathways/commit/a2c1c36aa5537be2aeec9b8e7bb744ed9a1243a8))
- :sparkles: Initialize core Flowcore Pathways library with essential components
  ([dfa32f1](https://github.com/flowcore-io/flowcore-pathways/commit/dfa32f1601b1c6851d87fb749fd9688b13fe398e))
- Add pathway state management with key-value storage support
  ([c717a2e](https://github.com/flowcore-io/flowcore-pathways/commit/c717a2e3757445cfb52b580c0ca67c746a16a622))
- **pathways:** :sparkles: Add audit functionality to PathwaysBuilder
  ([bdc68fe](https://github.com/flowcore-io/flowcore-pathways/commit/bdc68fe36d49c8b1827d63ee686f15afd76769bb))
- **pathways:** :sparkles: Implement error handling and retry mechanism in PathwaysBuilder
  ([5300036](https://github.com/flowcore-io/flowcore-pathways/commit/5300036370ce573a06dac65fc5b0fdf1ee9f3ffd))
- **pathways:** :sparkles: Introduce logging functionality in PathwaysBuilder
  ([9cef56a](https://github.com/flowcore-io/flowcore-pathways/commit/9cef56a8111336d2cfd26e72edbc01784cc0ef1a))
- **postgres:** :sparkles: Integrate PostgreSQL support for pathway state management
  ([710ab3f](https://github.com/flowcore-io/flowcore-pathways/commit/710ab3f24e4865f5259f773a73cbae12fe3f8795))
- **router:** :sparkles: Add secret key validation for PathwayRouter
  ([3a6a70a](https://github.com/flowcore-io/flowcore-pathways/commit/3a6a70a194ac8589ea6cc56cae741f2902befcf0))
- **tests:** :sparkles: Add comprehensive tests for pathways and router functionality
  ([ae032fd](https://github.com/flowcore-io/flowcore-pathways/commit/ae032fdec6c2f9c58c95dd1153c951d0d9915b2c))

### Bug Fixes

- **deno.json:** :art: Update test commands to remove unstable flag
  ([7fb6dd2](https://github.com/flowcore-io/flowcore-pathways/commit/7fb6dd22e4cf7f778725fb53a84b0f3ab222fa0a))
- **deno.lock:** :wrench: Fixed the npm building by updating the imports to be consisten
  ([5700b20](https://github.com/flowcore-io/flowcore-pathways/commit/5700b20ee2b93dca88ce3d4cc3d826ffe6db2cc4))
- **readme:** :fire: Remove Example Projects section from README
  ([bd27270](https://github.com/flowcore-io/flowcore-pathways/commit/bd27270ae0f80c46074def5ef3a08410677ebb68))
- **readme:** :memo: Update library description and installation instructions in README
  ([cbd3d33](https://github.com/flowcore-io/flowcore-pathways/commit/cbd3d330a8e6809c9cbb1bc8582598d3cd1992d5))
- **readme:** :memo: Update README with detailed usage examples and core concepts
  ([5e26e9b](https://github.com/flowcore-io/flowcore-pathways/commit/5e26e9b8f2b06c56e0c65dbce09d6c7e2fe36edd))
- **router:** :bug: Improve error handling in PathwayRouter processing
  ([56a00eb](https://github.com/flowcore-io/flowcore-pathways/commit/56a00eb9c9d8b056b40398eb40c213fd876e4226))
- **router:** :bug: Update processEvent method to include return type
  ([af5b821](https://github.com/flowcore-io/flowcore-pathways/commit/af5b821187404d72c1566f2ebdb9c7fbcbb0bb3b))
