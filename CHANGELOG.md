# Changelog

## [0.2.3](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.2...v0.2.3) (2025-03-15)


### Bug Fixes

* **dependencies:** :art: Update deno.lock with new package versions and metadata ([62d85f5](https://github.com/flowcore-io/flowcore-pathways/commit/62d85f56a05c49fdc43c66781901197868e0dc29))

## [0.2.2](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.1...v0.2.2) (2025-03-15)


### Bug Fixes

* **deno.json:** :memo: Update library description to specify TypeScript ([014f1fc](https://github.com/flowcore-io/flowcore-pathways/commit/014f1fc88d5e5545dd92ff78fa0aae86a9aceebb))
* **mod:** :memo: Add module declaration comments to pathways and mod files ([275f41a](https://github.com/flowcore-io/flowcore-pathways/commit/275f41ac9d75b2f2509ddf78ddbd31fc8cd3914a))
* **types:** :memo: Add module declaration comments and improve JSDoc formatting ([21781f2](https://github.com/flowcore-io/flowcore-pathways/commit/21781f2ab8b8064935d831dfc9f2965f703af343))

## [0.2.1](https://github.com/flowcore-io/flowcore-pathways/compare/v0.2.0...v0.2.1) (2025-03-15)


### Bug Fixes

* **dependencies:** :art: Update package versions in deno.lock and import statements ([e47298e](https://github.com/flowcore-io/flowcore-pathways/commit/e47298e565743ebc7381d7d9683ddc7f33b98bf0))

## [0.2.0](https://github.com/flowcore-io/flowcore-pathways/compare/v0.1.0...v0.2.0) (2025-03-15)


### Features

* :sparkles: Add support for file-based pathways and multi-event writes ([e9d9bcc](https://github.com/flowcore-io/flowcore-pathways/commit/e9d9bcc8d4f6ff8959f99760c736671ba422e4a5))
* :sparkles: Allow non-async pathway handlers in PathwaysBuilder ([4313e63](https://github.com/flowcore-io/flowcore-pathways/commit/4313e635c04a6bd9a395f9df782482a0c18d0f37))
* :sparkles: Enhance PathwaysBuilder with improved type safety and writable pathway support ([a2c1c36](https://github.com/flowcore-io/flowcore-pathways/commit/a2c1c36aa5537be2aeec9b8e7bb744ed9a1243a8))
* :sparkles: Initialize core Flowcore Pathways library with essential components ([dfa32f1](https://github.com/flowcore-io/flowcore-pathways/commit/dfa32f1601b1c6851d87fb749fd9688b13fe398e))
* Add pathway state management with key-value storage support ([c717a2e](https://github.com/flowcore-io/flowcore-pathways/commit/c717a2e3757445cfb52b580c0ca67c746a16a622))
* **pathways:** :sparkles: Add audit functionality to PathwaysBuilder ([bdc68fe](https://github.com/flowcore-io/flowcore-pathways/commit/bdc68fe36d49c8b1827d63ee686f15afd76769bb))
* **pathways:** :sparkles: Implement error handling and retry mechanism in PathwaysBuilder ([5300036](https://github.com/flowcore-io/flowcore-pathways/commit/5300036370ce573a06dac65fc5b0fdf1ee9f3ffd))
* **pathways:** :sparkles: Introduce logging functionality in PathwaysBuilder ([9cef56a](https://github.com/flowcore-io/flowcore-pathways/commit/9cef56a8111336d2cfd26e72edbc01784cc0ef1a))
* **postgres:** :sparkles: Integrate PostgreSQL support for pathway state management ([710ab3f](https://github.com/flowcore-io/flowcore-pathways/commit/710ab3f24e4865f5259f773a73cbae12fe3f8795))
* **router:** :sparkles: Add secret key validation for PathwayRouter ([3a6a70a](https://github.com/flowcore-io/flowcore-pathways/commit/3a6a70a194ac8589ea6cc56cae741f2902befcf0))
* **tests:** :sparkles: Add comprehensive tests for pathways and router functionality ([ae032fd](https://github.com/flowcore-io/flowcore-pathways/commit/ae032fdec6c2f9c58c95dd1153c951d0d9915b2c))


### Bug Fixes

* **deno.json:** :art: Update test commands to remove unstable flag ([7fb6dd2](https://github.com/flowcore-io/flowcore-pathways/commit/7fb6dd22e4cf7f778725fb53a84b0f3ab222fa0a))
* **deno.lock:** :wrench: Fixed the npm building by updating the imports to be consisten ([5700b20](https://github.com/flowcore-io/flowcore-pathways/commit/5700b20ee2b93dca88ce3d4cc3d826ffe6db2cc4))
* **readme:** :fire: Remove Example Projects section from README ([bd27270](https://github.com/flowcore-io/flowcore-pathways/commit/bd27270ae0f80c46074def5ef3a08410677ebb68))
* **readme:** :memo: Update library description and installation instructions in README ([cbd3d33](https://github.com/flowcore-io/flowcore-pathways/commit/cbd3d330a8e6809c9cbb1bc8582598d3cd1992d5))
* **readme:** :memo: Update README with detailed usage examples and core concepts ([5e26e9b](https://github.com/flowcore-io/flowcore-pathways/commit/5e26e9b8f2b06c56e0c65dbce09d6c7e2fe36edd))
* **router:** :bug: Improve error handling in PathwayRouter processing ([56a00eb](https://github.com/flowcore-io/flowcore-pathways/commit/56a00eb9c9d8b056b40398eb40c213fd876e4226))
* **router:** :bug: Update processEvent method to include return type ([af5b821](https://github.com/flowcore-io/flowcore-pathways/commit/af5b821187404d72c1566f2ebdb9c7fbcbb0bb3b))
