# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html),
and is generated by [Changie](https://github.com/miniscruff/changie).


## 0.4.3 - 2023-02-21
### Changed
* Use singerlib native types and accessors in stream generation

## 0.1.11 - 2022-09-02
### Fixed
* serialize objectid types

## 0.1.10 - 2022-09-02
### Changed
* updated dependencies and added srv extra

## 0.1.9 - 2022-08-30
### Changed
* changed package name for pypi upload, named -z for z3z1ma
### Fixed
* Fixed properties being truncated by singer sdk in our inherently schemaless tap

## 0.1.8 - 2022-08-03
### Added
* Started using changie for changelog management
### Fixed
* Overrode property pruning in SDK until something upstream comes by thats better
