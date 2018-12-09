# Prooph Event Store HTTP Client

PHP 7.2 Event Store HTTP Client Implementation.

[![Build Status](https://travis-ci.org/prooph/event-store-http-client.svg?branch=master)](https://travis-ci.org/prooph/event-store-http-client)
[![Coverage Status](https://coveralls.io/repos/github/prooph/event-store-http-client/badge.svg?branch=master)](https://coveralls.io/github/prooph/event-store-http-client?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

## Overview

Prooph Event Store HTTP Client supports communication via HTTP to [EventStore](https://eventstore.org/).

## Installation

### Client

You can install prooph/event-store-http-client via composer by adding `"prooph/event-store-http-client": "dev-master"` as requirement to your composer.json.

### Server

Using docker:

```bash
docker run --name eventstore-node -it -p 2113:2113 -p 1113:1113 eventstore/eventstore
```

Please refer to the documentation of [eventstore.org](https://eventstore.org).

See [server section](https://eventstore.org/docs/server/index.html).

In the docker-folder you'll find three different docker-compose setups (single node, 3-node-cluster and 3-node-dns-cluster).

## Unit tests

Run the server with memory database

```console
./run-node.sh --run-projections=all --mem-db
```

```console
./vendor/bin/phpunit
```

Those are tests that only work against an empty database and can only be run manually.

Before next run, restart the server. This way you can always start with a clean server.

## Documentation

Documentation is on the [prooph website](http://docs.getprooph.org/).

## Support

- Ask questions on Stack Overflow tagged with [#prooph](https://stackoverflow.com/questions/tagged/prooph).
- File issues at [https://github.com/prooph/event-store-http-client/issues](https://github.com/prooph/event-store-http-client/issues).
- Say hello in the [prooph gitter](https://gitter.im/prooph/improoph) chat.

## Contribute

Please feel free to fork and extend existing or add new plugins and send a pull request with your changes!
To establish a consistent code quality, please provide unit tests for all your changes and may adapt the documentation.

## License

Released under the [New BSD License](LICENSE).
