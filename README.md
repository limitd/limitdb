[![Build Status](https://travis-ci.org/limitd/limitdb.svg)](https://travis-ci.org/limitd/limitdb)

limitdb is a database for limits on top of leveldb.

Currently limitdb uses the [Token Bucket Algorithm](https://en.wikipedia.org/wiki/Token_bucket).

## Installation

```
npm i limitdb
```

## Configure

Create an instance of Limitdb as follows:

```javascript
const Limitdb = require('limitdb');
const limitdb = new Limitdb({
  path: '/tmp/limitdb',
  types: {
    ip: {
      size: 10,
      per_second: 5
    }
  }
});
```

Options available:

- `path` (string): a path for the leveldb database.
- `inMemory` (boolean): true to run with [memdown](https://github.com/Level/memdown). This is useful for tests.
- `types` (object): setup your bucket types.

The type defines the characteristics of a the bucket:

- `size` is the maximun content of the bucket. This is the maximun burst you allow.
- `per_interval` is the amount of tokens that the bucket receive on every interval.
- `interval` defines the inverval in milliseconds.

You can also define your rates using `per_second`, `per_minute`, `per_hour`, `per_day`. So `per_second: 1` is equivalent to `per_interval: 1, interval: 1000`.

If you omit `size`, limitdb assumes that `size` is the value of `per_interval`. So `size: 10, per_second: 10` is the same than `per_second: 10`.

If you don't specify a filling rate with `per_interval` or any other `per_x`, the bucket is fixed and you have to manually reset it using `PUT`.

You can also define `overrides` inside your type definitions as follows:

```javascript
const Limitdb = require('limitdb');
const limitdb = new Limitdb({
  path: '/tmp/limitdb',
  types: {
    ip: {
      size: 10,
      per_second: 5,
      overrides: {
        '127.0.0.1': {
          size: 100,
          per_second: 50
        }
      }
    }
  }
});
```

In this case the specific bucket for `127.0.0.1` of type `ip` will have a greater limit.

It is also possible to define overrides by regex:

```
overrides: {
  'local-ips': {
    match:      /192\.168\./
    size:       100,
    per_second: 50
  }
}
```

and also it is possible to configure expiration of overrides:


```
overrides: {
  '54.32.12.31': {
    size:       100,
    per_second: 50,
    until:      new Date(2016, 4, 1)
  }
}
```


## TAKE

```javascript
limitdb.take({ type: 'ip', key: '54.21.23.12' }, (err, result) => {
  console.dir(result);
});
```

`limitdb.take` takes as argument an object with:

-  `type`: the bucket type.
-  `key`: the identifier of the bucket.
-  `count`: the amount of tokens you need. This is optional and the default is 1.

The result object has:

-  `conformant` (boolean): true if the requested amount is conformant to the limit.
-  `remaining` (int): the amount of remaining tokens in the bucket.
-  `reset` (int / unix timestamp): unix timestamp of the date when the bucket will be full again.
-  `limit` (int): the size of the bucket.

## PUT

You can manually reset a fill a bucket using PUT:

```javascript
limitdb.put({ type: 'ip', key: '54.21.23.12' }, err => {});
```

`limitdb.put` takes as argument an object with:

-  `type`: the bucket type.
-  `key`: the identifier of the bucket.
-  `count`: the amount of tokens you want to put in the bucket. This is optional and the default is the size of the bucket.

## STATUS

```javascript
limitdb.status({ type: 'ip', prefix: '54' }, (err, result) => {
  console.dir(result.items);
});
```

`limitdb.status` takes as argument an object with

-  `type`: the bucket type.
-  `prefix`: a prefix of buckets to search.

The result object has:

-  `items`: an array of buckets with:
  - `key`: the key of the bucket.
  - `size`: the size of the bucket.
  - `reset`: the reset time.

## Author

[Auth0](auth0.com)

## License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) file for more info.
