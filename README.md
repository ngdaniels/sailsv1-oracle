# Oracle Database Sails/Waterline Adapter

A [Waterline](https://github.com/balderdashy/waterline) adapter for Oracle Database that uses [oracledb](https://github.com/oracle/node-oracledb) (v4.0.1).
 
This adapter is compatible for Sails.js (v1.2.3) and using [waterline-sql-builder](https://github.com/sailshq/waterline-sql-builder) (v1.0.0) as query builder.

## How to install

`oracledb` driver module is the main dependency of `sails-oracle-database`, so before installing it, you MUST read [How to Install oracledb](https://github.com/oracle/node-oracledb/blob/master/INSTALL.md).

## Configuration parameters

The following configuration options are available along with their default values:

```javascript
oracle: {
    adapter: 'sailsv1-oracle',
    connectString: 'host:port/databaseName',
    logQueries:true,
    debug:true,
    user: 'user',
    password: 'password',
    skipDescribe:true,
    prefetchRows:10000,
    poolMax: 50,
    poolMin: 0,
    poolIncrement: 1, //only grow the pool by one connection at a time
    poolTimeout: 0 //never terminate idle connections (seconds)
};
```