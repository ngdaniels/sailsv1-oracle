/**
 * Module Dependencies
 */
var oracledb = require('oracledb');
var _ = require('lodash');
var async = require('async');
var Sequel = require('waterline-sequel');
var utils = require('./utils');
var Processor = require('./processor');
var hop = utils.object.hasOwnProperty;
var sql = require('./sql.js');
var SQLBuilder = require('waterline-sql-builder');
var compile = SQLBuilder({ dialect: 'oracle' }).generate;

var LOG_QUERIES = false; //It shows all executed queries
var LOG_DEBUG = false; //It show messages which allow you to follow the code
var DATE_FORMAT = 'dd-mm-yyyy hh24:mi:ss'
var SKIP_VERIFICATION = true;
var PREFETCH = 10000;
module.exports = (function() {


    var sqlOptions = {
        parameterized: true,
        caseSensitive: false,
        escapeCharacter: '"',
        casting: true,
        canReturnValues: true,
        escapeInserts: true,
        declareDeleteAlias: false
    };

    // Connection specific overrides from config
    var connectionOverrides = {};

    // You'll want to maintain a reference to each connection
    // that gets registered with this adapter.
    var connections = {};


    var adapter = {

		// Waterline Adapter API Version
		adapterApiVersion: 1,

        identity: 'sails-oracle-database',
        // Which type of primary key is used by default
        pkFormat: 'integer',
        syncable: true,
        // Default configuration for connections
        defaults: {

            schema: true,
            ssl: false

        },
        /**
         *
         * This method runs when a model is initially registered
         * at server-start-time.  This is the only required method.
         *
         * @param  {[Object]}   connection The connection object.
         * @param  {[Array]}   collections Models from application.
         * @param  {Function} cb         
         */
        registerDatastore: function(connection, collections, cb) {
            if (connection.dateFormat)
                DATE_FORMAT = connection.dateFormat
            if (connection.debug)
                LOG_DEBUG = true
            if (connection.logQueries)
                LOG_QUERIES = true
            if (connection.hasOwnProperty("skipDescribe")) {
                SKIP_VERIFICATION = connection.skipDescribe;
            }
            if(connection.prefetchRows){
                PREFETCH = connection.prefetchRows
            }
            if (LOG_DEBUG) {
                console.log("BEGIN registerDatastore");
            }

            if (!connection.identity)
                return cb(new Error('Connection is missing an identity.'));
            if (connections[connection.identity])
                return cb(new Error('Connection is already registered.'));

            var self = this;

            // Store any connection overrides
            connectionOverrides[connection.identity] = {};

            // Look for the WL Next key
            if (hop(connection, 'wlNext')) {
                connectionOverrides[connection.identity].wlNext = _.cloneDeep(connection.wlNext);
            }

            // Build up a schema for this connection that can be used throughout the adapter
            var schema = {};

            _.each(_.keys(collections), function(coll) {
                var collection = collections[coll];
                if (!collection)
                    return;

                var _schema = collection.waterline && collection.waterline.schema && collection.waterline.schema[collection.identity];
                if (!_schema)
                    return;

                // Set defaults to ensure values are set
                if (!_schema.attributes)
                    _schema.attributes = {};
                if (!_schema.tableName)
                    _schema.tableName = coll;

                // If the connection names are't the same we don't need it in the schema
                if (!_.includes(collections[coll].connection, connection.identity)) {
                    return;
                }

                // If the tableName is different from the identity, store the tableName in the schema
                var schemaKey = coll;
                if (_schema.tableName != coll) {
                    schemaKey = _schema.tableName;
                }

                schema[schemaKey] = _schema;
            });
            connection.poolMax = connection.poolMax | 50; // maximum size of the pool
            connection.poolMin =connection.poolMin | 0; // let the pool shrink completely
            connection.poolIncrement = connection.poolIncrement | 1; // only grow the pool by one connection at a time
            connection.poolTimeout = connection.poolTimeout | 0; // never terminate idle connections


            // Create pool
            oracledb.createPool(connection,
                function(err, p) {
                    if (err) {
                        return handleQueryError(err, 'registerDatastore');
                    }

                    // Store the connection
                    connections[connection.identity] = {
                        config: connection,
                        collections: collections,
                        schema: collections,
                        pool: p
                    };


                    // Always call describe
                    async.eachSeries(Object.keys(collections), function(colName, cb) {
                        self.describe(connection.identity, colName, cb);
                    }, cb);
                });

        },
        /**
         * Fired when a model is unregistered, typically when the server
         * is killed. Useful for tearing-down remaining open connections,
         * etc.
         *
         * @param  {Function} cb [description]
         * @return {[type]}      [description]
         */
        // Teardown a Connection
        teardown: function(conn, cb) {
            if (LOG_DEBUG) {
                console.log("BEGIN tearDown");
            }

            if (typeof conn == 'function') {
                cb = conn;
                conn = null;
            }
            if (!conn) {
                connections = {};
                return cb();
            }
            if (!connections[conn])
                return cb();
            delete connections[conn];
            cb();
        
        },
        // Raw Query Interface OK
        query: function(connectionName, table, query, data, cb) {
            var connectionObject = connections[connectionName];
            if (LOG_DEBUG) {
                console.log("BEGIN query");
            }

            if (_.isFunction(data)) {
                cb = data;
                data = null;
            }

            // Run query
            if (!data)
                data = {};
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        return handleQueryError(err, 'query');;
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query);
                    }
                    connection.execute(query, data, {
                        outFormat: oracledb.OBJECT
                    }, function(err, result) {
                        if (err) {
                            doRelease(connection);
                            return cb(err, result);
                        }

                        return castClobs(result, function(err, result) {
                            if (err) {
                                doRelease(connection);
                                return cb(err, result);
                            }
                            if (LOG_QUERIES) {
                                console.log("Length: " + result.rows.length);
                            }
                            doRelease(connection);
                            return cb(err, result);
                        });
                    });
                });
        
        },
        // Return attributes - OK 
        describe: function(connectionName, table, cb) {
            if (SKIP_VERIFICATION) {
                return cb();
            }
            if (LOG_DEBUG) {
                console.log("BEGIN describe");
            }
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[table];

            if (!collection) {
                return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
            }

            var queries = [];
            queries[0] = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + table + "'";
            queries[1] = "SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE table_name = '" + table + "'";
            queries[2] = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner " +
                "FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '" + table +
                "' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner " +
                "ORDER BY cols.table_name, cols.position";


            // Run Query
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        handleQueryError(err, 'describe');
                        return;
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + queries[0]);
                    }
                    connection.execute(queries[0], {}, {
                        outFormat: oracledb.OBJECT
                    }, function __SCHEMA__(err, result) {

                        if (err) {
                            /* Release the connection back to the connection pool */
                            doRelease(connection);
                            return cb(handleQueryError(err, 'describe'));
                        }

                        var schema = result.rows;
                        if (LOG_QUERIES) {
                            console.log('Executing query: ' + queries[1]);
                        }
                        connection.execute(queries[1], {}, {
                            outFormat: oracledb.OBJECT
                        }, function __DEFINE__(err, result) {

                            if (err) {
                                /* Release the connection back to the connection pool */
                                doRelease(connection);
                                return cb(handleQueryError(err, 'describe'));
                            }

                            var indexes = result.rows;

                            if (LOG_QUERIES) {
                                console.log('Executing query: ' + queries[2]);
                            }
                            connection.execute(queries[2], {}, {
                                outFormat: oracledb.OBJECT
                            }, function __DEFINE__(err, result) {

                                if (err) {
                                    /* Release the connection back to the connection pool */
                                    doRelease(connection);
                                    return cb(handleQueryError(err, 'describe'));
                                }

                                var tablePrimaryKeys = result.rows;
                                if (schema.length === 0) {
                                    doRelease(connection);
                                    return cb();
                                }

                                // Loop through Schema and attach extra attributes
                                schema.forEach(function(attribute) {
                                    tablePrimaryKeys.forEach(function(pk) {
                                        // Set Primary Key Attribute
                                        if (attribute.COLUMN_NAME === pk.COLUMN_NAME) {
                                            attribute.primaryKey = true;
                                            // If also a number set auto increment attribute
                                            if (attribute.DATA_TYPE === 'NUMBER') {
                                                attribute.autoIncrement = true;
                                            }
                                        }
                                    });
                                    // Set Unique Attribute
                                    if (attribute.NULLABLE === 'N') {
                                        attribute.required = true;
                                    }

                                });
                                // Loop Through Indexes and Add Properties
                                indexes.forEach(function(index) {
                                    schema.forEach(function(attribute) {
                                        if (attribute.COLUMN_NAME === index.COLUMN_NAME) {
                                            attribute.indexed = true;
                                        }
                                    });
                                });
                                // Convert mysql format to standard javascript object

                                //var normalizedSchema = sql.normalizeSchema(schema, collection.attributes);
                                // Set Internal Schema Mapping
                                //collection.schema = normalizedSchema;

                                /* Release the connection back to the connection pool */
                                doRelease(connection);
                                // TODO: check that what was returned actually matches the cache
                                cb(null, schema);

                            });

                        });

                    });
                });

        },
        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         * - OK -
         */
        define: function(connectionName, collectionName, definition, cb) {
            if (LOG_DEBUG) {
                console.log('BEGIN define');
            }

            // Define a new "table" or "collection" schema in the data store
            var self = this;

            var connectionObject = connections[connectionName];
            var collectionN = connectionObject.collections[collectionName];
            if (!collectionN) {
                return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
            }

            var tableName = collectionName;

            var schema = sql.schema(tableName, definition);

            // Build query
            var query = 'CREATE TABLE "' + tableName + '" (' + schema + ')';

            if (connectionObject.config.charset) {
                query += ' DEFAULT CHARSET ' + connectionObject.config.charset;
            }

            if (connectionObject.config.collation) {
                if (!connectionObject.config.charset)
                    query += ' DEFAULT ';
                query += ' COLLATE ' + connectionObject.config.collation;
            }


            // Run query
            execQuery(connections[connectionName], query, [], function __DEFINE__(err, result) {
                if (err) {

                    return cb(err);
                }
                // TODO:
                // Determine if this can safely be changed to the `adapter` closure var
                // (i.e. this is the last remaining usage of the "this" context in the MySQLAdapter)
                //

                self.describe(connectionName, collectionName, function(err) {
                    cb(err, result);
                });
            });
       
        },
        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         */

        // Drop a table - OK 
        drop: function(connectionName, table, relations, cb) {
            var connectionObject = connections[connectionName];
            if (LOG_DEBUG) {
                console.log("BEGIN drop");
            }

            if (typeof relations === 'function') {
                cb = relations;
                relations = [];
            }



            // Drop any relations
            function dropTable(item, next) {

                // Build Query
                var query = 'DROP TABLE ' + utils.escapeName(item) + '';

                // Run Query

                connectionObject.pool.getConnection(function(err, connection) {
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query);
                    }
                    connection.execute(query, {}, function(err, result) {
                        doRelease(connection);
                        next(null, result);
                    });
                });




            }

            async.eachSeries(relations, dropTable, function(err) {
                if (err)
                    return cb(err);
                dropTable(table, cb);
            });

        },
        // Add a column to a table

        // Select Query Logic - OK
        find: function(connectionName, table, cb) {
            if (LOG_DEBUG) {
                console.log('BEGIN find');
            }
            var connectionObject = connections[connectionName];

            // Build a query for the specific query strategy
            var query = compile({
              select: table.criteria.select,
              where: table.criteria.where,
              skip: table.criteria.skip,
              limit: table.criteria.limit,
              from: table.using
            });

            // Run Query
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        console.log(err);
                        doRelease(connection);
                        return cb(handleQueryError(err, 'find'));
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query.sql);
                        console.log('Data for query: ' + JSON.stringify(query.bindings));
                    }
                    var values = []
                    connection.execute(query.sql, query.bindings, {
                        outFormat: oracledb.OBJECT,
                        resultSet: true,
                        prefetchRows: PREFETCH
                    }, function __FIND__(err, result) {
                        if (err) {
                            /* Release the connection back to the connection pool */
                            doRelease(connection);
                            return cb(handleQueryError(err, 'find'));
                        }
                        var queryStream = result.resultSet.toQueryStream();
                        var values = [];

                        queryStream.on('data', function(row) {
                            values.push(row);
                        });

                        queryStream.on('error', function(err) {
                            doRelease(connection);
                            return cb(handleQueryError(err, 'find'));
                        });

                        queryStream.on('end', function() {
                            doRelease(connection);
                            return cb(null,values)
                        });
                    });
                });


            if (LOG_DEBUG) {
                console.log('END find');
            }

        },

        // Count Query Logic - OK
        count: function(connectionName, table, cb) {
            if (LOG_DEBUG) {
                console.log('BEGIN find');
            }
            var connectionObject = connections[connectionName];

            // Build a query for the specific query strategy
            var query = compile({
              count: [],
              where: table.criteria.where,
              from: table.using
            });

            // Run Query
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        console.log(err);
                        doRelease(connection);
                        return cb(handleQueryError(err, 'find'));
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query.sql);
                        console.log('Data for query: ' + JSON.stringify(query.bindings));
                    }
                    var values = []
                    connection.execute(query.sql, query.bindings, {
                        outFormat: oracledb.OBJECT,
                        resultSet: true,
                        prefetchRows: PREFETCH
                    }, function __FIND__(err, result) {
                        if (err) {
                            /* Release the connection back to the connection pool */
                            doRelease(connection);
                            return cb(handleQueryError(err, 'find'));
                        }
                        var queryStream = result.resultSet.toQueryStream();
                        var values = [];

                        queryStream.on('data', function(row) {
                            values.push(row);
                        });

                        queryStream.on('error', function(err) {
                            doRelease(connection);
                            return cb(handleQueryError(err, 'find'));
                        });

                        queryStream.on('end', function() {
                            doRelease(connection);
                            return cb(null,values[0]['COUNT(*)'])
                        });
                    });
                });


            if (LOG_DEBUG) {
                console.log('END find');
            }

        },

        // Add a new row to the table - OK
        create: function(connectionName, table, cb) {

            if (LOG_DEBUG) {
                console.log("BEGIN create");
            }

            var connectionObject = connections[connectionName];
            var schema = connectionObject.schema;
            var processor = new Processor(schema);

            // Build up a SQL Query
            var query = compile({
              insert: table.newRecord,
              into: table.using
            });

            //Set date string to date object
            query.bindings.forEach((binding, index) => {
                let check = Date.parse(binding)

                if (check >= 0) {
                    query.bindings[index] = new Date(binding);
                }
            });

            // Run Query
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        handleQueryError(err, 'create');
                        return;
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query.sql);
                        console.log('Data for query: ' + JSON.stringify(query.bindings));
                    }
                    connection.execute(query.sql, query.bindings, { /*outFormat: oracledb.OBJECT, */
                        autoCommit: false
                    }, function __CREATE__(err, result) {

                        if (err) {
                            // Release the connection back to the connection pool
                            doRelease(connection);
                            return cb(handleQueryError(err, 'create'));
                        }

                        var selectQuery = 'select * from "' + table.using + '" order by "' + _getPK(connectionName, table) + '" desc';
                        connection.execute(selectQuery, [], {
                            maxRows: 1,
                            outFormat: oracledb.OBJECT
                        }, function __CREATE_SELECT__(err, result) {

                            if (err) {
                                // Release the connection back to the connection pool
                                doRelease(connection);
                                return cb(handleQueryError(err, 'create_select'));
                            }
                            // Cast special values
                            var values = processor.cast(table.using, result.rows[0]);

                            connection.commit(function(err) {

                                // Release the connection back to the connection pool
                                doRelease(connection);
                                if (err) {
                                    return cb(handleQueryError(err, 'create_commit'));
                                }

                                cb(null, values);
                            });
                        });
                    });
                });

        },
        // Update one or more models in the collection - PENDIENTE
        update: function(connectionName, table, cb) {
            if (LOG_DEBUG) {
                console.log("BEGIN update");
            }

            var connectionObject = connections[connectionName];
            var schema = connectionObject.schema;
            var processor = new Processor(schema);

            // Build a query for the specific query strategy
            var query = compile({
              update: table.valuesToSet,
              where: table.criteria.where,
              using: table.using
            });

            //Set date string to date object
            query.bindings.forEach((binding, index) => {
                let check = Date.parse(binding)

                if (check >= 0) {
                    query.bindings[index] = new Date(binding);
                }
            });

            // Run Query
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        handleQueryError(err, 'update');
                        return;
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query.sql);
                        console.log('Data for query: ' + JSON.stringify(query.bindings));
                    }
                    connection.execute(query.sql, query.bindings, { /*outFormat: oracledb.OBJECT, */
                        autoCommit: false
                    }, function __UPDATE__(err, result) {

                        if (err) {
                            // Release the connection back to the connection pool
                            return connection.rollback(function(rollerr) {
                                doRelease(connection);
                                return cb(handleQueryError(err, 'update'));
                            });
                        }

                        // Build a query for the specific query strategy
                        try {
                            // Build a query for the specific query strategy
                            var findQuery = compile({
                              select: table.criteria.select,
                              where: table.criteria.where,
                              from: table.using
                            });
                        } catch (e) {
                            return cb(handleQueryError(e, 'update'));
                        }

                        connection.execute(findQuery.sql, findQuery.bindings, {
                            outFormat: oracledb.OBJECT
                        }, function __UPDATE_SELECT__(err, result) {

                            if (err) {
                                // Release the connection back to the connection pool
                                return connection.rollback(function(rollerr) {
                                    doRelease(connection);
                                    return cb(handleQueryError(err, 'update_select'));
                                });
                            }

                            // Cast special values
                            var values = [];
                            result.rows.forEach(function(row) {
                                values.push(processor.cast(table.using, _.omit(row, 'LINE_NUMBER')));
                            });

                            connection.commit(function(err) {

                                // Release the connection back to the connection pool
                                doRelease(connection);
                                if (err) {
                                    return cb(handleQueryError(err, 'update_commit'));
                                }

                                cb(null, values);
                            });
                        });
                    });
                });

        },
        // Delete one or more models from the collection - PENDIENTE
        destroy: function(connectionName, table, cb) {
            if (LOG_DEBUG) {
                console.log("BEGIN destroy");
            }
            var connectionObject = connections[connectionName];

            console.log(table);
            // Build up a SQL Query
            var query = compile({
              del: true,
              from: table.using,
              where: table.criteria.where
            });
            console.log(query);

            // Run Query
            connectionObject.pool.getConnection(
                function(err, connection) {
                    if (err) {
                        handleQueryError(err, 'destroy');
                        return;
                    }
                    if (LOG_QUERIES) {
                        console.log('Executing query: ' + query.sql);
                        console.log('Data for query: ' + JSON.stringify(query.bindings));
                    }
                    connection.execute(query.sql, query.bindings, {
                        autoCommit: true,
                        outFormat: oracledb.OBJECT
                    }, function __DELETE__(err, result) {
                        if (err) {
                            /* Release the connection back to the connection pool */
                            doRelease(connection);
                            return cb(handleQueryError(err, 'destroy'));
                        }

                        /* Release the connection back to the connection pool */
                        doRelease(connection);

                        cb(null, result.rows);

                    });
                });
        }
    };

    /*************************************************************************/
    /* Private Methods
     /*************************************************************************/

    /**
     * Lookup the primary key for the given collection
     *
     * @param  {String} connectionName
     * @param  {String} collectionName
     * @return {String}
     * @api private
     */
    function _getPK(connectionName, collectionName) {

        var collectionDefinition;

        try {
            collectionDefinition = connections[connectionName].collections[collectionName.using].definition;
            var pk;

            pk = _.find(Object.keys(collectionDefinition), function _findPK(key) {
                var attrDef = collectionDefinition[key];
                if (attrDef && attrDef.primaryKey)
                    return key;
                else
                    return false;
            });

            if (!pk)
                pk = 'id';
            return pk;
        } catch (e) {
            throw new Error('Unable to determine primary key for collection `' + collectionName + '` because ' +
                'an error was encountered acquiring the collection definition:\n' + require('util').inspect(e, false, null));
        }
   
    }
    /**
     *
     * @param  {[type]} err [description]
     * @return {[type]}     [description]
     * @api private
     */
    function handleQueryError(err, func) {
        //TODO: Formatear errores si procede
        console.log(func);
        return err;
   
    }

    function doRelease(connection) {
        if (connection) {
            connection.release(
                function(err) {
                    if (err) {
                        return handleQueryError(err.message);
                    }
                });
        } else {
            return handleQueryError("connection not defined");
        }
   
    }

    //check if column or attribute is a boolean
    function fieldIsBoolean(column) {
        return (!_.isUndefined(column.type) && column.type === 'boolean');
    }

    function fieldIsDatetime(column) {
        return (!_.isUndefined(column.type) && column.type === 'datetime');
    }

    function fieldIsAutoIncrement(column) {
        return (!_.isUndefined(column.autoIncrement) && column.autoIncrement);
    }

    function dateField(date) {
        //TODO: dynamic format
        return 'TO_DATE(' + date + `,'${DATE_FORMAT}')`;
    }

    function execQuery(connection, query, data, cb) {
        if (LOG_QUERIES) {
            console.log('Executing query: ' + query);
            console.log('Data: ' + JSON.stringify(data));
        }
        connection.pool.getConnection(function(err, conn) {
            conn.execute(query, data, {
                autoCommit: true
            }, function(err, result) {
                doRelease(conn);
                cb(err, result);
            });
        });
    
    }

    function castClobs(result, cb) {
        // process all rows in parallel
        async.map(result.rows, function iterator(row, cbRow) {

                // process all columns of row in parallel
                async.forEachOf(row, function(column, key, cbColumn) {

                    if (column && column.iLob) {
                        if (LOG_DEBUG) {
                            console.log("Found CLOB.");
                        }

                        var lob = oracledb.newLob(column.iLob);
                        var clob = '';
                        if (lob === null || lob.length == 0) {
                            // empty clob
                            row[key] = (lob === null) ? null : '';
                            return cbColumn();
                        }
                        lob.setEncoding('utf8');
                        lob.on('data', function(chunk) {
                            clob += chunk;
                        });
                        lob.on('end', function() {
                            console.log('Completed write ' + clob.length);
                        });
                        lob.on('close', function() {
                            row[key] = clob; // change clob to its content value
                            return cbColumn();
                        });
                        lob.on('error', function(err) {
                            return cbColumn(err);
                        });
                    } else {
                        // clob column not found, nothing to do
                        return cbColumn();
                    }
                }, function(err, newRow) {
                    if (err) {
                        return cbRow(err);
                    }
                    return cbRow(null, row);
                })
            },
            function(err, newRows) {
                if (err) {
                    return cb(err);
                }
                result.rows = newRows;
                return cb(null, result);
            });
    
    }

    // Expose adapter definition
    return adapter;

})();
