#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const http = require('http');
const zlib = require('zlib');
const winston = require('winston');
const ProgressBar = require('progress');
const parse = require('csv').parse;
const transform = require('csv').transform;
const stringify = require('csv').stringify;
const _ = require('lodash');
const mysql = require('mysql');

const argv = require('minimist')(process.argv.slice(2), {});

const isDryRun = argv['dry-run'];
// File with an array with links to process (google n-gram links)
const file = path.resolve(__dirname, '..', 'assets', 'google-ngram.json');
// File with an array with links to process (google n-gram links)
const destination = path.resolve(__dirname, '..', 'assets', 'google-ngram');

const dbHost = argv['db-host'];
const dbUser = argv['db-user'];
const dbPassword = argv['db-password'];
const dbSchema = argv['db-schema'];

const contents = JSON.parse(fs.readFileSync(file, {encoding: 'utf8'}));

if (!dbHost || !dbUser || !dbPassword || !dbSchema) {
    winston.error(`Invalid connection details`);
    return process.exit(1);
}

// Try using a connection pool if ECONRESET errors persist
// const db = require('mysql').createPool({...});
const connection = mysql.createConnection({
    host     : dbHost,
    user     : dbUser,
    password : dbPassword,
    database : dbSchema
});

var nGramCurrentIndex = 0;
var nGramMaxIndex = contents.length ? contents.length - 1 : 0;

try {
    connection.connect(err => {
        if (err) {
            return winston.error(`Failed to connect database ${dbSchema}. Exiting now.`);
        }

        winston.info(`Connected ${dbSchema}`);
        winston.info(`Starting the sequence.`);

        processUrl(contents[nGramCurrentIndex], processUrlCallback);
    });
} catch (e) {
    processUrlCallback(e);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
function processUrlCallback(err) {
    if (err) {
        winston.error(`Failed processing n-gram url "${contents[nGramCurrentIndex]}"`);
        winston.error(`${err}`);
    } else {
        winston.info(`Finished processing n-gram url "${contents[nGramCurrentIndex]}"`);
    }

    if (nGramCurrentIndex < nGramMaxIndex && contents[nGramCurrentIndex + 1]) {
        nGramCurrentIndex += 1;
        winston.info('\n');
        try {
            processUrl(contents[nGramCurrentIndex], processUrlCallback);
        } catch (e) {
            processUrlCallback(e);
        }
    } else {
        winston.info(`Nothing more to do. Exiting sequence.`);
        connection.end();
        process.exit(0);
    }
}

function processUrl(nGramUrl, cb=function() {}) {
    const tblName = nGramUrl.split('/').pop().replace(/\.gz$/, '');

    createTable(tblName, err => {
        if (err) {
            winston.error(`Failed to create table ${tblName}. Skipping.`);
            return cb(err);
        }

        download(nGramUrl, tblName, function(err) {
            if (err) {
                winston.error(`Failed inserting into ${tblName}`);
                winston.error(err);
                return cb(err);
            }

            winston.info(`Finished inserting into ${tblName}`);
            return cb();
        });
    });
}

function download(url, tblName, cb) {
    const parser = parse({
        delimiter: '\t',
        relax: true,
        skip_empty_lines: true,
        trim: true
    });
    const fileName = url.split('/').pop().replace(/\.gz$/, '.json');
    const filePath = destination + '/' + fileName;
    const file = fs.createWriteStream(filePath);
    const gunzip = zlib.createGunzip();
    const chunkSize = 100;
    let chunk = [];
    let processedRecordsCount = 0;

    winston.info(`Downloading ${url}`);
    winston.info(`Destination ${filePath}`);

    if (isDryRun) {
        return cb();
    }

    const request = http.get(url, function(response) {
        const contentLength = parseInt(response.headers['content-length'], 10);
        const progressBar = new ProgressBar('[:bar] :percent Elapsed :elapsed Processed :processedRecordsCount', {
            width: 60,
            total: contentLength,
            clear: true
        });

        const transformJson = record => {
            try {
                return JSON.stringify(record);
            } catch (err) {
                winston.error(err);
                return [];
            }
        };

        response.on('end', function() {});

        response.on('data', function(dataChunk) {
            progressBar.tick(dataChunk.length, {processedRecordsCount: processedRecordsCount});
        });

        response
            .pipe(gunzip)
            .pipe(parser)
            .pipe(transform(function(record, transformCb) {
                chunk.push(record);
                processedRecordsCount += 1;

                // Gather chunks of data to be inserted in batches into the DB
                if (chunk.length < chunkSize) {
                    return transformCb(null, transformJson(record));
                }

                let chunkToInsert = chunk;

                chunk = [];

                return insert(tblName, chunkToInsert, err => {
                    return transformCb(err, transformJson(record));
                });
            }, {consume: true, parallel: 10}))
            .pipe(file);

        file.on('finish', function() {
            winston.info(`Finished writing to file ${filePath}`);
            file.close(cb);  // close() is async, call cb after close completes.
        }).on('error', function(err) {
            winston.error(`Removing file ${filePath} due to a failure`);
            fs.unlink(filePath);
            return cb(err);
        });
    }).on('error', function(e) { // Handle errors
        winston.error(`Removing file ${filePath} due to a failure`);
        fs.unlink(filePath);
        return cb(e);
    });
}

function insert(tableName, contents, cb) {
    const query = 'INSERT INTO `' + tableName + '`'
        + ' (`ngram`, `year`, `match_count`, `volume_count`)'
        + ' VALUES ?;';

    if (isDryRun) {
        winston.info(`Inserting ${contents.length} rows into ${tableName}`);
        return cb();
    }

    connection.query(query, [contents], function(err, results) {
        if (err) {
            winston.error(`Failed to execute query on ${tableName}: ${err}`);
        }
        return cb(err);
    });
}

function createTable(tableName, cb) {
    const dropQuery = 'DROP TABLE IF EXISTS `' + tableName + '`;';
    const createQuery = 'CREATE TABLE `'+ tableName + '`'
        + ' (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,'
        + ' `ngram` varchar(255) NOT NULL,'
        + ' `year` varchar(255) NOT NULL,'
        + ' `match_count` varchar(255) NOT NULL,'
        + ' `volume_count` varchar(255) NOT NULL,'
        + ' PRIMARY KEY (`id`)'
        + ')'
        + ' ENGINE=InnoDB DEFAULT CHARSET=utf8;';

    winston.info(`Creating table ${tableName} if not exists`);

    if (isDryRun) {
        return cb();
    }

    connection.query(dropQuery, function(err) {
        if (err) {
            return cb(err);
        }

        connection.query(createQuery, function (err, results, fields) {
            if (err) {
                winston.error(`Failed to create table ${tableName} with error ${err}`);
            }

            return cb(err);
        });
    });
}
