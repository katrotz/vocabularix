'use strict';
const mysql = require('mysql');

class Mysql {
    constructor() {
        this.connection = null;
    }

    connect(host, username, password, db) {
        this.connection = mysql.createConnection({
            host     : host,
            user     : username,
            password : password,
            database : db
        });

        return new Promise((resolve, reject) => {
            this.connection.connect(err => err ? reject(err) : resolve(this));
        });
    }

    disconnect() {
        return new Promise((resolve, reject) => {
            if (!this.connection) {
                return reject(`No active connection to disconnect`);
            }

            this.connection.end(err => err ? reject(err) : this.connection = null && resolve(this));
        });
    }

    getConnection() {
        return this.connection;
    }

    query() {
        return new Promise((resolve, reject) => {
            if (!this.connection) {
                return reject(`Database not connected`);
            }

            this.connection.query.apply(this.connection, [...arguments, (err, r, f) => err ? reject(err) : resolve(r, f)]);
        });
    }
}

module.exports = Mysql;