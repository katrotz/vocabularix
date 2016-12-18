'use strict';

const Db = require('./lib/db');
const dbHost = null;
const dbUser = null;
const dbPassword = null;

module.exports = function() {
    const db = new Db();

    db.useAdapter('mysql');

    db.connect(dbHost, dbUser, dbPassword)
        .then(() => {
            db.getAdapter().query('SELECT * FROM `tokens`')
                // .then((r, f) => console.log(r, f))
                .catch(e => console.log(e));
        });
};