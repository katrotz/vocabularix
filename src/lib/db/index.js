'use strict';

const DEFAULT_ADAPTER_CLASS = require('./adapters/mysql');

class Db {
    static get dbName() {
        return 'vocabularix';
    }

    constructor() {
        /**
         * Class used to instantiate the adapter
         * @type {null}
         */
        this.adapterClass = DEFAULT_ADAPTER_CLASS;

        /**
         * The adapter instance
         * @type {null}
         */
        this.adapter = null;
    }

    getAdapter() {
        return this.adapter;
    }

    useAdapter(adapter) {
        if (this.adapter) {
            throw new Error(`Can not switch adapters while being connected. Please disconnect from the current database first.`);
        }

        try {
            this.adapter = new (require(`./adapters/${adapter}`))();
        } catch (e) {
            throw new Error(`Unknown adapter ${adapter}`);
        }
    }

    connect(host, username, password) {
        return this.adapter.connect(host, username, password, Db.dbName);
    }

    disconnect() {
        return this.adapter.disconnect().then(err => (err ? null : this.adapter = null) || this);
    }

    //TBD
    query() {}
}

module.exports = Db;