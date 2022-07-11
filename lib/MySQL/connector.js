"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySqlConnector = void 0;
const mysql2_1 = __importDefault(require("mysql2"));
const uuid_1 = require("uuid");
const assert_1 = __importDefault(require("assert"));
class MySqlConnector {
    pool;
    configuration;
    txnDict;
    constructor(configuration) {
        this.configuration = configuration;
        this.txnDict = {};
    }
    connect() {
        this.pool = mysql2_1.default.createPool(this.configuration);
    }
    disconnect() {
        this.pool.end();
    }
    startTransaction(option) {
        return new Promise((resolve, reject) => {
            this.pool.getConnection((err, connection) => {
                if (err) {
                    return reject(err);
                }
                const { isolationLevel } = option || {};
                const startTxn = () => {
                    let sql = 'START TRANSACTION;';
                    connection.query(sql, (err2) => {
                        if (err2) {
                            connection.release();
                            return reject(err2);
                        }
                        const id = (0, uuid_1.v4)();
                        Object.assign(this.txnDict, {
                            [id]: connection,
                        });
                        resolve(id);
                    });
                };
                if (isolationLevel) {
                    connection.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`, (err2) => {
                        if (err2) {
                            connection.release();
                            return reject(err2);
                        }
                        startTxn();
                    });
                }
                else {
                    startTxn();
                }
            });
        });
    }
    async exec(sql, txn) {
        if (process.env.NODE_ENV === 'development') {
            console.log(sql);
        }
        if (txn) {
            const connection = this.txnDict[txn];
            (0, assert_1.default)(connection);
            return new Promise((resolve, reject) => {
                connection.query(sql, (err, result) => {
                    if (err) {
                        console.error(`sql exec err: ${sql}`, err);
                        return reject(err);
                    }
                    resolve(result);
                });
            });
        }
        else {
            return new Promise((resolve, reject) => {
                // if (process.env.DEBUG) {
                //  console.log(sql);
                //}
                this.pool.query(sql, (err, result) => {
                    if (err) {
                        console.error(`sql exec err: ${sql}`, err);
                        return reject(err);
                    }
                    resolve(result);
                });
            });
        }
    }
    commitTransaction(txn) {
        const connection = this.txnDict[txn];
        (0, assert_1.default)(connection);
        return new Promise((resolve, reject) => {
            connection.query('COMMIT;', (err) => {
                if (err) {
                    return reject(err);
                }
                connection.release();
                resolve();
            });
        });
    }
    rollbackTransaction(txn) {
        const connection = this.txnDict[txn];
        (0, assert_1.default)(connection);
        return new Promise((resolve, reject) => {
            connection.query('ROLLBACK;', (err) => {
                if (err) {
                    return reject(err);
                }
                connection.release();
                resolve();
            });
        });
    }
}
exports.MySqlConnector = MySqlConnector;
