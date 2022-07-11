import mysql from 'mysql2';
import { v4 } from 'uuid';
import { TxnOption } from 'oak-domain/lib/types';
import { MySQLConfiguration } from './types/Configuration';
import assert from 'assert';

export class MySqlConnector {
    pool?: mysql.Pool;
    configuration: MySQLConfiguration;
    txnDict: Record<string, mysql.PoolConnection>;

    constructor(configuration: MySQLConfiguration) {
        this.configuration = configuration;
        this.txnDict = {};
    }

    connect() {
        this.pool = mysql.createPool(this.configuration);
    }

    disconnect() {
        this.pool!.end();
    }

    startTransaction(option?: TxnOption): Promise<string> {
        return new Promise(
            (resolve, reject) => {
                this.pool!.getConnection((err, connection) => {
                    if (err) {
                        return reject(err);
                    }
                    const { isolationLevel } = option || {};
                    const startTxn = () => {
                        let sql = 'START TRANSACTION;';
                        connection.query(sql, (err2: Error) => {
                            if (err2) {
                                connection.release();
                                return reject(err2);
                            }

                            const id = v4();
                            Object.assign(this.txnDict, {
                                [id]: connection,
                            });
                            
                            resolve(id);
                        });
                    }
                    if (isolationLevel) {
                        connection.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`, (err2: Error) => {
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
                })  
            }
        );
    }

    async exec(sql: string, txn?: string): Promise<any> {
        if (process.env.NODE_ENV === 'development') {
            console.log(sql);
        }
        if (txn) {
            const connection = this.txnDict[txn];
            assert(connection);
            
            return new Promise(
                (resolve, reject) => {
                    connection.query(sql, (err, result) => {
                        if (err) {
                            console.error(`sql exec err: ${sql}`, err);
                            return reject(err);
                        }
    
                        resolve(result);
                    });
                }
            );
        }
        else {
            return new Promise(
                (resolve, reject) => {
                    // if (process.env.DEBUG) {
                    //  console.log(sql);
                    //}
                    this.pool!.query(sql, (err, result) => {
                        if (err) {
                            console.error(`sql exec err: ${sql}`, err);
                            return reject(err);
                        }
    
                        resolve(result);
                    })
                }
            );
        }
    }

    commitTransaction(txn: string): Promise<void> {
        const connection = this.txnDict[txn];
        assert(connection);
        return new Promise(
            (resolve, reject) => {
                connection.query('COMMIT;', (err) => {
                    if (err) {
                        return reject(err);
                    }
                    connection.release();
                    resolve();
                });
            }
        );
    }

    rollbackTransaction(txn: string): Promise<void> {
        const connection = this.txnDict[txn];
        assert(connection);
        return new Promise(
            (resolve, reject) => {
                connection.query('ROLLBACK;', (err: Error) => {
                    if (err) {
                        return reject(err);
                    }
                    connection.release();
                    resolve();
                });
            }
        );
    }
}