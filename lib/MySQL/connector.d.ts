import mysql from 'mysql2';
import { TxnOption } from 'oak-domain/lib/types';
import { MySQLConfiguration } from './types/Configuration';
export declare class MySqlConnector {
    pool?: mysql.Pool;
    configuration: MySQLConfiguration;
    txnDict: Record<string, mysql.PoolConnection>;
    constructor(configuration: MySQLConfiguration);
    connect(): void;
    disconnect(): void;
    startTransaction(option?: TxnOption): Promise<string>;
    exec(sql: string, txn?: string): Promise<any>;
    commitTransaction(txn: string): Promise<void>;
    rollbackTransaction(txn: string): Promise<void>;
}
