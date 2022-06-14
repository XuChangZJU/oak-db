import { EntityDict, Context, DeduceCreateSingleOperation, DeduceRemoveOperation, DeduceUpdateOperation, OperateParams, OperationResult, SelectionResult, TxnOption, SelectRowShape, StorageSchema } from 'oak-domain/lib/types';
import { CascadeStore } from 'oak-domain/lib/store/CascadeStore';
import { MySQLConfiguration } from './types/Configuration';
import { MySqlConnector } from './connector';
import { translateCreateDatabase } from './Translator';

export class MysqlStore<ED extends EntityDict, Cxt extends Context<ED>> extends CascadeStore<ED, Cxt> {
    connector: MySqlConnector;
    constructor(storageSchema: StorageSchema<ED>, configuration: MySQLConfiguration) {
        super(storageSchema);
        this.connector = new MySqlConnector(configuration);
    }
    protected supportManyToOneJoin(): boolean {
        return true;
    }
    protected selectAbjointRow<T extends keyof ED, S extends ED[T]['Selection']>(entity: T, Selection: S, context: Cxt, params?: OperateParams): Promise<SelectRowShape<ED[T]['Schema'], S['data']>[]> {
        throw new Error('Method not implemented.');
    }
    protected updateAbjointRow<T extends keyof ED>(entity: T, operation: DeduceCreateSingleOperation<ED[T]['Schema']> | DeduceUpdateOperation<ED[T]['Schema']> | DeduceRemoveOperation<ED[T]['Schema']>, context: Cxt, params?: OperateParams): Promise<number> {
        throw new Error('Method not implemented.');
    }
    operate<T extends keyof ED>(entity: T, operation: ED[T]['Operation'], context: Cxt, params?: OperateParams): Promise<OperationResult<ED>> {
        throw new Error('Method not implemented.');
    }
    select<T extends keyof ED, S extends ED[T]['Selection']>(entity: T, selection: S, context: Cxt, params?: Object): Promise<SelectionResult<ED[T]['Schema'], S['data']>> {
        throw new Error('Method not implemented.');
    }
    count<T extends keyof ED>(entity: T, selection: Omit<ED[T]['Selection'], 'data' | 'sorter' | 'action'>, context: Cxt, params?: Object): Promise<number> {
        throw new Error('Method not implemented.');
    }
    begin(option?: TxnOption): Promise<string> {
        throw new Error('Method not implemented.');
    }
    commit(txnId: string): Promise<void> {
        throw new Error('Method not implemented.');
    }
    rollback(txnId: string): Promise<void> {
        throw new Error('Method not implemented.');
    }
    connect() {
        this.connector.connect();
    }
    disconnect() {
        this.connector.disconnect();
    }
    async initialize(dropIfExists?: boolean) {
        const sql = translateCreateDatabase(this.connector.configuration.database, this.connector.configuration.charset, dropIfExists);
        for (const stmt of sql) {
            await this.connector.exec(stmt);
        }        
    }
}