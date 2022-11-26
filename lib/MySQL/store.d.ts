import { EntityDict, DeduceCreateSingleOperation, DeduceRemoveOperation, DeduceUpdateOperation, OperateOption, OperationResult, TxnOption, StorageSchema, DeduceCreateMultipleOperation, SelectOption } from 'oak-domain/lib/types';
import { EntityDict as BaseEntityDict } from 'oak-domain/lib/base-app-domain';
import { CascadeStore } from 'oak-domain/lib/store/CascadeStore';
import { MySQLConfiguration } from './types/Configuration';
import { MySqlConnector } from './connector';
import { MySqlTranslator, MySqlSelectOption, MysqlOperateOption } from './translator';
import { AsyncContext, AsyncRowStore } from 'oak-domain/lib/store/AsyncRowStore';
import { SyncContext } from 'oak-domain/lib/store/SyncRowStore';
export declare class MysqlStore<ED extends EntityDict & BaseEntityDict, Cxt extends AsyncContext<ED>> extends CascadeStore<ED> implements AsyncRowStore<ED, Cxt> {
    protected selectAbjointRow<T extends keyof ED, OP extends SelectOption>(entity: T, selection: ED[T]['Selection'], context: SyncContext<ED>, option: OP): Partial<ED[T]['Schema']>[];
    protected updateAbjointRow<T extends keyof ED, OP extends OperateOption>(entity: T, operation: ED[T]['Operation'], context: SyncContext<ED>, option: OP): number;
    connector: MySqlConnector;
    translator: MySqlTranslator<ED>;
    constructor(storageSchema: StorageSchema<ED>, configuration: MySQLConfiguration);
    protected supportManyToOneJoin(): boolean;
    protected supportMultipleCreate(): boolean;
    private formResult;
    protected selectAbjointRowAsync<T extends keyof ED>(entity: T, selection: ED[T]['Selection'], context: AsyncContext<ED>, option?: MySqlSelectOption): Promise<Partial<ED[T]['Schema']>[]>;
    protected updateAbjointRowAsync<T extends keyof ED>(entity: T, operation: DeduceCreateMultipleOperation<ED[T]['Schema']> | DeduceCreateSingleOperation<ED[T]['Schema']> | DeduceUpdateOperation<ED[T]['Schema']> | DeduceRemoveOperation<ED[T]['Schema']>, context: AsyncContext<ED>, option?: MysqlOperateOption): Promise<number>;
    operate<T extends keyof ED>(entity: T, operation: ED[T]['Operation'], context: Cxt, option: OperateOption): Promise<OperationResult<ED>>;
    select<T extends keyof ED>(entity: T, selection: ED[T]['Selection'], context: Cxt, option: SelectOption): Promise<Partial<ED[T]['Schema']>[]>;
    count<T extends keyof ED>(entity: T, selection: Pick<ED[T]['Selection'], 'filter' | 'count'>, context: Cxt, option: SelectOption): Promise<number>;
    begin(option?: TxnOption): Promise<string>;
    commit(txnId: string): Promise<void>;
    rollback(txnId: string): Promise<void>;
    connect(): void;
    disconnect(): void;
    initialize(dropIfExists?: boolean): Promise<void>;
}
