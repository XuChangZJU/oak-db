import { EntityDict, Context, DeduceCreateSingleOperation, DeduceRemoveOperation, DeduceUpdateOperation, OperateOption, OperationResult, SelectionResult, TxnOption, SelectRowShape, StorageSchema, DeduceCreateMultipleOperation, SelectOption } from 'oak-domain/lib/types';
import { CascadeStore } from 'oak-domain/lib/store/CascadeStore';
import { MySQLConfiguration } from './types/Configuration';
import { MySqlConnector } from './connector';
import { MySqlTranslator, MySqlSelectOption, MysqlOperateOption } from './translator';
export declare class MysqlStore<ED extends EntityDict, Cxt extends Context<ED>> extends CascadeStore<ED, Cxt> {
    connector: MySqlConnector;
    translator: MySqlTranslator<ED>;
    constructor(storageSchema: StorageSchema<ED>, configuration: MySQLConfiguration);
    protected supportManyToOneJoin(): boolean;
    protected supportMultipleCreate(): boolean;
    private formResult;
    protected selectAbjointRow<T extends keyof ED, S extends ED[T]['Selection']>(entity: T, selection: S, context: Cxt, option?: MySqlSelectOption): Promise<SelectRowShape<ED[T]['Schema'], S['data']>[]>;
    protected updateAbjointRow<T extends keyof ED>(entity: T, operation: DeduceCreateMultipleOperation<ED[T]['Schema']> | DeduceCreateSingleOperation<ED[T]['Schema']> | DeduceUpdateOperation<ED[T]['Schema']> | DeduceRemoveOperation<ED[T]['Schema']>, context: Cxt, option?: MysqlOperateOption): Promise<number>;
    operate<T extends keyof ED>(entity: T, operation: ED[T]['Operation'], context: Cxt, params?: OperateOption): Promise<OperationResult<ED>>;
    select<T extends keyof ED, S extends ED[T]['Selection']>(entity: T, selection: S, context: Cxt, option?: SelectOption): Promise<SelectionResult<ED[T]['Schema'], S['data']>>;
    count<T extends keyof ED>(entity: T, selection: Pick<ED[T]['Selection'], 'filter' | 'count'>, context: Cxt, option?: SelectOption): Promise<number>;
    begin(option?: TxnOption): Promise<string>;
    commit(txnId: string): Promise<void>;
    rollback(txnId: string): Promise<void>;
    connect(): void;
    disconnect(): void;
    initialize(dropIfExists?: boolean): Promise<void>;
}
