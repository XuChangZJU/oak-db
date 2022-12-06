import { EntityDict, DeduceCreateSingleOperation, DeduceRemoveOperation, DeduceUpdateOperation, OperateOption,
    OperationResult, TxnOption, StorageSchema, DeduceCreateMultipleOperation, SelectOption } from 'oak-domain/lib/types';
import { EntityDict as BaseEntityDict } from 'oak-domain/lib/base-app-domain';
import { CascadeStore } from 'oak-domain/lib/store/CascadeStore';
import { MySQLConfiguration } from './types/Configuration';
import { MySqlConnector } from './connector';
import { MySqlTranslator, MySqlSelectOption, MysqlOperateOption } from './translator';
import { assign } from 'lodash';
import assert from 'assert';
import { judgeRelation } from 'oak-domain/lib/store/relation';
import { AsyncContext, AsyncRowStore } from 'oak-domain/lib/store/AsyncRowStore';
import { SyncContext } from 'oak-domain/lib/store/SyncRowStore';


function convertGeoTextToObject(geoText: string): object {
    if (geoText.startsWith('POINT')) {
        const coord = geoText.match((/(\d|\.)+(?=\)|\s)/g)) as string[];

        return {
            type: 'Point',
            coordinate: coord.map(
                ele => parseFloat(ele)
            ),
        };
    }
    else {
        throw new Error('only support Point now');
    }
}

export class MysqlStore<ED extends EntityDict & BaseEntityDict, Cxt extends AsyncContext<ED>> extends CascadeStore<ED> implements AsyncRowStore<ED, Cxt>{
    protected selectAbjointRow<T extends keyof ED, OP extends SelectOption>(entity: T, selection: ED[T]['Selection'], context: SyncContext<ED>, option: OP): Partial<ED[T]['Schema']>[] {
        throw new Error('MySQL store不支持同步取数据，不应该跑到这儿');
    }
    protected updateAbjointRow<T extends keyof ED, OP extends OperateOption>(entity: T, operation: ED[T]['Operation'], context: SyncContext<ED>, option: OP): number {
        throw new Error('MySQL store不支持同步更新数据，不应该跑到这儿');
    }
    connector: MySqlConnector;
    translator: MySqlTranslator<ED>;
    constructor(storageSchema: StorageSchema<ED>, configuration: MySQLConfiguration) {
        super(storageSchema);
        this.connector = new MySqlConnector(configuration);
        this.translator = new MySqlTranslator(storageSchema);
    }
    protected supportManyToOneJoin(): boolean {
        return true;
    }
    protected supportMultipleCreate(): boolean {
        return true;
    }
    private formResult<T extends keyof ED>(entity: T, result: any): any {
        const schema = this.getSchema();
        function resolveAttribute<E extends keyof ED>(entity2: E, r: Record<string, any>, attr: string, value: any) {
            const { attributes, view } = schema[entity2];
            if (!view) {
                const i = attr.indexOf(".");
                if (i !== -1) {
                    const attrHead = attr.slice(0, i);
                    const attrTail = attr.slice(i + 1);
                    if (!r[attrHead]) {
                        r[attrHead] = {};
                    }
                    const rel = judgeRelation(schema, entity2, attrHead);
                    if (rel === 2) {
                        resolveAttribute(attrHead, r[attrHead], attrTail, value);
                    }
                    else {
                        assert (typeof rel === 'string');
                        resolveAttribute(rel, r[attrHead], attrTail, value);
                    }
                }
                else if (attributes[attr]) {
                    const { type } = attributes[attr];
                    switch (type) {
                        case 'date':
                        case 'time': {
                            if (value instanceof Date) {
                                r[attr] = value.valueOf();
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'geometry': {
                            if (typeof value === 'string') {
                                r[attr] = convertGeoTextToObject(value);
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'object':
                        case 'array': {
                            if (typeof value === 'string') {
                                r[attr] = JSON.parse(value.replace(/[\r]/g, '\\r').replace(/[\n]/g, '\\n'));
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'function': {
                            if (typeof value === 'string') {
                                // 函数的执行环境需要的参数只有创建函数者知悉，只能由上层再创建Function
                                r[attr] = `return ${Buffer.from(value, 'base64').toString()}`;
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'bool':
                        case 'boolean': {
                            if (value === 0) {
                                r[attr] = false;
                            }
                            else if (value === 1) {
                                r[attr] = true;
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        default: {
                            r[attr] = value;
                        }
                    }
                }
                else {
                    r[attr] = value;
                }
            }
            else {
                assign(r, {
                    [attr]: value,
                });
            }
        }


        function removeNullObjects<E extends keyof ED>(r: Record<string, any>, e: E) {
            assert(r.id && typeof r.id === 'string', `对象${<string>e}取数据时发现id为非法值${r.id},rowId是${r.id}`)

            for (let attr in r) {
                const rel = judgeRelation(schema, e, attr);
                if (rel === 2) {
                    // 边界，如果是toModi的对象，这里的外键确实有可能为空
                    assert (schema[e].toModi || r.entity !== attr || r.entityId === r[attr].id, `对象${<string>e}取数据时，发现entityId与连接的对象的主键不一致，rowId是${r.id}，其entityId值为${r.entityId}，连接的对象的主键为${r[attr].id}`);
                    if (r[attr].id === null) {
                        assert(schema[e].toModi || r.entity !== attr);
                        delete r[attr];
                        continue;
                    }
                    assert (r.entity === attr, `对象${<string>e}取数据时，发现entity值与连接的外键对象不一致，rowId是${r.id}，其entity值为${r.entity}，连接的对象为${attr}`);
                    removeNullObjects(r[attr], attr);
                }
                else if (typeof rel === 'string') {
                    // 边界，如果是toModi的对象，这里的外键确实有可能为空
                    assert (schema[e].toModi || r[`${attr}Id`] === r[attr].id, `对象${<string>e}取数据时，发现其外键与连接的对象的主键不一致，rowId是${r.id}，其${attr}Id值为${r[`${attr}Id`]}，连接的对象的主键为${r[attr].id}`);
                    if (r[attr].id === null) {
                        assert(schema[e].toModi || r[`${attr}Id`] === null);
                        delete r[attr];
                        continue;
                    }
                    removeNullObjects(r[attr], rel);
                }
            }
        }

        function formSingleRow(r: any): any {
            let result2 = {};
            for (let attr in r) {
                const value = r[attr];
                resolveAttribute(entity, result2, attr, value);
            }

            removeNullObjects(result2, entity);
            return result2 as any;
        }

        if (result instanceof Array) {
            return result.map(
                r => formSingleRow(r)
            );
        }
        return formSingleRow(result);
    }
    protected async selectAbjointRowAsync<T extends keyof ED>(
        entity: T,
        selection: ED[T]['Selection'],
        context: AsyncContext<ED>,
        option?: MySqlSelectOption
    ): Promise<Partial<ED[T]['Schema']>[]> {
        const sql = this.translator.translateSelect(entity, selection, option);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());

        return this.formResult(entity, result);
    }
    protected async updateAbjointRowAsync<T extends keyof ED>(
        entity: T,
        operation: DeduceCreateMultipleOperation<ED[T]['Schema']> | DeduceCreateSingleOperation<ED[T]['Schema']> | DeduceUpdateOperation<ED[T]['Schema']> | DeduceRemoveOperation<ED[T]['Schema']>,
        context: AsyncContext<ED>,
        option?: MysqlOperateOption
    ): Promise<number> {
        const { translator, connector } = this;
        const { action } = operation;
        const txn = context.getCurrentTxnId();

        switch (action) {
            case 'create': {
                const { data } = operation as DeduceCreateMultipleOperation<ED[T]['Schema']> | DeduceCreateSingleOperation<ED[T]['Schema']>;
                const sql = translator.translateInsert(entity, data instanceof Array ? data : [data]);
                await connector.exec(sql, txn);
                if (!option?.dontCollect) {
                    context.opRecords.push({
                        a: 'c',
                        d: data as any,
                        e: entity,
                    });
                }
                return data instanceof Array ? data.length : 1;
            }
            case 'remove': {
                const sql = translator.translateRemove(entity, operation as ED[T]['Remove'], option);
                await connector.exec(sql, txn);

                // todo 这里对sorter和indexfrom/count的支持不完整
                if (!option?.dontCollect) {
                    context.opRecords.push({
                        a: 'r',
                        e: entity,
                        f: (operation as ED[T]['Remove']).filter,
                    });
                }
                return 1;
            }
            default: {
                assert(!['select', 'download', 'stat'].includes(action));
                const sql = translator.translateUpdate(entity, operation as ED[T]['Update'], option);
                await connector.exec(sql, txn);

                // todo 这里对sorter和indexfrom/count的支持不完整
                if (!option?.dontCollect) {
                    context.opRecords.push({
                        a: 'u',
                        e: entity,
                        d: (operation as ED[T]['Update']).data,
                        f: (operation as ED[T]['Update']).filter,
                    });
                }
                return 1;
            }
        }
    }
    async operate<T extends keyof ED>(entity: T, operation: ED[T]['Operation'], context: Cxt, option: OperateOption): Promise<OperationResult<ED>> {
        const { action } = operation;
        assert(!['select', 'download', 'stat'].includes(action), '现在不支持使用select operation');
        return await this.cascadeUpdateAsync(entity, operation as any, context, option);
    }
    async select<T extends keyof ED>(entity: T, selection: ED[T]['Selection'], context: Cxt, option: SelectOption): Promise<Partial<ED[T]['Schema']>[]> {
        const result = await this.cascadeSelectAsync(entity, selection, context, option);
        return result;
    }
    async count<T extends keyof ED>(entity: T, selection: Pick<ED[T]['Selection'], 'filter' | 'count'>, context: Cxt, option: SelectOption): Promise<number> {
        const sql = this.translator.translateCount(entity, selection, option);

        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return result[0].cnt as number;
    }
    async begin(option?: TxnOption): Promise<string> {
        const txn = await this.connector.startTransaction(option);
        return txn;
    }
    async commit(txnId: string): Promise<void> {
        await this.connector.commitTransaction(txnId);
    }
    async rollback(txnId: string): Promise<void> {
        await this.connector.rollbackTransaction(txnId);
    }
    connect() {
        this.connector.connect();
    }
    disconnect() {
        this.connector.disconnect();
    }
    async initialize(dropIfExists?: boolean) {
        const schema = this.getSchema();
        for (const entity in schema) {
            const sqls = this.translator.translateCreateEntity(entity, { replace: dropIfExists });
            for (const sql of sqls) {
                await this.connector.exec(sql);
            }
        }
    }
}