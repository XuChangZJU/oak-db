import { EntityDict, Context, DeduceCreateSingleOperation, DeduceRemoveOperation, DeduceUpdateOperation, OperateOption, OperationResult, SelectionResult, TxnOption, SelectRowShape, StorageSchema, DeduceCreateMultipleOperation, SelectOption } from 'oak-domain/lib/types';
import { CascadeStore } from 'oak-domain/lib/store/CascadeStore';
import { MySQLConfiguration } from './types/Configuration';
import { MySqlConnector } from './connector';
import { MySqlTranslator, MySqlSelectOption, MysqlOperateOption } from './translator';
import { assign } from 'lodash';
import assert from 'assert';
import { judgeRelation } from 'oak-domain/lib/store/relation';


function convertGeoTextToObject(geoText: string): object {
    if (geoText.startsWith('POINT')) {
        const coord = geoText.match((/(\d|\.)+(?=\)|\s)/g)) as string[];

        return {
            type: 'Point',
            coordinates: coord.map(
                ele => parseFloat(ele)
            ),
        };
    }
    else {
        throw new Error('only support Point now');
    }
}

export class MysqlStore<ED extends EntityDict, Cxt extends Context<ED>> extends CascadeStore<ED, Cxt> {
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
                    assert(rel === 2 || typeof rel === 'string');
                    resolveAttribute(typeof rel === 'string' ? rel : attrHead, r[attrHead], attrTail, value);
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


        function formalizeNullObject<E extends keyof ED>(r: Record<string, any>, e: E) {
            const { attributes: a2 } = schema[e];
            let allowFormalize = true;
            for (let attr in r) {
                if (typeof r[attr] === 'object' && a2[attr] && a2[attr].type === 'ref') {
                    if (formalizeNullObject(r[attr], a2[attr].ref!)) {
                        r[attr] = null;
                    }
                    else {
                        allowFormalize = false;
                    }
                }
                else if (r[attr] !== null) {
                    allowFormalize = false;
                }
            }

            return allowFormalize;
        }

        function formSingleRow(r: any): any {
            let result2 = {};
            for (let attr in r) {
                const value = r[attr];
                resolveAttribute(entity, result2, attr, value);
            }

            formalizeNullObject(result2, entity);
            return result2 as any;
        }

        if (result instanceof Array) {
            return result.map(
                r => formSingleRow(r)
            );
        }
        return formSingleRow(result);
    }
    protected async selectAbjointRow<T extends keyof ED, S extends ED[T]['Selection']>(
        entity: T,
        selection: S,
        context: Cxt,
        option?: MySqlSelectOption
    ): Promise<SelectRowShape<ED[T]['Schema'], S['data']>[]> {
        const sql = this.translator.translateSelect(entity, selection, option);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());

        return this.formResult(entity, result);
    }
    protected async updateAbjointRow<T extends keyof ED>(
        entity: T,
        operation: DeduceCreateMultipleOperation<ED[T]['Schema']> | DeduceCreateSingleOperation<ED[T]['Schema']> | DeduceUpdateOperation<ED[T]['Schema']> | DeduceRemoveOperation<ED[T]['Schema']>,
        context: Cxt,
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
    async operate<T extends keyof ED>(entity: T, operation: ED[T]['Operation'], context: Cxt, params?: OperateOption): Promise<OperationResult<ED>> {
        const { action } = operation;
        assert(!['select', 'download', 'stat'].includes(action), '现在不支持使用select operation');
        return await this.cascadeUpdate(entity, operation as any, context, params);
    }
    async select<T extends keyof ED, S extends ED[T]['Selection']>(entity: T, selection: S, context: Cxt, option?: SelectOption): Promise<SelectionResult<ED[T]['Schema'], S['data']>> {
        const result = await this.cascadeSelect(entity, selection, context, option);
        return {
            result,
        };
    }
    async count<T extends keyof ED>(entity: T, selection: Pick<ED[T]['Selection'], 'filter' | 'count'>, context: Cxt, option?: SelectOption): Promise<number> {
        const sql = this.translator.translateCount(entity, selection, option);

        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return result.count as number;
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