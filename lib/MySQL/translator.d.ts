import { EntityDict, Q_FullTextValue, RefOrExpression, Ref, StorageSchema } from "oak-domain/lib/types";
import { DataType } from "oak-domain/lib/types/schema/DataTypes";
import { SqlOperateOption, SqlSelectOption, SqlTranslator } from "../sqlTranslator";
export interface MySqlSelectOption extends SqlSelectOption {
}
export interface MysqlOperateOption extends SqlOperateOption {
}
export declare class MySqlTranslator<ED extends EntityDict> extends SqlTranslator<ED> {
    protected getDefaultSelectFilter(alias: string, option?: MySqlSelectOption): string;
    private makeUpSchema;
    constructor(schema: StorageSchema<ED>);
    static supportedDataTypes: DataType[];
    static spatialTypes: DataType[];
    static withLengthDataTypes: DataType[];
    static withPrecisionDataTypes: DataType[];
    static withScaleDataTypes: DataType[];
    static unsignedAndZerofillTypes: DataType[];
    static withWidthDataTypes: DataType[];
    static dataTypeDefaults: {
        varchar: {
            length: number;
        };
        nvarchar: {
            length: number;
        };
        "national varchar": {
            length: number;
        };
        char: {
            length: number;
        };
        binary: {
            length: number;
        };
        varbinary: {
            length: number;
        };
        decimal: {
            precision: number;
            scale: number;
        };
        dec: {
            precision: number;
            scale: number;
        };
        numeric: {
            precision: number;
            scale: number;
        };
        fixed: {
            precision: number;
            scale: number;
        };
        float: {
            precision: number;
        };
        double: {
            precision: number;
        };
        time: {
            precision: number;
        };
        datetime: {
            precision: number;
        };
        timestamp: {
            precision: number;
        };
        bit: {
            width: number;
        };
        int: {
            width: number;
        };
        integer: {
            width: number;
        };
        tinyint: {
            width: number;
        };
        smallint: {
            width: number;
        };
        mediumint: {
            width: number;
        };
        bigint: {
            width: number;
        };
    };
    maxAliasLength: number;
    private populateDataTypeDef;
    protected translateAttrProjection(dataType: DataType, alias: string, attr: string): string;
    protected translateAttrValue(dataType: DataType | Ref, value: any): string;
    protected translateFullTextSearch<T extends keyof ED>(value: Q_FullTextValue, entity: T, alias: string): string;
    translateCreateEntity<T extends keyof ED>(entity: T, options?: {
        replace?: boolean;
    }): string[];
    private translateFnName;
    protected translateExpression<T extends keyof ED>(alias: string, expression: RefOrExpression<keyof ED[T]["OpSchema"]>, refDict: Record<string, string>): string;
    protected populateSelectStmt<T extends keyof ED>(projectionText: string, fromText: string, selection: ED[T]['Selection'], aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: MySqlSelectOption): string;
    protected populateUpdateStmt(updateText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: MysqlOperateOption): string;
    protected populateRemoveStmt(removeText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: MysqlOperateOption): string;
}
