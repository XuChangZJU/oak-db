import { EntityDict, OperateOption, Q_FullTextValue, Ref, RefOrExpression, SelectOption, StorageSchema } from "oak-domain/lib/types";
import { DataType } from "oak-domain/lib/types/schema/DataTypes";
export interface SqlSelectOption extends SelectOption {
}
export interface SqlOperateOption extends OperateOption {
}
export declare abstract class SqlTranslator<ED extends EntityDict> {
    readonly schema: StorageSchema<ED>;
    constructor(schema: StorageSchema<ED>);
    private makeFullSchema;
    protected abstract getDefaultSelectFilter<OP extends SqlSelectOption>(alias: string, option?: OP): string;
    protected abstract translateAttrProjection(dataType: DataType, alias: string, attr: string): string;
    protected abstract translateAttrValue(dataType: DataType | Ref, value: any): string;
    protected abstract translateFullTextSearch<T extends keyof ED>(value: Q_FullTextValue, entity: T, alias: string): string;
    abstract translateCreateEntity<T extends keyof ED>(entity: T, option: {
        replace?: boolean;
    }): string[];
    protected abstract populateSelectStmt<T extends keyof ED, OP extends SqlSelectOption>(projectionText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, groupByText?: string, indexFrom?: number, count?: number, option?: OP, selection?: ED[T]['Selection'], aggregation?: ED[T]['Aggregation']): string;
    protected abstract populateUpdateStmt<OP extends SqlOperateOption>(updateText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: OP): string;
    protected abstract populateRemoveStmt<OP extends SqlOperateOption>(removeText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: OP): string;
    protected abstract translateExpression<T extends keyof ED>(entity: T, alias: string, expression: RefOrExpression<keyof ED[T]['OpSchema']>, refDict: Record<string, [string, keyof ED]>): string;
    private getStorageName;
    translateInsert<T extends keyof ED>(entity: T, data: ED[T]['CreateMulti']['data']): string;
    /**
     * analyze the join relations in projection/query/sort
     * 所有的层次关系都当成left join处理，如果有内表为空的情况，请手动处理
     * {
     *      b: {
     *          name: {
     *              $exists: false,
     *          }
*           }
     * }
     * 这样的query会把内表为空的行也返回
     * @param param0
     */
    private analyzeJoin;
    private translateComparison;
    private translateElement;
    private translateEvaluation;
    private translateFilter;
    private translateSorter;
    private translateProjection;
    private translateSelectInner;
    translateSelect<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: ED[T]['Selection'], option?: OP): string;
    translateWhere<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: ED[T]['Selection'], option?: OP): string;
    translateAggregate<T extends keyof ED, OP extends SqlSelectOption>(entity: T, aggregation: ED[T]['Aggregation'], option?: OP): string;
    translateCount<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: Pick<ED[T]['Selection'], 'filter' | 'count'>, option?: OP): string;
    translateRemove<T extends keyof ED, OP extends SqlOperateOption>(entity: T, operation: ED[T]['Remove'], option?: OP): string;
    translateUpdate<T extends keyof ED, OP extends SqlOperateOption>(entity: T, operation: ED[T]['Update'], option?: OP): string;
    translateDestroyEntity(entity: string, truncate?: boolean): string;
    escapeStringValue(value: string): string;
}
