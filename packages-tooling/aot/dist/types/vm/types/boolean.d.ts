import { $AnyNonError, PotentialNonEmptyCompletionType, CompletionTarget, $Any } from './_shared';
import { Realm, ExecutionContext } from '../realm';
import { $Object } from './object';
import { $String } from './string';
import { $Number } from './number';
import { $BooleanLiteral } from '../ast/literals';
import { I$Node } from '../ast/_shared';
export declare class $Boolean<T extends boolean = boolean> {
    readonly realm: Realm;
    readonly sourceNode: $BooleanLiteral | null;
    readonly conversionSource: $AnyNonError | null;
    readonly '<$Boolean>': unknown;
    readonly id: number;
    readonly IntrinsicName: 'boolean';
    '[[Type]]': PotentialNonEmptyCompletionType;
    readonly '[[Value]]': T;
    '[[Target]]': CompletionTarget;
    get isAbrupt(): false;
    get Type(): 'Boolean';
    get isEmpty(): false;
    get isUndefined(): false;
    get isNull(): false;
    get isNil(): false;
    get isBoolean(): true;
    get isNumber(): false;
    get isString(): false;
    get isSymbol(): false;
    get isPrimitive(): true;
    get isObject(): false;
    get isArray(): false;
    get isProxy(): false;
    get isFunction(): false;
    get isBoundFunction(): false;
    get isTruthy(): T;
    get isFalsey(): T extends true ? false : true;
    get isSpeculative(): false;
    get hasValue(): true;
    get isList(): false;
    readonly nodeStack: I$Node[];
    ctx: ExecutionContext | null;
    stack: string;
    constructor(realm: Realm, value: T, type?: PotentialNonEmptyCompletionType, target?: CompletionTarget, sourceNode?: $BooleanLiteral | null, conversionSource?: $AnyNonError | null);
    is(other: $AnyNonError): other is $Boolean<T>;
    enrichWith(ctx: ExecutionContext, node: I$Node): this;
    [Symbol.toPrimitive](): string;
    [Symbol.toStringTag](): string;
    ToCompletion(type: PotentialNonEmptyCompletionType, target: CompletionTarget): this;
    UpdateEmpty(value: $Any): this;
    ToObject(ctx: ExecutionContext): $Object;
    ToPropertyKey(ctx: ExecutionContext): $String;
    ToLength(ctx: ExecutionContext): $Number;
    ToPrimitive(ctx: ExecutionContext): this;
    ToBoolean(ctx: ExecutionContext): this;
    ToNumber(ctx: ExecutionContext): $Number;
    ToInt32(ctx: ExecutionContext): $Number;
    ToUint32(ctx: ExecutionContext): $Number;
    ToInt16(ctx: ExecutionContext): $Number;
    ToUint16(ctx: ExecutionContext): $Number;
    ToInt8(ctx: ExecutionContext): $Number;
    ToUint8(ctx: ExecutionContext): $Number;
    ToUint8Clamp(ctx: ExecutionContext): $Number;
    ToString(ctx: ExecutionContext): $String;
    GetValue(ctx: ExecutionContext): this;
}
//# sourceMappingURL=boolean.d.ts.map