import { IModule, Realm, ExecutionContext } from '../realm';
import { $AnyNonError, $AnyNonEmpty, $AnyObject } from './_shared';
import { $String } from './string';
import { $Null } from './null';
import { $Boolean } from './boolean';
import { $Empty } from './empty';
import { $Undefined } from './undefined';
import { $Function } from './function';
import { ILogger, IDisposable, Writable } from '@aurelia/kernel';
import { $Error } from './error';
export type $EnvRec = ($DeclarativeEnvRec | $ObjectEnvRec | $FunctionEnvRec | $GlobalEnvRec | $ModuleEnvRec);
export declare class $Binding implements IDisposable {
    isMutable: boolean;
    isStrict: boolean;
    isInitialized: boolean;
    canBeDeleted: boolean;
    value: $AnyNonError;
    name: string;
    origin: $EnvRec;
    M: IModule | null;
    N2: $String | null;
    readonly '<$Binding>': unknown;
    readonly id: number;
    get isIndirect(): boolean;
    constructor(isMutable: boolean, isStrict: boolean, isInitialized: boolean, canBeDeleted: boolean, value: $AnyNonError, name: string, origin: $EnvRec, M?: IModule | null, N2?: $String | null);
    dispose(this: Writable<Partial<$Binding>>): void;
}
export declare class $DeclarativeEnvRec implements IDisposable {
    readonly logger: ILogger;
    readonly realm: Realm;
    readonly outer: $EnvRec | $Null;
    readonly '<$DeclarativeEnvRec>': unknown;
    readonly bindings: Map<string, $Binding>;
    get isEmpty(): false;
    get isUndefined(): false;
    get isNull(): false;
    get isNil(): false;
    get isBoolean(): false;
    get isNumber(): false;
    get isString(): false;
    get isSymbol(): false;
    get isPrimitive(): false;
    get isObject(): false;
    get isFunction(): false;
    constructor(logger: ILogger, realm: Realm, outer: $EnvRec | $Null);
    HasBinding(ctx: ExecutionContext, N: $String): $Boolean;
    CreateMutableBinding(ctx: ExecutionContext, N: $String, D: $Boolean): $Empty;
    CreateImmutableBinding(ctx: ExecutionContext, N: $String, S: $Boolean): $Empty;
    InitializeBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty): $Empty;
    SetMutableBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty, S: $Boolean): $Empty | $Error;
    GetBindingValue(ctx: ExecutionContext, N: $String, S: $Boolean): $AnyNonEmpty;
    DeleteBinding(ctx: ExecutionContext, N: $String): $Boolean;
    HasThisBinding(ctx: ExecutionContext): $Boolean;
    HasSuperBinding(ctx: ExecutionContext): $Boolean;
    WithBaseObject(ctx: ExecutionContext): $Undefined;
    dispose(this: Writable<Partial<$DeclarativeEnvRec>>): void;
}
export declare class $ObjectEnvRec implements IDisposable {
    readonly logger: ILogger;
    readonly realm: Realm;
    readonly outer: $EnvRec | $Null;
    readonly bindingObject: $AnyObject;
    readonly '<$ObjectEnvRec>': unknown;
    withEnvironment: boolean;
    get isEmpty(): false;
    get isUndefined(): false;
    get isNull(): false;
    get isNil(): false;
    get isBoolean(): false;
    get isNumber(): false;
    get isString(): false;
    get isSymbol(): false;
    get isPrimitive(): false;
    get isObject(): false;
    get isFunction(): false;
    constructor(logger: ILogger, realm: Realm, outer: $EnvRec | $Null, bindingObject: $AnyObject);
    HasBinding(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    CreateMutableBinding(ctx: ExecutionContext, N: $String, D: $Boolean): $Boolean | $Error;
    CreateImmutableBinding(ctx: ExecutionContext, N: $String, S: $Boolean): $Boolean | $Error;
    InitializeBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty): $Boolean | $Error;
    SetMutableBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty, S: $Boolean): $Boolean | $Error;
    GetBindingValue(ctx: ExecutionContext, N: $String, S: $Boolean): $AnyNonEmpty;
    DeleteBinding(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    HasThisBinding(ctx: ExecutionContext): $Boolean<false>;
    HasSuperBinding(ctx: ExecutionContext): $Boolean<false>;
    WithBaseObject(ctx: ExecutionContext): $AnyObject | $Undefined;
    dispose(this: Writable<Partial<$ObjectEnvRec>>): void;
}
export type BindingStatus = 'lexical' | 'initialized' | 'uninitialized';
export declare class $FunctionEnvRec extends $DeclarativeEnvRec implements IDisposable {
    readonly logger: ILogger;
    readonly '<$FunctionEnvRec>': unknown;
    '[[ThisValue]]': $AnyNonEmpty;
    '[[ThisBindingStatus]]': BindingStatus;
    '[[FunctionObject]]': $Function;
    '[[HomeObject]]': $AnyObject | $Undefined;
    '[[NewTarget]]': $AnyObject | $Undefined;
    get isEmpty(): false;
    get isUndefined(): false;
    get isNull(): false;
    get isNil(): false;
    get isBoolean(): false;
    get isNumber(): false;
    get isString(): false;
    get isSymbol(): false;
    get isPrimitive(): false;
    get isObject(): false;
    get isFunction(): false;
    constructor(logger: ILogger, realm: Realm, F: $Function, newTarget: $AnyObject | $Undefined);
    HasThisBinding(ctx: ExecutionContext): $Boolean;
    HasSuperBinding(ctx: ExecutionContext): $Boolean;
    BindThisValue<T extends $AnyNonEmpty>(ctx: ExecutionContext, V: T): T | $Error;
    GetThisBinding(ctx: ExecutionContext): $AnyNonEmpty;
    GetSuperBase(ctx: ExecutionContext): $AnyObject | $Null | $Undefined | $Error;
    dispose(this: Writable<Partial<$FunctionEnvRec>>): void;
}
export declare class $GlobalEnvRec implements IDisposable {
    readonly logger: ILogger;
    readonly realm: Realm;
    readonly '<$GlobalEnvRec>': unknown;
    '[[ObjectRecord]]': $ObjectEnvRec;
    '[[GlobalThisValue]]': $AnyObject;
    '[[DeclarativeRecord]]': $DeclarativeEnvRec;
    '[[VarNames]]': string[];
    readonly outer: $Null;
    get isEmpty(): false;
    get isUndefined(): false;
    get isNull(): false;
    get isNil(): false;
    get isBoolean(): false;
    get isNumber(): false;
    get isString(): false;
    get isSymbol(): false;
    get isPrimitive(): false;
    get isObject(): false;
    get isFunction(): false;
    constructor(logger: ILogger, realm: Realm, G: $AnyObject, thisValue: $AnyObject);
    HasBinding(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    CreateMutableBinding(ctx: ExecutionContext, N: $String, D: $Boolean): $Empty | $Error;
    CreateImmutableBinding(ctx: ExecutionContext, N: $String, S: $Boolean): $Empty | $Error;
    InitializeBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty): $Boolean | $Empty | $Error;
    SetMutableBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty, S: $Boolean): $Boolean | $Empty | $Error;
    GetBindingValue(ctx: ExecutionContext, N: $String, S: $Boolean): $AnyNonEmpty;
    DeleteBinding(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    HasThisBinding(ctx: ExecutionContext): $Boolean<true>;
    HasSuperBinding(ctx: ExecutionContext): $Boolean<false>;
    WithBaseObject(ctx: ExecutionContext): $Undefined;
    GetThisBinding(ctx: ExecutionContext): $AnyObject;
    HasVarDeclaration(ctx: ExecutionContext, N: $String): $Boolean;
    HasLexicalDeclaration(ctx: ExecutionContext, N: $String): $Boolean;
    HasRestrictedGlobalProperty(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    CanDeclareGlobalVar(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    CanDeclareGlobalFunction(ctx: ExecutionContext, N: $String): $Boolean | $Error;
    CreateGlobalVarBinding(ctx: ExecutionContext, N: $String, D: $Boolean): $Empty | $Error;
    CreateGlobalFunctionBinding(ctx: ExecutionContext, N: $String, V: $AnyNonEmpty, D: $Boolean): $Empty | $Error;
    dispose(this: Writable<Partial<$GlobalEnvRec>>): void;
}
export declare class $ModuleEnvRec extends $DeclarativeEnvRec implements IDisposable {
    readonly logger: ILogger;
    readonly '<$ModuleEnvRec>': unknown;
    get isEmpty(): false;
    get isUndefined(): false;
    get isNull(): false;
    get isNil(): false;
    get isBoolean(): false;
    get isNumber(): false;
    get isString(): false;
    get isSymbol(): false;
    get isPrimitive(): false;
    get isObject(): false;
    get isFunction(): false;
    constructor(logger: ILogger, realm: Realm, outer: $EnvRec);
    GetBindingValue(ctx: ExecutionContext, N: $String, S: $Boolean): $AnyNonEmpty;
    DeleteBinding(ctx: ExecutionContext, N: never): never;
    HasThisBinding(ctx: ExecutionContext): $Boolean<true>;
    GetThisBinding(ctx: ExecutionContext): $Undefined;
    CreateImportBinding(ctx: ExecutionContext, N: $String, M: IModule, N2: $String): $Empty;
}
//# sourceMappingURL=environment-record.d.ts.map