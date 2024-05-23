import { type IServiceLocator } from '@aurelia/kernel';
import { type Scope } from '../binding/scope';
import type { ICollectionSubscriber, IConnectable, IObservable, IObserverLocator, IObserverLocatorBasedConnectable, ISubscriber } from '@aurelia/runtime';
import type { IWatcherCallback } from '../watch';
import { IsBindingBehavior } from '@aurelia/expression-parser';
import { IBinding } from '../binding/interfaces-bindings';
export interface ComputedWatcher extends IObserverLocatorBasedConnectable, IServiceLocator {
}
export declare class ComputedWatcher implements IBinding, ISubscriber, ICollectionSubscriber {
    readonly obj: IObservable;
    readonly $get: (obj: object, watcher: IConnectable) => unknown;
    readonly useProxy: boolean;
    isBound: boolean;
    private running;
    get value(): unknown;
    constructor(obj: IObservable, observerLocator: IObserverLocator, $get: (obj: object, watcher: IConnectable) => unknown, cb: IWatcherCallback<object>, useProxy: boolean);
    handleChange(): void;
    handleCollectionChange(): void;
    bind(): void;
    unbind(): void;
    private run;
    private compute;
}
export interface ExpressionWatcher extends IObserverLocatorBasedConnectable, /* a hack, but it's only for internal */ IServiceLocator {
}
export declare class ExpressionWatcher implements IBinding, IObserverLocatorBasedConnectable {
    scope: Scope;
    l: IServiceLocator;
    oL: IObserverLocator;
    isBound: boolean;
    get value(): unknown;
    constructor(scope: Scope, l: IServiceLocator, oL: IObserverLocator, expression: IsBindingBehavior, callback: IWatcherCallback<object>);
    handleChange(value: unknown): void;
    bind(): void;
    unbind(): void;
}
//# sourceMappingURL=watchers.d.ts.map