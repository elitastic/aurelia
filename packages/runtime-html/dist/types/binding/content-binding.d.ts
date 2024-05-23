import { IAstEvaluator } from '../ast.eval';
import { type IServiceLocator } from '@aurelia/kernel';
import type { TaskQueue } from '@aurelia/platform';
import type { ICollectionSubscriber, IObserverLocator, IObserverLocatorBasedConnectable, ISubscriber } from '@aurelia/runtime';
import { type Scope } from './scope';
import type { IPlatform } from '../platform';
import type { BindingMode, IBinding, IBindingController } from './interfaces-bindings';
import { IsExpression } from '@aurelia/expression-parser';
export interface ContentBinding extends IAstEvaluator, IServiceLocator, IObserverLocatorBasedConnectable {
}
/**
 * A binding for handling the element content interpolation
 */
export declare class ContentBinding implements IBinding, ISubscriber, ICollectionSubscriber {
    private readonly p;
    readonly ast: IsExpression;
    readonly target: Text;
    isBound: boolean;
    readonly mode: BindingMode;
    strict: boolean;
    constructor(controller: IBindingController, locator: IServiceLocator, observerLocator: IObserverLocator, taskQueue: TaskQueue, p: IPlatform, ast: IsExpression, target: Text);
    updateTarget(value: unknown): void;
    handleChange(): void;
    handleCollectionChange(): void;
    bind(_scope: Scope): void;
    unbind(): void;
}
//# sourceMappingURL=content-binding.d.ts.map