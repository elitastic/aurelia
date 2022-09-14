import { IServiceLocator } from '@aurelia/kernel';
import { BindingBehaviorExpression, IConnectableBinding, IObserverLocator, LifecycleFlags, Scope } from '@aurelia/runtime';
import { BindingInterceptor } from '@aurelia/runtime-html';
import { BindingWithBehavior, ValidationEvent, ValidationResultsSubscriber } from './validation-controller';
/**
 * Validation triggers.
 */
export declare enum ValidationTrigger {
    /**
     * Manual validation.  Use the controller's `validate()` and  `reset()` methods to validate all bindings.
     */
    manual = "manual",
    /**
     * Validate the binding when the binding's target element fires a DOM 'blur' event.
     */
    blur = "blur",
    /**
     * Validate the binding when the binding's target element fires a DOM 'focusout' event.
     */
    focusout = "focusout",
    /**
     * Validate the binding when it updates the model due to a change in the source property (usually triggered by some change in view)
     */
    change = "change",
    /**
     * Validate the binding when the binding's target element fires a DOM 'blur' event and when it updates the model due to a change in the view.
     */
    changeOrBlur = "changeOrBlur",
    /**
     * Validate the binding when the binding's target element fires a DOM 'focusout' event and when it updates the model due to a change in the view.
     */
    changeOrFocusout = "changeOrFocusout"
}
/**
 * Binding behavior. Indicates the bound property should be validated.
 */
export declare class ValidateBindingBehavior extends BindingInterceptor implements ValidationResultsSubscriber {
    readonly binding: BindingWithBehavior;
    private propertyBinding;
    private target;
    private trigger;
    private readonly scopedController?;
    private controller;
    private isChangeTrigger;
    private readonly defaultTrigger;
    private scope;
    private readonly triggerMediator;
    private readonly controllerMediator;
    private readonly rulesMediator;
    private isDirty;
    private validatedOnce;
    private triggerEvent;
    private bindingInfo;
    private readonly platform;
    constructor(binding: BindingWithBehavior, expr: BindingBehaviorExpression);
    updateSource(value: unknown, flags: LifecycleFlags): void;
    handleEvent(_event: Event): void;
    $bind(flags: LifecycleFlags, scope: Scope): void;
    $unbind(flags: LifecycleFlags): void;
    handleTriggerChange(newValue: unknown, _previousValue: unknown, _flags: LifecycleFlags): void;
    handleControllerChange(newValue: unknown, _previousValue: unknown, _flags: LifecycleFlags): void;
    handleRulesChange(newValue: unknown, _previousValue: unknown, _flags: LifecycleFlags): void;
    handleValidationEvent(event: ValidationEvent): void;
    private task;
    private validateBinding;
    private setTriggerEvent;
    private setBindingInfo;
}
declare type MediatedBinding<K extends string> = {
    [key in K]: (newValue: unknown, previousValue: unknown, flags: LifecycleFlags) => void;
};
export interface BindingMediator<K extends string> extends IConnectableBinding {
}
export declare class BindingMediator<K extends string> implements IConnectableBinding {
    readonly key: K;
    readonly binding: MediatedBinding<K>;
    oL: IObserverLocator;
    locator: IServiceLocator;
    interceptor: this;
    constructor(key: K, binding: MediatedBinding<K>, oL: IObserverLocator, locator: IServiceLocator);
    $bind(): void;
    $unbind(): void;
    handleChange(newValue: unknown, previousValue: unknown, flags: LifecycleFlags): void;
}
export {};
//# sourceMappingURL=validate-binding-behavior.d.ts.map