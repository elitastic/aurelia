import { IContainer } from '@aurelia/kernel';
import { Aurelia as $Aurelia, IPlatform, CustomElementType, ICustomElementViewModel } from '@aurelia/runtime-html';
import { BrowserPlatform } from '@aurelia/platform-browser';
import type { ISinglePageAppConfig, IEnhancementConfig } from '@aurelia/runtime-html';
export declare const PLATFORM: BrowserPlatform<typeof globalThis>;
export { IPlatform };
export declare class Aurelia extends $Aurelia {
    constructor(container?: IContainer);
    static app(config: ISinglePageAppConfig<object> | CustomElementType): Omit<Aurelia, 'register' | 'app' | 'enhance'>;
    static enhance<T extends ICustomElementViewModel>(config: IEnhancementConfig<T>): ReturnType<$Aurelia['enhance']>;
    static register(...params: readonly unknown[]): Aurelia;
    app(config: ISinglePageAppConfig<object> | CustomElementType): Omit<this, 'register' | 'app' | 'enhance'>;
}
export default Aurelia;
export { Metadata, } from '@aurelia/metadata';
export { type ITask, Platform, type QueueTaskOptions, Task, TaskAbortError, TaskQueue, type TaskStatus } from '@aurelia/platform';
export { all, DI, IContainer, type IFactory, inject, resolve, type IRegistration, type IRegistry, type IResolver, IServiceLocator, type Key, lazy, factory, newInstanceOf, newInstanceForScope, optional, resource, allResources, ignore, Registration, singleton, transient, InstanceProvider, type Resolved, type Class, type Constructable, type ConstructableClass, type IDisposable, type IIndexable, type ColorOptions, ILogger, ConsoleSink, LoggerConfiguration, emptyArray, emptyObject, noop, LogLevel, EventAggregator, IEventAggregator, isArrayIndex, camelCase, kebabCase, pascalCase, toArray, bound, } from '@aurelia/kernel';
export { type CollectionKind, batch, ComputedObserver, IObserverLocator, ISignaler, subscriberCollection, type BindingBehaviorInstance, observable, type ValueConverterInstance, type IndexMap, } from '@aurelia/runtime';
export { customAttribute, CustomAttribute, templateController, containerless, customElement, CustomElement, capture, useShadowDOM, AppTask, BindingMode, bindable, type PartialBindableDefinition, Bindable, coercer, type PartialChildrenDefinition, children, Controller, ViewFactory, type ISinglePageAppConfig, IAppRoot, INode, IEventTarget, IRenderLocation, type ICustomAttributeViewModel, type ICustomElementViewModel, IController, IViewFactory, IFlushQueue, FlushQueue, type IFlushable, renderer, IAurelia, NodeObserverLocator, type IAuSlot, IAuSlotsInfo, AuSlotsInfo, IAuSlotWatcher, slotted, ChildrenBinding, ITemplateCompiler, ITemplateCompilerHooks, TemplateCompilerHooks, templateCompilerHooks, attributePattern, IAttributePattern, IAttrMapper, alias, registerAliases, bindingBehavior, BindingBehavior, valueConverter, ValueConverter, bindingCommand, type BindingCommandInstance, type IEnhancementConfig, type IHydratedParentController, ShortHandBindingSyntax, StyleConfiguration, type IShadowDOMConfiguration, cssModules, shadowCSS, processContent, ILifecycleHooks, type LifecycleHook, LifecycleHooks, lifecycleHooks, watch, IKeyMapping, IModifiedEventHandlerCreator, IEventModifier, type IModifiedEventHandler, } from '@aurelia/runtime-html';
//# sourceMappingURL=index.d.ts.map