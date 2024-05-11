import { DI, Registration } from '@aurelia/kernel';
export { ConsoleSink, DI, EventAggregator, IContainer, IEventAggregator, ILogger, IServiceLocator, InstanceProvider, LogLevel, LoggerConfiguration, Registration, all, allResources, bound, camelCase, emptyArray, emptyObject, factory, ignore, inject, isArrayIndex, kebabCase, lazy, newInstanceForScope, newInstanceOf, noop, optional, pascalCase, resolve, resource, singleton, toArray, transient } from '@aurelia/kernel';
import { Aurelia as Aurelia$1, CustomElement, IPlatform, StandardConfiguration } from '@aurelia/runtime-html';
export { AppTask, ArrayLikeHandler, AuSlotsInfo, Bindable, BindingBehavior, BindingMode, ChildrenBinding, Controller, CustomAttribute, CustomElement, FlushQueue, IAppRoot, IAuSlotWatcher, IAuSlotsInfo, IAurelia, IController, IEventModifier, IEventTarget, IFlushQueue, IKeyMapping, ILifecycleHooks, IModifiedEventHandlerCreator, INode, IPlatform, IRenderLocation, IRepeatableHandler, IRepeatableHandlerResolver, ISignaler, IViewFactory, LifecycleHooks, NodeObserverLocator, RuntimeTemplateCompilerImplementation, Scope, ShortHandBindingSyntax, StyleConfiguration, ValueConverter, ViewFactory, alias, bindable, bindingBehavior, capture, children, coercer, containerless, cssModules, customAttribute, customElement, lifecycleHooks, processContent, registerAliases, renderer, shadowCSS, slotted, templateController, useShadowDOM, valueConverter, watch } from '@aurelia/runtime-html';
import { BrowserPlatform } from '@aurelia/platform-browser';
export { Platform, Task, TaskAbortError, TaskQueue } from '@aurelia/platform';
export { CustomExpression, IExpressionParser } from '@aurelia/expression-parser';
export { ComputedObserver, IObserverLocator, batch, observable, subscriberCollection } from '@aurelia/runtime';
export { AttributePattern, BindingCommand, IAttrMapper, IAttributeParser, IAttributePattern, ITemplateCompiler, ITemplateCompilerHooks, ITemplateElementFactory, TemplateCompilerHooks, attributePattern, bindingCommand, templateCompilerHooks } from '@aurelia/template-compiler';

const PLATFORM = BrowserPlatform.getOrCreate(globalThis);
function createContainer() {
    return DI.createContainer()
        .register(Registration.instance(IPlatform, PLATFORM), StandardConfiguration);
}
class Aurelia extends Aurelia$1 {
    constructor(container = createContainer()) {
        super(container);
    }
    static app(config) {
        return new Aurelia().app(config);
    }
    static enhance(config) {
        return new Aurelia().enhance(config);
    }
    static register(...params) {
        return new Aurelia().register(...params);
    }
    app(config) {
        if (CustomElement.isType(config)) {
            // Default to custom element element name
            const definition = CustomElement.getDefinition(config);
            let host = document.querySelector(definition.name);
            if (host === null) {
                // When no target is found, default to body.
                // For example, when user forgot to write <my-app></my-app> in html.
                host = document.body;
            }
            return super.app({
                host: host,
                component: config
            });
        }
        return super.app(config);
    }
}

export { Aurelia, PLATFORM, Aurelia as default };
//# sourceMappingURL=index.dev.mjs.map
