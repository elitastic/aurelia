import { DI as e, Registration as r } from "../../../@aurelia/kernel/dist/native-modules/index.mjs";

export { ConsoleSink, DI, EventAggregator, IContainer, IEventAggregator, ILogger, IServiceLocator, InstanceProvider, LogLevel, LoggerConfiguration, Registration, all, allResources, bound, camelCase, emptyArray, emptyObject, factory, ignore, inject, isArrayIndex, kebabCase, lazy, newInstanceForScope, newInstanceOf, noop, optional, pascalCase, resolve, resource, singleton, toArray, transient } from "../../../@aurelia/kernel/dist/native-modules/index.mjs";

import { Aurelia as t, CustomElement as o, IPlatform as a, StandardConfiguration as n } from "../../../@aurelia/runtime-html/dist/native-modules/index.mjs";

export { AppTask, ArrayLikeHandler, AuSlotsInfo, Bindable, BindingBehavior, BindingMode, ChildrenBinding, Controller, CustomAttribute, CustomElement, FlushQueue, IAppRoot, IAuSlotWatcher, IAuSlotsInfo, IAurelia, IController, IEventModifier, IEventTarget, IFlushQueue, IKeyMapping, ILifecycleHooks, IModifiedEventHandlerCreator, INode, IPlatform, IRenderLocation, IRepeatableHandler, IRepeatableHandlerResolver, ISignaler, IViewFactory, LifecycleHooks, NodeObserverLocator, RuntimeTemplateCompilerImplementation, Scope, ShortHandBindingSyntax, StyleConfiguration, ValueConverter, ViewFactory, alias, bindable, bindingBehavior, capture, children, coercer, containerless, cssModules, customAttribute, customElement, lifecycleHooks, processContent, registerAliases, renderer, shadowCSS, slotted, templateController, useShadowDOM, valueConverter, watch } from "../../../@aurelia/runtime-html/dist/native-modules/index.mjs";

import { BrowserPlatform as i } from "../../../@aurelia/platform-browser/dist/native-modules/index.mjs";

export { Platform, Task, TaskAbortError, TaskQueue } from "../../../@aurelia/platform/dist/native-modules/index.mjs";

export { CustomExpression, IExpressionParser } from "../../../@aurelia/expression-parser/dist/native-modules/index.mjs";

export { ComputedObserver, IObserverLocator, batch, observable, subscriberCollection } from "../../../@aurelia/runtime/dist/native-modules/index.mjs";

export { AttributePattern, BindingCommand, IAttrMapper, IAttributeParser, IAttributePattern, ITemplateCompiler, ITemplateCompilerHooks, ITemplateElementFactory, TemplateCompilerHooks, attributePattern, bindingCommand, templateCompilerHooks } from "../../../@aurelia/template-compiler/dist/native-modules/index.mjs";

const l = i.getOrCreate(globalThis);

function createContainer() {
    return e.createContainer().register(r.instance(a, l), n);
}

class Aurelia extends t {
    constructor(e = createContainer()) {
        super(e);
    }
    static app(e) {
        return (new Aurelia).app(e);
    }
    static enhance(e) {
        return (new Aurelia).enhance(e);
    }
    static register(...e) {
        return (new Aurelia).register(...e);
    }
    app(e) {
        if (o.isType(e)) {
            const r = o.getDefinition(e);
            let t = document.querySelector(r.name);
            if (t === null) {
                t = document.body;
            }
            return super.app({
                host: t,
                component: e
            });
        }
        return super.app(e);
    }
}

export { Aurelia, l as PLATFORM, Aurelia as default };

