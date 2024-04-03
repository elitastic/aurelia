"use strict";

Object.defineProperty(exports, "__esModule", {
    value: true
});

var e = require("@aurelia/kernel");

var r = require("@aurelia/runtime-html");

var t = require("@aurelia/platform-browser");

var s = require("@aurelia/metadata");

var o = require("@aurelia/platform");

var p = require("@aurelia/runtime");

const x = t.BrowserPlatform.getOrCreate(globalThis);

function createContainer() {
    return e.DI.createContainer().register(e.Registration.instance(r.IPlatform, x), r.StandardConfiguration);
}

class Aurelia extends r.Aurelia {
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
        if (r.CustomElement.isType(e)) {
            const t = r.CustomElement.getDefinition(e);
            let s = document.querySelector(t.name);
            if (s === null) {
                s = document.body;
            }
            return super.app({
                host: s,
                component: e
            });
        }
        return super.app(e);
    }
}

exports.ConsoleSink = e.ConsoleSink;

exports.DI = e.DI;

exports.EventAggregator = e.EventAggregator;

exports.IContainer = e.IContainer;

exports.IEventAggregator = e.IEventAggregator;

exports.ILogger = e.ILogger;

exports.IServiceLocator = e.IServiceLocator;

exports.InstanceProvider = e.InstanceProvider;

exports.LogLevel = e.LogLevel;

exports.LoggerConfiguration = e.LoggerConfiguration;

exports.Registration = e.Registration;

exports.all = e.all;

exports.allResources = e.allResources;

exports.bound = e.bound;

exports.camelCase = e.camelCase;

exports.emptyArray = e.emptyArray;

exports.emptyObject = e.emptyObject;

exports.factory = e.factory;

exports.ignore = e.ignore;

exports.inject = e.inject;

exports.isArrayIndex = e.isArrayIndex;

exports.kebabCase = e.kebabCase;

exports.lazy = e.lazy;

exports.newInstanceForScope = e.newInstanceForScope;

exports.newInstanceOf = e.newInstanceOf;

exports.noop = e.noop;

exports.optional = e.optional;

exports.pascalCase = e.pascalCase;

exports.resolve = e.resolve;

exports.resource = e.resource;

exports.singleton = e.singleton;

exports.toArray = e.toArray;

exports.transient = e.transient;

exports.AppTask = r.AppTask;

exports.AuSlotsInfo = r.AuSlotsInfo;

exports.Bindable = r.Bindable;

exports.BindingBehavior = r.BindingBehavior;

exports.BindingMode = r.BindingMode;

exports.ChildrenBinding = r.ChildrenBinding;

exports.Controller = r.Controller;

exports.CustomAttribute = r.CustomAttribute;

exports.CustomElement = r.CustomElement;

exports.FlushQueue = r.FlushQueue;

exports.IAppRoot = r.IAppRoot;

exports.IAttrMapper = r.IAttrMapper;

exports.IAttributePattern = r.IAttributePattern;

exports.IAuSlotWatcher = r.IAuSlotWatcher;

exports.IAuSlotsInfo = r.IAuSlotsInfo;

exports.IAurelia = r.IAurelia;

exports.IController = r.IController;

exports.IEventModifier = r.IEventModifier;

exports.IEventTarget = r.IEventTarget;

exports.IFlushQueue = r.IFlushQueue;

exports.IKeyMapping = r.IKeyMapping;

exports.ILifecycleHooks = r.ILifecycleHooks;

exports.IModifiedEventHandlerCreator = r.IModifiedEventHandlerCreator;

exports.INode = r.INode;

exports.IPlatform = r.IPlatform;

exports.IRenderLocation = r.IRenderLocation;

exports.ITemplateCompiler = r.ITemplateCompiler;

exports.ITemplateCompilerHooks = r.ITemplateCompilerHooks;

exports.IViewFactory = r.IViewFactory;

exports.LifecycleHooks = r.LifecycleHooks;

exports.NodeObserverLocator = r.NodeObserverLocator;

exports.ShortHandBindingSyntax = r.ShortHandBindingSyntax;

exports.StyleConfiguration = r.StyleConfiguration;

exports.TemplateCompilerHooks = r.TemplateCompilerHooks;

exports.ValueConverter = r.ValueConverter;

exports.ViewFactory = r.ViewFactory;

exports.alias = r.alias;

exports.attributePattern = r.attributePattern;

exports.bindable = r.bindable;

exports.bindingBehavior = r.bindingBehavior;

exports.bindingCommand = r.bindingCommand;

exports.capture = r.capture;

exports.children = r.children;

exports.coercer = r.coercer;

exports.containerless = r.containerless;

exports.cssModules = r.cssModules;

exports.customAttribute = r.customAttribute;

exports.customElement = r.customElement;

exports.lifecycleHooks = r.lifecycleHooks;

exports.processContent = r.processContent;

exports.registerAliases = r.registerAliases;

exports.renderer = r.renderer;

exports.shadowCSS = r.shadowCSS;

exports.slotted = r.slotted;

exports.templateCompilerHooks = r.templateCompilerHooks;

exports.templateController = r.templateController;

exports.useShadowDOM = r.useShadowDOM;

exports.valueConverter = r.valueConverter;

exports.watch = r.watch;

exports.Metadata = s.Metadata;

exports.Platform = o.Platform;

exports.Task = o.Task;

exports.TaskAbortError = o.TaskAbortError;

exports.TaskQueue = o.TaskQueue;

exports.ComputedObserver = p.ComputedObserver;

exports.IObserverLocator = p.IObserverLocator;

exports.ISignaler = p.ISignaler;

exports.batch = p.batch;

exports.observable = p.observable;

exports.subscriberCollection = p.subscriberCollection;

exports.Aurelia = Aurelia;

exports.PLATFORM = x;

exports.default = Aurelia;
//# sourceMappingURL=index.cjs.map
