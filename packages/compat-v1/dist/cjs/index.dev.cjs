'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var runtime = require('@aurelia/runtime');
var runtimeHtml = require('@aurelia/runtime-html');
var kernel = require('@aurelia/kernel');

let defined$1 = false;
function defineAstMethods() {
    if (defined$1) {
        return;
    }
    defined$1 = true;
    const def = (Klass, name, value) => Object.defineProperty(Klass.prototype, name, { configurable: true, enumerable: false, writable: true, value });
    [
        runtime.BindingBehaviorExpression,
        runtime.ValueConverterExpression,
        runtime.AssignExpression,
        runtime.ConditionalExpression,
        runtime.AccessThisExpression,
        runtime.AccessScopeExpression,
        runtime.AccessMemberExpression,
        runtime.AccessKeyedExpression,
        runtime.CallScopeExpression,
        runtime.CallMemberExpression,
        runtime.CallFunctionExpression,
        runtime.BinaryExpression,
        runtime.UnaryExpression,
        runtime.PrimitiveLiteralExpression,
        runtime.ArrayLiteralExpression,
        runtime.ObjectLiteralExpression,
        runtime.TemplateExpression,
        runtime.TaggedTemplateExpression,
        runtime.ArrayBindingPattern,
        runtime.ObjectBindingPattern,
        runtime.BindingIdentifier,
        runtime.ForOfStatement,
        runtime.Interpolation,
        runtime.DestructuringAssignmentExpression,
        runtime.DestructuringAssignmentSingleExpression,
        runtime.DestructuringAssignmentRestExpression,
        runtime.ArrowFunction,
    ].forEach(ast => {
        def(ast, 'evaluate', function (...args) {
            return runtime.astEvaluate(this, ...args);
        });
        def(ast, 'assign', function (...args) {
            return runtime.astAssign(this, ...args);
        });
        def(ast, 'accept', function (...args) {
            return runtime.astVisit(this, ...args);
        });
        def(ast, 'bind', function (...args) {
            return runtime.astBind(this, ...args);
        });
        def(ast, 'unbind', function (...args) {
            return runtime.astUnbind(this, ...args);
        });
    });
    console.warn('"evaluate"/"assign"/"accept"/"visit"/"bind"/"unbind" are only valid on AST with ast $kind "Custom".'
        + ' Or import and use astEvaluate/astAssign/astVisit/astBind/astUnbind accordingly.');
}

/******************************************************************************
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
***************************************************************************** */
/* global Reflect, Promise */


function __decorate(decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
}

/** @internal */ const createLookup = () => Object.create(null);
// eslint-disable-next-line @typescript-eslint/ban-types
/** @internal */ const isFunction = (v) => typeof v === 'function';
/** @internal */ const isString = (v) => typeof v === 'string';
// /** @internal */ export const rethrow = (err: unknown) => { throw err; };
// /** @internal */ export const areEqual = Object.is;
/** @internal */
const def = Reflect.defineProperty;
/** @internal */
const defineHiddenProp = (obj, key, value) => {
    def(obj, key, {
        enumerable: false,
        configurable: true,
        writable: true,
        value
    });
    return value;
};
/** @internal */
const ensureExpression = (parser, srcOrExpr, expressionType) => {
    if (isString(srcOrExpr)) {
        return parser.parse(srcOrExpr, expressionType);
    }
    return srcOrExpr;
};
/** @internal */ const etIsFunction = 'IsFunction';

const callRegisteredContainer = new WeakSet();
const callSyntax = {
    register(container) {
        if (!callRegisteredContainer.has(container)) {
            callRegisteredContainer.add(container);
            container.register(CallBindingCommand, exports.CallBindingRenderer);
        }
    }
};
const instructionType = 'rh';
class CallBindingInstruction {
    constructor(from, to) {
        this.from = from;
        this.to = to;
        this.type = instructionType;
    }
}
class CallBindingCommand {
    get ignoreAttr() { return false; }
    build(info, exprParser) {
        const target = info.bindable === null
            ? kernel.camelCase(info.attr.target)
            : info.bindable.name;
        return new CallBindingInstruction(exprParser.parse(info.attr.rawValue, etIsFunction), target);
    }
}
CallBindingCommand.$au = {
    type: 'binding-command',
    name: 'call',
};
exports.CallBindingRenderer = class CallBindingRenderer {
    render(renderingCtrl, target, instruction, platform, exprParser, observerLocator) {
        const expr = ensureExpression(exprParser, instruction.from, etIsFunction);
        renderingCtrl.addBinding(new CallBinding(renderingCtrl.container, observerLocator, expr, getTarget(target), instruction.to));
    }
};
exports.CallBindingRenderer = __decorate([
    runtimeHtml.renderer(instructionType)
], exports.CallBindingRenderer);
function getTarget(potentialTarget) {
    if (potentialTarget.viewModel != null) {
        return potentialTarget.viewModel;
    }
    return potentialTarget;
}
class CallBinding {
    constructor(locator, observerLocator, ast, target, targetProperty) {
        this.ast = ast;
        this.target = target;
        this.targetProperty = targetProperty;
        this.isBound = false;
        // see Listener binding for explanation
        /** @internal */
        this.boundFn = false;
        this.l = locator;
        this.targetObserver = observerLocator.getAccessor(target, targetProperty);
    }
    callSource(args) {
        const overrideContext = this._scope.overrideContext;
        overrideContext.$event = args;
        const result = runtime.astEvaluate(this.ast, this._scope, this, null);
        Reflect.deleteProperty(overrideContext, '$event');
        return result;
    }
    bind(_scope) {
        if (this.isBound) {
            if (this._scope === _scope) {
                return;
            }
            this.unbind();
        }
        this._scope = _scope;
        runtime.astBind(this.ast, _scope, this);
        this.targetObserver.setValue(($args) => this.callSource($args), this.target, this.targetProperty);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        runtime.astUnbind(this.ast, this._scope, this);
        this._scope = void 0;
        this.targetObserver.setValue(null, this.target, this.targetProperty);
    }
}
runtimeHtml.mixinUseScope(CallBinding);
runtimeHtml.mixingBindingLimited(CallBinding, () => 'callSource');
runtimeHtml.mixinAstEvaluator(true)(CallBinding);

const preventDefaultRegisteredContainer = new WeakSet();
const eventPreventDefaultBehavior = {
    /* istanbul ignore next */
    register(container) {
        if (preventDefaultRegisteredContainer.has(container)) {
            return;
        }
        preventDefaultRegisteredContainer.add(container);
        container.get(runtimeHtml.IListenerBindingOptions).prevent = true;
    }
};
const delegateRegisteredContainer = new WeakSet();
const delegateSyntax = {
    /* istanbul ignore next */
    register(container) {
        if (!delegateRegisteredContainer.has(container)) {
            delegateRegisteredContainer.add(container);
            container.register(IEventDelegator, DelegateBindingCommand, exports.ListenerBindingRenderer);
        }
    }
};
class DelegateBindingCommand {
    get ignoreAttr() { return true; }
    build(info, exprParser) {
        return new DelegateBindingInstruction(exprParser.parse(info.attr.rawValue, etIsFunction), info.attr.target, true);
    }
}
DelegateBindingCommand.$au = {
    type: 'binding-command',
    name: 'delegate',
};
exports.ListenerBindingRenderer = class ListenerBindingRenderer {
    constructor() {
        /** @internal */
        this._eventDelegator = kernel.resolve(IEventDelegator);
    }
    render(renderingCtrl, target, instruction, platform, exprParser) {
        const expr = ensureExpression(exprParser, instruction.from, etIsFunction);
        renderingCtrl.addBinding(new DelegateListenerBinding(renderingCtrl.container, expr, target, instruction.to, this._eventDelegator, new DelegateListenerOptions(instruction.preventDefault)));
    }
};
exports.ListenerBindingRenderer = __decorate([
    runtimeHtml.renderer('dl')
    /** @internal */
], exports.ListenerBindingRenderer);
class DelegateBindingInstruction {
    constructor(from, to, preventDefault) {
        this.from = from;
        this.to = to;
        this.preventDefault = preventDefault;
        this.type = runtimeHtml.InstructionType.listenerBinding;
    }
}
class DelegateListenerOptions {
    constructor(prevent) {
        this.prevent = prevent;
    }
}
/**
 * Listener binding. Handle event binding between view and view model
 */
class DelegateListenerBinding {
    constructor(locator, ast, target, targetEvent, eventDelegator, options) {
        this.ast = ast;
        this.target = target;
        this.targetEvent = targetEvent;
        this.eventDelegator = eventDelegator;
        this.isBound = false;
        this.handler = null;
        /**
         * Indicates if this binding evaluates an ast and get a function, that function should be bound
         * to the instance it is on
         *
         * @internal
         */
        this.boundFn = true;
        this.l = locator;
        this._options = options;
    }
    callSource(event) {
        const overrideContext = this._scope.overrideContext;
        overrideContext.$event = event;
        let result = runtime.astEvaluate(this.ast, this._scope, this, null);
        delete overrideContext.$event;
        if (isFunction(result)) {
            result = result(event);
        }
        if (result !== true && this._options.prevent) {
            event.preventDefault();
        }
        return result;
    }
    handleEvent(event) {
        this.callSource(event);
    }
    bind(_scope) {
        if (this.isBound) {
            if (this._scope === _scope) {
                return;
            }
            this.unbind();
        }
        this._scope = _scope;
        runtime.astBind(this.ast, _scope, this);
        this.handler = this.eventDelegator.addEventListener(this.l.get(runtimeHtml.IEventTarget), this.target, this.targetEvent, this);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        runtime.astUnbind(this.ast, this._scope, this);
        this._scope = void 0;
        this.handler.dispose();
        this.handler = null;
    }
}
runtimeHtml.mixinUseScope(DelegateListenerBinding);
runtimeHtml.mixingBindingLimited(DelegateListenerBinding, () => 'callSource');
runtimeHtml.mixinAstEvaluator(true, true)(DelegateListenerBinding);
const defaultOptions = {
    capture: false,
};
class ListenerTracker {
    constructor(_publisher, _eventName, _options = defaultOptions) {
        this._publisher = _publisher;
        this._eventName = _eventName;
        this._options = _options;
        this._count = 0;
        this._captureLookups = new Map();
        this._bubbleLookups = new Map();
    }
    _increment() {
        if (++this._count === 1) {
            this._publisher.addEventListener(this._eventName, this, this._options);
        }
    }
    _decrement() {
        if (--this._count === 0) {
            this._publisher.removeEventListener(this._eventName, this, this._options);
        }
    }
    dispose() {
        if (this._count > 0) {
            this._count = 0;
            this._publisher.removeEventListener(this._eventName, this, this._options);
        }
        this._captureLookups.clear();
        this._bubbleLookups.clear();
    }
    _getLookup(target) {
        const lookups = this._options.capture === true ? this._captureLookups : this._bubbleLookups;
        let lookup = lookups.get(target);
        if (lookup === void 0) {
            lookups.set(target, lookup = createLookup());
        }
        return lookup;
    }
    handleEvent(event) {
        const lookups = this._options.capture === true ? this._captureLookups : this._bubbleLookups;
        const path = event.composedPath();
        if (this._options.capture === true) {
            path.reverse();
        }
        for (const target of path) {
            const lookup = lookups.get(target);
            if (lookup === void 0) {
                continue;
            }
            const listener = lookup[this._eventName];
            if (listener === void 0) {
                continue;
            }
            if (isFunction(listener)) {
                listener(event);
            }
            else {
                listener.handleEvent(event);
            }
            if (event.cancelBubble === true) {
                return;
            }
        }
    }
}
/**
 * Enable dispose() pattern for `delegate` & `capture` commands
 */
class DelegateSubscription {
    constructor(_tracker, _lookup, _eventName, callback) {
        this._tracker = _tracker;
        this._lookup = _lookup;
        this._eventName = _eventName;
        _tracker._increment();
        _lookup[_eventName] = callback;
    }
    dispose() {
        this._tracker._decrement();
        this._lookup[this._eventName] = void 0;
    }
}
const IEventDelegator = /*@__PURE__*/ kernel.DI.createInterface('IEventDelegator', x => x.cachedCallback((handler) => {
    const instance = handler.invoke(EventDelegator);
    handler.register(runtimeHtml.AppTask.deactivating(() => instance.dispose()));
    return instance;
}));
class EventDelegator {
    constructor() {
        /** @internal */
        this._trackerMaps = createLookup();
    }
    addEventListener(publisher, target, eventName, listener, options) {
        const trackerMap = this._trackerMaps[eventName] ??= new Map();
        let tracker = trackerMap.get(publisher);
        if (tracker === void 0) {
            trackerMap.set(publisher, tracker = new ListenerTracker(publisher, eventName, options));
        }
        return new DelegateSubscription(tracker, tracker._getLookup(target), eventName, listener);
    }
    dispose() {
        for (const eventName in this._trackerMaps) {
            const trackerMap = this._trackerMaps[eventName];
            for (const tracker of trackerMap.values()) {
                tracker.dispose();
            }
            trackerMap.clear();
        }
    }
}

/* eslint-disable no-console */
let defined = false;
const defineBindingMethods = () => {
    if (defined)
        return;
    defined = true;
    [
        [runtimeHtml.PropertyBinding, 'Property binding'],
        [runtimeHtml.AttributeBinding, 'Attribute binding'],
        [runtimeHtml.ListenerBinding, 'Listener binding'],
        [CallBinding, 'Call binding'],
        [runtimeHtml.LetBinding, 'Let binding'],
        [runtimeHtml.InterpolationPartBinding, 'Interpolation binding'],
        [runtimeHtml.ContentBinding, 'Text binding'],
        [runtimeHtml.RefBinding, 'Ref binding'],
        [DelegateListenerBinding, 'Delegate Listener binding']
    ].forEach(([b, name]) => {
        Object.defineProperty(b.prototype, 'sourceExpression', {
            configurable: true,
            enumerable: false,
            get() {
                console.warn(getMessage(name, this.ast));
                return this.ast;
            },
            set(v) {
                console.warn(getMessage(name, this.ast));
                Reflect.set(this, 'ast', v);
            }
        });
    });
    const getMessage = (name, ast) => console.warn(`@deprecated "sourceExpression" property for expression on ${name}. It has been renamed to "ast". expression: "${runtime.Unparser.unparse(ast)}"`);
};

let compatEnabled = false;
let addedMetadata = false;
const prototype = runtimeHtml.AuCompose.prototype;
const ignore = Symbol();
const originalAttaching = prototype.attaching;
// eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
const originalPropertyChanged = prototype.propertyChanged;
/**
 * Ensure `<au-compose/>` works with v1 syntaxes:
 * - Prop: viewModel -> component:
 * - template syntax: view-model.bind -> component.bind
 *
 * - Prop: view -> template
 * - template syntax: view.bind -> template.bind
 */
function enableComposeCompat() {
    if (compatEnabled) {
        return;
    }
    compatEnabled = true;
    if (!addedMetadata) {
        addedMetadata = true;
        const def = runtimeHtml.CustomElement.getDefinition(runtimeHtml.AuCompose);
        def.bindables.viewModel = runtimeHtml.BindableDefinition.create('viewModel', runtimeHtml.AuCompose);
        def.bindables.view = runtimeHtml.BindableDefinition.create('view', runtimeHtml.AuCompose);
    }
    defineHiddenProp(prototype, 'viewModelChanged', function (value) {
        this.component = value;
    });
    defineHiddenProp(prototype, 'viewChanged', function (value) {
        this.template = value;
    });
    defineHiddenProp(prototype, 'attaching', function (...rest) {
        this[ignore] = true;
        if (this.viewModel !== void 0) {
            this.component = this.viewModel;
        }
        if (this.view !== void 0) {
            this.template = this.view;
        }
        this[ignore] = false;
        return originalAttaching.apply(this, rest);
    });
    defineHiddenProp(prototype, 'propertyChanged', function (name) {
        // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
        if (this[ignore]) {
            return;
        }
        switch (name) {
            // already handled via the change handler calls
            case 'viewModel':
            case 'view': return;
        }
        return originalPropertyChanged.call(this, name);
    });
}
function disableComposeCompat() {
    if (!compatEnabled) {
        return;
    }
    if (addedMetadata) {
        addedMetadata = false;
        const def = runtimeHtml.CustomElement.getDefinition(runtimeHtml.AuCompose);
        delete def.bindables.viewModel;
        delete def.bindables.view;
    }
    compatEnabled = false;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    delete prototype.viewModelChanged;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    delete prototype.viewChanged;
    defineHiddenProp(prototype, 'attaching', originalAttaching);
    defineHiddenProp(prototype, 'propertyChanged', originalPropertyChanged);
}

class BindingEngine {
    constructor() {
        this.parser = kernel.resolve(runtime.IExpressionParser);
        this.observerLocator = kernel.resolve(runtime.IObserverLocator);
    }
    propertyObserver(object, prop) {
        return {
            subscribe: (callback) => {
                const observer = this.observerLocator.getObserver(object, prop);
                const subscriber = {
                    handleChange: (newValue, oldValue) => callback(newValue, oldValue)
                };
                observer.subscribe(subscriber);
                return {
                    dispose: () => observer.unsubscribe(subscriber)
                };
            },
        };
    }
    collectionObserver(collection) {
        return {
            subscribe: (callback) => {
                const observer = runtime.getCollectionObserver(collection);
                const subscriber = {
                    handleCollectionChange: (collection, indexMap) => callback(collection, indexMap)
                };
                observer?.subscribe(subscriber);
                return {
                    dispose: () => observer?.unsubscribe(subscriber)
                };
            }
        };
    }
    expressionObserver(bindingContext, expression) {
        const scope = runtime.Scope.create(bindingContext, {}, true);
        return {
            subscribe: callback => {
                const observer = new runtimeHtml.ExpressionWatcher(scope, null, this.observerLocator, this.parser.parse(expression, 'IsProperty'), callback);
                observer.bind();
                return {
                    dispose: () => observer.unbind()
                };
            }
        };
    }
}

/**
 * Register all services/functionalities necessary for a v1 app to work with Aurelia v2.
 *
 * Ideally should only be used for migration as there maybe be perf penalties to application doing it this way.
 */
const compatRegistration = {
    register(container) {
        defineAstMethods();
        defineBindingMethods();
        enableComposeCompat();
        container.register(eventPreventDefaultBehavior, delegateSyntax, callSyntax);
    }
};

exports.BindingEngine = BindingEngine;
exports.CallBinding = CallBinding;
exports.CallBindingCommand = CallBindingCommand;
exports.CallBindingInstruction = CallBindingInstruction;
exports.DelegateBindingCommand = DelegateBindingCommand;
exports.DelegateBindingInstruction = DelegateBindingInstruction;
exports.DelegateListenerBinding = DelegateListenerBinding;
exports.DelegateListenerOptions = DelegateListenerOptions;
exports.EventDelegator = EventDelegator;
exports.IEventDelegator = IEventDelegator;
exports.callSyntax = callSyntax;
exports.compatRegistration = compatRegistration;
exports.delegateSyntax = delegateSyntax;
exports.disableComposeCompat = disableComposeCompat;
exports.enableComposeCompat = enableComposeCompat;
exports.eventPreventDefaultBehavior = eventPreventDefaultBehavior;
//# sourceMappingURL=index.dev.cjs.map
