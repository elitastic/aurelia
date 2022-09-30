import { DI, Registration, optional, all, ILogger, camelCase } from '@aurelia/kernel';
import { implementAstEvaluator, mixingBindingLimited, bindingBehavior, attributePattern, bindingCommand, renderer, AttrSyntax, IPlatform, lifecycleHooks, CustomElement, CustomAttribute, ILifecycleHooks } from '@aurelia/runtime-html';
import { Scope, connectable, astEvaluate, astBind, astUnbind, IExpressionParser, IObserverLocator } from '@aurelia/runtime';

const IActionHandler = DI.createInterface('IActionHandler');
const IStore = DI.createInterface('IStore');
const IState = DI.createInterface('IState');

const actionHandlerSymbol = '__au_ah__';
const ActionHandler = Object.freeze({
    define(reducer) {
        function registry(state, action, ...params) {
            return reducer(state, action, ...params);
        }
        registry[actionHandlerSymbol] = true;
        registry.register = function (c) {
            Registration.instance(IActionHandler, reducer).register(c);
        };
        return registry;
    },
    isType: (r) => typeof r === 'function' && actionHandlerSymbol in r,
});

class Store {
    constructor(initialState, reducers, logger) {
        this._subs = new Set();
        this._dispatching = 0;
        this._dispatchQueues = [];
        this._state = initialState ?? new State();
        this._handlers = reducers;
        this._logger = logger;
    }
    static register(c) {
        Registration.singleton(IStore, this).register(c);
    }
    subscribe(subscriber) {
        {
            if (this._subs.has(subscriber)) {
                this._logger.warn('A subscriber is trying to subscribe to state change again.');
                return;
            }
        }
        this._subs.add(subscriber);
    }
    unsubscribe(subscriber) {
        {
            if (!this._subs.has(subscriber)) {
                this._logger.warn('Unsubscribing a non-listening subscriber');
                return;
            }
        }
        this._subs.delete(subscriber);
    }
    _setState(state) {
        const prevState = this._state;
        this._state = state;
        this._subs.forEach(sub => sub.handleStateChange(state, prevState));
    }
    getState() {
        {
            return new Proxy(this._state, new StateProxyHandler(this, this._logger));
        }
    }
    dispatch(type, ...params) {
        if (this._dispatching > 0) {
            this._dispatchQueues.push({ type, params });
            return;
        }
        this._dispatching++;
        let $$action;
        const reduce = ($state, $action, params) => this._handlers.reduce(($state, handler) => {
            if ($state instanceof Promise) {
                return $state.then($ => handler($, $action, ...params ?? []));
            }
            return handler($state, $action, ...params ?? []);
        }, $state);
        const afterDispatch = ($state) => {
            if (this._dispatchQueues.length > 0) {
                $$action = this._dispatchQueues.shift();
                const newState = reduce($state, $$action.type, $$action.params);
                if (newState instanceof Promise) {
                    return newState.then($ => afterDispatch($));
                }
                else {
                    return afterDispatch(newState);
                }
            }
        };
        const newState = reduce(this._state, type, params);
        if (newState instanceof Promise) {
            return newState.then($state => {
                this._setState($state);
                this._dispatching--;
                return afterDispatch(this._state);
            }, ex => {
                this._dispatching--;
                throw ex;
            });
        }
        else {
            this._setState(newState);
            this._dispatching--;
            return afterDispatch(this._state);
        }
    }
}
Store.inject = [optional(IState), all(IActionHandler), ILogger];
class State {
}
class StateProxyHandler {
    constructor(_owner, _logger) {
        this._owner = _owner;
        this._logger = _logger;
    }
    set(target, prop, value, receiver) {
        this._logger.warn(`Setting State is immutable. Dispatch an action to create a new state`);
        return true;
    }
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

function __decorate(decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
}

function createStateBindingScope(state, scope) {
    const overrideContext = { bindingContext: state };
    const stateScope = Scope.create(state, overrideContext, true);
    stateScope.parent = scope;
    return stateScope;
}
function isSubscribable$1(v) {
    return v instanceof Object && 'subscribe' in v;
}

class StateBinding {
    constructor(controller, locator, observerLocator, taskQueue, ast, target, prop, store) {
        this.isBound = false;
        this.task = null;
        this._value = void 0;
        this._sub = void 0;
        this._updateCount = 0;
        this.boundFn = false;
        this.mode = 2;
        this._controller = controller;
        this.locator = locator;
        this.taskQueue = taskQueue;
        this._store = store;
        this.oL = observerLocator;
        this.ast = ast;
        this.target = target;
        this.targetProperty = prop;
    }
    updateTarget(value) {
        const targetAccessor = this.targetObserver;
        const target = this.target;
        const prop = this.targetProperty;
        const updateCount = this._updateCount++;
        const isCurrentValue = () => updateCount === this._updateCount - 1;
        this._unsub();
        if (isSubscribable(value)) {
            this._sub = value.subscribe($value => {
                if (isCurrentValue()) {
                    targetAccessor.setValue($value, target, prop);
                }
            });
            return;
        }
        if (value instanceof Promise) {
            void value.then($value => {
                if (isCurrentValue()) {
                    targetAccessor.setValue($value, target, prop);
                }
            }, () => { });
            return;
        }
        targetAccessor.setValue(value, target, prop);
    }
    $bind(scope) {
        if (this.isBound) {
            return;
        }
        this.targetObserver = this.oL.getAccessor(this.target, this.targetProperty);
        this._store.subscribe(this);
        this.updateTarget(this._value = astEvaluate(this.ast, this.scope = createStateBindingScope(this._store.getState(), scope), this, this.mode > 1 ? this : null));
        this.isBound = true;
    }
    $unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this._unsub();
        this._updateCount++;
        this.scope = void 0;
        this.task?.cancel();
        this.task = null;
        this._store.unsubscribe(this);
    }
    handleChange(newValue) {
        if (!this.isBound) {
            return;
        }
        const shouldQueueFlush = this._controller.state !== 1 && (this.targetObserver.type & 4) > 0;
        const obsRecord = this.obs;
        obsRecord.version++;
        newValue = astEvaluate(this.ast, this.scope, this, this);
        obsRecord.clear();
        let task;
        if (shouldQueueFlush) {
            task = this.task;
            this.task = this.taskQueue.queueTask(() => {
                this.updateTarget(newValue);
                this.task = null;
            }, updateTaskOpts);
            task?.cancel();
            task = null;
        }
        else {
            this.updateTarget(newValue);
        }
    }
    handleStateChange() {
        if (!this.isBound) {
            return;
        }
        const state = this._store.getState();
        const scope = this.scope;
        const overrideContext = scope.overrideContext;
        scope.bindingContext = overrideContext.bindingContext = overrideContext.$state = state;
        const value = astEvaluate(this.ast, scope, this, this.mode > 1 ? this : null);
        const shouldQueueFlush = this._controller.state !== 1 && (this.targetObserver.type & 4) > 0;
        if (value === this._value) {
            return;
        }
        this._value = value;
        let task = null;
        if (shouldQueueFlush) {
            task = this.task;
            this.task = this.taskQueue.queueTask(() => {
                this.updateTarget(value);
                this.task = null;
            }, updateTaskOpts);
            task?.cancel();
        }
        else {
            this.updateTarget(this._value);
        }
    }
    _unsub() {
        if (typeof this._sub === 'function') {
            this._sub();
        }
        else if (this._sub !== void 0) {
            this._sub.dispose?.();
            this._sub.unsubscribe?.();
        }
        this._sub = void 0;
    }
}
function isSubscribable(v) {
    return v instanceof Object && 'subscribe' in v;
}
const updateTaskOpts = {
    reusable: false,
    preempt: true,
};
connectable(StateBinding);
implementAstEvaluator(true)(StateBinding);
mixingBindingLimited(StateBinding, () => 'updateTarget');

const bindingStateSubscriberMap = new WeakMap();
let StateBindingBehavior = class StateBindingBehavior {
    constructor(store) {
        this._store = store;
    }
    bind(scope, binding) {
        const isStateBinding = binding instanceof StateBinding;
        scope = isStateBinding ? scope : createStateBindingScope(this._store.getState(), scope);
        let subscriber;
        if (!isStateBinding) {
            subscriber = bindingStateSubscriberMap.get(binding);
            if (subscriber == null) {
                bindingStateSubscriberMap.set(binding, subscriber = new StateSubscriber(binding, scope));
            }
            else {
                subscriber._wrappedScope = scope;
            }
            this._store.subscribe(subscriber);
            binding.useScope(scope);
        }
    }
    unbind(scope, binding) {
        const isStateBinding = binding instanceof StateBinding;
        if (!isStateBinding) {
            this._store.unsubscribe(bindingStateSubscriberMap.get(binding));
            bindingStateSubscriberMap.delete(binding);
        }
    }
};
StateBindingBehavior.inject = [IStore];
StateBindingBehavior = __decorate([
    bindingBehavior('state')
], StateBindingBehavior);
class StateSubscriber {
    constructor(_binding, _wrappedScope) {
        this._binding = _binding;
        this._wrappedScope = _wrappedScope;
    }
    handleStateChange(state) {
        const scope = this._wrappedScope;
        const overrideContext = scope.overrideContext;
        scope.bindingContext = overrideContext.bindingContext = overrideContext.$state = state;
        this._binding.handleChange?.(undefined, undefined);
    }
}

class StateDispatchBinding {
    constructor(locator, expr, target, prop, store) {
        this.isBound = false;
        this.boundFn = false;
        this.locator = locator;
        this._store = store;
        this.ast = expr;
        this.target = target;
        this.targetProperty = prop;
    }
    callSource(e) {
        const scope = this.scope;
        scope.overrideContext.$event = e;
        const value = astEvaluate(this.ast, scope, this, null);
        delete scope.overrideContext.$event;
        if (!this.isAction(value)) {
            throw new Error(`Invalid dispatch value from expression on ${this.target} on event: "${e.type}"`);
        }
        void this._store.dispatch(value.type, ...(value.params instanceof Array ? value.params : []));
    }
    handleEvent(e) {
        this.callSource(e);
    }
    $bind(scope) {
        if (this.isBound) {
            return;
        }
        astBind(this.ast, scope, this);
        this.scope = createStateBindingScope(this._store.getState(), scope);
        this.target.addEventListener(this.targetProperty, this);
        this._store.subscribe(this);
        this.isBound = true;
    }
    $unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        astUnbind(this.ast, this.scope, this);
        this.scope = void 0;
        this.target.removeEventListener(this.targetProperty, this);
        this._store.unsubscribe(this);
    }
    handleStateChange(state) {
        const scope = this.scope;
        const overrideContext = scope.overrideContext;
        scope.bindingContext = overrideContext.bindingContext = state;
    }
    isAction(value) {
        return value != null
            && typeof value === 'object'
            && 'type' in value;
    }
}
connectable(StateDispatchBinding);
implementAstEvaluator(true)(StateDispatchBinding);
mixingBindingLimited(StateDispatchBinding, () => 'callSource');

let StateAttributePattern = class StateAttributePattern {
    'PART.state'(rawName, rawValue, parts) {
        return new AttrSyntax(rawName, rawValue, parts[0], 'state');
    }
};
StateAttributePattern = __decorate([
    attributePattern({ pattern: 'PART.state', symbols: '.' })
], StateAttributePattern);
let DispatchAttributePattern = class DispatchAttributePattern {
    'PART.dispatch'(rawName, rawValue, parts) {
        return new AttrSyntax(rawName, rawValue, parts[0], 'dispatch');
    }
};
DispatchAttributePattern = __decorate([
    attributePattern({ pattern: 'PART.dispatch', symbols: '.' })
], DispatchAttributePattern);
let StateBindingCommand = class StateBindingCommand {
    get type() { return 0; }
    get name() { return 'state'; }
    build(info, parser, attrMapper) {
        const attr = info.attr;
        let target = attr.target;
        let value = attr.rawValue;
        if (info.bindable == null) {
            target = attrMapper.map(info.node, target)
                ?? camelCase(target);
        }
        else {
            if (value === '' && info.def.type === 1) {
                value = camelCase(target);
            }
            target = info.bindable.property;
        }
        return new StateBindingInstruction(value, target);
    }
};
StateBindingCommand = __decorate([
    bindingCommand('state')
], StateBindingCommand);
let DispatchBindingCommand = class DispatchBindingCommand {
    get type() { return 1; }
    get name() { return 'dispatch'; }
    build(info) {
        const attr = info.attr;
        return new DispatchBindingInstruction(attr.target, attr.rawValue);
    }
};
DispatchBindingCommand = __decorate([
    bindingCommand('dispatch')
], DispatchBindingCommand);
class StateBindingInstruction {
    constructor(from, to) {
        this.from = from;
        this.to = to;
        this.type = 'sb';
    }
}
class DispatchBindingInstruction {
    constructor(from, ast) {
        this.from = from;
        this.ast = ast;
        this.type = 'sd';
    }
}
let StateBindingInstructionRenderer = class StateBindingInstructionRenderer {
    constructor(_exprParser, _observerLocator, _stateContainer, p) {
        this._exprParser = _exprParser;
        this._observerLocator = _observerLocator;
        this._stateContainer = _stateContainer;
        this.p = p;
    }
    render(renderingCtrl, target, instruction) {
        const binding = new StateBinding(renderingCtrl, renderingCtrl.container, this._observerLocator, this.p.domWriteQueue, ensureExpression(this._exprParser, instruction.from, 4), target, instruction.to, this._stateContainer);
        renderingCtrl.addBinding(binding);
    }
};
StateBindingInstructionRenderer.inject = [IExpressionParser, IObserverLocator, IStore, IPlatform];
StateBindingInstructionRenderer = __decorate([
    renderer('sb')
], StateBindingInstructionRenderer);
let DispatchBindingInstructionRenderer = class DispatchBindingInstructionRenderer {
    constructor(_exprParser, _stateContainer) {
        this._exprParser = _exprParser;
        this._stateContainer = _stateContainer;
    }
    render(renderingCtrl, target, instruction) {
        const expr = ensureExpression(this._exprParser, instruction.ast, 8);
        renderingCtrl.addBinding(new StateDispatchBinding(renderingCtrl.container, expr, target, instruction.from, this._stateContainer));
    }
};
DispatchBindingInstructionRenderer.inject = [IExpressionParser, IStore];
DispatchBindingInstructionRenderer = __decorate([
    renderer('sd')
], DispatchBindingInstructionRenderer);
function ensureExpression(parser, srcOrExpr, expressionType) {
    if (typeof srcOrExpr === 'string') {
        return parser.parse(srcOrExpr, expressionType);
    }
    return srcOrExpr;
}

const standardRegistrations = [
    StateAttributePattern,
    StateBindingCommand,
    StateBindingInstructionRenderer,
    DispatchAttributePattern,
    DispatchBindingCommand,
    DispatchBindingInstructionRenderer,
    StateBindingBehavior,
    Store,
];
const createConfiguration = (initialState, reducers) => {
    return {
        register: (c) => {
            c.register(Registration.instance(IState, initialState), ...standardRegistrations, ...reducers.map(ActionHandler.define));
        },
        init: (state, ...reducers) => createConfiguration(state, reducers),
    };
};
const StateDefaultConfiguration = createConfiguration({}, []);

let StateGetterBinding = class StateGetterBinding {
    constructor(locator, target, prop, store, getValue) {
        this.isBound = false;
        this._value = void 0;
        this._sub = void 0;
        this._updateCount = 0;
        this.locator = locator;
        this._store = store;
        this.$get = getValue;
        this.target = target;
        this.key = prop;
    }
    updateTarget(value) {
        const target = this.target;
        const prop = this.key;
        const updateCount = this._updateCount++;
        const isCurrentValue = () => updateCount === this._updateCount - 1;
        this._unsub();
        if (isSubscribable$1(value)) {
            this._sub = value.subscribe($value => {
                if (isCurrentValue()) {
                    target[prop] = $value;
                }
            });
            return;
        }
        if (value instanceof Promise) {
            void value.then($value => {
                if (isCurrentValue()) {
                    target[prop] = $value;
                }
            }, () => { });
            return;
        }
        target[prop] = value;
    }
    $bind(scope) {
        if (this.isBound) {
            return;
        }
        const state = this._store.getState();
        this.scope = createStateBindingScope(state, scope);
        this._store.subscribe(this);
        this.updateTarget(this._value = this.$get(state));
        this.isBound = true;
    }
    $unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this._unsub();
        this._updateCount++;
        this.scope = void 0;
        this._store.unsubscribe(this);
    }
    handleStateChange(state) {
        const scope = this.scope;
        const overrideContext = scope.overrideContext;
        scope.bindingContext = overrideContext.bindingContext = overrideContext.$state = state;
        const value = this.$get(this._store.getState());
        if (value === this._value) {
            return;
        }
        this._value = value;
        this.updateTarget(value);
    }
    _unsub() {
        if (typeof this._sub === 'function') {
            this._sub();
        }
        else if (this._sub !== void 0) {
            this._sub.dispose?.();
            this._sub.unsubscribe?.();
        }
        this._sub = void 0;
    }
};
StateGetterBinding = __decorate([
    connectable()
], StateGetterBinding);

function fromState(getValue) {
    return function (target, key, desc) {
        if (typeof target === 'function') {
            throw new Error(`Invalid usage. @state can only be used on a field`);
        }
        if (typeof desc?.value !== 'undefined') {
            throw new Error(`Invalid usage. @state can only be used on a field`);
        }
        target = target.constructor;
        let dependencies = CustomElement.getAnnotation(target, dependenciesKey);
        if (dependencies == null) {
            CustomElement.annotate(target, dependenciesKey, dependencies = []);
        }
        dependencies.push(new HydratingLifecycleHooks(getValue, key));
        dependencies = CustomAttribute.getAnnotation(target, dependenciesKey);
        if (dependencies == null) {
            CustomElement.annotate(target, dependenciesKey, dependencies = []);
        }
        dependencies.push(new CreatedLifecycleHooks(getValue, key));
    };
}
const dependenciesKey = 'dependencies';
let HydratingLifecycleHooks = class HydratingLifecycleHooks {
    constructor($get, key) {
        this.$get = $get;
        this.key = key;
    }
    register(c) {
        Registration.instance(ILifecycleHooks, this).register(c);
    }
    hydrating(vm, controller) {
        const container = controller.container;
        controller.addBinding(new StateGetterBinding(container, vm, this.key, container.get(IStore), this.$get));
    }
};
HydratingLifecycleHooks = __decorate([
    lifecycleHooks()
], HydratingLifecycleHooks);
let CreatedLifecycleHooks = class CreatedLifecycleHooks {
    constructor($get, key) {
        this.$get = $get;
        this.key = key;
    }
    register(c) {
        Registration.instance(ILifecycleHooks, this).register(c);
    }
    created(vm, controller) {
        const container = controller.container;
        controller.addBinding(new StateGetterBinding(container, vm, this.key, container.get(IStore), this.$get));
    }
};
CreatedLifecycleHooks = __decorate([
    lifecycleHooks()
], CreatedLifecycleHooks);

export { ActionHandler, DispatchAttributePattern, DispatchBindingCommand, DispatchBindingInstruction, DispatchBindingInstructionRenderer, IActionHandler, IState, IStore, StateAttributePattern, StateBinding, StateBindingBehavior, StateBindingCommand, StateBindingInstruction, StateBindingInstructionRenderer, StateDefaultConfiguration, StateDispatchBinding, fromState };
//# sourceMappingURL=index.dev.mjs.map
