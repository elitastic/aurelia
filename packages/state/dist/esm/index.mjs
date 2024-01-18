import { DI as t, Registration as i, optional as s, all as e, ILogger as n, camelCase as h } from "@aurelia/kernel";

import { mixinAstEvaluator as r, mixingBindingLimited as c, bindingBehavior as o, attributePattern as a, bindingCommand as u, renderer as l, AttrSyntax as d, lifecycleHooks as f, CustomElement as g, CustomAttribute as S, ILifecycleHooks as b } from "@aurelia/runtime-html";

import { Scope as p, connectable as B, astEvaluate as _, astBind as m, astUnbind as y } from "@aurelia/runtime";

const v = /*@__PURE__*/ t.createInterface("IActionHandler");

const w = /*@__PURE__*/ t.createInterface("IStore");

const I = /*@__PURE__*/ t.createInterface("IState");

const D = "__au_ah__";

const P = Object.freeze({
    define(t) {
        function registry(i, s) {
            return t(i, s);
        }
        registry[D] = true;
        registry.register = function(s) {
            i.instance(v, t).register(s);
        };
        return registry;
    },
    isType: t => typeof t === "function" && D in t
});

class Store {
    static register(t) {
        i.singleton(w, this).register(t);
    }
    constructor(t, i, s) {
        this.t = new Set;
        this.i = 0;
        this.h = [];
        this._state = t ?? new State;
        this.u = i;
        this.B = s;
    }
    subscribe(t) {
        this.t.add(t);
    }
    unsubscribe(t) {
        this.t.delete(t);
    }
    _(t) {
        const i = this._state;
        this._state = t;
        this.t.forEach((s => s.handleStateChange(t, i)));
    }
    getState() {
        return this._state;
    }
    dispatch(t) {
        if (this.i > 0) {
            this.h.push(t);
            return;
        }
        this.i++;
        let i;
        const reduce = (t, i) => this.u.reduce(((t, s) => {
            if (t instanceof Promise) {
                return t.then((t => s(t, i)));
            }
            return s(t, i);
        }), t);
        const afterDispatch = t => {
            if (this.h.length > 0) {
                i = this.h.shift();
                const s = reduce(t, i);
                if (s instanceof Promise) {
                    return s.then((t => afterDispatch(t)));
                } else {
                    return afterDispatch(s);
                }
            }
        };
        const s = reduce(this._state, t);
        if (s instanceof Promise) {
            return s.then((t => {
                this._(t);
                this.i--;
                return afterDispatch(this._state);
            }), (t => {
                this.i--;
                throw t;
            }));
        } else {
            this._(s);
            this.i--;
            return afterDispatch(this._state);
        }
    }
}

Store.inject = [ s(I), e(v), n ];

class State {}

function __decorate(t, i, s, e) {
    var n = arguments.length, h = n < 3 ? i : e === null ? e = Object.getOwnPropertyDescriptor(i, s) : e, r;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") h = Reflect.decorate(t, i, s, e); else for (var c = t.length - 1; c >= 0; c--) if (r = t[c]) h = (n < 3 ? r(h) : n > 3 ? r(i, s, h) : r(i, s)) || h;
    return n > 3 && h && Object.defineProperty(i, s, h), h;
}

function createStateBindingScope(t, i) {
    const s = {
        bindingContext: t
    };
    const e = p.create(t, s, true);
    e.parent = i;
    return e;
}

function isSubscribable$1(t) {
    return t instanceof Object && "subscribe" in t;
}

class StateBinding {
    constructor(t, i, s, e, n, h, r, c) {
        this.isBound = false;
        this.I = null;
        this.v = void 0;
        this.P = void 0;
        this.C = 0;
        this.boundFn = false;
        this.mode = 2;
        this.R = t;
        this.l = i;
        this.A = e;
        this.T = c;
        this.oL = s;
        this.ast = n;
        this.target = h;
        this.targetProperty = r;
    }
    updateTarget(t) {
        const i = this.j;
        const s = this.target;
        const e = this.targetProperty;
        const n = this.C++;
        const isCurrentValue = () => n === this.C - 1;
        this.O();
        if (isSubscribable(t)) {
            this.P = t.subscribe((t => {
                if (isCurrentValue()) {
                    i.setValue(t, s, e);
                }
            }));
            return;
        }
        if (t instanceof Promise) {
            void t.then((t => {
                if (isCurrentValue()) {
                    i.setValue(t, s, e);
                }
            }), (() => {}));
            return;
        }
        i.setValue(t, s, e);
    }
    bind(t) {
        if (this.isBound) {
            return;
        }
        this.j = this.oL.getAccessor(this.target, this.targetProperty);
        this.T.subscribe(this);
        this.updateTarget(this.v = _(this.ast, this.s = createStateBindingScope(this.T.getState(), t), this, this.mode > 1 ? this : null));
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this.O();
        this.C++;
        this.s = void 0;
        this.I?.cancel();
        this.I = null;
        this.T.unsubscribe(this);
    }
    handleChange(t) {
        if (!this.isBound) {
            return;
        }
        const i = this.R.state !== 1 && (this.j.type & 4) > 0;
        const s = this.obs;
        s.version++;
        t = _(this.ast, this.s, this, this);
        s.clear();
        let e;
        if (i) {
            e = this.I;
            this.I = this.A.queueTask((() => {
                this.updateTarget(t);
                this.I = null;
            }), C);
            e?.cancel();
            e = null;
        } else {
            this.updateTarget(t);
        }
    }
    handleStateChange() {
        if (!this.isBound) {
            return;
        }
        const t = this.T.getState();
        const i = this.s;
        const s = i.overrideContext;
        i.bindingContext = s.bindingContext = s.$state = t;
        const e = _(this.ast, i, this, this.mode > 1 ? this : null);
        const n = this.R.state !== 1 && (this.j.type & 4) > 0;
        if (e === this.v) {
            return;
        }
        this.v = e;
        let h = null;
        if (n) {
            h = this.I;
            this.I = this.A.queueTask((() => {
                this.updateTarget(e);
                this.I = null;
            }), C);
            h?.cancel();
        } else {
            this.updateTarget(this.v);
        }
    }
    O() {
        if (typeof this.P === "function") {
            this.P();
        } else if (this.P !== void 0) {
            this.P.dispose?.();
            this.P.unsubscribe?.();
        }
        this.P = void 0;
    }
}

function isSubscribable(t) {
    return t instanceof Object && "subscribe" in t;
}

const C = {
    reusable: false,
    preempt: true
};

B(StateBinding);

r(true)(StateBinding);

c(StateBinding, (() => "updateTarget"));

const R = new WeakMap;

let A = class StateBindingBehavior {
    constructor(t) {
        this.T = t;
    }
    bind(t, i) {
        const s = i instanceof StateBinding;
        t = s ? t : createStateBindingScope(this.T.getState(), t);
        let e;
        if (!s) {
            e = R.get(i);
            if (e == null) {
                R.set(i, e = new StateSubscriber(i, t));
            } else {
                e.H = t;
            }
            this.T.subscribe(e);
            i.useScope?.(t);
        }
    }
    unbind(t, i) {
        const s = i instanceof StateBinding;
        if (!s) {
            this.T.unsubscribe(R.get(i));
            R.delete(i);
        }
    }
};

A.inject = [ w ];

A = __decorate([ o("state") ], A);

class StateSubscriber {
    constructor(t, i) {
        this.L = t;
        this.H = i;
    }
    handleStateChange(t) {
        const i = this.H;
        const s = i.overrideContext;
        i.bindingContext = s.bindingContext = s.$state = t;
        this.L.handleChange?.(undefined, undefined);
    }
}

class StateDispatchBinding {
    constructor(t, i, s, e, n) {
        this.isBound = false;
        this.boundFn = false;
        this.l = t;
        this.T = n;
        this.ast = i;
        this.$ = s;
        this.F = e;
    }
    callSource(t) {
        const i = this.s;
        i.overrideContext.$event = t;
        const s = _(this.ast, i, this, null);
        delete i.overrideContext.$event;
        void this.T.dispatch(s);
    }
    handleEvent(t) {
        this.callSource(t);
    }
    bind(t) {
        if (this.isBound) {
            return;
        }
        m(this.ast, t, this);
        this.s = createStateBindingScope(this.T.getState(), t);
        this.$.addEventListener(this.F, this);
        this.T.subscribe(this);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        y(this.ast, this.s, this);
        this.s = void 0;
        this.$.removeEventListener(this.F, this);
        this.T.unsubscribe(this);
    }
    handleStateChange(t) {
        const i = this.s;
        const s = i.overrideContext;
        i.bindingContext = s.bindingContext = t;
    }
}

B(StateDispatchBinding);

r(true)(StateDispatchBinding);

c(StateDispatchBinding, (() => "callSource"));

let T = class StateAttributePattern {
    "PART.state"(t, i, s) {
        return new d(t, i, s[0], "state");
    }
};

T = __decorate([ a({
    pattern: "PART.state",
    symbols: "."
}) ], T);

let E = class DispatchAttributePattern {
    "PART.dispatch"(t, i, s) {
        return new d(t, i, s[0], "dispatch");
    }
};

E = __decorate([ a({
    pattern: "PART.dispatch",
    symbols: "."
}) ], E);

let j = class StateBindingCommand {
    get type() {
        return "None";
    }
    get name() {
        return "state";
    }
    build(t, i, s) {
        const e = t.attr;
        let n = e.target;
        let r = e.rawValue;
        if (t.bindable == null) {
            n = s.map(t.node, n) ?? h(n);
        } else {
            if (r === "" && t.def.type === "Element") {
                r = h(n);
            }
            n = t.bindable.name;
        }
        return new StateBindingInstruction(r, n);
    }
};

j = __decorate([ u("state") ], j);

let x = class DispatchBindingCommand {
    get type() {
        return "IgnoreAttr";
    }
    get name() {
        return "dispatch";
    }
    build(t) {
        const i = t.attr;
        return new DispatchBindingInstruction(i.target, i.rawValue);
    }
};

x = __decorate([ u("dispatch") ], x);

class StateBindingInstruction {
    constructor(t, i) {
        this.from = t;
        this.to = i;
        this.type = "sb";
    }
}

class DispatchBindingInstruction {
    constructor(t, i) {
        this.from = t;
        this.ast = i;
        this.type = "sd";
    }
}

let O = class StateBindingInstructionRenderer {
    constructor(t) {
        this.G = t;
    }
    render(t, i, s, e, n, h) {
        t.addBinding(new StateBinding(t, t.container, h, e.domWriteQueue, ensureExpression(n, s.from, "IsFunction"), i, s.to, this.G));
    }
};

O.inject = [ w ];

O = __decorate([ l("sb") ], O);

let k = class DispatchBindingInstructionRenderer {
    constructor(t) {
        this.G = t;
    }
    render(t, i, s, e, n) {
        const h = ensureExpression(n, s.ast, "IsProperty");
        t.addBinding(new StateDispatchBinding(t.container, h, i, s.from, this.G));
    }
};

k.inject = [ w ];

k = __decorate([ l("sd") ], k);

function ensureExpression(t, i, s) {
    if (typeof i === "string") {
        return t.parse(i, s);
    }
    return i;
}

const H = [ T, j, O, E, x, k, A, Store ];

const createConfiguration = (t, s) => ({
    register: e => {
        e.register(i.instance(I, t), ...H, ...s.map(P.define));
    },
    init: (t, ...i) => createConfiguration(t, i)
});

const L = createConfiguration({}, []);

let $ = class StateGetterBinding {
    constructor(t, i, s, e) {
        this.isBound = false;
        this.v = void 0;
        this.P = void 0;
        this.C = 0;
        this.T = s;
        this.$get = e;
        this.target = t;
        this.key = i;
    }
    updateTarget(t) {
        const i = this.target;
        const s = this.key;
        const e = this.C++;
        const isCurrentValue = () => e === this.C - 1;
        this.O();
        if (isSubscribable$1(t)) {
            this.P = t.subscribe((t => {
                if (isCurrentValue()) {
                    i[s] = t;
                }
            }));
            return;
        }
        if (t instanceof Promise) {
            void t.then((t => {
                if (isCurrentValue()) {
                    i[s] = t;
                }
            }), (() => {}));
            return;
        }
        i[s] = t;
    }
    bind(t) {
        if (this.isBound) {
            return;
        }
        const i = this.T.getState();
        this.s = createStateBindingScope(i, t);
        this.T.subscribe(this);
        this.updateTarget(this.v = this.$get(i));
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this.O();
        this.C++;
        this.s = void 0;
        this.T.unsubscribe(this);
    }
    handleStateChange(t) {
        const i = this.s;
        const s = i.overrideContext;
        i.bindingContext = s.bindingContext = s.$state = t;
        const e = this.$get(this.T.getState());
        if (e === this.v) {
            return;
        }
        this.v = e;
        this.updateTarget(e);
    }
    O() {
        if (typeof this.P === "function") {
            this.P();
        } else if (this.P !== void 0) {
            this.P.dispose?.();
            this.P.unsubscribe?.();
        }
        this.P = void 0;
    }
};

$ = __decorate([ B() ], $);

function fromState(t) {
    return function(i, s, e) {
        if (typeof i === "function") {
            throw new Error(`Invalid usage. @state can only be used on a field`);
        }
        if (typeof e?.value !== "undefined") {
            throw new Error(`Invalid usage. @state can only be used on a field`);
        }
        i = i.constructor;
        let n = g.getAnnotation(i, F);
        if (n == null) {
            g.annotate(i, F, n = []);
        }
        n.push(new G(t, s));
        n = S.getAnnotation(i, F);
        if (n == null) {
            g.annotate(i, F, n = []);
        }
        n.push(new M(t, s));
    };
}

const F = "dependencies";

let G = class HydratingLifecycleHooks {
    constructor(t, i) {
        this.$get = t;
        this.key = i;
    }
    register(t) {
        i.instance(b, this).register(t);
    }
    hydrating(t, i) {
        const s = i.container;
        i.addBinding(new $(t, this.key, s.get(w), this.$get));
    }
};

G = __decorate([ f() ], G);

let M = class CreatedLifecycleHooks {
    constructor(t, i) {
        this.$get = t;
        this.key = i;
    }
    register(t) {
        i.instance(b, this).register(t);
    }
    created(t, i) {
        const s = i.container;
        i.addBinding(new $(t, this.key, s.get(w), this.$get));
    }
};

M = __decorate([ f() ], M);

export { P as ActionHandler, E as DispatchAttributePattern, x as DispatchBindingCommand, DispatchBindingInstruction, k as DispatchBindingInstructionRenderer, v as IActionHandler, I as IState, w as IStore, T as StateAttributePattern, StateBinding, A as StateBindingBehavior, j as StateBindingCommand, StateBindingInstruction, O as StateBindingInstructionRenderer, L as StateDefaultConfiguration, StateDispatchBinding, fromState };
//# sourceMappingURL=index.mjs.map
