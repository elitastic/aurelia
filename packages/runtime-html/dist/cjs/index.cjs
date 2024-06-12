"use strict";

var t = require("@aurelia/expression-parser");

var e = require("@aurelia/kernel");

var s = require("@aurelia/runtime");

var i = require("@aurelia/template-compiler");

var n = require("@aurelia/metadata");

var r = require("@aurelia/platform-browser");

var l = require("@aurelia/platform");

const a = Object;

const h = String;

const c = a.prototype;

const u = c.hasOwnProperty;

const f = a.freeze;

const d = a.assign;

const p = a.getOwnPropertyNames;

const g = a.keys;

const m = /*@__PURE__*/ e.createLookup();

const isDataAttribute = (t, s, i) => {
    if (m[s] === true) {
        return true;
    }
    if (!e.isString(s)) {
        return false;
    }
    const n = s.slice(0, 5);
    return m[s] = n === "aria-" || n === "data-" || i.isStandardSvgAttribute(t, s);
};

const rethrow = t => {
    throw t;
};

const x = Reflect.defineProperty;

const defineHiddenProp = (t, e, s) => {
    x(t, e, {
        enumerable: false,
        configurable: true,
        writable: true,
        value: s
    });
    return s;
};

const addSignalListener = (t, e, s) => t.addSignalListener(e, s);

const removeSignalListener = (t, e, s) => t.removeSignalListener(e, s);

const v = "Interpolation";

const b = "IsIterator";

const w = "IsFunction";

const y = "IsProperty";

const k = "pending";

const C = "running";

const B = s.AccessorType.Observer;

const S = s.AccessorType.Node;

const A = s.AccessorType.Layout;

const createMappedError = (t, ...e) => new Error(`AUR${h(t).padStart(4, "0")}:${e.map(h)}`);

class Scope {
    constructor(t, e, s, i) {
        this.parent = t;
        this.bindingContext = e;
        this.overrideContext = s;
        this.isBoundary = i;
    }
    static getContext(t, e, s) {
        if (t == null) {
            throw createMappedError(203);
        }
        let i = t.overrideContext;
        let n = t;
        if (s > 0) {
            while (s > 0) {
                s--;
                n = n.parent;
                if (n == null) {
                    return void 0;
                }
            }
            i = n.overrideContext;
            return e in i ? i : n.bindingContext;
        }
        while (n != null && !n.isBoundary && !(e in n.overrideContext) && !(e in n.bindingContext)) {
            n = n.parent;
        }
        if (n == null) {
            return t.bindingContext;
        }
        i = n.overrideContext;
        return e in i ? i : n.bindingContext;
    }
    static create(t, e, s) {
        if (t == null) {
            throw createMappedError(204);
        }
        return new Scope(null, t, e ?? new OverrideContext, s ?? false);
    }
    static fromParent(t, e) {
        if (t == null) {
            throw createMappedError(203);
        }
        return new Scope(t, e, new OverrideContext, false);
    }
}

class BindingContext {
    constructor(t, e) {
        if (t !== void 0) {
            this[t] = e;
        }
    }
}

class OverrideContext {}

const {astAssign: E, astEvaluate: R, astBind: T, astUnbind: L} = /*@__PURE__*/ (() => {
    const s = "AccessThis";
    const i = "AccessBoundary";
    const n = "AccessGlobal";
    const r = "AccessScope";
    const l = "ArrayLiteral";
    const a = "ObjectLiteral";
    const c = "PrimitiveLiteral";
    const u = "Template";
    const f = "Unary";
    const d = "CallScope";
    const p = "CallMember";
    const g = "CallFunction";
    const m = "CallGlobal";
    const x = "AccessMember";
    const v = "AccessKeyed";
    const b = "TaggedTemplate";
    const w = "Binary";
    const y = "Conditional";
    const k = "Assign";
    const C = "ArrowFunction";
    const B = "ValueConverter";
    const S = "BindingBehavior";
    const A = "ArrayBindingPattern";
    const E = "ObjectBindingPattern";
    const R = "BindingIdentifier";
    const T = "ForOfStatement";
    const L = "Interpolation";
    const M = "ArrayDestructuring";
    const D = "ObjectDestructuring";
    const q = "DestructuringAssignmentLeaf";
    const I = "Custom";
    const P = Scope.getContext;
    function astEvaluate(t, F, V, O) {
        switch (t.$kind) {
          case s:
            {
                let e = F.overrideContext;
                let s = F;
                let i = t.ancestor;
                while (i-- && e) {
                    s = s.parent;
                    e = s?.overrideContext ?? null;
                }
                return i < 1 && s ? s.bindingContext : void 0;
            }

          case i:
            {
                let t = F;
                while (t != null && !t.isBoundary) {
                    t = t.parent;
                }
                return t ? t.bindingContext : void 0;
            }

          case r:
            {
                const s = P(F, t.name, t.ancestor);
                if (O !== null) {
                    O.observe(s, t.name);
                }
                const i = s[t.name];
                if (i == null && t.name === "$host") {
                    throw createMappedError(105);
                }
                if (V?.strict) {
                    return V?.boundFn && e.isFunction(i) ? i.bind(s) : i;
                }
                return i == null ? "" : V?.boundFn && e.isFunction(i) ? i.bind(s) : i;
            }

          case n:
            return globalThis[t.name];

          case m:
            {
                const s = globalThis[t.name];
                if (e.isFunction(s)) {
                    return s(...t.args.map((t => astEvaluate(t, F, V, O))));
                }
                if (!V?.strictFnCall && s == null) {
                    return void 0;
                }
                throw createMappedError(107);
            }

          case l:
            return t.elements.map((t => astEvaluate(t, F, V, O)));

          case a:
            {
                const e = {};
                for (let s = 0; s < t.keys.length; ++s) {
                    e[t.keys[s]] = astEvaluate(t.values[s], F, V, O);
                }
                return e;
            }

          case c:
            return t.value;

          case u:
            {
                let e = t.cooked[0];
                for (let s = 0; s < t.expressions.length; ++s) {
                    e += String(astEvaluate(t.expressions[s], F, V, O));
                    e += t.cooked[s + 1];
                }
                return e;
            }

          case f:
            switch (t.operation) {
              case "void":
                return void astEvaluate(t.expression, F, V, O);

              case "typeof":
                return typeof astEvaluate(t.expression, F, V, O);

              case "!":
                return !astEvaluate(t.expression, F, V, O);

              case "-":
                return -astEvaluate(t.expression, F, V, O);

              case "+":
                return +astEvaluate(t.expression, F, V, O);

              default:
                throw createMappedError(109, t.operation);
            }

          case d:
            {
                const e = t.args.map((t => astEvaluate(t, F, V, O)));
                const s = P(F, t.name, t.ancestor);
                const i = getFunction(V?.strictFnCall, s, t.name);
                if (i) {
                    return i.apply(s, e);
                }
                return void 0;
            }

          case p:
            {
                const s = astEvaluate(t.object, F, V, O);
                const i = t.args.map((t => astEvaluate(t, F, V, O)));
                const n = getFunction(V?.strictFnCall, s, t.name);
                let r;
                if (n) {
                    r = n.apply(s, i);
                    if (e.isArray(s) && _.includes(t.name)) {
                        O?.observeCollection(s);
                    }
                }
                return r;
            }

          case g:
            {
                const s = astEvaluate(t.func, F, V, O);
                if (e.isFunction(s)) {
                    return s(...t.args.map((t => astEvaluate(t, F, V, O))));
                }
                if (!V?.strictFnCall && s == null) {
                    return void 0;
                }
                throw createMappedError(107);
            }

          case C:
            {
                const func = (...e) => {
                    const s = t.args;
                    const i = t.rest;
                    const n = s.length - 1;
                    const r = s.reduce(((t, s, r) => {
                        if (i && r === n) {
                            t[s.name] = e.slice(r);
                        } else {
                            t[s.name] = e[r];
                        }
                        return t;
                    }), {});
                    const l = Scope.fromParent(F, r);
                    return astEvaluate(t.body, l, V, O);
                };
                return func;
            }

          case x:
            {
                const s = astEvaluate(t.object, F, V, O);
                let i;
                if (V?.strict) {
                    if (s == null) {
                        return undefined;
                    }
                    if (O !== null && !t.accessGlobal) {
                        O.observe(s, t.name);
                    }
                    i = s[t.name];
                    if (V?.boundFn && e.isFunction(i)) {
                        return i.bind(s);
                    }
                    return i;
                }
                if (O !== null && e.isObject(s) && !t.accessGlobal) {
                    O.observe(s, t.name);
                }
                if (s) {
                    i = s[t.name];
                    if (V?.boundFn && e.isFunction(i)) {
                        return i.bind(s);
                    }
                    return i;
                }
                return "";
            }

          case v:
            {
                const s = astEvaluate(t.object, F, V, O);
                const i = astEvaluate(t.key, F, V, O);
                if (e.isObject(s)) {
                    if (O !== null && !t.accessGlobal) {
                        O.observe(s, i);
                    }
                    return s[i];
                }
                return s == null ? void 0 : s[i];
            }

          case b:
            {
                const s = t.expressions.map((t => astEvaluate(t, F, V, O)));
                const i = astEvaluate(t.func, F, V, O);
                if (!e.isFunction(i)) {
                    throw createMappedError(110);
                }
                return i(t.cooked, ...s);
            }

          case w:
            {
                const s = t.left;
                const i = t.right;
                switch (t.operation) {
                  case "&&":
                    return astEvaluate(s, F, V, O) && astEvaluate(i, F, V, O);

                  case "||":
                    return astEvaluate(s, F, V, O) || astEvaluate(i, F, V, O);

                  case "??":
                    return astEvaluate(s, F, V, O) ?? astEvaluate(i, F, V, O);

                  case "==":
                    return astEvaluate(s, F, V, O) == astEvaluate(i, F, V, O);

                  case "===":
                    return astEvaluate(s, F, V, O) === astEvaluate(i, F, V, O);

                  case "!=":
                    return astEvaluate(s, F, V, O) != astEvaluate(i, F, V, O);

                  case "!==":
                    return astEvaluate(s, F, V, O) !== astEvaluate(i, F, V, O);

                  case "instanceof":
                    {
                        const t = astEvaluate(i, F, V, O);
                        if (e.isFunction(t)) {
                            return astEvaluate(s, F, V, O) instanceof t;
                        }
                        return false;
                    }

                  case "in":
                    {
                        const t = astEvaluate(i, F, V, O);
                        if (e.isObject(t)) {
                            return astEvaluate(s, F, V, O) in t;
                        }
                        return false;
                    }

                  case "+":
                    {
                        const t = astEvaluate(s, F, V, O);
                        const e = astEvaluate(i, F, V, O);
                        if (V?.strict) {
                            return t + e;
                        }
                        if (!t || !e) {
                            if (isNumberOrBigInt(t) || isNumberOrBigInt(e)) {
                                return (t || 0) + (e || 0);
                            }
                            if (isStringOrDate(t) || isStringOrDate(e)) {
                                return (t || "") + (e || "");
                            }
                        }
                        return t + e;
                    }

                  case "-":
                    return astEvaluate(s, F, V, O) - astEvaluate(i, F, V, O);

                  case "*":
                    return astEvaluate(s, F, V, O) * astEvaluate(i, F, V, O);

                  case "/":
                    return astEvaluate(s, F, V, O) / astEvaluate(i, F, V, O);

                  case "%":
                    return astEvaluate(s, F, V, O) % astEvaluate(i, F, V, O);

                  case "<":
                    return astEvaluate(s, F, V, O) < astEvaluate(i, F, V, O);

                  case ">":
                    return astEvaluate(s, F, V, O) > astEvaluate(i, F, V, O);

                  case "<=":
                    return astEvaluate(s, F, V, O) <= astEvaluate(i, F, V, O);

                  case ">=":
                    return astEvaluate(s, F, V, O) >= astEvaluate(i, F, V, O);

                  default:
                    throw createMappedError(108, t.operation);
                }
            }

          case y:
            return astEvaluate(t.condition, F, V, O) ? astEvaluate(t.yes, F, V, O) : astEvaluate(t.no, F, V, O);

          case k:
            return astAssign(t.target, F, V, astEvaluate(t.value, F, V, O));

          case B:
            {
                const e = V?.getConverter?.(t.name);
                if (e == null) {
                    throw createMappedError(103, t.name);
                }
                if ("toView" in e) {
                    return e.toView(astEvaluate(t.expression, F, V, O), ...t.args.map((t => astEvaluate(t, F, V, O))));
                }
                return astEvaluate(t.expression, F, V, O);
            }

          case S:
            return astEvaluate(t.expression, F, V, O);

          case R:
            return t.name;

          case T:
            return astEvaluate(t.iterable, F, V, O);

          case L:
            if (t.isMulti) {
                let e = t.parts[0];
                let s = 0;
                for (;s < t.expressions.length; ++s) {
                    e += h(astEvaluate(t.expressions[s], F, V, O));
                    e += t.parts[s + 1];
                }
                return e;
            } else {
                return `${t.parts[0]}${astEvaluate(t.firstExpression, F, V, O)}${t.parts[1]}`;
            }

          case q:
            return astEvaluate(t.target, F, V, O);

          case M:
            {
                return t.list.map((t => astEvaluate(t, F, V, O)));
            }

          case A:
          case E:
          case D:
          default:
            return void 0;

          case I:
            return t.evaluate(F, V, O);
        }
    }
    function astAssign(s, i, n, l) {
        switch (s.$kind) {
          case r:
            {
                if (s.name === "$host") {
                    throw createMappedError(106);
                }
                const t = P(i, s.name, s.ancestor);
                return t[s.name] = l;
            }

          case x:
            {
                const t = astEvaluate(s.object, i, n, null);
                if (e.isObject(t)) {
                    if (s.name === "length" && e.isArray(t) && !isNaN(l)) {
                        t.splice(l);
                    } else {
                        t[s.name] = l;
                    }
                } else {
                    astAssign(s.object, i, n, {
                        [s.name]: l
                    });
                }
                return l;
            }

          case v:
            {
                const t = astEvaluate(s.object, i, n, null);
                const r = astEvaluate(s.key, i, n, null);
                if (e.isArray(t)) {
                    if (r === "length" && !isNaN(l)) {
                        t.splice(l);
                        return l;
                    }
                    if (e.isArrayIndex(r)) {
                        t.splice(r, 1, l);
                        return l;
                    }
                }
                return t[r] = l;
            }

          case k:
            astAssign(s.value, i, n, l);
            return astAssign(s.target, i, n, l);

          case B:
            {
                const t = n?.getConverter?.(s.name);
                if (t == null) {
                    throw createMappedError(103, s.name);
                }
                if ("fromView" in t) {
                    l = t.fromView(l, ...s.args.map((t => astEvaluate(t, i, n, null))));
                }
                return astAssign(s.expression, i, n, l);
            }

          case S:
            return astAssign(s.expression, i, n, l);

          case M:
          case D:
            {
                const t = s.list;
                const e = t.length;
                let r;
                let a;
                for (r = 0; r < e; r++) {
                    a = t[r];
                    switch (a.$kind) {
                      case q:
                        astAssign(a, i, n, l);
                        break;

                      case M:
                      case D:
                        {
                            if (typeof l !== "object" || l === null) {
                                throw createMappedError(112);
                            }
                            let t = astEvaluate(a.source, Scope.create(l), n, null);
                            if (t === void 0 && a.initializer) {
                                t = astEvaluate(a.initializer, i, n, null);
                            }
                            astAssign(a, i, n, t);
                            break;
                        }
                    }
                }
                break;
            }

          case q:
            {
                if (s instanceof t.DestructuringAssignmentSingleExpression) {
                    if (l == null) {
                        return;
                    }
                    if (typeof l !== "object") {
                        throw createMappedError(112);
                    }
                    let t = astEvaluate(s.source, Scope.create(l), n, null);
                    if (t === void 0 && s.initializer) {
                        t = astEvaluate(s.initializer, i, n, null);
                    }
                    astAssign(s.target, i, n, t);
                } else {
                    if (l == null) {
                        return;
                    }
                    if (typeof l !== "object") {
                        throw createMappedError(112);
                    }
                    const t = s.indexOrProperties;
                    let r;
                    if (e.isArrayIndex(t)) {
                        if (!Array.isArray(l)) {
                            throw createMappedError(112);
                        }
                        r = l.slice(t);
                    } else {
                        r = Object.entries(l).reduce(((e, [s, i]) => {
                            if (!t.includes(s)) {
                                e[s] = i;
                            }
                            return e;
                        }), {});
                    }
                    astAssign(s.target, i, n, r);
                }
                break;
            }

          case I:
            return s.assign(i, n, l);

          default:
            return void 0;
        }
    }
    function astBind(t, e, s) {
        switch (t.$kind) {
          case S:
            {
                const i = t.name;
                const n = t.key;
                const r = s.getBehavior?.(i);
                if (r == null) {
                    throw createMappedError(101, i);
                }
                if (s[n] === void 0) {
                    s[n] = r;
                    r.bind?.(e, s, ...t.args.map((t => astEvaluate(t, e, s, null))));
                } else {
                    throw createMappedError(102, i);
                }
                astBind(t.expression, e, s);
                return;
            }

          case B:
            {
                const i = t.name;
                const n = s.getConverter?.(i);
                if (n == null) {
                    throw createMappedError(103, i);
                }
                const r = n.signals;
                if (r != null) {
                    const t = s.getSignaler?.();
                    const e = r.length;
                    let i = 0;
                    for (;i < e; ++i) {
                        t?.addSignalListener(r[i], s);
                    }
                }
                astBind(t.expression, e, s);
                return;
            }

          case T:
            {
                astBind(t.iterable, e, s);
                break;
            }

          case I:
            {
                t.bind?.(e, s);
            }
        }
    }
    function astUnbind(t, e, s) {
        switch (t.$kind) {
          case S:
            {
                const i = t.key;
                const n = s;
                if (n[i] !== void 0) {
                    n[i].unbind?.(e, s);
                    n[i] = void 0;
                }
                astUnbind(t.expression, e, s);
                break;
            }

          case B:
            {
                const i = s.getConverter?.(t.name);
                if (i?.signals === void 0) {
                    return;
                }
                const n = s.getSignaler?.();
                let r = 0;
                for (;r < i.signals.length; ++r) {
                    n?.removeSignalListener(i.signals[r], s);
                }
                astUnbind(t.expression, e, s);
                break;
            }

          case T:
            {
                astUnbind(t.iterable, e, s);
                break;
            }

          case I:
            {
                t.unbind?.(e, s);
            }
        }
    }
    const getFunction = (t, s, i) => {
        const n = s == null ? null : s[i];
        if (e.isFunction(n)) {
            return n;
        }
        if (!t && n == null) {
            return null;
        }
        throw createMappedError(111, i);
    };
    const isNumberOrBigInt = t => {
        switch (typeof t) {
          case "number":
          case "bigint":
            return true;

          default:
            return false;
        }
    };
    const isStringOrDate = t => {
        switch (typeof t) {
          case "string":
            return true;

          case "object":
            return t instanceof Date;

          default:
            return false;
        }
    };
    const _ = "at map filter includes indexOf lastIndexOf findIndex find flat flatMap join reduce reduceRight slice every some sort".split(" ");
    return {
        astEvaluate: astEvaluate,
        astAssign: astAssign,
        astBind: astBind,
        astUnbind: astUnbind
    };
})();

const {default: M, oneTime: D, toView: q, fromView: I, twoWay: P} = i.BindingMode;

const _ = n.Metadata.get;

const F = n.Metadata.has;

const V = n.Metadata.define;

const {annotation: O} = e.Protocol;

const H = O.keyFor;

function bindable(t, s) {
    let i = void 0;
    function decorator(t, s) {
        let n;
        switch (s.kind) {
          case "getter":
          case "field":
            {
                const t = s.name;
                if (typeof t !== "string") throw createMappedError(227);
                n = t;
                break;
            }

          case "class":
            if (i == null) throw createMappedError(228);
            if (typeof i == "string") {
                n = i;
            } else {
                const t = i.name;
                if (!t) throw createMappedError(229);
                if (typeof t !== "string") throw createMappedError(227);
                n = t;
            }
            break;
        }
        const r = i == null || typeof i === "string" ? {
            name: n
        } : i;
        const l = s.metadata[$] ??= e.createLookup();
        l[n] = BindableDefinition.create(n, r);
    }
    if (arguments.length > 1) {
        i = {};
        decorator(t, s);
        return;
    } else if (e.isString(t)) {
        i = t;
        return decorator;
    }
    i = t === void 0 ? {} : t;
    return decorator;
}

const $ = /*@__PURE__*/ H("bindables");

const N = f({
    name: $,
    keyFrom: t => `${$}:${t}`,
    from(...t) {
        const s = {};
        const i = Array.isArray;
        function addName(t) {
            s[t] = BindableDefinition.create(t);
        }
        function addDescription(t, e) {
            s[t] = e instanceof BindableDefinition ? e : BindableDefinition.create(t, e === true ? {} : e);
        }
        function addList(t) {
            if (i(t)) {
                t.forEach((t => e.isString(t) ? addName(t) : addDescription(t.name, t)));
            } else if (t instanceof BindableDefinition) {
                s[t.name] = t;
            } else if (t !== void 0) {
                g(t).forEach((e => addDescription(e, t[e])));
            }
        }
        t.forEach(addList);
        return s;
    },
    getAll(t) {
        const s = [];
        const i = e.getPrototypeChain(t);
        let n = i.length;
        let r;
        while (--n >= 0) {
            r = i[n];
            const t = _($, r);
            if (t == null) continue;
            s.push(...Object.values(t));
        }
        return s;
    }
});

class BindableDefinition {
    constructor(t, e, s, i, n, r) {
        this.attribute = t;
        this.callback = e;
        this.mode = s;
        this.primary = i;
        this.name = n;
        this.set = r;
    }
    static create(t, s = {}) {
        const n = s.mode ?? q;
        return new BindableDefinition(s.attribute ?? e.kebabCase(t), s.callback ?? `${t}Changed`, e.isString(n) ? i.BindingMode[n] ?? M : n, s.primary ?? false, s.name ?? t, s.set ?? getInterceptor(s));
    }
}

function coercer(t, e) {
    e.addInitializer((function() {
        W.define(this, e.name);
    }));
}

const W = {
    key: /*@__PURE__*/ H("coercer"),
    define(t, e) {
        V(t[e].bind(t), t, W.key);
    },
    for(t) {
        return _(W.key, t);
    }
};

function getInterceptor(t = {}) {
    const s = t.type ?? null;
    if (s == null) {
        return e.noop;
    }
    let i;
    switch (s) {
      case Number:
      case Boolean:
      case String:
      case BigInt:
        i = s;
        break;

      default:
        {
            const t = s.coerce;
            i = typeof t === "function" ? t.bind(s) : W.for(s) ?? e.noop;
            break;
        }
    }
    return i === e.noop ? i : createCoercer(i, t.nullable);
}

function createCoercer(t, e) {
    return function(s, i) {
        if (!i?.enableCoercion) return s;
        return (e ?? (i?.coerceNullish ?? false ? false : true)) && s == null ? s : t(s, i);
    };
}

const j = e.DI.createInterface;

const z = e.Registration.singleton;

const U = e.Registration.aliasTo;

const G = e.Registration.instance;

e.Registration.callback;

e.Registration.transient;

const registerResolver = (t, e, s) => t.registerResolver(e, s);

function alias(...t) {
    return function(e, s) {
        s.addInitializer((function() {
            const e = H("aliases");
            const s = _(e, this);
            if (s === void 0) {
                V(t, this, e);
            } else {
                s.push(...t);
            }
        }));
    };
}

function registerAliases(t, s, i, n) {
    for (let r = 0, l = t.length; r < l; ++r) {
        e.Registration.aliasTo(i, s.keyFrom(t[r])).register(n);
    }
}

const K = "custom-element";

const X = "custom-attribute";

const getDefinitionFromStaticAu = (t, e, s, i = "__au_static_resource__") => {
    let n = _(i, t);
    if (n == null) {
        if (t.$au?.type === e) {
            n = s(t.$au, t);
            V(n, t, i);
        }
    }
    return n;
};

function bindingBehavior(t) {
    return function(e, s) {
        s.addInitializer((function() {
            Z.define(t, this);
        }));
        return e;
    };
}

class BindingBehaviorDefinition {
    constructor(t, e, s, i) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
    }
    static create(t, s) {
        let i;
        let n;
        if (e.isString(t)) {
            i = t;
            n = {
                name: i
            };
        } else {
            i = t.name;
            n = t;
        }
        return new BindingBehaviorDefinition(s, e.firstDefined(getBehaviorAnnotation(s, "name"), i), e.mergeArrays(getBehaviorAnnotation(s, "aliases"), n.aliases, s.aliases), Z.keyFrom(i));
    }
    register(t, e) {
        const s = this.Type;
        const i = typeof e === "string" ? getBindingBehaviorKeyFrom(e) : this.key;
        const n = this.aliases;
        if (!t.has(i, false)) {
            t.register(t.has(s, false) ? null : z(s, s), U(s, i), ...n.map((t => U(s, getBindingBehaviorKeyFrom(t)))));
        }
    }
}

const Q = "binding-behavior";

const Y = /*@__PURE__*/ e.getResourceKeyFor(Q);

const getBehaviorAnnotation = (t, e) => _(H(e), t);

const getBindingBehaviorKeyFrom = t => `${Y}:${t}`;

const Z = /*@__PURE__*/ f({
    name: Y,
    keyFrom: getBindingBehaviorKeyFrom,
    isType(t) {
        return e.isFunction(t) && (F(Y, t) || t.$au?.type === Q);
    },
    define(t, s) {
        const i = BindingBehaviorDefinition.create(t, s);
        const n = i.Type;
        V(i, n, Y, e.resourceBaseName);
        return n;
    },
    getDefinition(t) {
        const e = _(Y, t) ?? getDefinitionFromStaticAu(t, Q, BindingBehaviorDefinition.create);
        if (e === void 0) {
            throw createMappedError(151, t);
        }
        return e;
    },
    find(t, e) {
        const s = t.find(Q, e);
        return s == null ? null : _(Y, s) ?? getDefinitionFromStaticAu(s, Q, BindingBehaviorDefinition.create) ?? null;
    },
    get(t, s) {
        return t.get(e.resource(getBindingBehaviorKeyFrom(s)));
    }
});

const J = new Map;

const createConfig = t => ({
    type: Q,
    name: t
});

class BindingModeBehavior {
    bind(t, e) {
        J.set(e, e.mode);
        e.mode = this.mode;
    }
    unbind(t, e) {
        e.mode = J.get(e);
        J.delete(e);
    }
}

class OneTimeBindingBehavior extends BindingModeBehavior {
    get mode() {
        return D;
    }
}

OneTimeBindingBehavior.$au = createConfig("oneTime");

class ToViewBindingBehavior extends BindingModeBehavior {
    get mode() {
        return q;
    }
}

ToViewBindingBehavior.$au = createConfig("toView");

class FromViewBindingBehavior extends BindingModeBehavior {
    get mode() {
        return I;
    }
}

FromViewBindingBehavior.$au = createConfig("fromView");

class TwoWayBindingBehavior extends BindingModeBehavior {
    get mode() {
        return P;
    }
}

TwoWayBindingBehavior.$au = createConfig("twoWay");

const tt = new WeakMap;

const et = 200;

class DebounceBindingBehavior {
    constructor() {
        this.p = e.resolve(e.IPlatform);
    }
    bind(t, s, i, n) {
        const r = {
            type: "debounce",
            delay: i ?? et,
            now: this.p.performanceNow,
            queue: this.p.taskQueue,
            signals: e.isString(n) ? [ n ] : n ?? e.emptyArray
        };
        const l = s.limit?.(r);
        if (l == null) ; else {
            tt.set(s, l);
        }
    }
    unbind(t, e) {
        tt.get(e)?.dispose();
        tt.delete(e);
    }
}

DebounceBindingBehavior.$au = {
    type: Q,
    name: "debounce"
};

const st = /*@__PURE__*/ j("ISignaler", (t => t.singleton(Signaler)));

class Signaler {
    constructor() {
        this.signals = e.createLookup();
    }
    dispatchSignal(t) {
        const e = this.signals[t];
        if (e === undefined) {
            return;
        }
        let s;
        for (s of e.keys()) {
            s.handleChange(undefined, undefined);
        }
    }
    addSignalListener(t, e) {
        (this.signals[t] ??= new Set).add(e);
    }
    removeSignalListener(t, e) {
        this.signals[t]?.delete(e);
    }
}

class SignalBindingBehavior {
    constructor() {
        this.i = new Map;
        this.h = e.resolve(st);
    }
    bind(t, e, ...s) {
        if (!("handleChange" in e)) {
            throw createMappedError(817);
        }
        if (s.length === 0) {
            throw createMappedError(818);
        }
        this.i.set(e, s);
        let i;
        for (i of s) {
            addSignalListener(this.h, i, e);
        }
    }
    unbind(t, e) {
        const s = this.i.get(e);
        this.i.delete(e);
        let i;
        for (i of s) {
            removeSignalListener(this.h, i, e);
        }
    }
}

SignalBindingBehavior.$au = {
    type: Q,
    name: "signal"
};

const it = new WeakMap;

const nt = 200;

class ThrottleBindingBehavior {
    constructor() {
        ({performanceNow: this.u, taskQueue: this.C} = e.resolve(e.IPlatform));
    }
    bind(t, s, i, n) {
        const r = {
            type: "throttle",
            delay: i ?? nt,
            now: this.u,
            queue: this.C,
            signals: e.isString(n) ? [ n ] : n ?? e.emptyArray
        };
        const l = s.limit?.(r);
        if (l == null) ; else {
            it.set(s, l);
        }
    }
    unbind(t, e) {
        it.get(e)?.dispose();
        it.delete(e);
    }
}

ThrottleBindingBehavior.$au = {
    type: Q,
    name: "throttle"
};

const rt = /*@__PURE__*/ j("IAppTask");

class $AppTask {
    constructor(t, e, s) {
        this.c = void 0;
        this.slot = t;
        this.k = e;
        this.cb = s;
    }
    register(t) {
        return this.c = t.register(G(rt, this));
    }
    run() {
        const t = this.k;
        const e = this.cb;
        return t === null ? e() : e(this.c.get(t));
    }
}

const ot = f({
    creating: createAppTaskSlotHook("creating"),
    hydrating: createAppTaskSlotHook("hydrating"),
    hydrated: createAppTaskSlotHook("hydrated"),
    activating: createAppTaskSlotHook("activating"),
    activated: createAppTaskSlotHook("activated"),
    deactivating: createAppTaskSlotHook("deactivating"),
    deactivated: createAppTaskSlotHook("deactivated")
});

function createAppTaskSlotHook(t) {
    function appTaskFactory(s, i) {
        if (e.isFunction(i)) {
            return new $AppTask(t, s, i);
        }
        return new $AppTask(t, null, s);
    }
    return appTaskFactory;
}

const lt = e.IPlatform;

function watch(t, s) {
    if (t == null) {
        throw createMappedError(772);
    }
    return function decorator(i, n) {
        const r = n.kind === "class";
        if (r) {
            if (!e.isFunction(s) && (s == null || !(s in i.prototype))) {
                throw createMappedError(773, `${h(s)}@${i.name}}`);
            }
        } else if (!e.isFunction(i)) {
            throw createMappedError(774, n.name);
        }
        const l = new WatchDefinition(t, r ? s : i);
        if (r) {
            addDefinition(i);
        } else {
            n.addInitializer((function() {
                addDefinition(this.constructor);
            }));
        }
        function addDefinition(t) {
            at.add(t, l);
            if (isAttributeType(t)) {
                getAttributeDefinition(t).watches.push(l);
            }
            if (isElementType(t)) {
                getElementDefinition(t).watches.push(l);
            }
        }
    };
}

class WatchDefinition {
    constructor(t, e) {
        this.expression = t;
        this.callback = e;
    }
}

const at = /*@__PURE__*/ (() => {
    const t = new WeakMap;
    return f({
        add(e, s) {
            let i = t.get(e);
            if (i == null) {
                t.set(e, i = []);
            }
            i.push(s);
        },
        getDefinitions(s) {
            return t.get(s) ?? e.emptyArray;
        }
    });
})();

function customAttribute(t) {
    return function(e, s) {
        s.addInitializer((function() {
            defineAttribute(t, this);
        }));
        return e;
    };
}

function templateController(t) {
    return function(s, i) {
        i.addInitializer((function() {
            defineAttribute(e.isString(t) ? {
                isTemplateController: true,
                name: t
            } : {
                isTemplateController: true,
                ...t
            }, this);
        }));
        return s;
    };
}

class CustomAttributeDefinition {
    get type() {
        return X;
    }
    constructor(t, e, s, i, n, r, l, a, h, c, u) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
        this.defaultBindingMode = n;
        this.isTemplateController = r;
        this.bindables = l;
        this.noMultiBindings = a;
        this.watches = h;
        this.dependencies = c;
        this.containerStrategy = u;
    }
    static create(t, s) {
        let n;
        let r;
        if (e.isString(t)) {
            n = t;
            r = {
                name: n
            };
        } else {
            n = t.name;
            r = t;
        }
        const l = e.firstDefined(getAttributeAnnotation(s, "defaultBindingMode"), r.defaultBindingMode, s.defaultBindingMode, q);
        return new CustomAttributeDefinition(s, e.firstDefined(getAttributeAnnotation(s, "name"), n), e.mergeArrays(getAttributeAnnotation(s, "aliases"), r.aliases, s.aliases), getAttributeKeyFrom(n), e.isString(l) ? i.BindingMode[l] ?? M : l, e.firstDefined(getAttributeAnnotation(s, "isTemplateController"), r.isTemplateController, s.isTemplateController, false), N.from(...N.getAll(s), getAttributeAnnotation(s, "bindables"), s.bindables, r.bindables), e.firstDefined(getAttributeAnnotation(s, "noMultiBindings"), r.noMultiBindings, s.noMultiBindings, false), e.mergeArrays(at.getDefinitions(s), s.watches), e.mergeArrays(getAttributeAnnotation(s, "dependencies"), r.dependencies, s.dependencies), e.firstDefined(getAttributeAnnotation(s, "containerStrategy"), r.containerStrategy, s.containerStrategy, "reuse"));
    }
    register(t, s) {
        const i = this.Type;
        const n = typeof s === "string" ? getAttributeKeyFrom(s) : this.key;
        const r = this.aliases;
        if (!t.has(n, false)) {
            t.register(t.has(i, false) ? null : z(i, i), U(i, n), ...r.map((t => U(i, getAttributeKeyFrom(t)))));
        } else {
            if (CustomAttributeDefinition.warnDuplicate) {
                t.get(e.ILogger).warn(createMappedError(154, this.name));
            }
        }
    }
    toString() {
        return `au:ca:${this.name}`;
    }
}

CustomAttributeDefinition.warnDuplicate = true;

const ht = "custom-attribute";

const ct = /*@__PURE__*/ e.getResourceKeyFor(ht);

const getAttributeKeyFrom = t => `${ct}:${t}`;

const getAttributeAnnotation = (t, e) => _(H(e), t);

const isAttributeType = t => e.isFunction(t) && (F(ct, t) || t.$au?.type === ht);

const findAttributeControllerFor = (t, e) => getRef(t, getAttributeKeyFrom(e)) ?? void 0;

const defineAttribute = (t, s) => {
    const i = CustomAttributeDefinition.create(t, s);
    const n = i.Type;
    V(i, n, ct, e.resourceBaseName);
    return n;
};

const getAttributeDefinition = t => {
    const e = _(ct, t) ?? getDefinitionFromStaticAu(t, ht, CustomAttributeDefinition.create);
    if (e === void 0) {
        throw createMappedError(759, t);
    }
    return e;
};

const findClosestControllerByName = (t, s) => {
    let i = "";
    let n = "";
    if (e.isString(s)) {
        i = getAttributeKeyFrom(s);
        n = s;
    } else {
        const t = getAttributeDefinition(s);
        i = t.key;
        n = t.name;
    }
    let r = t;
    while (r !== null) {
        const t = getRef(r, i);
        if (t?.is(n)) {
            return t;
        }
        r = getEffectiveParentNode(r);
    }
    return null;
};

const ut = /*@__PURE__*/ f({
    name: ct,
    keyFrom: getAttributeKeyFrom,
    isType: isAttributeType,
    for: findAttributeControllerFor,
    closest: findClosestControllerByName,
    define: defineAttribute,
    getDefinition: getAttributeDefinition,
    annotate(t, e, s) {
        V(s, t, H(e));
    },
    getAnnotation: getAttributeAnnotation,
    find(t, e) {
        const s = t.find(ht, e);
        return s === null ? null : _(ct, s) ?? getDefinitionFromStaticAu(s, ht, CustomAttributeDefinition.create) ?? null;
    }
});

const ft = /*@__PURE__*/ j("ILifecycleHooks");

class LifecycleHooksEntry {
    constructor(t, e) {
        this.definition = t;
        this.instance = e;
    }
}

class LifecycleHooksDefinition {
    constructor(t, e) {
        this.Type = t;
        this.propertyNames = e;
    }
    static create(t, e) {
        const s = new Set;
        let i = e.prototype;
        while (i !== c) {
            for (const t of p(i)) {
                if (t !== "constructor" && !t.startsWith("_")) {
                    s.add(t);
                }
            }
            i = Object.getPrototypeOf(i);
        }
        return new LifecycleHooksDefinition(e, s);
    }
}

const dt = /*@__PURE__*/ (() => {
    const t = new WeakMap;
    const e = new WeakMap;
    return f({
        define(t, s) {
            const i = LifecycleHooksDefinition.create(t, s);
            const n = i.Type;
            e.set(n, i);
            return {
                register(t) {
                    z(ft, n).register(t);
                }
            };
        },
        resolve(s) {
            let i = t.get(s);
            if (i === void 0) {
                t.set(s, i = new LifecycleHooksLookupImpl);
                const n = s.root;
                const r = n === s ? s.getAll(ft) : s.has(ft, false) ? n.getAll(ft).concat(s.getAll(ft)) : n.getAll(ft);
                let l;
                let a;
                let h;
                let c;
                let u;
                for (l of r) {
                    a = e.get(l.constructor);
                    h = new LifecycleHooksEntry(a, l);
                    for (c of a.propertyNames) {
                        u = i[c];
                        if (u === void 0) {
                            i[c] = [ h ];
                        } else {
                            u.push(h);
                        }
                    }
                }
            }
            return i;
        }
    });
})();

class LifecycleHooksLookupImpl {}

function lifecycleHooks(t, s) {
    function decorator(t, s) {
        const i = s?.metadata ?? (t[Symbol.metadata] ??= Object.create(null));
        i[e.registrableMetadataKey] = dt.define({}, t);
        return t;
    }
    return t == null ? decorator : decorator(t, s);
}

function valueConverter(t) {
    return function(e, s) {
        s.addInitializer((function() {
            mt.define(t, this);
        }));
        return e;
    };
}

class ValueConverterDefinition {
    constructor(t, e, s, i) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
    }
    static create(t, s) {
        let i;
        let n;
        if (e.isString(t)) {
            i = t;
            n = {
                name: i
            };
        } else {
            i = t.name;
            n = t;
        }
        return new ValueConverterDefinition(s, e.firstDefined(getConverterAnnotation(s, "name"), i), e.mergeArrays(getConverterAnnotation(s, "aliases"), n.aliases, s.aliases), mt.keyFrom(i));
    }
    register(t, e) {
        const s = this.Type;
        const i = typeof e === "string" ? getValueConverterKeyFrom(e) : this.key;
        const n = this.aliases;
        if (!t.has(i, false)) {
            t.register(t.has(s, false) ? null : z(s, s), U(s, i), ...n.map((t => U(s, getValueConverterKeyFrom(t)))));
        }
    }
}

const pt = "value-converter";

const gt = /*@__PURE__*/ e.getResourceKeyFor(pt);

const getConverterAnnotation = (t, e) => _(H(e), t);

const getValueConverterKeyFrom = t => `${gt}:${t}`;

const mt = f({
    name: gt,
    keyFrom: getValueConverterKeyFrom,
    isType(t) {
        return e.isFunction(t) && (F(gt, t) || t.$au?.type === pt);
    },
    define(t, s) {
        const i = ValueConverterDefinition.create(t, s);
        const n = i.Type;
        V(i, n, gt, e.resourceBaseName);
        return n;
    },
    getDefinition(t) {
        const e = _(gt, t) ?? getDefinitionFromStaticAu(t, pt, ValueConverterDefinition.create);
        if (e === void 0) {
            throw createMappedError(152, t);
        }
        return e;
    },
    annotate(t, e, s) {
        V(s, t, H(e));
    },
    getAnnotation: getConverterAnnotation,
    find(t, e) {
        const s = t.find(pt, e);
        return s == null ? null : _(gt, s) ?? getDefinitionFromStaticAu(s, pt, ValueConverterDefinition.create) ?? null;
    },
    get(t, s) {
        return t.get(e.resource(getValueConverterKeyFrom(s)));
    }
});

class BindingTargetSubscriber {
    constructor(t, e) {
        this.v = void 0;
        this.b = t;
        this.B = e;
    }
    flush() {
        this.b.updateSource(this.v);
    }
    handleChange(t, e) {
        const s = this.b;
        if (t !== R(s.ast, s.s, s, null)) {
            this.v = t;
            this.B.add(this);
        }
    }
}

const xt = /*@__PURE__*/ (() => {
    function useScope(t) {
        this.s = t;
    }
    return t => {
        defineHiddenProp(t.prototype, "useScope", useScope);
    };
})();

const vt = /*@__PURE__*/ (() => {
    const t = new WeakMap;
    const e = new WeakMap;
    function evaluatorGet(t) {
        return this.l.get(t);
    }
    function evaluatorGetSignaler() {
        return this.l.root.get(st);
    }
    function evaluatorGetConverter(e) {
        let s = t.get(this);
        if (s == null) {
            t.set(this, s = new ResourceLookup);
        }
        return s[e] ??= mt.get(this.l, e);
    }
    function evaluatorGetBehavior(t) {
        let s = e.get(this);
        if (s == null) {
            e.set(this, s = new ResourceLookup);
        }
        return s[t] ??= Z.get(this.l, t);
    }
    return (t, e = true) => s => {
        const i = s.prototype;
        if (t != null) {
            x(i, "strict", {
                enumerable: true,
                get: function() {
                    return t;
                }
            });
        }
        x(i, "strictFnCall", {
            enumerable: true,
            get: function() {
                return e;
            }
        });
        defineHiddenProp(i, "get", evaluatorGet);
        defineHiddenProp(i, "getSignaler", evaluatorGetSignaler);
        defineHiddenProp(i, "getConverter", evaluatorGetConverter);
        defineHiddenProp(i, "getBehavior", evaluatorGetBehavior);
    };
})();

class ResourceLookup {}

const bt = /*@__PURE__*/ j("IFlushQueue", (t => t.singleton(FlushQueue)));

class FlushQueue {
    constructor() {
        this.A = false;
        this.R = new Set;
    }
    get count() {
        return this.R.size;
    }
    add(t) {
        this.R.add(t);
        if (this.A) {
            return;
        }
        this.A = true;
        try {
            this.R.forEach(flushItem);
        } finally {
            this.A = false;
        }
    }
    clear() {
        this.R.clear();
        this.A = false;
    }
}

const flushItem = function(t, e, s) {
    s.delete(t);
    t.flush();
};

const wt = /*@__PURE__*/ (() => {
    const t = new WeakSet;
    const debounced = (t, e, s) => {
        let i;
        let n;
        let r;
        let l = false;
        const a = t.queue;
        const callOriginalCallback = () => e(r);
        const fn = e => {
            r = e;
            if (s.isBound) {
                n = i;
                i = a.queueTask(callOriginalCallback, {
                    delay: t.delay,
                    reusable: false
                });
                n?.cancel();
            } else {
                callOriginalCallback();
            }
        };
        const h = fn.dispose = () => {
            n?.cancel();
            i?.cancel();
            n = i = void 0;
        };
        fn.flush = () => {
            l = i?.status === k;
            h();
            if (l) {
                callOriginalCallback();
            }
        };
        return fn;
    };
    const throttled = (t, e, s) => {
        let i;
        let n;
        let r = 0;
        let l = 0;
        let a;
        let h = false;
        const c = t.queue;
        const now = () => t.now();
        const callOriginalCallback = () => e(a);
        const fn = e => {
            a = e;
            if (s.isBound) {
                l = now() - r;
                n = i;
                if (l > t.delay) {
                    r = now();
                    callOriginalCallback();
                } else {
                    i = c.queueTask((() => {
                        r = now();
                        callOriginalCallback();
                    }), {
                        delay: t.delay - l,
                        reusable: false
                    });
                }
                n?.cancel();
            } else {
                callOriginalCallback();
            }
        };
        const u = fn.dispose = () => {
            n?.cancel();
            i?.cancel();
            n = i = void 0;
        };
        fn.flush = () => {
            h = i?.status === k;
            u();
            if (h) {
                callOriginalCallback();
            }
        };
        return fn;
    };
    return (e, s) => {
        defineHiddenProp(e.prototype, "limit", (function(e) {
            if (t.has(this)) {
                throw createMappedError(9996);
            }
            t.add(this);
            const i = s(this, e);
            const n = e.signals;
            const r = n.length > 0 ? this.get(st) : null;
            const l = this[i];
            const callOriginal = (...t) => l.call(this, ...t);
            const a = e.type === "debounce" ? debounced(e, callOriginal, this) : throttled(e, callOriginal, this);
            const h = r ? {
                handleChange: a.flush
            } : null;
            this[i] = a;
            if (r) {
                n.forEach((t => addSignalListener(r, t, h)));
            }
            return {
                dispose: () => {
                    if (r) {
                        n.forEach((t => removeSignalListener(r, t, h)));
                    }
                    t.delete(this);
                    a.dispose();
                    delete this[i];
                }
            };
        }));
    };
})();

const yt = ((t = new WeakSet) => e => function() {
    if (!t.has(this)) {
        t.add(this);
        e.call(this);
    }
})();

const kt = {
    reusable: false,
    preempt: true
};

class AttributeBinding {
    constructor(t, e, s, i, n, r, l, a, h) {
        this.targetAttribute = l;
        this.targetProperty = a;
        this.mode = h;
        this.isBound = false;
        this.s = void 0;
        this.T = null;
        this.v = void 0;
        this.boundFn = false;
        this.l = e;
        this.ast = n;
        this.L = t;
        this.target = r;
        this.oL = s;
        this.C = i;
    }
    updateTarget(t) {
        const s = this.target;
        const i = this.targetAttribute;
        const n = this.targetProperty;
        switch (i) {
          case "class":
            s.classList.toggle(n, !!t);
            break;

          case "style":
            {
                let i = "";
                let r = h(t);
                if (e.isString(r) && r.includes("!important")) {
                    i = "important";
                    r = r.replace("!important", "");
                }
                s.style.setProperty(n, r, i);
                break;
            }

          default:
            {
                if (t == null) {
                    s.removeAttribute(i);
                } else {
                    s.setAttribute(i, h(t));
                }
            }
        }
    }
    handleChange() {
        if (!this.isBound) {
            return;
        }
        let t;
        this.obs.version++;
        const e = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        this.obs.clear();
        if (e !== this.v) {
            this.v = e;
            const s = this.L.state !== Ie;
            if (s) {
                t = this.T;
                this.T = this.C.queueTask((() => {
                    this.T = null;
                    this.updateTarget(e);
                }), kt);
                t?.cancel();
            } else {
                this.updateTarget(e);
            }
        }
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        T(this.ast, t, this);
        if (this.mode & (q | D)) {
            this.updateTarget(this.v = R(this.ast, t, this, (this.mode & q) > 0 ? this : null));
        }
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        this.s = void 0;
        this.v = void 0;
        this.T?.cancel();
        this.T = null;
        this.obs.clearAll();
    }
}

AttributeBinding.mix = yt((() => {
    xt(AttributeBinding);
    wt(AttributeBinding, (() => "updateTarget"));
    s.connectable(AttributeBinding, null);
    vt(true)(AttributeBinding);
}));

const Ct = {
    reusable: false,
    preempt: true
};

class InterpolationBinding {
    constructor(t, e, s, i, n, r, l, a) {
        this.ast = n;
        this.target = r;
        this.targetProperty = l;
        this.mode = a;
        this.isBound = false;
        this.s = void 0;
        this.T = null;
        this.L = t;
        this.oL = s;
        this.C = i;
        this.M = s.getAccessor(r, l);
        const h = n.expressions;
        const c = this.partBindings = Array(h.length);
        const u = h.length;
        let f = 0;
        for (;u > f; ++f) {
            c[f] = new InterpolationPartBinding(h[f], r, l, e, s, this);
        }
    }
    q() {
        this.updateTarget();
    }
    updateTarget() {
        const t = this.partBindings;
        const e = this.ast.parts;
        const s = t.length;
        let i = "";
        let n = 0;
        if (s === 1) {
            i = e[0] + t[0].v + e[1];
        } else {
            i = e[0];
            for (;s > n; ++n) {
                i += t[n].v + e[n + 1];
            }
        }
        const r = this.M;
        const l = this.L.state !== Ie && (r.type & A) > 0;
        let a;
        if (l) {
            a = this.T;
            this.T = this.C.queueTask((() => {
                this.T = null;
                r.setValue(i, this.target, this.targetProperty);
            }), Ct);
            a?.cancel();
            a = null;
        } else {
            r.setValue(i, this.target, this.targetProperty);
        }
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        const e = this.partBindings;
        const s = e.length;
        let i = 0;
        for (;s > i; ++i) {
            e[i].bind(t);
        }
        this.updateTarget();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this.s = void 0;
        const t = this.partBindings;
        const e = t.length;
        let s = 0;
        for (;e > s; ++s) {
            t[s].unbind();
        }
        this.T?.cancel();
        this.T = null;
    }
    useAccessor(t) {
        this.M = t;
    }
}

class InterpolationPartBinding {
    constructor(t, e, s, i, n, r) {
        this.ast = t;
        this.target = e;
        this.targetProperty = s;
        this.owner = r;
        this.mode = q;
        this.task = null;
        this.isBound = false;
        this.v = "";
        this.boundFn = false;
        this.l = i;
        this.oL = n;
    }
    updateTarget() {
        this.owner.q();
    }
    handleChange() {
        if (!this.isBound) {
            return;
        }
        this.obs.version++;
        const t = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        this.obs.clear();
        if (t != this.v) {
            this.v = t;
            if (e.isArray(t)) {
                this.observeCollection(t);
            }
            this.updateTarget();
        }
    }
    handleCollectionChange() {
        this.updateTarget();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        T(this.ast, t, this);
        this.v = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        if (e.isArray(this.v)) {
            this.observeCollection(this.v);
        }
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

InterpolationPartBinding.mix = yt((() => {
    xt(InterpolationPartBinding);
    wt(InterpolationPartBinding, (() => "updateTarget"));
    s.connectable(InterpolationPartBinding, null);
    vt(true)(InterpolationPartBinding);
}));

const Bt = {
    reusable: false,
    preempt: true
};

class ContentBinding {
    constructor(t, e, s, i, n, r, l) {
        this.p = n;
        this.ast = r;
        this.target = l;
        this.isBound = false;
        this.mode = q;
        this.T = null;
        this.v = "";
        this.I = false;
        this.boundFn = false;
        this.strict = true;
        this.l = e;
        this.L = t;
        this.oL = s;
        this.C = i;
    }
    updateTarget(t) {
        const e = this.target;
        const s = this.v;
        this.v = t;
        if (this.I) {
            s.parentNode?.removeChild(s);
            this.I = false;
        }
        if (t instanceof this.p.Node) {
            e.parentNode?.insertBefore(t, e);
            t = "";
            this.I = true;
        }
        e.textContent = h(t ?? "");
    }
    handleChange() {
        if (!this.isBound) {
            return;
        }
        this.obs.version++;
        const t = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        this.obs.clear();
        if (t === this.v) {
            this.T?.cancel();
            this.T = null;
            return;
        }
        const e = this.L.state !== Ie;
        if (e) {
            this.P(t);
        } else {
            this.updateTarget(t);
        }
    }
    handleCollectionChange() {
        if (!this.isBound) {
            return;
        }
        this.obs.version++;
        const t = this.v = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        this.obs.clear();
        if (e.isArray(t)) {
            this.observeCollection(t);
        }
        const s = this.L.state !== Ie;
        if (s) {
            this.P(t);
        } else {
            this.updateTarget(t);
        }
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        T(this.ast, t, this);
        const s = this.v = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        if (e.isArray(s)) {
            this.observeCollection(s);
        }
        this.updateTarget(s);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        if (this.I) {
            this.v.parentNode?.removeChild(this.v);
        }
        this.s = void 0;
        this.obs.clearAll();
        this.T?.cancel();
        this.T = null;
    }
    P(t) {
        const e = this.T;
        this.T = this.C.queueTask((() => {
            this.T = null;
            this.updateTarget(t);
        }), Bt);
        e?.cancel();
    }
}

ContentBinding.mix = yt((() => {
    xt(ContentBinding);
    wt(ContentBinding, (() => "updateTarget"));
    s.connectable(ContentBinding, null);
    vt(void 0, false)(ContentBinding);
}));

class LetBinding {
    constructor(t, e, s, i, n = false) {
        this.ast = s;
        this.targetProperty = i;
        this.isBound = false;
        this.s = void 0;
        this.target = null;
        this.boundFn = false;
        this.l = t;
        this.oL = e;
        this._ = n;
    }
    updateTarget() {
        this.target[this.targetProperty] = this.v;
    }
    handleChange() {
        if (!this.isBound) {
            return;
        }
        this.obs.version++;
        this.v = R(this.ast, this.s, this, this);
        this.obs.clear();
        this.updateTarget();
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        this.target = this._ ? t.bindingContext : t.overrideContext;
        T(this.ast, t, this);
        this.v = R(this.ast, this.s, this, this);
        this.updateTarget();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

LetBinding.mix = yt((() => {
    xt(LetBinding);
    wt(LetBinding, (() => "updateTarget"));
    s.connectable(LetBinding, null);
    vt(true)(LetBinding);
}));

class PropertyBinding {
    constructor(t, e, s, i, n, r, l, a) {
        this.ast = n;
        this.target = r;
        this.targetProperty = l;
        this.mode = a;
        this.isBound = false;
        this.s = void 0;
        this.M = void 0;
        this.T = null;
        this.F = null;
        this.boundFn = false;
        this.l = e;
        this.L = t;
        this.C = i;
        this.oL = s;
    }
    updateTarget(t) {
        this.M.setValue(t, this.target, this.targetProperty);
    }
    updateSource(t) {
        E(this.ast, this.s, this, t);
    }
    handleChange() {
        if (!this.isBound) {
            return;
        }
        this.obs.version++;
        const t = R(this.ast, this.s, this, (this.mode & q) > 0 ? this : null);
        this.obs.clear();
        const e = this.L.state !== Ie && (this.M.type & A) > 0;
        if (e) {
            St = this.T;
            this.T = this.C.queueTask((() => {
                this.updateTarget(t);
                this.T = null;
            }), At);
            St?.cancel();
            St = null;
        } else {
            this.updateTarget(t);
        }
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        T(this.ast, t, this);
        const e = this.oL;
        const s = this.mode;
        let i = this.M;
        if (!i) {
            if (s & I) {
                i = e.getObserver(this.target, this.targetProperty);
            } else {
                i = e.getAccessor(this.target, this.targetProperty);
            }
            this.M = i;
        }
        const n = (s & q) > 0;
        if (s & (q | D)) {
            this.updateTarget(R(this.ast, this.s, this, n ? this : null));
        }
        if (s & I) {
            i.subscribe(this.F ??= new BindingTargetSubscriber(this, this.l.get(bt)));
            if (!n) {
                this.updateSource(i.getValue(this.target, this.targetProperty));
            }
        }
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        this.s = void 0;
        if (this.F) {
            this.M.unsubscribe(this.F);
            this.F = null;
        }
        this.T?.cancel();
        this.T = null;
        this.obs.clearAll();
    }
    useTargetObserver(t) {
        this.M?.unsubscribe(this);
        (this.M = t).subscribe(this);
    }
    useTargetSubscriber(t) {
        if (this.F != null) {
            throw createMappedError(9995);
        }
        this.F = t;
    }
}

PropertyBinding.mix = yt((() => {
    xt(PropertyBinding);
    wt(PropertyBinding, (t => t.mode & I ? "updateSource" : "updateTarget"));
    s.connectable(PropertyBinding, null);
    vt(true, false)(PropertyBinding);
}));

let St = null;

const At = {
    reusable: false,
    preempt: true
};

class RefBinding {
    constructor(t, e, s) {
        this.ast = e;
        this.target = s;
        this.isBound = false;
        this.s = void 0;
        this.l = t;
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        T(this.ast, t, this);
        E(this.ast, this.s, this, this.target);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        if (R(this.ast, this.s, this, null) === this.target) {
            E(this.ast, this.s, this, null);
        }
        L(this.ast, this.s, this);
        this.s = void 0;
    }
}

RefBinding.mix = yt((() => {
    vt(false)(RefBinding);
}));

class ListenerBindingOptions {
    constructor(t, e = false) {
        this.prevent = t;
        this.capture = e;
    }
}

class ListenerBinding {
    constructor(t, e, s, i, n, r) {
        this.ast = e;
        this.target = s;
        this.targetEvent = i;
        this.isBound = false;
        this.self = false;
        this.boundFn = true;
        this.V = null;
        this.l = t;
        this.O = n;
        this.V = r;
    }
    callSource(t) {
        const s = this.s.overrideContext;
        s.$event = t;
        let i = R(this.ast, this.s, this, null);
        delete s.$event;
        if (e.isFunction(i)) {
            i = i(t);
        }
        if (i !== true && this.O.prevent) {
            t.preventDefault();
        }
        return i;
    }
    handleEvent(t) {
        if (this.self) {
            if (this.target !== t.composedPath()[0]) {
                return;
            }
        }
        if (this.V?.(t) !== false) {
            this.callSource(t);
        }
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) {
                return;
            }
            this.unbind();
        }
        this.s = t;
        T(this.ast, t, this);
        this.target.addEventListener(this.targetEvent, this, this.O);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        this.s = void 0;
        this.target.removeEventListener(this.targetEvent, this, this.O);
    }
}

ListenerBinding.mix = yt((function() {
    xt(ListenerBinding);
    wt(ListenerBinding, (() => "callSource"));
    vt(true, true)(ListenerBinding);
}));

const Et = /*@__PURE__*/ j("IEventModifier");

const Rt = /*@__PURE__*/ j("IKeyMapping", (t => t.instance({
    meta: f([ "ctrl", "alt", "shift", "meta" ]),
    keys: {
        escape: "Escape",
        enter: "Enter",
        space: "Space",
        tab: "tab",
        ...Array.from({
            length: 25
        }).reduce(((t, e, s) => {
            let i = String.fromCharCode(s + 65);
            t[s + 65] = i;
            i = String.fromCharCode(s + 97);
            t[s + 97] = t[i] = i;
            return t;
        }), {})
    }
})));

class ModifiedMouseEventHandler {
    constructor() {
        this.type = [ "click", "mousedown", "mousemove", "mouseup", "dblclick", "contextmenu" ];
        this.H = e.resolve(Rt);
        this.$ = [ "left", "middle", "right" ];
    }
    static register(t) {
        t.register(z(Et, ModifiedMouseEventHandler));
    }
    getHandler(t) {
        const e = t.split(/[:+.]/);
        return t => {
            let s = false;
            let i = false;
            let n;
            for (n of e) {
                switch (n) {
                  case "prevent":
                    s = true;
                    continue;

                  case "stop":
                    i = true;
                    continue;

                  case "left":
                  case "middle":
                  case "right":
                    if (t.button !== this.$.indexOf(n)) return false;
                    continue;
                }
                if (this.H.meta.includes(n) && t[`${n}Key`] !== true) {
                    return false;
                }
            }
            if (s) t.preventDefault();
            if (i) t.stopPropagation();
            return true;
        };
    }
}

class ModifiedKeyboardEventHandler {
    constructor() {
        this.H = e.resolve(Rt);
        this.type = [ "keydown", "keyup" ];
    }
    static register(t) {
        t.register(z(Et, ModifiedKeyboardEventHandler));
    }
    getHandler(t) {
        const e = t.split(/[:+.]/);
        return t => {
            let s = false;
            let i = false;
            let n;
            for (n of e) {
                switch (n) {
                  case "prevent":
                    s = true;
                    continue;

                  case "stop":
                    i = true;
                    continue;
                }
                if (this.H.meta.includes(n)) {
                    if (t[`${n}Key`] !== true) {
                        return false;
                    }
                    continue;
                }
                const e = this.H.keys[n];
                if (e !== t.key) {
                    return false;
                }
            }
            if (s) t.preventDefault();
            if (i) t.stopPropagation();
            return true;
        };
    }
}

class ModifiedEventHandler {
    constructor() {
        this.type = [ "$ALL" ];
    }
    static register(t) {
        t.register(z(Et, ModifiedEventHandler));
    }
    getHandler(t) {
        const e = t.split(/[:+.]/);
        return t => {
            let s = false;
            let i = false;
            let n;
            for (n of e) {
                switch (n) {
                  case "prevent":
                    s = true;
                    continue;

                  case "stop":
                    i = true;
                    continue;
                }
            }
            if (s) t.preventDefault();
            if (i) t.stopPropagation();
            return true;
        };
    }
}

const Tt = /*@__PURE__*/ j("IEventModifierHandler", (t => t.instance({
    getHandler: () => null
})));

class EventModifier {
    constructor() {
        this.N = e.resolve(e.all(Et)).reduce(((t, s) => {
            const i = e.isArray(s.type) ? s.type : [ s.type ];
            i.forEach((e => t[e] = s));
            return t;
        }), {});
    }
    static register(t) {
        t.register(z(Tt, EventModifier));
    }
    getHandler(t, s) {
        return e.isString(s) ? (this.N[t] ?? this.N.$ALL)?.getHandler(s) ?? null : null;
    }
}

const Lt = {
    register(t) {
        t.register(EventModifier, ModifiedMouseEventHandler, ModifiedKeyboardEventHandler, ModifiedEventHandler);
    }
};

const Mt = /*@__PURE__*/ j("IViewFactory");

class ViewFactory {
    constructor(t, e) {
        this.isCaching = false;
        this.W = null;
        this.j = -1;
        this.name = e.name;
        this.container = t;
        this.def = e;
    }
    setCacheSize(t, s) {
        if (t) {
            if (t === "*") {
                t = ViewFactory.maxCacheSize;
            } else if (e.isString(t)) {
                t = parseInt(t, 10);
            }
            if (this.j === -1 || !s) {
                this.j = t;
            }
        }
        if (this.j > 0) {
            this.W = [];
        } else {
            this.W = null;
        }
        this.isCaching = this.j > 0;
    }
    canReturnToCache(t) {
        return this.W != null && this.W.length < this.j;
    }
    tryReturnToCache(t) {
        if (this.canReturnToCache(t)) {
            this.W.push(t);
            return true;
        }
        return false;
    }
    create(t) {
        const e = this.W;
        let s;
        if (e != null && e.length > 0) {
            s = e.pop();
            return s;
        }
        s = Controller.$view(this, t);
        return s;
    }
}

ViewFactory.maxCacheSize = 65535;

const Dt = /*@__PURE__*/ (() => {
    const createComment = (t, e) => t.document.createComment(e);
    return t => {
        const e = createComment(t, "au-end");
        e.$start = createComment(t, "au-start");
        return e;
    };
})();

const insertManyBefore = (t, e, s) => {
    if (t === null) {
        return;
    }
    const i = s.length;
    let n = 0;
    while (i > n) {
        t.insertBefore(s[n], e);
        ++n;
    }
};

const createMutationObserver = (t, e) => new t.ownerDocument.defaultView.MutationObserver(e);

const isElement = t => t.nodeType === 1;

const qt = "default";

const It = "au-slot";

const Pt = /*@__PURE__*/ j("IAuSlotsInfo");

class AuSlotsInfo {
    constructor(t) {
        this.projectedSlots = t;
    }
}

const _t = /*@__PURE__*/ j("IAuSlotWatcher");

class AuSlotWatcherBinding {
    constructor(t, s, i, n) {
        this.U = new Set;
        this.G = e.emptyArray;
        this.isBound = false;
        this.cb = (this.o = t)[s];
        this.slotName = i;
        this.K = n;
    }
    bind() {
        this.isBound = true;
    }
    unbind() {
        this.isBound = false;
    }
    getValue() {
        return this.G;
    }
    watch(t) {
        if (!this.U.has(t)) {
            this.U.add(t);
            t.subscribe(this);
        }
    }
    unwatch(t) {
        if (this.U.delete(t)) {
            t.unsubscribe(this);
        }
    }
    handleSlotChange(t, e) {
        if (!this.isBound) {
            return;
        }
        const s = this.G;
        const i = [];
        const n = this.K;
        let r;
        let l;
        for (r of this.U) {
            for (l of r === t ? e : r.nodes) {
                if (n === "$all" || isElement(l) && (n === "*" || l.matches(n))) {
                    i[i.length] = l;
                }
            }
        }
        if (i.length !== s.length || i.some(((t, e) => t !== s[e]))) {
            this.G = i;
            this.cb?.call(this.o, i);
            this.subs.notify(i, s);
        }
    }
    get() {
        throw createMappedError(99, "get");
    }
}

class SlottedLifecycleHooks {
    constructor(t) {
        this.X = t;
    }
    register(t) {
        G(ft, this).register(t);
    }
    hydrating(t, e) {
        const s = this.X;
        const i = new AuSlotWatcherBinding(t, s.callback ?? `${h(s.name)}Changed`, s.slotName ?? "default", s.query ?? "*");
        x(t, s.name, {
            enumerable: true,
            configurable: true,
            get: d((() => i.getValue()), {
                getObserver: () => i
            }),
            set: () => {}
        });
        G(_t, i).register(e.container);
        e.addBinding(i);
    }
}

function slotted(t, e) {
    if (!Ft) {
        Ft = true;
        s.subscriberCollection(AuSlotWatcherBinding, null);
        lifecycleHooks()(SlottedLifecycleHooks, null);
    }
    const i = H("dependencies");
    function decorator(s, n) {
        if (n.kind !== "field") throw createMappedError(9990);
        const r = typeof t === "object" ? t : {
            query: t,
            slotName: e,
            name: ""
        };
        r.name = n.name;
        const l = n.metadata[i] ??= [];
        l.push(new SlottedLifecycleHooks(r));
    }
    return decorator;
}

let Ft = false;

class SpreadBinding {
    static create(t, s, n, r, l, a, h, c) {
        const u = [];
        const f = r.renderers;
        const getHydrationContext = e => {
            let s = e;
            let i = t;
            while (i != null && s > 0) {
                i = i.parent;
                --s;
            }
            if (i == null) {
                throw createMappedError(9999);
            }
            return i;
        };
        const renderSpreadInstruction = t => {
            const r = getHydrationContext(t);
            const d = new SpreadBinding(r);
            const p = l.compileSpread(r.controller.definition, r.instruction?.captures ?? e.emptyArray, r.controller.container, s, n);
            let g;
            for (g of p) {
                switch (g.type) {
                  case i.InstructionType.spreadTransferedBinding:
                    renderSpreadInstruction(t + 1);
                    break;

                  case i.InstructionType.spreadElementProp:
                    f[g.instruction.type].render(d, findElementControllerFor(s), g.instruction, a, h, c);
                    break;

                  default:
                    f[g.type].render(d, s, g, a, h, c);
                }
            }
            u.push(d);
        };
        renderSpreadInstruction(0);
        return u;
    }
    get container() {
        return this.locator;
    }
    get definition() {
        return this.$controller.definition;
    }
    get state() {
        return this.$controller.state;
    }
    constructor(t) {
        this.isBound = false;
        this.Y = [];
        this.locator = (this.$controller = (this.Z = t).controller).container;
    }
    get(t) {
        return this.locator.get(t);
    }
    bind(t) {
        if (this.isBound) {
            return;
        }
        this.isBound = true;
        const e = this.scope = this.Z.controller.scope.parent ?? void 0;
        if (e == null) {
            throw createMappedError(9999);
        }
        this.Y.forEach((t => t.bind(e)));
    }
    unbind() {
        this.Y.forEach((t => t.unbind()));
        this.isBound = false;
    }
    addBinding(t) {
        this.Y.push(t);
    }
    addChild(t) {
        if (t.vmKind !== Me) {
            throw createMappedError(9998);
        }
        this.$controller.addChild(t);
    }
}

class SpreadValueBinding {
    constructor(t, e, s, i, n, r, l) {
        this.target = e;
        this.targetKeys = s;
        this.ast = i;
        this.isBound = false;
        this.s = void 0;
        this.boundFn = false;
        this.J = {};
        this.tt = new WeakMap;
        this.L = t;
        this.oL = n;
        this.l = r;
        this.C = l;
    }
    updateTarget() {
        this.obs.version++;
        const t = R(this.ast, this.s, this, this);
        this.obs.clear();
        this.et(t, true);
    }
    handleChange() {
        if (!this.isBound) {
            return;
        }
        this.updateTarget();
    }
    handleCollectionChange() {
        if (!this.isBound) {
            return;
        }
        this.updateTarget();
    }
    bind(t) {
        if (this.isBound) {
            if (t === this.s) {
                return;
            }
            this.unbind();
        }
        this.isBound = true;
        this.s = t;
        T(this.ast, t, this);
        const e = R(this.ast, t, this, this);
        this.et(e, false);
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        L(this.ast, this.s, this);
        this.s = void 0;
        let t;
        for (t in this.J) {
            this.J[t].unbind();
        }
    }
    et(s, n) {
        let r;
        if (!e.isObject(s)) {
            for (r in this.J) {
                this.J[r]?.unbind();
            }
            return;
        }
        let l;
        let a = this.tt.get(s);
        if (a == null) {
            this.tt.set(s, a = Scope.fromParent(this.s, s));
        }
        for (r of this.targetKeys) {
            l = this.J[r];
            if (r in s) {
                if (l == null) {
                    l = this.J[r] = new PropertyBinding(this.L, this.l, this.oL, this.C, SpreadValueBinding.st[r] ??= new t.AccessScopeExpression(r, 0), this.target, r, i.BindingMode.toView);
                }
                l.bind(a);
            } else if (n) {
                l?.unbind();
            }
        }
    }
}

SpreadValueBinding.mix = yt((() => {
    xt(SpreadValueBinding);
    wt(SpreadValueBinding, (() => "updateTarget"));
    s.connectable(SpreadValueBinding, null);
    vt(true, false)(SpreadValueBinding);
}));

SpreadValueBinding.st = {};

const addListener = (t, e, s, i) => {
    t.addEventListener(e, s, i);
};

const removeListener = (t, e, s, i) => {
    t.removeEventListener(e, s, i);
};

const mixinNodeObserverUseConfig = t => {
    let e;
    const s = t.prototype;
    defineHiddenProp(s, "subscribe", (function(t) {
        if (this.subs.add(t) && this.subs.count === 1) {
            for (e of this.cf.events) {
                addListener(this.it, e, this);
            }
            this.nt = true;
            this.rt?.();
        }
    }));
    defineHiddenProp(s, "unsubscribe", (function(t) {
        if (this.subs.remove(t) && this.subs.count === 0) {
            for (e of this.cf.events) {
                removeListener(this.it, e, this);
            }
            this.nt = false;
            this.ot?.();
        }
    }));
    defineHiddenProp(s, "useConfig", (function(t) {
        this.cf = t;
        if (this.nt) {
            for (e of this.cf.events) {
                removeListener(this.it, e, this);
            }
            for (e of this.cf.events) {
                addListener(this.it, e, this);
            }
        }
    }));
};

const mixinNoopSubscribable = t => {
    defineHiddenProp(t.prototype, "subscribe", e.noop);
    defineHiddenProp(t.prototype, "unsubscribe", e.noop);
};

class ClassAttributeAccessor {
    get doNotCache() {
        return true;
    }
    constructor(t, e = {}) {
        this.obj = t;
        this.mapping = e;
        this.type = S | A;
        this.v = "";
        this.lt = {};
        this.ht = 0;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        if (t !== this.v) {
            this.v = t;
            this.ct();
        }
    }
    ct() {
        const t = this.lt;
        const e = ++this.ht;
        const s = this.obj.classList;
        const i = getClassesToAdd(this.v);
        const n = i.length;
        let r = 0;
        let l;
        if (n > 0) {
            for (;r < n; r++) {
                l = i[r];
                l = this.mapping[l] || l;
                if (l.length === 0) {
                    continue;
                }
                t[l] = this.ht;
                s.add(l);
            }
        }
        if (e === 1) {
            return;
        }
        for (l in t) {
            l = this.mapping[l] || l;
            if (t[l] === e) {
                continue;
            }
            s.remove(l);
        }
    }
}

(() => {
    mixinNoopSubscribable(ClassAttributeAccessor);
})();

function getClassesToAdd(t) {
    if (e.isString(t)) {
        return splitClassString(t);
    }
    if (typeof t !== "object") {
        return e.emptyArray;
    }
    if (e.isArray(t)) {
        const s = t.length;
        if (s > 0) {
            const e = [];
            let i = 0;
            for (;s > i; ++i) {
                e.push(...getClassesToAdd(t[i]));
            }
            return e;
        } else {
            return e.emptyArray;
        }
    }
    const s = [];
    let i;
    for (i in t) {
        if (Boolean(t[i])) {
            if (i.includes(" ")) {
                s.push(...splitClassString(i));
            } else {
                s.push(i);
            }
        }
    }
    return s;
}

function splitClassString(t) {
    const s = t.match(/\S+/g);
    if (s === null) {
        return e.emptyArray;
    }
    return s;
}

const fromHydrationContext = t => ({
    $isResolver: true,
    resolve(s, i) {
        return i.get(Ne).controller.container.get(e.own(t));
    }
});

const Vt = /*@__PURE__*/ j("IRenderer");

function renderer(t, s) {
    const i = s?.metadata ?? (t[Symbol.metadata] ??= Object.create(null));
    i[e.registrableMetadataKey] = {
        register(e) {
            z(Vt, t).register(e);
        }
    };
    return t;
}

function ensureExpression(t, s, i) {
    if (e.isString(s)) {
        return t.parse(s, i);
    }
    return s;
}

function getTarget(t) {
    if (t.viewModel != null) {
        return t.viewModel;
    }
    return t;
}

function getRefTarget(t, e) {
    if (e === "element") {
        return t;
    }
    switch (e) {
      case "controller":
        return findElementControllerFor(t);

      case "view":
        throw createMappedError(750);

      case "component":
        return findElementControllerFor(t).viewModel;

      default:
        {
            const s = findAttributeControllerFor(t, e);
            if (s !== void 0) {
                return s.viewModel;
            }
            const i = findElementControllerFor(t, {
                name: e
            });
            if (i === void 0) {
                throw createMappedError(751, e);
            }
            return i.viewModel;
        }
    }
}

const Ot = /*@__PURE__*/ renderer(class SetPropertyRenderer {
    constructor() {
        this.target = i.InstructionType.setProperty;
    }
    render(t, e, s) {
        const i = getTarget(e);
        if (i.$observers?.[s.to] !== void 0) {
            i.$observers[s.to].setValue(s.value);
        } else {
            i[s.to] = s.value;
        }
    }
}, null);

const Ht = /*@__PURE__*/ renderer(class CustomElementRenderer {
    constructor() {
        this.r = e.resolve(ue);
        this.target = i.InstructionType.hydrateElement;
    }
    render(t, e, s, i, n, r) {
        let l;
        let a;
        let h;
        const c = s.res;
        const u = s.projections;
        const f = t.container;
        switch (typeof c) {
          case "string":
            l = os.find(f, c);
            if (l == null) {
                throw createMappedError(752, s, t);
            }
            break;

          default:
            l = c;
        }
        const d = s.containerless || l.containerless;
        const p = d ? convertToRenderLocation(e) : null;
        const m = createElementContainer(i, t, e, s, p, u == null ? void 0 : new AuSlotsInfo(g(u)));
        a = m.invoke(l.Type);
        h = Controller.$el(m, a, e, s, l, p);
        setRef(e, l.key, h);
        const x = this.r.renderers;
        const v = s.props;
        const b = v.length;
        let w = 0;
        let y;
        while (b > w) {
            y = v[w];
            x[y.type].render(t, h, y, i, n, r);
            ++w;
        }
        t.addChild(h);
    }
}, null);

const $t = /*@__PURE__*/ renderer(class CustomAttributeRenderer {
    constructor() {
        this.r = e.resolve(ue);
        this.target = i.InstructionType.hydrateAttribute;
    }
    render(t, e, s, i, n, r) {
        let l = t.container;
        let a;
        switch (typeof s.res) {
          case "string":
            a = ut.find(l, s.res);
            if (a == null) {
                throw createMappedError(753, s, t);
            }
            break;

          default:
            a = s.res;
        }
        const h = invokeAttribute(i, a, t, e, s, void 0, void 0);
        const c = Controller.$attr(h.ctn, h.vm, e, a);
        setRef(e, a.key, c);
        const u = this.r.renderers;
        const f = s.props;
        const d = f.length;
        let p = 0;
        let g;
        while (d > p) {
            g = f[p];
            u[g.type].render(t, c, g, i, n, r);
            ++p;
        }
        t.addChild(c);
    }
}, null);

const Nt = /*@__PURE__*/ renderer(class TemplateControllerRenderer {
    constructor() {
        this.r = e.resolve(ue);
        this.target = i.InstructionType.hydrateTemplateController;
    }
    render(t, e, s, i, n, r) {
        let l = t.container;
        let a;
        switch (typeof s.res) {
          case "string":
            a = ut.find(l, s.res);
            if (a == null) {
                throw createMappedError(754, s, t);
            }
            break;

          default:
            a = s.res;
        }
        const h = this.r.getViewFactory(s.def, a.containerStrategy === "new" ? l.createChild({
            inheritParentResources: true
        }) : l);
        const c = convertToRenderLocation(e);
        const u = invokeAttribute(i, a, t, e, s, h, c);
        const f = Controller.$attr(u.ctn, u.vm, e, a);
        setRef(c, a.key, f);
        u.vm.link?.(t, f, e, s);
        const d = this.r.renderers;
        const p = s.props;
        const g = p.length;
        let m = 0;
        let x;
        while (g > m) {
            x = p[m];
            d[x.type].render(t, f, x, i, n, r);
            ++m;
        }
        t.addChild(f);
    }
}, null);

const Wt = /*@__PURE__*/ renderer(class LetElementRenderer {
    constructor() {
        this.target = i.InstructionType.hydrateLetElement;
        LetBinding.mix();
    }
    render(t, e, s, i, n, r) {
        e.remove();
        const l = s.instructions;
        const a = s.toBindingContext;
        const h = t.container;
        const c = l.length;
        let u;
        let f;
        let d = 0;
        while (c > d) {
            u = l[d];
            f = ensureExpression(n, u.from, y);
            t.addBinding(new LetBinding(h, r, f, u.to, a));
            ++d;
        }
    }
}, null);

const jt = /*@__PURE__*/ renderer(class RefBindingRenderer {
    constructor() {
        this.target = i.InstructionType.refBinding;
    }
    render(t, e, s, i, n) {
        t.addBinding(new RefBinding(t.container, ensureExpression(n, s.from, y), getRefTarget(e, s.to)));
    }
}, null);

const zt = /*@__PURE__*/ renderer(class InterpolationBindingRenderer {
    constructor() {
        this.target = i.InstructionType.interpolation;
        InterpolationPartBinding.mix();
    }
    render(t, e, s, i, n, r) {
        const l = t.container;
        const a = new InterpolationBinding(t, l, r, i.domQueue, ensureExpression(n, s.from, v), getTarget(e), s.to, q);
        if (s.to === "class" && a.target.nodeType > 0) {
            const t = l.get(fromHydrationContext(Xe));
            a.useAccessor(new ClassAttributeAccessor(a.target, t));
        }
        t.addBinding(a);
    }
}, null);

const Ut = /*@__PURE__*/ renderer(class PropertyBindingRenderer {
    constructor() {
        this.target = i.InstructionType.propertyBinding;
        PropertyBinding.mix();
    }
    render(t, e, s, i, n, r) {
        const l = t.container;
        const a = new PropertyBinding(t, l, r, i.domQueue, ensureExpression(n, s.from, y), getTarget(e), s.to, s.mode);
        if (s.to === "class" && a.target.nodeType > 0) {
            const t = l.get(fromHydrationContext(Xe));
            a.useTargetObserver(new ClassAttributeAccessor(a.target, t));
        }
        t.addBinding(a);
    }
}, null);

const Gt = /*@__PURE__*/ renderer(class IteratorBindingRenderer {
    constructor() {
        this.target = i.InstructionType.iteratorBinding;
        PropertyBinding.mix();
    }
    render(t, e, s, i, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, i.domQueue, ensureExpression(n, s.forOf, b), getTarget(e), s.to, q));
    }
}, null);

const Kt = /*@__PURE__*/ renderer(class TextBindingRenderer {
    constructor() {
        this.target = i.InstructionType.textBinding;
        ContentBinding.mix();
    }
    render(t, e, s, i, n, r) {
        t.addBinding(new ContentBinding(t, t.container, r, i.domQueue, i, ensureExpression(n, s.from, y), e));
    }
}, null);

const Xt = j("IListenerBindingOptions", (t => t.instance({
    prevent: false
})));

const Qt = /*@__PURE__*/ renderer(class ListenerBindingRenderer {
    constructor() {
        this.target = i.InstructionType.listenerBinding;
        this.ut = e.resolve(Tt);
        this.ft = e.resolve(Xt);
        ListenerBinding.mix();
    }
    render(t, e, s, i, n) {
        t.addBinding(new ListenerBinding(t.container, ensureExpression(n, s.from, w), e, s.to, new ListenerBindingOptions(this.ft.prevent, s.capture), this.ut.getHandler(s.to, s.modifier)));
    }
}, null);

const Yt = /*@__PURE__*/ renderer(class SetAttributeRenderer {
    constructor() {
        this.target = i.InstructionType.setAttribute;
    }
    render(t, e, s) {
        e.setAttribute(s.to, s.value);
    }
}, null);

const Zt = /*@__PURE__*/ renderer(class SetClassAttributeRenderer {
    constructor() {
        this.target = i.InstructionType.setClassAttribute;
    }
    render(t, e, s) {
        addClasses(e.classList, s.value);
    }
}, null);

const Jt = /*@__PURE__*/ renderer(class SetStyleAttributeRenderer {
    constructor() {
        this.target = i.InstructionType.setStyleAttribute;
    }
    render(t, e, s) {
        e.style.cssText += s.value;
    }
}, null);

const te = /*@__PURE__*/ renderer(class StylePropertyBindingRenderer {
    constructor() {
        this.target = i.InstructionType.stylePropertyBinding;
        PropertyBinding.mix();
    }
    render(t, e, s, i, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, i.domQueue, ensureExpression(n, s.from, y), e.style, s.to, q));
    }
}, null);

const ee = /*@__PURE__*/ renderer(class AttributeBindingRenderer {
    constructor() {
        this.target = i.InstructionType.attributeBinding;
        AttributeBinding.mix();
    }
    render(t, e, s, i, n, r) {
        const l = t.container;
        const a = l.has(Xe, false) ? l.get(Xe) : null;
        t.addBinding(new AttributeBinding(t, l, r, i.domQueue, ensureExpression(n, s.from, y), e, s.attr, a == null ? s.to : s.to.split(/\s/g).map((t => a[t] ?? t)).join(" "), q));
    }
}, null);

const se = /*@__PURE__*/ renderer(class SpreadRenderer {
    constructor() {
        this.dt = e.resolve(i.ITemplateCompiler);
        this.r = e.resolve(ue);
        this.target = i.InstructionType.spreadTransferedBinding;
    }
    render(t, e, s, i, n, r) {
        SpreadBinding.create(t.container.get(Ne), e, void 0, this.r, this.dt, i, n, r).forEach((e => t.addBinding(e)));
    }
}, null);

const ie = /*@__PURE__*/ renderer(class SpreadValueRenderer {
    constructor() {
        this.target = i.InstructionType.spreadValueBinding;
        SpreadValueBinding.mix();
    }
    render(t, e, s, i, n, r) {
        const l = s.target;
        if (l === "$bindables") {
            t.addBinding(new SpreadValueBinding(t, e.viewModel, g(e.definition.bindables), n.parse(s.from, y), r, t.container, i.domQueue));
        } else {
            throw createMappedError(820, l);
        }
    }
}, null);

function addClasses(t, e) {
    const s = e.length;
    let i = 0;
    for (let n = 0; n < s; ++n) {
        if (e.charCodeAt(n) === 32) {
            if (n !== i) {
                t.add(e.slice(i, n));
            }
            i = n + 1;
        } else if (n + 1 === s) {
            t.add(e.slice(i));
        }
    }
}

const ne = "IController";

const re = "IInstruction";

const oe = "IRenderLocation";

const le = "ISlotsInfo";

function createElementContainer(t, s, n, r, l, a) {
    const h = s.container.createChild();
    registerHostNode(h, n, t);
    registerResolver(h, $e, new e.InstanceProvider(ne, s));
    registerResolver(h, i.IInstruction, new e.InstanceProvider(re, r));
    registerResolver(h, Ke, l == null ? ae : new RenderLocationProvider(l));
    registerResolver(h, Mt, he);
    registerResolver(h, Pt, a == null ? ce : new e.InstanceProvider(le, a));
    return h;
}

class ViewFactoryProvider {
    get $isResolver() {
        return true;
    }
    constructor(t) {
        this.f = t;
    }
    resolve() {
        const t = this.f;
        if (t === null) {
            throw createMappedError(755);
        }
        if (!e.isString(t.name) || t.name.length === 0) {
            throw createMappedError(756);
        }
        return t;
    }
}

function invokeAttribute(t, s, n, r, l, a, h, c) {
    const u = n instanceof Controller ? n : n.$controller;
    const f = u.container.createChild();
    registerHostNode(f, r, t);
    registerResolver(f, $e, new e.InstanceProvider(ne, u));
    registerResolver(f, i.IInstruction, new e.InstanceProvider(re, l));
    registerResolver(f, Ke, h == null ? ae : new e.InstanceProvider(oe, h));
    registerResolver(f, Mt, a == null ? he : new ViewFactoryProvider(a));
    registerResolver(f, Pt, c == null ? ce : new e.InstanceProvider(le, c));
    return {
        vm: f.invoke(s.Type),
        ctn: f
    };
}

class RenderLocationProvider {
    get name() {
        return "IRenderLocation";
    }
    get $isResolver() {
        return true;
    }
    constructor(t) {
        this.l = t;
    }
    resolve() {
        return this.l;
    }
}

const ae = new RenderLocationProvider(null);

const he = new ViewFactoryProvider(null);

const ce = new e.InstanceProvider(le, new AuSlotsInfo(e.emptyArray));

const ue = /*@__PURE__*/ j("IRendering", (t => t.singleton(Rendering)));

class Rendering {
    get renderers() {
        return this.gt ??= this.xt.getAll(Vt, false).reduce(((t, e) => {
            t[e.target] ??= e;
            return t;
        }), e.createLookup());
    }
    constructor() {
        this.vt = new WeakMap;
        this.bt = new WeakMap;
        const i = this.xt = e.resolve(e.IContainer).root;
        const n = this.p = i.get(lt);
        this.ep = i.get(t.IExpressionParser);
        this.oL = i.get(s.IObserverLocator);
        this.wt = n.document.createElement("au-m");
        this.yt = new FragmentNodeSequence(n, n.document.createDocumentFragment());
    }
    compile(t, e) {
        const s = e.get(i.ITemplateCompiler);
        const n = this.vt;
        let r = n.get(t);
        if (r == null) {
            n.set(t, r = CustomElementDefinition.create(t.needsCompile ? s.compile(t, e) : t));
        }
        return r;
    }
    getViewFactory(t, e) {
        return new ViewFactory(e, CustomElementDefinition.getOrCreate(t));
    }
    createNodes(t) {
        if (t.enhance === true) {
            return new FragmentNodeSequence(this.p, this.kt(t.template));
        }
        let s;
        let i = false;
        const n = this.bt;
        const r = this.p;
        const l = r.document;
        if (n.has(t)) {
            s = n.get(t);
        } else {
            const a = t.template;
            let h;
            if (a == null) {
                s = null;
            } else if (a instanceof r.Node) {
                if (a.nodeName === "TEMPLATE") {
                    s = a.content;
                    i = true;
                } else {
                    (s = l.createDocumentFragment()).appendChild(a.cloneNode(true));
                }
            } else {
                h = l.createElement("template");
                if (e.isString(a)) {
                    h.innerHTML = a;
                }
                s = h.content;
                i = true;
            }
            this.kt(s);
            n.set(t, s);
        }
        return s == null ? this.yt : new FragmentNodeSequence(this.p, i ? l.importNode(s, true) : l.adoptNode(s.cloneNode(true)));
    }
    render(t, e, s, i) {
        const n = s.instructions;
        const r = this.renderers;
        const l = e.length;
        let a = 0;
        let h = 0;
        let c = n.length;
        let u;
        let f;
        let d;
        if (l !== c) {
            throw createMappedError(757, l, c);
        }
        if (l > 0) {
            while (l > a) {
                u = n[a];
                d = e[a];
                h = 0;
                c = u.length;
                while (c > h) {
                    f = u[h];
                    r[f.type].render(t, d, f, this.p, this.ep, this.oL);
                    ++h;
                }
                ++a;
            }
        }
        if (i != null) {
            u = s.surrogates;
            if ((c = u.length) > 0) {
                h = 0;
                while (c > h) {
                    f = u[h];
                    r[f.type].render(t, i, f, this.p, this.ep, this.oL);
                    ++h;
                }
            }
        }
    }
    kt(t) {
        if (t == null) {
            return null;
        }
        const e = this.p.document.createTreeWalker(t, 128);
        let s;
        while ((s = e.nextNode()) != null) {
            if (s.nodeValue === "au*") {
                s.parentNode.replaceChild(e.currentNode = this.wt.cloneNode(), s);
            }
        }
        return t;
    }
}

function cssModules(...t) {
    return new CSSModulesProcessorRegistry(t);
}

class CSSModulesProcessorRegistry {
    constructor(t) {
        this.modules = t;
    }
    register(t) {
        let s = t.get(e.own(Xe));
        if (s == null) {
            t.register(G(Xe, s = e.createLookup()));
        }
        {
            d(s, ...this.modules);
        }
        class CompilingHook {
            compiling(t) {
                const i = t.tagName === "TEMPLATE";
                const n = i ? t.content : t;
                const r = [ t, ...e.toArray(n.querySelectorAll("[class]")) ];
                for (const t of r) {
                    const e = t.getAttributeNode("class");
                    if (e == null) {
                        continue;
                    }
                    const i = e.value.split(/\s+/g).map((t => s[t] || t)).join(" ");
                    e.value = i;
                }
            }
        }
        t.register(i.TemplateCompilerHooks.define(CompilingHook));
    }
}

function shadowCSS(...t) {
    return new ShadowDOMRegistry(t);
}

const fe = /*@__PURE__*/ j("IShadowDOMStyleFactory", (t => t.cachedCallback((t => {
    if (AdoptedStyleSheetsStyles.supported(t.get(lt))) {
        return t.get(AdoptedStyleSheetsStylesFactory);
    }
    return t.get(StyleElementStylesFactory);
}))));

class ShadowDOMRegistry {
    constructor(t) {
        this.css = t;
    }
    register(t) {
        const e = t.get(pe);
        const s = t.get(fe);
        t.register(G(de, s.createStyles(this.css, e)));
    }
}

class AdoptedStyleSheetsStylesFactory {
    constructor() {
        this.p = e.resolve(lt);
        this.cache = new Map;
    }
    createStyles(t, e) {
        return new AdoptedStyleSheetsStyles(this.p, t, this.cache, e);
    }
}

class StyleElementStylesFactory {
    constructor() {
        this.p = e.resolve(lt);
    }
    createStyles(t, e) {
        return new StyleElementStyles(this.p, t, e);
    }
}

const de = /*@__PURE__*/ j("IShadowDOMStyles");

const pe = /*@__PURE__*/ j("IShadowDOMGlobalStyles", (t => t.instance({
    applyTo: e.noop
})));

class AdoptedStyleSheetsStyles {
    constructor(t, e, s, i = null) {
        this.sharedStyles = i;
        this.styleSheets = e.map((e => {
            let i;
            if (e instanceof t.CSSStyleSheet) {
                i = e;
            } else {
                i = s.get(e);
                if (i === void 0) {
                    i = new t.CSSStyleSheet;
                    i.replaceSync(e);
                    s.set(e, i);
                }
            }
            return i;
        }));
    }
    static supported(t) {
        return "adoptedStyleSheets" in t.ShadowRoot.prototype;
    }
    applyTo(t) {
        if (this.sharedStyles !== null) {
            this.sharedStyles.applyTo(t);
        }
        t.adoptedStyleSheets = [ ...t.adoptedStyleSheets, ...this.styleSheets ];
    }
}

class StyleElementStyles {
    constructor(t, e, s = null) {
        this.p = t;
        this.localStyles = e;
        this.sharedStyles = s;
    }
    applyTo(t) {
        const e = this.localStyles;
        const s = this.p;
        for (let i = e.length - 1; i > -1; --i) {
            const n = s.document.createElement("style");
            n.innerHTML = e[i];
            t.prepend(n);
        }
        if (this.sharedStyles !== null) {
            this.sharedStyles.applyTo(t);
        }
    }
}

const ge = {
    shadowDOM(t) {
        return ot.creating(e.IContainer, (e => {
            if (t.sharedStyles != null) {
                const s = e.get(fe);
                e.register(G(pe, s.createStyles(t.sharedStyles, null)));
            }
        }));
    }
};

const {enter: me, exit: xe} = s.ConnectableSwitcher;

const {wrap: ve, unwrap: be} = s.ProxyObservable;

class ComputedWatcher {
    get value() {
        return this.v;
    }
    constructor(t, e, s, i, n) {
        this.obj = t;
        this.$get = s;
        this.useProxy = n;
        this.isBound = false;
        this.running = false;
        this.v = void 0;
        this.cb = i;
        this.oL = e;
    }
    handleChange() {
        this.run();
    }
    handleCollectionChange() {
        this.run();
    }
    bind() {
        if (this.isBound) {
            return;
        }
        this.compute();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this.obs.clearAll();
    }
    run() {
        if (!this.isBound || this.running) {
            return;
        }
        const t = this.obj;
        const s = this.v;
        const i = this.compute();
        if (!e.areEqual(i, s)) {
            this.cb.call(t, i, s, t);
        }
    }
    compute() {
        this.running = true;
        this.obs.version++;
        try {
            me(this);
            return this.v = be(this.$get.call(void 0, this.useProxy ? ve(this.obj) : this.obj, this));
        } finally {
            this.obs.clear();
            this.running = false;
            xe(this);
        }
    }
}

(() => {
    s.connectable(ComputedWatcher, null);
})();

class ExpressionWatcher {
    get value() {
        return this.v;
    }
    constructor(t, e, s, i, n) {
        this.scope = t;
        this.l = e;
        this.oL = s;
        this.isBound = false;
        this.boundFn = false;
        this.obj = t.bindingContext;
        this.Ct = i;
        this.cb = n;
    }
    handleChange(t) {
        const s = this.Ct;
        const i = this.obj;
        const n = this.v;
        const r = s.$kind === "AccessScope" && this.obs.count === 1;
        if (!r) {
            this.obs.version++;
            t = R(s, this.scope, this, this);
            this.obs.clear();
        }
        if (!e.areEqual(t, n)) {
            this.v = t;
            this.cb.call(i, t, n, i);
        }
    }
    bind() {
        if (this.isBound) {
            return;
        }
        this.obs.version++;
        this.v = R(this.Ct, this.scope, this, this);
        this.obs.clear();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this.obs.clearAll();
        this.v = void 0;
    }
}

(() => {
    s.connectable(ExpressionWatcher, null);
    vt(true)(ExpressionWatcher);
})();

class Controller {
    get lifecycleHooks() {
        return this.Bt;
    }
    get isActive() {
        return (this.state & (Ie | Pe)) > 0 && (this.state & _e) === 0;
    }
    get name() {
        if (this.parent === null) {
            switch (this.vmKind) {
              case Me:
                return `[${this.definition.name}]`;

              case Le:
                return this.definition.name;

              case De:
                return this.viewFactory.name;
            }
        }
        switch (this.vmKind) {
          case Me:
            return `${this.parent.name}>[${this.definition.name}]`;

          case Le:
            return `${this.parent.name}>${this.definition.name}`;

          case De:
            return this.viewFactory.name === this.parent.definition?.name ? `${this.parent.name}[view]` : `${this.parent.name}[view:${this.viewFactory.name}]`;
        }
    }
    get viewModel() {
        return this.St;
    }
    set viewModel(t) {
        this.St = t;
        this.At = t == null || this.vmKind === De ? HooksDefinition.none : new HooksDefinition(t);
    }
    constructor(t, e, s, i, n, r, l) {
        this.container = t;
        this.vmKind = e;
        this.definition = s;
        this.viewFactory = i;
        this.host = r;
        this.head = null;
        this.tail = null;
        this.next = null;
        this.parent = null;
        this.bindings = null;
        this.children = null;
        this.hasLockedScope = false;
        this.scope = null;
        this.isBound = false;
        this.Et = false;
        this.hostController = null;
        this.mountTarget = ye;
        this.shadowRoot = null;
        this.nodes = null;
        this.location = null;
        this.Bt = null;
        this.state = qe;
        this.Rt = false;
        this.$initiator = null;
        this.$resolve = void 0;
        this.$reject = void 0;
        this.$promise = void 0;
        this.Tt = 0;
        this.Lt = 0;
        this.Mt = 0;
        this.St = n;
        this.At = e === De ? HooksDefinition.none : new HooksDefinition(n);
        this.location = l;
        this.r = t.root.get(ue);
        this.coercion = e === De ? void 0 : t.get(Ee);
    }
    static getCached(t) {
        return we.get(t);
    }
    static getCachedOrThrow(t) {
        const e = Controller.getCached(t);
        if (e === void 0) {
            throw createMappedError(500, t);
        }
        return e;
    }
    static $el(t, s, i, n, r = void 0, l = null) {
        if (we.has(s)) {
            return we.get(s);
        }
        {
            r = r ?? getElementDefinition(s.constructor);
        }
        registerResolver(t, r.Type, new e.InstanceProvider(r.key, s, r.Type));
        const a = new Controller(t, Le, r, null, s, i, l);
        const h = t.get(e.optional(Ne));
        if (r.dependencies.length > 0) {
            t.register(...r.dependencies);
        }
        registerResolver(t, Ne, new e.InstanceProvider("IHydrationContext", new HydrationContext(a, n, h)));
        we.set(s, a);
        if (n == null || n.hydrate !== false) {
            a.hE(n, h);
        }
        return a;
    }
    static $attr(t, s, i, n) {
        if (we.has(s)) {
            return we.get(s);
        }
        n = n ?? getAttributeDefinition(s.constructor);
        registerResolver(t, n.Type, new e.InstanceProvider(n.key, s, n.Type));
        const r = new Controller(t, Me, n, null, s, i, null);
        if (n.dependencies.length > 0) {
            t.register(...n.dependencies);
        }
        we.set(s, r);
        r.Dt();
        return r;
    }
    static $view(t, e = void 0) {
        const s = new Controller(t.container, De, null, t, null, null, null);
        s.parent = e ?? null;
        s.qt();
        return s;
    }
    hE(t, s) {
        const i = this.container;
        const n = this.St;
        const r = this.definition;
        this.scope = Scope.create(n, null, true);
        if (r.watches.length > 0) {
            createWatchers(this, i, r, n);
        }
        createObservers(this, r, n);
        this.Bt = dt.resolve(i);
        i.register(r.Type);
        if (r.injectable !== null) {
            registerResolver(i, r.injectable, new e.InstanceProvider("definition.injectable", n));
        }
        if (t == null || t.hydrate !== false) {
            this.hS();
            this.hC();
        }
    }
    hS() {
        if (this.Bt.hydrating != null) {
            this.Bt.hydrating.forEach(callHydratingHook, this);
        }
        if (this.At.It) {
            this.St.hydrating(this);
        }
        const t = this.definition;
        const e = this.Pt = this.r.compile(t, this.container);
        const s = e.shadowOptions;
        const i = e.hasSlots;
        const n = e.containerless;
        let r = this.host;
        let l = this.location;
        if ((this.hostController = findElementControllerFor(r, Ae)) !== null) {
            r = this.host = this.container.root.get(lt).document.createElement(t.name);
            if (n && l == null) {
                l = this.location = convertToRenderLocation(r);
            }
        }
        setRef(r, is, this);
        setRef(r, t.key, this);
        if (s !== null || i) {
            if (l != null) {
                throw createMappedError(501);
            }
            setRef(this.shadowRoot = r.attachShadow(s ?? Te), is, this);
            setRef(this.shadowRoot, t.key, this);
            this.mountTarget = Ce;
        } else if (l != null) {
            setRef(l, is, this);
            setRef(l, t.key, this);
            this.mountTarget = Be;
        } else {
            this.mountTarget = ke;
        }
        this.St.$controller = this;
        this.nodes = this.r.createNodes(e);
        if (this.Bt.hydrated !== void 0) {
            this.Bt.hydrated.forEach(callHydratedHook, this);
        }
        if (this.At._t) {
            this.St.hydrated(this);
        }
    }
    hC() {
        this.r.render(this, this.nodes.findTargets(), this.Pt, this.host);
        if (this.Bt.created !== void 0) {
            this.Bt.created.forEach(callCreatedHook, this);
        }
        if (this.At.Ft) {
            this.St.created(this);
        }
    }
    Dt() {
        const t = this.definition;
        const e = this.St;
        if (t.watches.length > 0) {
            createWatchers(this, this.container, t, e);
        }
        createObservers(this, t, e);
        e.$controller = this;
        this.Bt = dt.resolve(this.container);
        if (this.Bt.created !== void 0) {
            this.Bt.created.forEach(callCreatedHook, this);
        }
        if (this.At.Ft) {
            this.St.created(this);
        }
    }
    qt() {
        this.Pt = this.r.compile(this.viewFactory.def, this.container);
        this.r.render(this, (this.nodes = this.r.createNodes(this.Pt)).findTargets(), this.Pt, void 0);
    }
    activate(t, s, i) {
        switch (this.state) {
          case qe:
          case Fe:
            if (!(s === null || s.isActive)) {
                return;
            }
            this.state = Ie;
            break;

          case Pe:
            return;

          case Oe:
            throw createMappedError(502, this.name);

          default:
            throw createMappedError(503, this.name, stringifyState(this.state));
        }
        this.parent = s;
        switch (this.vmKind) {
          case Le:
            this.scope.parent = i ?? null;
            break;

          case Me:
            this.scope = i ?? null;
            break;

          case De:
            if (i === void 0 || i === null) {
                throw createMappedError(504, this.name);
            }
            if (!this.hasLockedScope) {
                this.scope = i;
            }
            break;
        }
        this.$initiator = t;
        this.Vt();
        let n = void 0;
        if (this.vmKind !== De && this.Bt.binding != null) {
            n = e.onResolveAll(...this.Bt.binding.map(callBindingHook, this));
        }
        if (this.At.Ot) {
            n = e.onResolveAll(n, this.St.binding(this.$initiator, this.parent));
        }
        if (e.isPromise(n)) {
            this.Ht();
            n.then((() => {
                this.Et = true;
                if (this.state !== Ie) {
                    this.$t();
                } else {
                    this.bind();
                }
            })).catch((t => {
                this.Nt(t);
            }));
            return this.$promise;
        }
        this.Et = true;
        this.bind();
        return this.$promise;
    }
    bind() {
        let t = 0;
        let s = 0;
        let i = void 0;
        if (this.bindings !== null) {
            t = 0;
            s = this.bindings.length;
            while (s > t) {
                this.bindings[t].bind(this.scope);
                ++t;
            }
        }
        if (this.vmKind !== De && this.Bt.bound != null) {
            i = e.onResolveAll(...this.Bt.bound.map(callBoundHook, this));
        }
        if (this.At.Wt) {
            i = e.onResolveAll(i, this.St.bound(this.$initiator, this.parent));
        }
        if (e.isPromise(i)) {
            this.Ht();
            i.then((() => {
                this.isBound = true;
                if (this.state !== Ie) {
                    this.$t();
                } else {
                    this.jt();
                }
            })).catch((t => {
                this.Nt(t);
            }));
            return;
        }
        this.isBound = true;
        this.jt();
    }
    zt(...t) {
        switch (this.mountTarget) {
          case ke:
            this.host.append(...t);
            break;

          case Ce:
            this.shadowRoot.append(...t);
            break;

          case Be:
            {
                let e = 0;
                for (;e < t.length; ++e) {
                    this.location.parentNode.insertBefore(t[e], this.location);
                }
                break;
            }
        }
    }
    jt() {
        if (this.hostController !== null) {
            switch (this.mountTarget) {
              case ke:
              case Ce:
                this.hostController.zt(this.host);
                break;

              case Be:
                this.hostController.zt(this.location.$start, this.location);
                break;
            }
        }
        switch (this.mountTarget) {
          case ke:
            this.nodes.appendTo(this.host, this.definition != null && this.definition.enhance);
            break;

          case Ce:
            {
                const t = this.container;
                const e = t.has(de, false) ? t.get(de) : t.get(pe);
                e.applyTo(this.shadowRoot);
                this.nodes.appendTo(this.shadowRoot);
                break;
            }

          case Be:
            this.nodes.insertBefore(this.location);
            break;
        }
        let t = 0;
        let s = void 0;
        if (this.vmKind !== De && this.Bt.attaching != null) {
            s = e.onResolveAll(...this.Bt.attaching.map(callAttachingHook, this));
        }
        if (this.At.Ut) {
            s = e.onResolveAll(s, this.St.attaching(this.$initiator, this.parent));
        }
        if (e.isPromise(s)) {
            this.Ht();
            this.Vt();
            s.then((() => {
                this.$t();
            })).catch((t => {
                this.Nt(t);
            }));
        }
        if (this.children !== null) {
            for (;t < this.children.length; ++t) {
                void this.children[t].activate(this.$initiator, this, this.scope);
            }
        }
        this.$t();
    }
    deactivate(t, s) {
        let i = void 0;
        switch (this.state & ~Ve) {
          case Pe:
            this.state = _e;
            break;

          case Ie:
            this.state = _e;
            i = this.$promise?.catch(e.noop);
            break;

          case qe:
          case Fe:
          case Oe:
          case Fe | Oe:
            return;

          default:
            throw createMappedError(505, this.name, this.state);
        }
        this.$initiator = t;
        if (t === this) {
            this.Gt();
        }
        let n = 0;
        let r;
        if (this.children !== null) {
            for (n = 0; n < this.children.length; ++n) {
                void this.children[n].deactivate(t, this);
            }
        }
        return e.onResolve(i, (() => {
            if (this.isBound) {
                if (this.vmKind !== De && this.Bt.detaching != null) {
                    r = e.onResolveAll(...this.Bt.detaching.map(callDetachingHook, this));
                }
                if (this.At.Kt) {
                    r = e.onResolveAll(r, this.St.detaching(this.$initiator, this.parent));
                }
            }
            if (e.isPromise(r)) {
                this.Ht();
                t.Gt();
                r.then((() => {
                    t.Xt();
                })).catch((e => {
                    t.Nt(e);
                }));
            }
            if (t.head === null) {
                t.head = this;
            } else {
                t.tail.next = this;
            }
            t.tail = this;
            if (t !== this) {
                return;
            }
            this.Xt();
            return this.$promise;
        }));
    }
    removeNodes() {
        switch (this.vmKind) {
          case Le:
          case De:
            this.nodes.remove();
            this.nodes.unlink();
        }
        if (this.hostController !== null) {
            switch (this.mountTarget) {
              case ke:
              case Ce:
                this.host.remove();
                break;

              case Be:
                this.location.$start.remove();
                this.location.remove();
                break;
            }
        }
    }
    unbind() {
        let t = 0;
        if (this.bindings !== null) {
            for (;t < this.bindings.length; ++t) {
                this.bindings[t].unbind();
            }
        }
        this.parent = null;
        switch (this.vmKind) {
          case Me:
            this.scope = null;
            break;

          case De:
            if (!this.hasLockedScope) {
                this.scope = null;
            }
            if ((this.state & Ve) === Ve && !this.viewFactory.tryReturnToCache(this) && this.$initiator === this) {
                this.dispose();
            }
            break;

          case Le:
            this.scope.parent = null;
            break;
        }
        this.state = Fe;
        this.$initiator = null;
        this.Qt();
    }
    Ht() {
        if (this.$promise === void 0) {
            this.$promise = new Promise(((t, e) => {
                this.$resolve = t;
                this.$reject = e;
            }));
            if (this.$initiator !== this) {
                this.parent.Ht();
            }
        }
    }
    Qt() {
        if (this.$promise !== void 0) {
            We = this.$resolve;
            this.$resolve = this.$reject = this.$promise = void 0;
            We();
            We = void 0;
        }
    }
    Nt(t) {
        if (this.$promise !== void 0) {
            je = this.$reject;
            this.$resolve = this.$reject = this.$promise = void 0;
            je(t);
            je = void 0;
        }
        if (this.$initiator !== this) {
            this.parent.Nt(t);
        }
    }
    Vt() {
        ++this.Tt;
        if (this.$initiator !== this) {
            this.parent.Vt();
        }
    }
    $t() {
        if (this.state !== Ie) {
            --this.Tt;
            this.Qt();
            if (this.$initiator !== this) {
                this.parent.$t();
            }
            return;
        }
        if (--this.Tt === 0) {
            if (this.vmKind !== De && this.Bt.attached != null) {
                ze = e.onResolveAll(...this.Bt.attached.map(callAttachedHook, this));
            }
            if (this.At.Yt) {
                ze = e.onResolveAll(ze, this.St.attached(this.$initiator));
            }
            if (e.isPromise(ze)) {
                this.Ht();
                ze.then((() => {
                    this.state = Pe;
                    this.Qt();
                    if (this.$initiator !== this) {
                        this.parent.$t();
                    }
                })).catch((t => {
                    this.Nt(t);
                }));
                ze = void 0;
                return;
            }
            ze = void 0;
            this.state = Pe;
            this.Qt();
        }
        if (this.$initiator !== this) {
            this.parent.$t();
        }
    }
    Gt() {
        ++this.Lt;
    }
    Xt() {
        if (--this.Lt === 0) {
            this.Zt();
            this.removeNodes();
            let t = this.$initiator.head;
            let s = void 0;
            while (t !== null) {
                if (t !== this) {
                    if (t.debug) {
                        t.logger.trace(`detach()`);
                    }
                    t.removeNodes();
                }
                if (t.Et) {
                    if (t.vmKind !== De && t.Bt.unbinding != null) {
                        s = e.onResolveAll(...t.Bt.unbinding.map(callUnbindingHook, t));
                    }
                    if (t.At.Jt) {
                        if (t.debug) {
                            t.logger.trace("unbinding()");
                        }
                        s = e.onResolveAll(s, t.viewModel.unbinding(t.$initiator, t.parent));
                    }
                }
                if (e.isPromise(s)) {
                    this.Ht();
                    this.Zt();
                    s.then((() => {
                        this.te();
                    })).catch((t => {
                        this.Nt(t);
                    }));
                }
                s = void 0;
                t = t.next;
            }
            this.te();
        }
    }
    Zt() {
        ++this.Mt;
    }
    te() {
        if (--this.Mt === 0) {
            let t = this.$initiator.head;
            let e = null;
            while (t !== null) {
                if (t !== this) {
                    t.Et = false;
                    t.isBound = false;
                    t.unbind();
                }
                e = t.next;
                t.next = null;
                t = e;
            }
            this.head = this.tail = null;
            this.Et = false;
            this.isBound = false;
            this.unbind();
        }
    }
    addBinding(t) {
        if (this.bindings === null) {
            this.bindings = [ t ];
        } else {
            this.bindings[this.bindings.length] = t;
        }
    }
    addChild(t) {
        if (this.children === null) {
            this.children = [ t ];
        } else {
            this.children[this.children.length] = t;
        }
    }
    is(t) {
        switch (this.vmKind) {
          case Me:
          case Le:
            {
                return this.definition.name === t;
            }

          case De:
            return this.viewFactory.name === t;
        }
    }
    lockScope(t) {
        this.scope = t;
        this.hasLockedScope = true;
    }
    setHost(t) {
        if (this.vmKind === Le) {
            setRef(t, is, this);
            setRef(t, this.definition.key, this);
        }
        this.host = t;
        this.mountTarget = ke;
        return this;
    }
    setShadowRoot(t) {
        if (this.vmKind === Le) {
            setRef(t, is, this);
            setRef(t, this.definition.key, this);
        }
        this.shadowRoot = t;
        this.mountTarget = Ce;
        return this;
    }
    setLocation(t) {
        if (this.vmKind === Le) {
            setRef(t, is, this);
            setRef(t, this.definition.key, this);
        }
        this.location = t;
        this.mountTarget = Be;
        return this;
    }
    release() {
        this.state |= Ve;
    }
    dispose() {
        if ((this.state & Oe) === Oe) {
            return;
        }
        this.state |= Oe;
        if (this.At.ee) {
            this.St.dispose();
        }
        if (this.children !== null) {
            this.children.forEach(callDispose);
            this.children = null;
        }
        this.hostController = null;
        this.scope = null;
        this.nodes = null;
        this.location = null;
        this.viewFactory = null;
        if (this.St !== null) {
            we.delete(this.St);
            this.St = null;
        }
        this.St = null;
        this.host = null;
        this.shadowRoot = null;
        this.container.disposeResolvers();
    }
    accept(t) {
        if (t(this) === true) {
            return true;
        }
        if (this.At.se && this.St.accept(t) === true) {
            return true;
        }
        if (this.children !== null) {
            const {children: e} = this;
            for (let s = 0, i = e.length; s < i; ++s) {
                if (e[s].accept(t) === true) {
                    return true;
                }
            }
        }
    }
}

const we = new WeakMap;

const ye = 0;

const ke = 1;

const Ce = 2;

const Be = 3;

const Se = f({
    none: ye,
    host: ke,
    shadowRoot: Ce,
    location: Be
});

const Ae = {
    optional: true
};

const Ee = e.optionalResource(s.ICoercionConfiguration);

function createObservers(t, i, n) {
    const r = i.bindables;
    const l = p(r);
    const a = l.length;
    const h = t.container.get(s.IObserverLocator);
    if (a > 0) {
        for (let s = 0; s < a; ++s) {
            const i = l[s];
            const a = r[i];
            const c = a.callback;
            const u = h.getObserver(n, i);
            if (a.set !== e.noop) {
                if (u.useCoercer?.(a.set, t.coercion) !== true) {
                    throw createMappedError(507, i);
                }
            }
            if (n[c] != null || n.propertyChanged != null) {
                const callback = (e, s) => {
                    if (t.isBound) {
                        n[c]?.(e, s);
                        n.propertyChanged?.(i, e, s);
                    }
                };
                if (u.useCallback?.(callback) !== true) {
                    throw createMappedError(508, i);
                }
            }
        }
    }
}

const Re = new Map;

const getAccessScopeAst = e => {
    let s = Re.get(e);
    if (s == null) {
        s = new t.AccessScopeExpression(e, 0);
        Re.set(e, s);
    }
    return s;
};

function createWatchers(i, n, r, l) {
    const a = n.get(s.IObserverLocator);
    const h = n.get(t.IExpressionParser);
    const c = r.watches;
    const u = i.vmKind === Le ? i.scope : Scope.create(l, null, true);
    const f = c.length;
    let d;
    let p;
    let g;
    let m = 0;
    for (;f > m; ++m) {
        ({expression: d, callback: p} = c[m]);
        p = e.isFunction(p) ? p : Reflect.get(l, p);
        if (!e.isFunction(p)) {
            throw createMappedError(506, p);
        }
        if (e.isFunction(d)) {
            i.addBinding(new ComputedWatcher(l, a, d, p, true));
        } else {
            g = e.isString(d) ? h.parse(d, y) : getAccessScopeAst(d);
            i.addBinding(new ExpressionWatcher(u, n, a, g, p));
        }
    }
}

function isCustomElementController(t) {
    return t instanceof Controller && t.vmKind === Le;
}

function isCustomElementViewModel(t) {
    return n.isObject(t) && isElementType(t.constructor);
}

class HooksDefinition {
    constructor(t) {
        this.ie = "define" in t;
        this.It = "hydrating" in t;
        this._t = "hydrated" in t;
        this.Ft = "created" in t;
        this.Ot = "binding" in t;
        this.Wt = "bound" in t;
        this.Ut = "attaching" in t;
        this.Yt = "attached" in t;
        this.Kt = "detaching" in t;
        this.Jt = "unbinding" in t;
        this.ee = "dispose" in t;
        this.se = "accept" in t;
    }
}

HooksDefinition.none = new HooksDefinition({});

const Te = {
    mode: "open"
};

const Le = "customElement";

const Me = "customAttribute";

const De = "synthetic";

const qe = 0;

const Ie = 1;

const Pe = 2;

const _e = 4;

const Fe = 8;

const Ve = 16;

const Oe = 32;

const He = /*@__PURE__*/ f({
    none: qe,
    activating: Ie,
    activated: Pe,
    deactivating: _e,
    deactivated: Fe,
    released: Ve,
    disposed: Oe
});

function stringifyState(t) {
    const e = [];
    if ((t & Ie) === Ie) {
        e.push("activating");
    }
    if ((t & Pe) === Pe) {
        e.push("activated");
    }
    if ((t & _e) === _e) {
        e.push("deactivating");
    }
    if ((t & Fe) === Fe) {
        e.push("deactivated");
    }
    if ((t & Ve) === Ve) {
        e.push("released");
    }
    if ((t & Oe) === Oe) {
        e.push("disposed");
    }
    return e.length === 0 ? "none" : e.join("|");
}

const $e = /*@__PURE__*/ j("IController");

const Ne = /*@__PURE__*/ j("IHydrationContext");

class HydrationContext {
    constructor(t, e, s) {
        this.instruction = e;
        this.parent = s;
        this.controller = t;
    }
}

function callDispose(t) {
    t.dispose();
}

function callCreatedHook(t) {
    t.instance.created(this.St, this);
}

function callHydratingHook(t) {
    t.instance.hydrating(this.St, this);
}

function callHydratedHook(t) {
    t.instance.hydrated(this.St, this);
}

function callBindingHook(t) {
    return t.instance.binding(this.St, this["$initiator"], this.parent);
}

function callBoundHook(t) {
    return t.instance.bound(this.St, this["$initiator"], this.parent);
}

function callAttachingHook(t) {
    return t.instance.attaching(this.St, this["$initiator"], this.parent);
}

function callAttachedHook(t) {
    return t.instance.attached(this.St, this["$initiator"]);
}

function callDetachingHook(t) {
    return t.instance.detaching(this.St, this["$initiator"], this.parent);
}

function callUnbindingHook(t) {
    return t.instance.unbinding(this.St, this["$initiator"], this.parent);
}

let We;

let je;

let ze;

class Refs {}

function getRef(t, e) {
    return t.$au?.[e] ?? null;
}

function setRef(t, e, s) {
    (t.$au ??= new Refs)[e] = s;
}

const Ue = /*@__PURE__*/ j("INode");

const Ge = /*@__PURE__*/ j("IEventTarget", (t => t.cachedCallback((t => {
    if (t.has(as, true)) {
        return t.get(as).host;
    }
    return t.get(lt).document;
}))));

const Ke = /*@__PURE__*/ j("IRenderLocation");

const Xe = /*@__PURE__*/ j("ICssClassMapping");

const Qe = new WeakMap;

function getEffectiveParentNode(t) {
    if (Qe.has(t)) {
        return Qe.get(t);
    }
    let e = 0;
    let s = t.nextSibling;
    while (s !== null) {
        if (s.nodeType === 8) {
            switch (s.textContent) {
              case "au-start":
                ++e;
                break;

              case "au-end":
                if (e-- === 0) {
                    return s;
                }
            }
        }
        s = s.nextSibling;
    }
    if (t.parentNode === null && t.nodeType === 11) {
        const e = findElementControllerFor(t, {
            optional: true
        });
        if (e == null) {
            return null;
        }
        if (e.mountTarget === Se.shadowRoot) {
            return getEffectiveParentNode(e.host);
        }
    }
    return t.parentNode;
}

function setEffectiveParentNode(t, e) {
    if (t.platform !== void 0 && !(t instanceof t.platform.Node)) {
        const s = t.childNodes;
        for (let t = 0, i = s.length; t < i; ++t) {
            Qe.set(s[t], e);
        }
    } else {
        Qe.set(t, e);
    }
}

function convertToRenderLocation(t) {
    if (isRenderLocation(t)) {
        return t;
    }
    const e = t.ownerDocument.createComment("au-end");
    const s = e.$start = t.ownerDocument.createComment("au-start");
    const i = t.parentNode;
    if (i !== null) {
        i.replaceChild(e, t);
        i.insertBefore(s, e);
    }
    return e;
}

function isRenderLocation(t) {
    return t.textContent === "au-end";
}

class FragmentNodeSequence {
    get firstChild() {
        return this.ne;
    }
    get lastChild() {
        return this.re;
    }
    constructor(t, e) {
        this.platform = t;
        this.next = void 0;
        this.oe = false;
        this.le = false;
        this.ref = null;
        const s = (this.f = e).querySelectorAll("au-m");
        let i = 0;
        let n = s.length;
        let r = this.t = Array(n);
        let l;
        let a;
        while (n > i) {
            a = s[i];
            l = a.nextSibling;
            a.remove();
            if (l.nodeType === 8) {
                a = l;
                (l = l.nextSibling).$start = a;
            }
            r[i] = l;
            ++i;
        }
        const h = e.childNodes;
        const c = this.childNodes = Array(n = h.length);
        i = 0;
        while (n > i) {
            c[i] = h[i];
            ++i;
        }
        this.ne = e.firstChild;
        this.re = e.lastChild;
    }
    findTargets() {
        return this.t;
    }
    insertBefore(t) {
        if (this.le && !!this.ref) {
            this.addToLinked();
        } else {
            const e = t.parentNode;
            if (this.oe) {
                let s = this.ne;
                let i;
                const n = this.re;
                while (s != null) {
                    i = s.nextSibling;
                    e.insertBefore(s, t);
                    if (s === n) {
                        break;
                    }
                    s = i;
                }
            } else {
                this.oe = true;
                t.parentNode.insertBefore(this.f, t);
            }
        }
    }
    appendTo(t, e = false) {
        if (this.oe) {
            let e = this.ne;
            let s;
            const i = this.re;
            while (e != null) {
                s = e.nextSibling;
                t.appendChild(e);
                if (e === i) {
                    break;
                }
                e = s;
            }
        } else {
            this.oe = true;
            if (!e) {
                t.appendChild(this.f);
            }
        }
    }
    remove() {
        if (this.oe) {
            this.oe = false;
            const t = this.f;
            const e = this.re;
            let s;
            let i = this.ne;
            while (i !== null) {
                s = i.nextSibling;
                t.appendChild(i);
                if (i === e) {
                    break;
                }
                i = s;
            }
        }
    }
    addToLinked() {
        const t = this.ref;
        const e = t.parentNode;
        if (this.oe) {
            let s = this.ne;
            let i;
            const n = this.re;
            while (s != null) {
                i = s.nextSibling;
                e.insertBefore(s, t);
                if (s === n) {
                    break;
                }
                s = i;
            }
        } else {
            this.oe = true;
            e.insertBefore(this.f, t);
        }
    }
    unlink() {
        this.le = false;
        this.next = void 0;
        this.ref = void 0;
    }
    link(t) {
        this.le = true;
        if (isRenderLocation(t)) {
            this.ref = t;
        } else {
            this.next = t;
            this.ae();
        }
    }
    ae() {
        if (this.next !== void 0) {
            this.ref = this.next.firstChild;
        } else {
            this.ref = void 0;
        }
    }
}

const Ye = /*@__PURE__*/ j("IWindow", (t => t.callback((t => t.get(lt).window))));

const Ze = /*@__PURE__*/ j("ILocation", (t => t.callback((t => t.get(Ye).location))));

const Je = /*@__PURE__*/ j("IHistory", (t => t.callback((t => t.get(Ye).history))));

const registerHostNode = (t, s, i = t.get(lt)) => {
    registerResolver(t, i.HTMLElement, registerResolver(t, i.Element, registerResolver(t, Ue, new e.InstanceProvider("ElementResolver", s))));
    return t;
};

function customElement(t) {
    return function(e, s) {
        s.addInitializer((function() {
            defineElement(t, this);
        }));
        return e;
    };
}

function useShadowDOM(t, s) {
    if (t === void 0) {
        return function(t, e) {
            e.addInitializer((function() {
                annotateElementMetadata(this, "shadowOptions", {
                    mode: "open"
                });
            }));
        };
    }
    if (!e.isFunction(t)) {
        return function(e, s) {
            s.addInitializer((function() {
                annotateElementMetadata(this, "shadowOptions", t);
            }));
        };
    }
    s.addInitializer((function() {
        annotateElementMetadata(this, "shadowOptions", {
            mode: "open"
        });
    }));
}

function containerless(t, e) {
    if (t === void 0) {
        return function(t, e) {
            e.addInitializer((function() {
                markContainerless(t);
            }));
        };
    }
    e.addInitializer((function() {
        markContainerless(t);
    }));
}

function markContainerless(t) {
    const e = _(is, t);
    if (e === void 0) {
        annotateElementMetadata(t, "containerless", true);
        return;
    }
    e.containerless = true;
}

const ts = new WeakMap;

class CustomElementDefinition {
    get type() {
        return K;
    }
    constructor(t, e, s, i, n, r, l, a, h, c, u, f, d, p, g, m, x, v) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
        this.capture = n;
        this.template = r;
        this.instructions = l;
        this.dependencies = a;
        this.injectable = h;
        this.needsCompile = c;
        this.surrogates = u;
        this.bindables = f;
        this.containerless = d;
        this.shadowOptions = p;
        this.hasSlots = g;
        this.enhance = m;
        this.watches = x;
        this.processContent = v;
    }
    static create(t, s = null) {
        if (s === null) {
            const i = t;
            if (e.isString(i)) {
                throw createMappedError(761, t);
            }
            const n = e.fromDefinitionOrDefault("name", i, ns);
            if (e.isFunction(i.Type)) {
                s = i.Type;
            } else {
                s = rs(e.pascalCase(n));
            }
            return new CustomElementDefinition(s, n, e.mergeArrays(i.aliases), e.fromDefinitionOrDefault("key", i, (() => getElementKeyFrom(n))), e.fromAnnotationOrDefinitionOrTypeOrDefault("capture", i, s, returnFalse), e.fromDefinitionOrDefault("template", i, returnNull), e.mergeArrays(i.instructions), e.mergeArrays(getElementAnnotation(s, "dependencies"), i.dependencies), e.fromDefinitionOrDefault("injectable", i, returnNull), e.fromDefinitionOrDefault("needsCompile", i, returnTrue), e.mergeArrays(i.surrogates), N.from(getElementAnnotation(s, "bindables"), i.bindables), e.fromAnnotationOrDefinitionOrTypeOrDefault("containerless", i, s, returnFalse), e.fromDefinitionOrDefault("shadowOptions", i, returnNull), e.fromDefinitionOrDefault("hasSlots", i, returnFalse), e.fromDefinitionOrDefault("enhance", i, returnFalse), e.fromDefinitionOrDefault("watches", i, returnEmptyArray), e.fromAnnotationOrTypeOrDefault("processContent", s, returnNull));
        }
        if (e.isString(t)) {
            return new CustomElementDefinition(s, t, e.mergeArrays(getElementAnnotation(s, "aliases"), s.aliases), getElementKeyFrom(t), e.fromAnnotationOrTypeOrDefault("capture", s, returnFalse), e.fromAnnotationOrTypeOrDefault("template", s, returnNull), e.mergeArrays(getElementAnnotation(s, "instructions"), s.instructions), e.mergeArrays(getElementAnnotation(s, "dependencies"), s.dependencies), e.fromAnnotationOrTypeOrDefault("injectable", s, returnNull), e.fromAnnotationOrTypeOrDefault("needsCompile", s, returnTrue), e.mergeArrays(getElementAnnotation(s, "surrogates"), s.surrogates), N.from(...N.getAll(s), getElementAnnotation(s, "bindables"), s.bindables), e.fromAnnotationOrTypeOrDefault("containerless", s, returnFalse), e.fromAnnotationOrTypeOrDefault("shadowOptions", s, returnNull), e.fromAnnotationOrTypeOrDefault("hasSlots", s, returnFalse), e.fromAnnotationOrTypeOrDefault("enhance", s, returnFalse), e.mergeArrays(at.getDefinitions(s), s.watches), e.fromAnnotationOrTypeOrDefault("processContent", s, returnNull));
        }
        const i = e.fromDefinitionOrDefault("name", t, ns);
        return new CustomElementDefinition(s, i, e.mergeArrays(getElementAnnotation(s, "aliases"), t.aliases, s.aliases), getElementKeyFrom(i), e.fromAnnotationOrDefinitionOrTypeOrDefault("capture", t, s, returnFalse), e.fromAnnotationOrDefinitionOrTypeOrDefault("template", t, s, returnNull), e.mergeArrays(getElementAnnotation(s, "instructions"), t.instructions, s.instructions), e.mergeArrays(getElementAnnotation(s, "dependencies"), t.dependencies, s.dependencies), e.fromAnnotationOrDefinitionOrTypeOrDefault("injectable", t, s, returnNull), e.fromAnnotationOrDefinitionOrTypeOrDefault("needsCompile", t, s, returnTrue), e.mergeArrays(getElementAnnotation(s, "surrogates"), t.surrogates, s.surrogates), N.from(...N.getAll(s), getElementAnnotation(s, "bindables"), s.bindables, t.bindables), e.fromAnnotationOrDefinitionOrTypeOrDefault("containerless", t, s, returnFalse), e.fromAnnotationOrDefinitionOrTypeOrDefault("shadowOptions", t, s, returnNull), e.fromAnnotationOrDefinitionOrTypeOrDefault("hasSlots", t, s, returnFalse), e.fromAnnotationOrDefinitionOrTypeOrDefault("enhance", t, s, returnFalse), e.mergeArrays(t.watches, at.getDefinitions(s), s.watches), e.fromAnnotationOrDefinitionOrTypeOrDefault("processContent", t, s, returnNull));
    }
    static getOrCreate(t) {
        if (t instanceof CustomElementDefinition) {
            return t;
        }
        if (ts.has(t)) {
            return ts.get(t);
        }
        const e = CustomElementDefinition.create(t);
        ts.set(t, e);
        V(e, e.Type, is);
        return e;
    }
    register(t, e) {
        const s = this.Type;
        const i = typeof e === "string" ? getElementKeyFrom(e) : this.key;
        const n = this.aliases;
        if (t.has(i, false)) {
            console.warn(createMappedError(153, this.name));
            return;
        }
        t.register(t.has(s, false) ? null : z(s, s), U(s, i), ...n.map((t => U(s, getElementKeyFrom(t)))));
    }
    toString() {
        return `au:ce:${this.name}`;
    }
}

const es = {
    name: undefined,
    searchParents: false,
    optional: false
};

const returnNull = () => null;

const returnFalse = () => false;

const returnTrue = () => true;

const returnEmptyArray = () => e.emptyArray;

const ss = "custom-element";

const is = /*@__PURE__*/ e.getResourceKeyFor(ss);

const getElementKeyFrom = t => `${is}:${t}`;

const ns = /*@__PURE__*/ (t => () => `unnamed-${++t}`)(0);

const annotateElementMetadata = (t, e, s) => {
    V(s, t, H(e));
};

const defineElement = (t, s) => {
    const i = CustomElementDefinition.create(t, s);
    const n = i.Type;
    V(i, n, is, e.resourceBaseName);
    return n;
};

const isElementType = t => e.isFunction(t) && (F(is, t) || t.$au?.type === ss);

const findElementControllerFor = (t, e = es) => {
    if (e.name === void 0 && e.searchParents !== true) {
        const s = getRef(t, is);
        if (s === null) {
            if (e.optional === true) {
                return null;
            }
            throw createMappedError(762, t);
        }
        return s;
    }
    if (e.name !== void 0) {
        if (e.searchParents !== true) {
            const s = getRef(t, is);
            if (s === null) {
                throw createMappedError(763, t);
            }
            if (s.is(e.name)) {
                return s;
            }
            return void 0;
        }
        let s = t;
        let i = false;
        while (s !== null) {
            const t = getRef(s, is);
            if (t !== null) {
                i = true;
                if (t.is(e.name)) {
                    return t;
                }
            }
            s = getEffectiveParentNode(s);
        }
        if (i) {
            return void 0;
        }
        throw createMappedError(764, t);
    }
    let s = t;
    while (s !== null) {
        const t = getRef(s, is);
        if (t !== null) {
            return t;
        }
        s = getEffectiveParentNode(s);
    }
    throw createMappedError(765, t);
};

const getElementAnnotation = (t, e) => _(H(e), t);

const getElementDefinition = t => {
    const e = _(is, t) ?? getDefinitionFromStaticAu(t, ss, CustomElementDefinition.create);
    if (e == null) {
        throw createMappedError(760, t);
    }
    return e;
};

const createElementInjectable = () => {
    const t = {
        $isInterface: false,
        register() {
            return {
                $isResolver: true,
                resolve(e, s) {
                    if (s.has(t, true)) {
                        return s.get(t);
                    } else {
                        return null;
                    }
                }
            };
        }
    };
    return t;
};

const rs = /*@__PURE__*/ function() {
    const t = {
        value: "",
        writable: false,
        enumerable: false,
        configurable: true
    };
    const e = {};
    return function(s, i = e) {
        const n = class Anonymous {};
        t.value = s;
        x(n, "name", t);
        if (i !== e) {
            d(n.prototype, i);
        }
        return n;
    };
}();

const os = /*@__PURE__*/ f({
    name: is,
    keyFrom: getElementKeyFrom,
    isType: isElementType,
    for: findElementControllerFor,
    define: defineElement,
    getDefinition: getElementDefinition,
    annotate: annotateElementMetadata,
    getAnnotation: getElementAnnotation,
    generateName: ns,
    createInjectable: createElementInjectable,
    generateType: rs,
    find(t, e) {
        const s = t.find(ss, e);
        return s == null ? null : _(is, s) ?? getDefinitionFromStaticAu(s, ss, CustomElementDefinition.create) ?? null;
    }
});

const ls = /*@__PURE__*/ H("processContent");

function processContent(t) {
    return t === void 0 ? function(t, e) {
        if (!e.static || e.kind !== "method") throw createMappedError(766, t);
        e.addInitializer((function() {
            V(t, this, ls);
        }));
    } : function(s, i) {
        i.addInitializer((function() {
            if (e.isString(t) || e.isSymbol(t)) {
                t = this[t];
            }
            if (!e.isFunction(t)) throw createMappedError(766, t);
            const s = _(is, this);
            if (s !== void 0) {
                s.processContent = t;
            } else {
                V(t, this, ls);
            }
        }));
        return s;
    };
}

function capture(t) {
    return function(s, i) {
        const n = e.isFunction(t) ? t : true;
        i.addInitializer((function() {
            annotateElementMetadata(this, "capture", n);
            if (isElementType(this)) {
                getElementDefinition(this).capture = n;
            }
        }));
    };
}

const as = /*@__PURE__*/ j("IAppRoot");

class AppRoot {
    get controller() {
        return this.L;
    }
    constructor(t, s, i, n = false) {
        this.config = t;
        this.container = s;
        this.he = void 0;
        this.ce = n;
        const r = this.host = t.host;
        i.prepare(this);
        registerResolver(s, Ge, new e.InstanceProvider("IEventTarget", r));
        registerHostNode(s, r, this.platform = this.ue(s, r));
        this.he = e.onResolve(this.fe("creating"), (() => {
            if (!t.allowActionlessForm !== false) {
                r.addEventListener("submit", (t => {
                    const e = t.target;
                    const s = !e.getAttribute("action");
                    if (e.tagName === "FORM" && s) {
                        t.preventDefault();
                    }
                }), false);
            }
            const i = n ? s : s.createChild();
            const l = t.component;
            let a;
            if (e.isFunction(l)) {
                a = i.invoke(l);
                G(l, a);
            } else {
                a = t.component;
            }
            const h = {
                hydrate: false,
                projections: null
            };
            const c = n ? CustomElementDefinition.create({
                name: ns(),
                template: this.host,
                enhance: true
            }) : void 0;
            const u = this.L = Controller.$el(i, a, r, h, c);
            u.hE(h, null);
            return e.onResolve(this.fe("hydrating"), (() => {
                u.hS();
                return e.onResolve(this.fe("hydrated"), (() => {
                    u.hC();
                    this.he = void 0;
                }));
            }));
        }));
    }
    activate() {
        return e.onResolve(this.he, (() => e.onResolve(this.fe("activating"), (() => e.onResolve(this.L.activate(this.L, null, void 0), (() => this.fe("activated")))))));
    }
    deactivate() {
        return e.onResolve(this.fe("deactivating"), (() => e.onResolve(this.L.deactivate(this.L, null), (() => this.fe("deactivated")))));
    }
    fe(t) {
        const s = this.container;
        const i = this.ce && !s.has(rt, false) ? [] : s.getAll(rt);
        return e.onResolveAll(...i.reduce(((e, s) => {
            if (s.slot === t) {
                e.push(s.run());
            }
            return e;
        }), []));
    }
    ue(t, e) {
        let s;
        if (!t.has(lt, false)) {
            if (e.ownerDocument.defaultView === null) {
                throw createMappedError(769);
            }
            s = new r.BrowserPlatform(e.ownerDocument.defaultView);
            t.register(G(lt, s));
        } else {
            s = t.get(lt);
        }
        return s;
    }
    dispose() {
        this.L?.dispose();
    }
}

const hs = /*@__PURE__*/ j("IAurelia");

class Aurelia {
    get isRunning() {
        return this.ir;
    }
    get isStarting() {
        return this.de;
    }
    get isStopping() {
        return this.pe;
    }
    get root() {
        if (this.ge == null) {
            if (this.next == null) {
                throw createMappedError(767);
            }
            return this.next;
        }
        return this.ge;
    }
    constructor(t = e.DI.createContainer()) {
        this.container = t;
        this.ir = false;
        this.de = false;
        this.pe = false;
        this.ge = void 0;
        this.next = void 0;
        this.me = void 0;
        this.xe = void 0;
        if (t.has(hs, true) || t.has(Aurelia, true)) {
            throw createMappedError(768);
        }
        registerResolver(t, hs, new e.InstanceProvider("IAurelia", this));
        registerResolver(t, Aurelia, new e.InstanceProvider("Aurelia", this));
        registerResolver(t, as, this.ve = new e.InstanceProvider("IAppRoot"));
    }
    register(...t) {
        this.container.register(...t);
        return this;
    }
    app(t) {
        this.next = new AppRoot(t, this.container, this.ve);
        return this;
    }
    enhance(t) {
        const s = t.container ?? this.container.createChild();
        const i = registerResolver(s, as, new e.InstanceProvider("IAppRoot"));
        const n = new AppRoot({
            host: t.host,
            component: t.component
        }, s, i, true);
        return e.onResolve(n.activate(), (() => n));
    }
    async waitForIdle() {
        const t = this.root.platform;
        await t.domQueue.yield();
        await t.taskQueue.yield();
    }
    start(t = this.next) {
        if (t == null) {
            throw createMappedError(770);
        }
        if (e.isPromise(this.me)) {
            return this.me;
        }
        return this.me = e.onResolve(this.stop(), (() => {
            Reflect.set(t.host, "$aurelia", this);
            this.ve.prepare(this.ge = t);
            this.de = true;
            return e.onResolve(t.activate(), (() => {
                this.ir = true;
                this.de = false;
                this.me = void 0;
                this.be(t, "au-started", t.host);
            }));
        }));
    }
    stop(t = false) {
        if (e.isPromise(this.xe)) {
            return this.xe;
        }
        if (this.ir === true) {
            const s = this.ge;
            this.ir = false;
            this.pe = true;
            return this.xe = e.onResolve(s.deactivate(), (() => {
                Reflect.deleteProperty(s.host, "$aurelia");
                if (t) {
                    s.dispose();
                }
                this.ge = void 0;
                this.ve.dispose();
                this.pe = false;
                this.be(s, "au-stopped", s.host);
            }));
        }
    }
    dispose() {
        if (this.ir || this.pe) {
            throw createMappedError(771);
        }
        this.container.dispose();
    }
    be(t, e, s) {
        const i = new t.platform.window.CustomEvent(e, {
            detail: this,
            bubbles: true,
            cancelable: true
        });
        s.dispatchEvent(i);
    }
}

const cs = /*@__PURE__*/ j("ISVGAnalyzer", (t => t.singleton(NoopSVGAnalyzer)));

const o = t => {
    const s = e.createLookup();
    t = e.isString(t) ? t.split(" ") : t;
    let i;
    for (i of t) {
        s[i] = true;
    }
    return s;
};

class NoopSVGAnalyzer {
    isStandardSvgAttribute(t, e) {
        return false;
    }
}

class SVGAnalyzer {
    static register(t) {
        t.register(z(this, this), U(this, cs));
    }
    constructor() {
        this.we = d(e.createLookup(), {
            a: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage target transform xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            altGlyph: o("class dx dy externalResourcesRequired format glyphRef id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            altglyph: e.createLookup(),
            altGlyphDef: o("id xml:base xml:lang xml:space"),
            altglyphdef: e.createLookup(),
            altGlyphItem: o("id xml:base xml:lang xml:space"),
            altglyphitem: e.createLookup(),
            animate: o("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateColor: o("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateMotion: o("accumulate additive begin by calcMode dur end externalResourcesRequired fill from id keyPoints keySplines keyTimes max min onbegin onend onload onrepeat origin path repeatCount repeatDur requiredExtensions requiredFeatures restart rotate systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateTransform: o("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to type values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            circle: o("class cx cy externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup r requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            clipPath: o("class clipPathUnits externalResourcesRequired id requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            "color-profile": o("id local name rendering-intent xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            cursor: o("externalResourcesRequired id requiredExtensions requiredFeatures systemLanguage x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            defs: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            desc: o("class id style xml:base xml:lang xml:space"),
            ellipse: o("class cx cy externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rx ry style systemLanguage transform xml:base xml:lang xml:space"),
            feBlend: o("class height id in in2 mode result style width x xml:base xml:lang xml:space y"),
            feColorMatrix: o("class height id in result style type values width x xml:base xml:lang xml:space y"),
            feComponentTransfer: o("class height id in result style width x xml:base xml:lang xml:space y"),
            feComposite: o("class height id in in2 k1 k2 k3 k4 operator result style width x xml:base xml:lang xml:space y"),
            feConvolveMatrix: o("bias class divisor edgeMode height id in kernelMatrix kernelUnitLength order preserveAlpha result style targetX targetY width x xml:base xml:lang xml:space y"),
            feDiffuseLighting: o("class diffuseConstant height id in kernelUnitLength result style surfaceScale width x xml:base xml:lang xml:space y"),
            feDisplacementMap: o("class height id in in2 result scale style width x xChannelSelector xml:base xml:lang xml:space y yChannelSelector"),
            feDistantLight: o("azimuth elevation id xml:base xml:lang xml:space"),
            feFlood: o("class height id result style width x xml:base xml:lang xml:space y"),
            feFuncA: o("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncB: o("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncG: o("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncR: o("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feGaussianBlur: o("class height id in result stdDeviation style width x xml:base xml:lang xml:space y"),
            feImage: o("class externalResourcesRequired height id preserveAspectRatio result style width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            feMerge: o("class height id result style width x xml:base xml:lang xml:space y"),
            feMergeNode: o("id xml:base xml:lang xml:space"),
            feMorphology: o("class height id in operator radius result style width x xml:base xml:lang xml:space y"),
            feOffset: o("class dx dy height id in result style width x xml:base xml:lang xml:space y"),
            fePointLight: o("id x xml:base xml:lang xml:space y z"),
            feSpecularLighting: o("class height id in kernelUnitLength result specularConstant specularExponent style surfaceScale width x xml:base xml:lang xml:space y"),
            feSpotLight: o("id limitingConeAngle pointsAtX pointsAtY pointsAtZ specularExponent x xml:base xml:lang xml:space y z"),
            feTile: o("class height id in result style width x xml:base xml:lang xml:space y"),
            feTurbulence: o("baseFrequency class height id numOctaves result seed stitchTiles style type width x xml:base xml:lang xml:space y"),
            filter: o("class externalResourcesRequired filterRes filterUnits height id primitiveUnits style width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            font: o("class externalResourcesRequired horiz-adv-x horiz-origin-x horiz-origin-y id style vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            "font-face": o("accent-height alphabetic ascent bbox cap-height descent font-family font-size font-stretch font-style font-variant font-weight hanging id ideographic mathematical overline-position overline-thickness panose-1 slope stemh stemv strikethrough-position strikethrough-thickness underline-position underline-thickness unicode-range units-per-em v-alphabetic v-hanging v-ideographic v-mathematical widths x-height xml:base xml:lang xml:space"),
            "font-face-format": o("id string xml:base xml:lang xml:space"),
            "font-face-name": o("id name xml:base xml:lang xml:space"),
            "font-face-src": o("id xml:base xml:lang xml:space"),
            "font-face-uri": o("id xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            foreignObject: o("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform width x xml:base xml:lang xml:space y"),
            g: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            glyph: o("arabic-form class d glyph-name horiz-adv-x id lang orientation style unicode vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            glyphRef: o("class dx dy format glyphRef id style x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            glyphref: e.createLookup(),
            hkern: o("g1 g2 id k u1 u2 xml:base xml:lang xml:space"),
            image: o("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage transform width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            line: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform x1 x2 xml:base xml:lang xml:space y1 y2"),
            linearGradient: o("class externalResourcesRequired gradientTransform gradientUnits id spreadMethod style x1 x2 xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space y1 y2"),
            marker: o("class externalResourcesRequired id markerHeight markerUnits markerWidth orient preserveAspectRatio refX refY style viewBox xml:base xml:lang xml:space"),
            mask: o("class externalResourcesRequired height id maskContentUnits maskUnits requiredExtensions requiredFeatures style systemLanguage width x xml:base xml:lang xml:space y"),
            metadata: o("id xml:base xml:lang xml:space"),
            "missing-glyph": o("class d horiz-adv-x id style vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            mpath: o("externalResourcesRequired id xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            path: o("class d externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup pathLength requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            pattern: o("class externalResourcesRequired height id patternContentUnits patternTransform patternUnits preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage viewBox width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            polygon: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup points requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            polyline: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup points requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            radialGradient: o("class cx cy externalResourcesRequired fx fy gradientTransform gradientUnits id r spreadMethod style xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space"),
            rect: o("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rx ry style systemLanguage transform width x xml:base xml:lang xml:space y"),
            script: o("externalResourcesRequired id type xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            set: o("attributeName attributeType begin dur end externalResourcesRequired fill id max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            stop: o("class id offset style xml:base xml:lang xml:space"),
            style: o("id media title type xml:base xml:lang xml:space"),
            svg: o("baseProfile class contentScriptType contentStyleType externalResourcesRequired height id onabort onactivate onclick onerror onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup onresize onscroll onunload onzoom preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage version viewBox width x xml:base xml:lang xml:space y zoomAndPan"),
            switch: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            symbol: o("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup preserveAspectRatio style viewBox xml:base xml:lang xml:space"),
            text: o("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength transform x xml:base xml:lang xml:space y"),
            textPath: o("class externalResourcesRequired id lengthAdjust method onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures spacing startOffset style systemLanguage textLength xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space"),
            title: o("class id style xml:base xml:lang xml:space"),
            tref: o("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength x xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space y"),
            tspan: o("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength x xml:base xml:lang xml:space y"),
            use: o("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            view: o("externalResourcesRequired id preserveAspectRatio viewBox viewTarget xml:base xml:lang xml:space zoomAndPan"),
            vkern: o("g1 g2 id k u1 u2 xml:base xml:lang xml:space")
        });
        this.ye = o("a altGlyph animate animateColor circle clipPath defs ellipse feBlend feColorMatrix feComponentTransfer feComposite feConvolveMatrix feDiffuseLighting feDisplacementMap feFlood feGaussianBlur feImage feMerge feMorphology feOffset feSpecularLighting feTile feTurbulence filter font foreignObject g glyph glyphRef image line linearGradient marker mask missing-glyph path pattern polygon polyline radialGradient rect stop svg switch symbol text textPath tref tspan use");
        this.ke = o("alignment-baseline baseline-shift clip-path clip-rule clip color-interpolation-filters color-interpolation color-profile color-rendering color cursor direction display dominant-baseline enable-background fill-opacity fill-rule fill filter flood-color flood-opacity font-family font-size-adjust font-size font-stretch font-style font-variant font-weight glyph-orientation-horizontal glyph-orientation-vertical image-rendering kerning letter-spacing lighting-color marker-end marker-mid marker-start mask opacity overflow pointer-events shape-rendering stop-color stop-opacity stroke-dasharray stroke-dashoffset stroke-linecap stroke-linejoin stroke-miterlimit stroke-opacity stroke-width stroke text-anchor text-decoration text-rendering unicode-bidi visibility word-spacing writing-mode");
        const t = e.resolve(lt);
        this.SVGElement = t.globalThis.SVGElement;
        const s = t.document.createElement("div");
        s.innerHTML = "<svg><altGlyph /></svg>";
        if (s.firstElementChild.nodeName === "altglyph") {
            const t = this.we;
            let e = t.altGlyph;
            t.altGlyph = t.altglyph;
            t.altglyph = e;
            e = t.altGlyphDef;
            t.altGlyphDef = t.altglyphdef;
            t.altglyphdef = e;
            e = t.altGlyphItem;
            t.altGlyphItem = t.altglyphitem;
            t.altglyphitem = e;
            e = t.glyphRef;
            t.glyphRef = t.glyphref;
            t.glyphref = e;
        }
    }
    isStandardSvgAttribute(t, e) {
        if (!(t instanceof this.SVGElement)) {
            return false;
        }
        return this.ye[t.nodeName] === true && this.ke[e] === true || this.we[t.nodeName]?.[e] === true;
    }
}

class AttrMapper {
    constructor() {
        this.fns = [];
        this.Ce = e.createLookup();
        this.Be = e.createLookup();
        this.svg = e.resolve(cs);
        this.useMapping({
            LABEL: {
                for: "htmlFor"
            },
            IMG: {
                usemap: "useMap"
            },
            INPUT: {
                maxlength: "maxLength",
                minlength: "minLength",
                formaction: "formAction",
                formenctype: "formEncType",
                formmethod: "formMethod",
                formnovalidate: "formNoValidate",
                formtarget: "formTarget",
                inputmode: "inputMode"
            },
            TEXTAREA: {
                maxlength: "maxLength"
            },
            TD: {
                rowspan: "rowSpan",
                colspan: "colSpan"
            },
            TH: {
                rowspan: "rowSpan",
                colspan: "colSpan"
            }
        });
        this.useGlobalMapping({
            accesskey: "accessKey",
            contenteditable: "contentEditable",
            tabindex: "tabIndex",
            textcontent: "textContent",
            innerhtml: "innerHTML",
            scrolltop: "scrollTop",
            scrollleft: "scrollLeft",
            readonly: "readOnly"
        });
    }
    useMapping(t) {
        let s;
        let i;
        let n;
        let r;
        for (n in t) {
            s = t[n];
            i = this.Ce[n] ??= e.createLookup();
            for (r in s) {
                if (i[r] !== void 0) {
                    throw createError(r, n);
                }
                i[r] = s[r];
            }
        }
    }
    useGlobalMapping(t) {
        const e = this.Be;
        for (const s in t) {
            if (e[s] !== void 0) {
                throw createError(s, "*");
            }
            e[s] = t[s];
        }
    }
    useTwoWay(t) {
        this.fns.push(t);
    }
    isTwoWay(t, e) {
        return shouldDefaultToTwoWay(t, e) || this.fns.length > 0 && this.fns.some((s => s(t, e)));
    }
    map(t, e) {
        return this.Ce[t.nodeName]?.[e] ?? this.Be[e] ?? (isDataAttribute(t, e, this.svg) ? e : null);
    }
}

AttrMapper.register = e.createImplementationRegister(i.IAttrMapper);

function shouldDefaultToTwoWay(t, e) {
    switch (t.nodeName) {
      case "INPUT":
        switch (t.type) {
          case "checkbox":
          case "radio":
            return e === "checked";

          default:
            return e === "value" || e === "files" || e === "value-as-number" || e === "value-as-date";
        }

      case "TEXTAREA":
      case "SELECT":
        return e === "value";

      default:
        switch (e) {
          case "textcontent":
          case "innerhtml":
            return t.hasAttribute("contenteditable");

          case "scrolltop":
          case "scrollleft":
            return true;

          default:
            return false;
        }
    }
}

function createError(t, e) {
    return createMappedError(719, t, e);
}

const us = {
    register(t) {
        t.register(i.TemplateCompiler, AttrMapper, ResourceResolver);
    }
};

class BindablesInfo {
    constructor(t, e, s) {
        this.attrs = t;
        this.bindables = e;
        this.primary = s;
    }
}

class ResourceResolver {
    constructor() {
        this.Se = new WeakMap;
        this.Ae = new WeakMap;
    }
    el(t, e) {
        let s = this.Se.get(t);
        if (s == null) {
            this.Se.set(t, s = new RecordCache);
        }
        return e in s.Ee ? s.Ee[e] : s.Ee[e] = os.find(t, e);
    }
    attr(t, e) {
        let s = this.Se.get(t);
        if (s == null) {
            this.Se.set(t, s = new RecordCache);
        }
        return e in s.Re ? s.Re[e] : s.Re[e] = ut.find(t, e);
    }
    bindables(t) {
        let s = this.Ae.get(t);
        if (s == null) {
            const i = t.bindables;
            const n = e.createLookup();
            let r;
            let l;
            let a = false;
            let h;
            let c;
            for (l in i) {
                r = i[l];
                c = r.attribute;
                if (r.primary === true) {
                    if (a) {
                        throw createMappedError(714, t);
                    }
                    a = true;
                    h = r;
                } else if (!a && h == null) {
                    h = r;
                }
                n[c] = BindableDefinition.create(l, r);
            }
            if (r == null && t.type === "custom-attribute") {
                h = n.value = BindableDefinition.create("value", {
                    mode: t.defaultBindingMode ?? M
                });
            }
            this.Ae.set(t, s = new BindablesInfo(n, i, h ?? null));
        }
        return s;
    }
}

ResourceResolver.register = e.createImplementationRegister(i.IResourceResolver);

class RecordCache {
    constructor() {
        this.Ee = e.createLookup();
        this.Re = e.createLookup();
    }
}

const fs = e.createLookup();

class AttributeNSAccessor {
    static forNs(t) {
        return fs[t] ??= new AttributeNSAccessor(t);
    }
    constructor(t) {
        this.ns = t;
        this.type = S | A;
    }
    getValue(t, e) {
        return t.getAttributeNS(this.ns, e);
    }
    setValue(t, e, s) {
        if (t == null) {
            e.removeAttributeNS(this.ns, s);
        } else {
            e.setAttributeNS(this.ns, s, t);
        }
    }
}

(() => {
    mixinNoopSubscribable(AttributeNSAccessor);
})();

class DataAttributeAccessor {
    constructor() {
        this.type = S | A;
    }
    getValue(t, e) {
        return t.getAttribute(e);
    }
    setValue(t, e, s) {
        if (t == null) {
            e.removeAttribute(s);
        } else {
            e.setAttribute(s, t);
        }
    }
}

(() => {
    mixinNoopSubscribable(DataAttributeAccessor);
})();

const ds = /*@__PURE__*/ new DataAttributeAccessor;

class SelectValueObserver {
    static Te(t) {
        const e = [];
        if (t.length === 0) {
            return e;
        }
        const s = t.length;
        let i = 0;
        let n;
        while (s > i) {
            n = t[i];
            if (n.selected) {
                e[e.length] = u.call(n, "model") ? n.model : n.value;
            }
            ++i;
        }
        return e;
    }
    static Le(t, e) {
        return t === e;
    }
    constructor(t, e, s, i) {
        this.type = S | B | A;
        this.v = void 0;
        this.ov = void 0;
        this.Me = false;
        this.De = void 0;
        this.qe = void 0;
        this.iO = false;
        this.nt = false;
        this.it = t;
        this.oL = i;
        this.cf = s;
    }
    getValue() {
        return this.iO ? this.v : this.it.multiple ? SelectValueObserver.Te(this.it.options) : this.it.value;
    }
    setValue(t) {
        this.ov = this.v;
        this.v = t;
        this.Me = t !== this.ov;
        this.Ie(t instanceof Array ? t : null);
        this.ct();
    }
    ct() {
        if (this.Me) {
            this.Me = false;
            this.syncOptions();
        }
    }
    handleCollectionChange() {
        this.syncOptions();
    }
    syncOptions() {
        const t = this.v;
        const s = this.it;
        const i = e.isArray(t);
        const n = s.matcher ?? SelectValueObserver.Le;
        const r = s.options;
        let l = r.length;
        while (l-- > 0) {
            const e = r[l];
            const s = u.call(e, "model") ? e.model : e.value;
            if (i) {
                e.selected = t.findIndex((t => !!n(s, t))) !== -1;
                continue;
            }
            e.selected = !!n(s, t);
        }
    }
    syncValue() {
        const t = this.it;
        const e = t.options;
        const s = e.length;
        const i = this.v;
        let n = 0;
        if (t.multiple) {
            if (!(i instanceof Array)) {
                return true;
            }
            let r;
            const l = t.matcher || SelectValueObserver.Le;
            const a = [];
            while (n < s) {
                r = e[n];
                if (r.selected) {
                    a.push(u.call(r, "model") ? r.model : r.value);
                }
                ++n;
            }
            let h;
            n = 0;
            while (n < i.length) {
                h = i[n];
                if (a.findIndex((t => !!l(h, t))) === -1) {
                    i.splice(n, 1);
                } else {
                    ++n;
                }
            }
            n = 0;
            while (n < a.length) {
                h = a[n];
                if (i.findIndex((t => !!l(h, t))) === -1) {
                    i.push(h);
                }
                ++n;
            }
            return false;
        }
        let r = null;
        let l;
        while (n < s) {
            l = e[n];
            if (l.selected) {
                r = u.call(l, "model") ? l.model : l.value;
                break;
            }
            ++n;
        }
        this.ov = this.v;
        this.v = r;
        return true;
    }
    rt() {
        (this.qe = createMutationObserver(this.it, this.Pe.bind(this))).observe(this.it, {
            childList: true,
            subtree: true,
            characterData: true
        });
        this.Ie(this.v instanceof Array ? this.v : null);
        this.iO = true;
    }
    ot() {
        this.qe.disconnect();
        this.De?.unsubscribe(this);
        this.qe = this.De = void 0;
        this.iO = false;
    }
    Ie(t) {
        this.De?.unsubscribe(this);
        this.De = void 0;
        if (t != null) {
            if (!this.it.multiple) {
                throw createMappedError(654);
            }
            (this.De = this.oL.getArrayObserver(t)).subscribe(this);
        }
    }
    handleEvent() {
        const t = this.syncValue();
        if (t) {
            this._e();
        }
    }
    Pe(t) {
        this.syncOptions();
        const e = this.syncValue();
        if (e) {
            this._e();
        }
    }
    _e() {
        const t = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, t);
    }
}

(() => {
    mixinNodeObserverUseConfig(SelectValueObserver);
    s.subscriberCollection(SelectValueObserver, null);
})();

const ps = "--";

class StyleAttributeAccessor {
    constructor(t) {
        this.obj = t;
        this.type = S | A;
        this.v = "";
        this.ov = "";
        this.styles = {};
        this.version = 0;
        this.Me = false;
    }
    getValue() {
        return this.obj.style.cssText;
    }
    setValue(t) {
        this.v = t;
        this.Me = t !== this.ov;
        this.ct();
    }
    Fe(t) {
        const e = [];
        const s = /url\([^)]+$/;
        let i = 0;
        let n = "";
        let r;
        let l;
        let a;
        let h;
        while (i < t.length) {
            r = t.indexOf(";", i);
            if (r === -1) {
                r = t.length;
            }
            n += t.substring(i, r);
            i = r + 1;
            if (s.test(n)) {
                n += ";";
                continue;
            }
            l = n.indexOf(":");
            a = n.substring(0, l).trim();
            h = n.substring(l + 1).trim();
            e.push([ a, h ]);
            n = "";
        }
        return e;
    }
    Ve(t) {
        let s;
        let i;
        const n = [];
        for (i in t) {
            s = t[i];
            if (s == null) {
                continue;
            }
            if (e.isString(s)) {
                if (i.startsWith(ps)) {
                    n.push([ i, s ]);
                    continue;
                }
                n.push([ e.kebabCase(i), s ]);
                continue;
            }
            n.push(...this.Oe(s));
        }
        return n;
    }
    He(t) {
        const s = t.length;
        if (s > 0) {
            const e = [];
            let i = 0;
            for (;s > i; ++i) {
                e.push(...this.Oe(t[i]));
            }
            return e;
        }
        return e.emptyArray;
    }
    Oe(t) {
        if (e.isString(t)) {
            return this.Fe(t);
        }
        if (t instanceof Array) {
            return this.He(t);
        }
        if (t instanceof Object) {
            return this.Ve(t);
        }
        return e.emptyArray;
    }
    ct() {
        if (this.Me) {
            this.Me = false;
            const t = this.v;
            const e = this.styles;
            const s = this.Oe(t);
            let i;
            let n = this.version;
            this.ov = t;
            let r;
            let l;
            let a;
            let h = 0;
            const c = s.length;
            for (;h < c; ++h) {
                r = s[h];
                l = r[0];
                a = r[1];
                this.setProperty(l, a);
                e[l] = n;
            }
            this.styles = e;
            this.version += 1;
            if (n === 0) {
                return;
            }
            n -= 1;
            for (i in e) {
                if (!u.call(e, i) || e[i] !== n) {
                    continue;
                }
                this.obj.style.removeProperty(i);
            }
        }
    }
    setProperty(t, s) {
        let i = "";
        if (s != null && e.isFunction(s.indexOf) && s.includes("!important")) {
            i = "important";
            s = s.replace("!important", "");
        }
        this.obj.style.setProperty(t, s, i);
    }
    bind() {
        this.v = this.ov = this.obj.style.cssText;
    }
}

(() => {
    mixinNoopSubscribable(StyleAttributeAccessor);
})();

class ValueAttributeObserver {
    constructor(t, e, s) {
        this.type = S | B | A;
        this.v = "";
        this.ov = "";
        this.Me = false;
        this.nt = false;
        this.it = t;
        this.k = e;
        this.cf = s;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        if (e.areEqual(t, this.v)) {
            return;
        }
        this.ov = this.v;
        this.v = t;
        this.Me = true;
        if (!this.cf.readonly) {
            this.ct();
        }
    }
    ct() {
        if (this.Me) {
            this.Me = false;
            this.it[this.k] = this.v ?? this.cf.default;
            this._e();
        }
    }
    handleEvent() {
        this.ov = this.v;
        this.v = this.it[this.k];
        if (this.ov !== this.v) {
            this.Me = false;
            this._e();
        }
    }
    rt() {
        this.v = this.ov = this.it[this.k];
    }
    _e() {
        const t = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, t);
    }
}

(() => {
    mixinNodeObserverUseConfig(ValueAttributeObserver);
    s.subscriberCollection(ValueAttributeObserver, null);
})();

const gs = (() => {
    const t = "http://www.w3.org/1999/xlink";
    const s = "http://www.w3.org/XML/1998/namespace";
    const i = "http://www.w3.org/2000/xmlns/";
    return d(e.createLookup(), {
        "xlink:actuate": [ "actuate", t ],
        "xlink:arcrole": [ "arcrole", t ],
        "xlink:href": [ "href", t ],
        "xlink:role": [ "role", t ],
        "xlink:show": [ "show", t ],
        "xlink:title": [ "title", t ],
        "xlink:type": [ "type", t ],
        "xml:lang": [ "lang", s ],
        "xml:space": [ "space", s ],
        xmlns: [ "xmlns", i ],
        "xmlns:xlink": [ "xlink", i ]
    });
})();

const ms = new s.PropertyAccessor;

ms.type = S | A;

class NodeObserverLocator {
    constructor() {
        this.allowDirtyCheck = true;
        this.$e = e.createLookup();
        this.Ne = e.createLookup();
        this.We = e.createLookup();
        this.je = e.createLookup();
        this.ze = e.resolve(e.IServiceLocator);
        this.p = e.resolve(lt);
        this.Ue = e.resolve(s.IDirtyChecker);
        this.svg = e.resolve(cs);
        const t = [ "change", "input" ];
        const i = {
            events: t,
            default: ""
        };
        this.useConfig({
            INPUT: {
                value: i,
                valueAsNumber: {
                    events: t,
                    default: 0
                },
                checked: {
                    type: CheckedObserver,
                    events: t
                },
                files: {
                    events: t,
                    readonly: true
                }
            },
            SELECT: {
                value: {
                    type: SelectValueObserver,
                    events: [ "change" ],
                    default: ""
                }
            },
            TEXTAREA: {
                value: i
            }
        });
        const n = {
            events: [ "change", "input", "blur", "keyup", "paste" ],
            default: ""
        };
        const r = {
            events: [ "scroll" ],
            default: 0
        };
        this.useConfigGlobal({
            scrollTop: r,
            scrollLeft: r,
            textContent: n,
            innerHTML: n
        });
        this.overrideAccessorGlobal("css", "style", "class");
        this.overrideAccessor({
            INPUT: [ "value", "checked", "model" ],
            SELECT: [ "value" ],
            TEXTAREA: [ "value" ]
        });
    }
    handles(t, e) {
        return t instanceof this.p.Node;
    }
    useConfig(t, s, i) {
        const n = this.$e;
        let r;
        if (e.isString(t)) {
            r = n[t] ??= e.createLookup();
            if (r[s] == null) {
                r[s] = i;
            } else {
                throwMappingExisted(t, s);
            }
        } else {
            for (const i in t) {
                r = n[i] ??= e.createLookup();
                const l = t[i];
                for (s in l) {
                    if (r[s] == null) {
                        r[s] = l[s];
                    } else {
                        throwMappingExisted(i, s);
                    }
                }
            }
        }
    }
    useConfigGlobal(t, e) {
        const s = this.Ne;
        if (typeof t === "object") {
            for (const e in t) {
                if (s[e] == null) {
                    s[e] = t[e];
                } else {
                    throwMappingExisted("*", e);
                }
            }
        } else {
            if (s[t] == null) {
                s[t] = e;
            } else {
                throwMappingExisted("*", t);
            }
        }
    }
    getAccessor(t, s, i) {
        if (s in this.je || s in (this.We[t.tagName] ?? e.emptyObject)) {
            return this.getObserver(t, s, i);
        }
        switch (s) {
          case "src":
          case "href":
          case "role":
          case "minLength":
          case "maxLength":
          case "placeholder":
          case "size":
          case "pattern":
          case "title":
          case "popovertarget":
          case "popovertargetaction":
            return ds;

          default:
            {
                const e = gs[s];
                if (e !== undefined) {
                    return AttributeNSAccessor.forNs(e[1]);
                }
                if (isDataAttribute(t, s, this.svg)) {
                    return ds;
                }
                return ms;
            }
        }
    }
    overrideAccessor(t, s) {
        let i;
        if (e.isString(t)) {
            i = this.We[t] ??= e.createLookup();
            i[s] = true;
        } else {
            for (const s in t) {
                for (const n of t[s]) {
                    i = this.We[s] ??= e.createLookup();
                    i[n] = true;
                }
            }
        }
    }
    overrideAccessorGlobal(...t) {
        for (const e of t) {
            this.je[e] = true;
        }
    }
    getNodeObserverConfig(t, e) {
        return this.$e[t.tagName]?.[e] ?? this.Ne[e];
    }
    getNodeObserver(t, e, i) {
        const n = this.$e[t.tagName]?.[e] ?? this.Ne[e];
        let r;
        if (n != null) {
            r = new (n.type ?? ValueAttributeObserver)(t, e, n, i, this.ze);
            if (!r.doNotCache) {
                s.getObserverLookup(t)[e] = r;
            }
            return r;
        }
        return null;
    }
    getObserver(t, e, i) {
        switch (e) {
          case "class":
            return new ClassAttributeAccessor(t);

          case "css":
          case "style":
            return new StyleAttributeAccessor(t);
        }
        const n = this.getNodeObserver(t, e, i);
        if (n != null) {
            return n;
        }
        const r = gs[e];
        if (r !== undefined) {
            return AttributeNSAccessor.forNs(r[1]);
        }
        if (isDataAttribute(t, e, this.svg)) {
            return ds;
        }
        if (e in t.constructor.prototype) {
            if (this.allowDirtyCheck) {
                return this.Ue.createProperty(t, e);
            }
            throw createMappedError(652, e);
        } else {
            return new s.SetterObserver(t, e);
        }
    }
}

NodeObserverLocator.register = e.createImplementationRegister(s.INodeObserverLocator);

function getCollectionObserver(t, e) {
    if (t instanceof Array) {
        return e.getArrayObserver(t);
    }
    if (t instanceof Map) {
        return e.getMapObserver(t);
    }
    if (t instanceof Set) {
        return e.getSetObserver(t);
    }
}

function throwMappingExisted(t, e) {
    throw createMappedError(653, t, e);
}

function defaultMatcher(t, e) {
    return t === e;
}

class CheckedObserver {
    constructor(t, e, s, i) {
        this.type = S | B | A;
        this.v = void 0;
        this.ov = void 0;
        this.Ge = void 0;
        this.Ke = void 0;
        this.nt = false;
        this.it = t;
        this.oL = i;
        this.cf = s;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        const e = this.v;
        if (t === e) {
            return;
        }
        this.v = t;
        this.ov = e;
        this.Xe();
        this.Qe();
        this._e();
    }
    handleCollectionChange() {
        this.Qe();
    }
    handleChange(t, e) {
        this.Qe();
    }
    Qe() {
        const t = this.v;
        const s = this.it;
        const i = u.call(s, "model") ? s.model : s.value;
        const n = s.type === "radio";
        const r = s.matcher !== void 0 ? s.matcher : defaultMatcher;
        if (n) {
            s.checked = !!r(t, i);
        } else if (t === true) {
            s.checked = true;
        } else {
            let n = false;
            if (e.isArray(t)) {
                n = t.findIndex((t => !!r(t, i))) !== -1;
            } else if (t instanceof Set) {
                for (const e of t) {
                    if (r(e, i)) {
                        n = true;
                        break;
                    }
                }
            } else if (t instanceof Map) {
                for (const e of t) {
                    const t = e[0];
                    const s = e[1];
                    if (r(t, i) && s === true) {
                        n = true;
                        break;
                    }
                }
            }
            s.checked = n;
        }
    }
    handleEvent() {
        let t = this.ov = this.v;
        const s = this.it;
        const i = u.call(s, "model") ? s.model : s.value;
        const n = s.checked;
        const r = s.matcher !== void 0 ? s.matcher : defaultMatcher;
        if (s.type === "checkbox") {
            if (e.isArray(t)) {
                const e = t.findIndex((t => !!r(t, i)));
                if (n && e === -1) {
                    t.push(i);
                } else if (!n && e !== -1) {
                    t.splice(e, 1);
                }
                return;
            } else if (t instanceof Set) {
                const e = {};
                let s = e;
                for (const e of t) {
                    if (r(e, i) === true) {
                        s = e;
                        break;
                    }
                }
                if (n && s === e) {
                    t.add(i);
                } else if (!n && s !== e) {
                    t.delete(s);
                }
                return;
            } else if (t instanceof Map) {
                let e;
                for (const s of t) {
                    const t = s[0];
                    if (r(t, i) === true) {
                        e = t;
                        break;
                    }
                }
                t.set(e, n);
                return;
            }
            t = n;
        } else if (n) {
            t = i;
        } else {
            return;
        }
        this.v = t;
        this._e();
    }
    rt() {
        this.Xe();
    }
    ot() {
        this.Ge?.unsubscribe(this);
        this.Ke?.unsubscribe(this);
        this.Ge = this.Ke = void 0;
    }
    _e() {
        xs = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, xs);
    }
    Xe() {
        const t = this.it;
        (this.Ke ??= t.$observers?.model ?? t.$observers?.value)?.subscribe(this);
        this.Ge?.unsubscribe(this);
        this.Ge = void 0;
        if (t.type === "checkbox") {
            (this.Ge = getCollectionObserver(this.v, this.oL))?.subscribe(this);
        }
    }
}

(() => {
    mixinNodeObserverUseConfig(CheckedObserver);
    s.subscriberCollection(CheckedObserver, null);
})();

let xs = void 0;

class AttrBindingBehavior {
    bind(t, e) {
        if (!(e instanceof PropertyBinding)) {
            throw createMappedError(9994, e);
        }
        e.useTargetObserver(ds);
    }
}

AttrBindingBehavior.$au = {
    type: Q,
    name: "attr"
};

class SelfBindingBehavior {
    bind(t, e) {
        if (!("handleEvent" in e)) {
            throw createMappedError(801);
        }
        e.self = true;
    }
    unbind(t, e) {
        e.self = false;
    }
}

SelfBindingBehavior.$au = {
    type: Q,
    name: "self"
};

class UpdateTriggerBindingBehavior {
    constructor() {
        this.oL = e.resolve(s.IObserverLocator);
        this.Ye = e.resolve(s.INodeObserverLocator);
    }
    bind(t, e, ...s) {
        if (!(this.Ye instanceof NodeObserverLocator)) {
            throw createMappedError(9993);
        }
        if (s.length === 0) {
            throw createMappedError(802);
        }
        if (!(e instanceof PropertyBinding) || !(e.mode & I)) {
            throw createMappedError(803);
        }
        const i = this.Ye.getNodeObserverConfig(e.target, e.targetProperty);
        if (i == null) {
            throw createMappedError(9992, e);
        }
        const n = this.Ye.getNodeObserver(e.target, e.targetProperty, this.oL);
        n.useConfig({
            readonly: i.readonly,
            default: i.default,
            events: s
        });
        e.useTargetObserver(n);
    }
}

UpdateTriggerBindingBehavior.$au = {
    type: Q,
    name: "updateTrigger"
};

class If {
    constructor() {
        this.elseFactory = void 0;
        this.elseView = void 0;
        this.ifView = void 0;
        this.view = void 0;
        this.value = false;
        this.cache = true;
        this.pending = void 0;
        this.Ze = false;
        this.Je = 0;
        this.ts = e.resolve(Mt);
        this.l = e.resolve(Ke);
    }
    attaching(t, e) {
        return this.es(this.value);
    }
    detaching(t, s) {
        this.Ze = true;
        return e.onResolve(this.pending, (() => {
            this.Ze = false;
            this.pending = void 0;
            void this.view?.deactivate(t, this.$controller);
        }));
    }
    valueChanged(t, e) {
        if (!this.$controller.isActive) return;
        t = !!t;
        e = !!e;
        if (t !== e) return this.es(t);
    }
    es(t) {
        const s = this.view;
        const i = this.$controller;
        const n = this.Je++;
        const isCurrent = () => !this.Ze && this.Je === n + 1;
        let r;
        return e.onResolve(this.pending, (() => this.pending = e.onResolve(s?.deactivate(s, i), (() => {
            if (!isCurrent()) {
                return;
            }
            if (t) {
                r = this.view = this.ifView = this.cache && this.ifView != null ? this.ifView : this.ts.create();
            } else {
                r = this.view = this.elseView = this.cache && this.elseView != null ? this.elseView : this.elseFactory?.create();
            }
            if (r == null) {
                return;
            }
            r.setLocation(this.l);
            return e.onResolve(r.activate(r, i, i.scope), (() => {
                if (isCurrent()) {
                    this.pending = void 0;
                }
            }));
        }))));
    }
    dispose() {
        this.ifView?.dispose();
        this.elseView?.dispose();
        this.ifView = this.elseView = this.view = void 0;
    }
    accept(t) {
        if (this.view?.accept(t) === true) {
            return true;
        }
    }
}

If.$au = {
    type: ht,
    name: "if",
    isTemplateController: true,
    bindables: {
        value: true,
        cache: {
            set: t => t === "" || !!t && t !== "false"
        }
    }
};

class Else {
    constructor() {
        this.f = e.resolve(Mt);
    }
    link(t, e, s, i) {
        const n = t.children;
        const r = n[n.length - 1];
        if (r instanceof If) {
            r.elseFactory = this.f;
        } else if (r.viewModel instanceof If) {
            r.viewModel.elseFactory = this.f;
        } else {
            throw createMappedError(810);
        }
    }
}

Else.$au = {
    type: "custom-attribute",
    name: "else",
    isTemplateController: true
};

function dispose(t) {
    t.dispose();
}

const vs = [ "BindingBehavior", "ValueConverter" ];

class Repeat {
    constructor() {
        this.views = [];
        this.ss = [];
        this.key = null;
        this.rs = new Map;
        this.os = new Map;
        this.ls = void 0;
        this.cs = false;
        this.us = false;
        this.ds = null;
        this.ps = void 0;
        this.gs = false;
        this.l = e.resolve(Ke);
        this.xs = e.resolve($e);
        this.f = e.resolve(Mt);
        this.bs = e.resolve(ks);
        const s = e.resolve(i.IInstruction);
        const n = s.props[0].props[0];
        if (n !== void 0) {
            const {to: s, value: i, command: r} = n;
            if (s === "key") {
                if (r === null) {
                    this.key = i;
                } else if (r === "bind") {
                    this.key = e.resolve(t.IExpressionParser).parse(i, y);
                } else {
                    throw createMappedError(775, r);
                }
            } else {
                throw createMappedError(776, s);
            }
        }
    }
    binding(t, e) {
        const s = this.xs.bindings;
        const i = s.length;
        let n = void 0;
        let r;
        let l = 0;
        for (;i > l; ++l) {
            n = s[l];
            if (n.target === this && n.targetProperty === "items") {
                r = this.forOf = n.ast;
                this.ws = n;
                let t = r.iterable;
                while (t != null && vs.includes(t.$kind)) {
                    t = t.expression;
                    this.cs = true;
                }
                this.ds = t;
                break;
            }
        }
        this.ys();
        const a = r.declaration;
        if (!(this.gs = a.$kind === "ArrayDestructuring" || a.$kind === "ObjectDestructuring")) {
            this.local = R(a, this.$controller.scope, n, null);
        }
    }
    attaching(t, s) {
        this.ks();
        return this.Cs(t, this.ps ?? e.emptyArray);
    }
    detaching(t, e) {
        this.ys();
        return this.Bs(t);
    }
    unbinding(t, e) {
        this.os.clear();
        this.rs.clear();
    }
    itemsChanged() {
        if (!this.$controller.isActive) {
            return;
        }
        this.ys();
        this.ks();
        this.Ss(this.items, void 0);
    }
    handleCollectionChange(t, e) {
        const s = this.$controller;
        if (!s.isActive) {
            return;
        }
        if (this.cs) {
            if (this.us) {
                return;
            }
            this.us = true;
            this.items = R(this.forOf.iterable, s.scope, this.ws, null);
            this.us = false;
            return;
        }
        this.ks();
        this.Ss(t, e);
    }
    Ss(t, i) {
        const n = this.views;
        this.ss = n.slice();
        const r = n.length;
        const l = this.key;
        const a = l !== null;
        if (a || i === void 0) {
            const t = this.local;
            const e = this.ps;
            const h = e.length;
            const c = this.forOf;
            const u = c.declaration;
            const f = this.ws;
            const d = this.gs;
            i = s.createIndexMap(h);
            let p = 0;
            if (r === 0) {
                for (;p < h; ++p) {
                    i[p] = -2;
                }
            } else if (h === 0) {
                if (d) {
                    for (p = 0; p < r; ++p) {
                        i.deletedIndices.push(p);
                        i.deletedItems.push(R(u, n[p].scope, f, null));
                    }
                } else {
                    for (p = 0; p < r; ++p) {
                        i.deletedIndices.push(p);
                        i.deletedItems.push(n[p].scope.bindingContext[t]);
                    }
                }
            } else {
                const s = Array(r);
                if (d) {
                    for (p = 0; p < r; ++p) {
                        s[p] = R(u, n[p].scope, f, null);
                    }
                } else {
                    for (p = 0; p < r; ++p) {
                        s[p] = n[p].scope.bindingContext[t];
                    }
                }
                let g;
                let m;
                let x;
                let v;
                let b = 0;
                const w = r - 1;
                const y = h - 1;
                const k = new Map;
                const C = new Map;
                const B = this.rs;
                const S = this.os;
                const A = this.$controller.scope;
                p = 0;
                t: {
                    while (true) {
                        if (a) {
                            g = s[p];
                            m = e[p];
                            x = getKeyValue(B, l, g, getScope(S, g, c, A, f, t, d), f);
                            v = getKeyValue(B, l, m, getScope(S, m, c, A, f, t, d), f);
                        } else {
                            g = x = ensureUnique(s[p], p);
                            m = v = ensureUnique(e[p], p);
                        }
                        if (x !== v) {
                            B.set(g, x);
                            B.set(m, v);
                            break;
                        }
                        ++p;
                        if (p > w || p > y) {
                            break t;
                        }
                    }
                    if (w !== y) {
                        break t;
                    }
                    b = y;
                    while (true) {
                        if (a) {
                            g = s[b];
                            m = e[b];
                            x = getKeyValue(B, l, g, getScope(S, g, c, A, f, t, d), f);
                            v = getKeyValue(B, l, m, getScope(S, m, c, A, f, t, d), f);
                        } else {
                            g = x = ensureUnique(s[p], p);
                            m = v = ensureUnique(e[p], p);
                        }
                        if (x !== v) {
                            B.set(g, x);
                            B.set(m, v);
                            break;
                        }
                        --b;
                        if (p > b) {
                            break t;
                        }
                    }
                }
                const E = p;
                const T = p;
                for (p = T; p <= y; ++p) {
                    if (B.has(m = a ? e[p] : ensureUnique(e[p], p))) {
                        v = B.get(m);
                    } else {
                        v = a ? getKeyValue(B, l, m, getScope(S, m, c, A, f, t, d), f) : m;
                        B.set(m, v);
                    }
                    C.set(v, p);
                }
                for (p = E; p <= w; ++p) {
                    if (B.has(g = a ? s[p] : ensureUnique(s[p], p))) {
                        x = B.get(g);
                    } else {
                        x = a ? getKeyValue(B, l, g, n[p].scope, f) : g;
                    }
                    k.set(x, p);
                    if (C.has(x)) {
                        i[C.get(x)] = p;
                    } else {
                        i.deletedIndices.push(p);
                        i.deletedItems.push(g);
                    }
                }
                for (p = T; p <= y; ++p) {
                    if (!k.has(B.get(a ? e[p] : ensureUnique(e[p], p)))) {
                        i[p] = -2;
                    }
                }
                k.clear();
                C.clear();
            }
        }
        if (i === void 0) {
            const t = e.onResolve(this.Bs(null), (() => this.Cs(null, this.ps ?? e.emptyArray)));
            if (e.isPromise(t)) {
                t.catch(rethrow);
            }
        } else {
            if (i.deletedIndices.length > 0) {
                const t = e.onResolve(this.As(i), (() => this.Es(r, i)));
                if (e.isPromise(t)) {
                    t.catch(rethrow);
                }
            } else {
                this.Es(r, i);
            }
        }
    }
    ys() {
        const t = this.$controller.scope;
        let s = this.Rs;
        let i = this.cs;
        let n;
        if (i) {
            s = this.Rs = R(this.ds, t, this.ws, null) ?? null;
            i = this.cs = !e.areEqual(this.items, s);
        }
        const r = this.ls;
        if (this.$controller.isActive) {
            const t = i ? s : this.items;
            n = this.ls = this.bs.resolve(t).getObserver?.(t);
            if (r !== n) {
                r?.unsubscribe(this);
                n?.subscribe(this);
            }
        } else {
            r?.unsubscribe(this);
            this.ls = undefined;
        }
    }
    ks() {
        const t = this.items;
        if (e.isArray(t)) {
            this.ps = t.slice(0);
            return;
        }
        const s = [];
        this.bs.resolve(t).iterate(t, ((t, e) => {
            s[e] = t;
        }));
        this.ps = s;
    }
    Cs(t, s) {
        let i = void 0;
        let n;
        let r;
        let l;
        const {$controller: a, f: h, local: c, l: u, os: f, ws: d, forOf: p, gs: g} = this;
        const m = a.scope;
        const x = s.length;
        const v = this.views = Array(x);
        s.forEach(((s, b) => {
            r = v[b] = h.create().setLocation(u);
            r.nodes.unlink();
            l = getScope(f, s, p, m, d, c, g);
            setContextualProperties(l.overrideContext, b, x);
            n = r.activate(t ?? r, a, l);
            if (e.isPromise(n)) {
                (i ??= []).push(n);
            }
        }));
        if (i !== void 0) {
            return i.length === 1 ? i[0] : Promise.all(i);
        }
    }
    Bs(t) {
        let s = void 0;
        let i;
        let n;
        let r = 0;
        const {views: l, $controller: a} = this;
        const h = l.length;
        for (;h > r; ++r) {
            n = l[r];
            n.release();
            i = n.deactivate(t ?? n, a);
            if (e.isPromise(i)) {
                (s ?? (s = [])).push(i);
            }
        }
        if (s !== void 0) {
            return s.length === 1 ? s[0] : Promise.all(s);
        }
    }
    As(t) {
        let s = void 0;
        let i;
        let n;
        const {$controller: r, views: l} = this;
        const a = t.deletedIndices.slice().sort(compareNumber);
        const h = a.length;
        let c = 0;
        for (;h > c; ++c) {
            n = l[a[c]];
            n.release();
            i = n.deactivate(n, r);
            if (e.isPromise(i)) {
                (s ?? (s = [])).push(i);
            }
        }
        c = 0;
        for (;h > c; ++c) {
            l.splice(a[c] - c, 1);
        }
        if (s !== void 0) {
            return s.length === 1 ? s[0] : Promise.all(s);
        }
    }
    Es(t, s) {
        let i = void 0;
        let n;
        let r;
        let l;
        let a = 0;
        const {$controller: h, f: c, local: u, ps: f, l: d, views: p, gs: g, ws: m, os: x, ss: v, forOf: b} = this;
        const w = s.length;
        for (;w > a; ++a) {
            if (s[a] === -2) {
                r = c.create();
                p.splice(a, 0, r);
            }
        }
        if (p.length !== w) {
            throw createMappedError(814, [ p.length, w ]);
        }
        const y = h.scope;
        const k = s.length;
        let C = 0;
        a = 0;
        for (;a < s.length; ++a) {
            if ((C = s[a]) !== -2) {
                p[a] = v[C];
            }
        }
        const B = longestIncreasingSubsequence(s);
        const S = B.length;
        const A = b.declaration;
        let R;
        let T = S - 1;
        a = k - 1;
        for (;a >= 0; --a) {
            r = p[a];
            R = p[a + 1];
            r.nodes.link(R?.nodes ?? d);
            if (s[a] === -2) {
                l = getScope(x, f[a], b, y, m, u, g);
                setContextualProperties(l.overrideContext, a, k);
                r.setLocation(d);
                n = r.activate(r, h, l);
                if (e.isPromise(n)) {
                    (i ?? (i = [])).push(n);
                }
            } else if (T < 0 || S === 1 || a !== B[T]) {
                if (g) {
                    E(A, r.scope, m, f[a]);
                } else {
                    r.scope.bindingContext[u] = f[a];
                }
                setContextualProperties(r.scope.overrideContext, a, k);
                r.nodes.insertBefore(r.location);
            } else {
                if (g) {
                    E(A, r.scope, m, f[a]);
                } else {
                    r.scope.bindingContext[u] = f[a];
                }
                if (t !== k) {
                    setContextualProperties(r.scope.overrideContext, a, k);
                }
                --T;
            }
        }
        if (i !== void 0) {
            return i.length === 1 ? i[0] : Promise.all(i);
        }
    }
    dispose() {
        this.views.forEach(dispose);
        this.views = void 0;
    }
    accept(t) {
        const {views: e} = this;
        if (e !== void 0) {
            for (let s = 0, i = e.length; s < i; ++s) {
                if (e[s].accept(t) === true) {
                    return true;
                }
            }
        }
    }
}

Repeat.$au = {
    type: ht,
    name: "repeat",
    isTemplateController: true,
    bindables: [ "items" ]
};

let bs = 16;

let ws = new Int32Array(bs);

let ys = new Int32Array(bs);

function longestIncreasingSubsequence(t) {
    const e = t.length;
    if (e > bs) {
        bs = e;
        ws = new Int32Array(e);
        ys = new Int32Array(e);
    }
    let s = 0;
    let i = 0;
    let n = 0;
    let r = 0;
    let l = 0;
    let a = 0;
    let h = 0;
    let c = 0;
    for (;r < e; r++) {
        i = t[r];
        if (i !== -2) {
            l = ws[s];
            n = t[l];
            if (n !== -2 && n < i) {
                ys[r] = l;
                ws[++s] = r;
                continue;
            }
            a = 0;
            h = s;
            while (a < h) {
                c = a + h >> 1;
                n = t[ws[c]];
                if (n !== -2 && n < i) {
                    a = c + 1;
                } else {
                    h = c;
                }
            }
            n = t[ws[a]];
            if (i < n || n === -2) {
                if (a > 0) {
                    ys[r] = ws[a - 1];
                }
                ws[a] = r;
            }
        }
    }
    r = ++s;
    const u = new Int32Array(r);
    i = ws[s - 1];
    while (s-- > 0) {
        u[s] = i;
        i = ys[i];
    }
    while (r-- > 0) ws[r] = 0;
    return u;
}

const setContextualProperties = (t, e, s) => {
    const i = e === 0;
    const n = e === s - 1;
    const r = e % 2 === 0;
    t.$index = e;
    t.$first = i;
    t.$last = n;
    t.$middle = !i && !n;
    t.$even = r;
    t.$odd = !r;
    t.$length = s;
};

const ks = /*@__PURE__*/ j("IRepeatableHandlerResolver", (t => t.singleton(RepeatableHandlerResolver)));

class RepeatableHandlerResolver {
    constructor() {
        this.Ts = e.resolve(e.all(Cs));
    }
    resolve(t) {
        if (Bs.handles(t)) {
            return Bs;
        }
        if (Ss.handles(t)) {
            return Ss;
        }
        if (As.handles(t)) {
            return As;
        }
        if (Es.handles(t)) {
            return Es;
        }
        if (Rs.handles(t)) {
            return Rs;
        }
        const e = this.Ts.find((e => e.handles(t)));
        if (e !== void 0) {
            return e;
        }
        return Ts;
    }
}

class ArrayLikeHandler {
    static register(t) {
        t.register(z(Cs, this));
    }
    handles(t) {
        return "length" in t && e.isNumber(t.length);
    }
    iterate(t, e) {
        for (let s = 0, i = t.length; s < i; ++s) {
            e(t[s], s, t);
        }
    }
}

const Cs = /*@__PURE__*/ j("IRepeatableHandler");

const Bs = {
    handles: e.isArray,
    getObserver: s.getCollectionObserver,
    iterate(t, e) {
        const s = t.length;
        let i = 0;
        for (;i < s; ++i) {
            e(t[i], i, t);
        }
    }
};

const Ss = {
    handles: e.isSet,
    getObserver: s.getCollectionObserver,
    iterate(t, e) {
        let s = 0;
        let i;
        for (i of t.keys()) {
            e(i, s++, t);
        }
    }
};

const As = {
    handles: e.isMap,
    getObserver: s.getCollectionObserver,
    iterate(t, e) {
        let s = 0;
        let i;
        for (i of t.entries()) {
            e(i, s++, t);
        }
    }
};

const Es = {
    handles: e.isNumber,
    iterate(t, e) {
        let s = 0;
        for (;s < t; ++s) {
            e(s, s, t);
        }
    }
};

const Rs = {
    handles: t => t == null,
    iterate() {}
};

const Ts = {
    handles(t) {
        return false;
    },
    iterate(t, e) {
        throw createMappedError(777, t);
    }
};

const getKeyValue = (t, e, s, i, n) => {
    let r = t.get(s);
    if (r === void 0) {
        if (typeof e === "string") {
            r = s[e];
        } else {
            r = R(e, i, n, null);
        }
        t.set(s, r);
    }
    return r;
};

const getScope = (t, e, s, i, n, r, l) => {
    let a = t.get(e);
    if (a === void 0) {
        if (l) {
            E(s.declaration, a = Scope.fromParent(i, new BindingContext), n, e);
        } else {
            a = Scope.fromParent(i, new BindingContext(r, e));
        }
        t.set(e, a);
    }
    return a;
};

const ensureUnique = (t, e) => {
    const s = typeof t;
    switch (s) {
      case "object":
        if (t !== null) return t;

      case "string":
      case "number":
      case "bigint":
      case "undefined":
      case "boolean":
        return `${e}${s}${t}`;

      default:
        return t;
    }
};

const compareNumber = (t, e) => t - e;

class With {
    constructor() {
        this.view = e.resolve(Mt).create().setLocation(e.resolve(Ke));
    }
    valueChanged(t, e) {
        const s = this.$controller;
        const i = this.view.bindings;
        let n;
        let r = 0, l = 0;
        if (s.isActive && i != null) {
            n = Scope.fromParent(s.scope, t === void 0 ? {} : t);
            for (l = i.length; l > r; ++r) {
                i[r].bind(n);
            }
        }
    }
    attaching(t, e) {
        const {$controller: s, value: i} = this;
        const n = Scope.fromParent(s.scope, i === void 0 ? {} : i);
        return this.view.activate(t, s, n);
    }
    detaching(t, e) {
        return this.view.deactivate(t, this.$controller);
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
    }
    accept(t) {
        if (this.view?.accept(t) === true) {
            return true;
        }
    }
}

With.$au = {
    type: ht,
    name: "with",
    isTemplateController: true,
    bindables: [ "value" ]
};

class Switch {
    constructor() {
        this.cases = [];
        this.activeCases = [];
        this.promise = void 0;
        this.f = e.resolve(Mt);
        this.l = e.resolve(Ke);
    }
    link(t, e, s, i) {
        this.view = this.f.create(this.$controller).setLocation(this.l);
    }
    attaching(t, e) {
        const s = this.view;
        const i = this.$controller;
        this.queue((() => s.activate(t, i, i.scope)));
        this.queue((() => this.swap(t, this.value)));
        return this.promise;
    }
    detaching(t, e) {
        this.queue((() => {
            const e = this.view;
            return e.deactivate(t, this.$controller);
        }));
        return this.promise;
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
    valueChanged(t, e) {
        if (!this.$controller.isActive) {
            return;
        }
        this.queue((() => this.swap(null, this.value)));
    }
    caseChanged(t) {
        this.queue((() => this.Ls(t)));
    }
    Ls(t) {
        const s = t.isMatch(this.value);
        const i = this.activeCases;
        const n = i.length;
        if (!s) {
            if (n > 0 && i[0].id === t.id) {
                return this.Ms(null);
            }
            return;
        }
        if (n > 0 && i[0].id < t.id) {
            return;
        }
        const r = [];
        let l = t.fallThrough;
        if (!l) {
            r.push(t);
        } else {
            const e = this.cases;
            const s = e.indexOf(t);
            for (let t = s, i = e.length; t < i && l; t++) {
                const s = e[t];
                r.push(s);
                l = s.fallThrough;
            }
        }
        return e.onResolve(this.Ms(null, r), (() => {
            this.activeCases = r;
            return this.Ds(null);
        }));
    }
    swap(t, s) {
        const i = [];
        let n = false;
        for (const t of this.cases) {
            if (n || t.isMatch(s)) {
                i.push(t);
                n = t.fallThrough;
            }
            if (i.length > 0 && !n) {
                break;
            }
        }
        const r = this.defaultCase;
        if (i.length === 0 && r !== void 0) {
            i.push(r);
        }
        return e.onResolve(this.activeCases.length > 0 ? this.Ms(t, i) : void 0, (() => {
            this.activeCases = i;
            if (i.length === 0) {
                return;
            }
            return this.Ds(t);
        }));
    }
    Ds(t) {
        const s = this.$controller;
        if (!s.isActive) {
            return;
        }
        const i = this.activeCases;
        const n = i.length;
        if (n === 0) {
            return;
        }
        const r = s.scope;
        if (n === 1) {
            return i[0].activate(t, r);
        }
        return e.onResolveAll(...i.map((e => e.activate(t, r))));
    }
    Ms(t, s = []) {
        const i = this.activeCases;
        const n = i.length;
        if (n === 0) {
            return;
        }
        if (n === 1) {
            const e = i[0];
            if (!s.includes(e)) {
                i.length = 0;
                return e.deactivate(t);
            }
            return;
        }
        return e.onResolve(e.onResolveAll(...i.reduce(((e, i) => {
            if (!s.includes(i)) {
                e.push(i.deactivate(t));
            }
            return e;
        }), [])), (() => {
            i.length = 0;
        }));
    }
    queue(t) {
        const s = this.promise;
        let i = void 0;
        i = this.promise = e.onResolve(e.onResolve(s, t), (() => {
            if (this.promise === i) {
                this.promise = void 0;
            }
        }));
    }
    accept(t) {
        if (this.$controller.accept(t) === true) {
            return true;
        }
        if (this.activeCases.some((e => e.accept(t)))) {
            return true;
        }
    }
}

Switch.$au = {
    type: ht,
    name: "switch",
    isTemplateController: true,
    bindables: [ "value" ]
};

let Ls = 0;

class Case {
    constructor() {
        this.id = ++Ls;
        this.fallThrough = false;
        this.view = void 0;
        this.f = e.resolve(Mt);
        this.ze = e.resolve(s.IObserverLocator);
        this.l = e.resolve(Ke);
        this.qs = e.resolve(e.ILogger).scopeTo(`${this.constructor.name}-#${this.id}`);
    }
    link(t, e, s, i) {
        const n = t.parent;
        const r = n?.viewModel;
        if (r instanceof Switch) {
            this.$switch = r;
            this.linkToSwitch(r);
        } else {
            throw createMappedError(815);
        }
    }
    detaching(t, e) {
        return this.deactivate(t);
    }
    isMatch(t) {
        this.qs.debug("isMatch()");
        const s = this.value;
        if (e.isArray(s)) {
            if (this.ls === void 0) {
                this.ls = this.Is(s);
            }
            return s.includes(t);
        }
        return s === t;
    }
    valueChanged(t, s) {
        if (e.isArray(t)) {
            this.ls?.unsubscribe(this);
            this.ls = this.Is(t);
        } else if (this.ls !== void 0) {
            this.ls.unsubscribe(this);
        }
        this.$switch.caseChanged(this);
    }
    handleCollectionChange() {
        this.$switch.caseChanged(this);
    }
    activate(t, e) {
        let s = this.view;
        if (s === void 0) {
            s = this.view = this.f.create().setLocation(this.l);
        }
        if (s.isActive) {
            return;
        }
        return s.activate(t ?? s, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (e === void 0 || !e.isActive) {
            return;
        }
        return e.deactivate(t ?? e, this.$controller);
    }
    dispose() {
        this.ls?.unsubscribe(this);
        this.view?.dispose();
        this.view = void 0;
    }
    linkToSwitch(t) {
        t.cases.push(this);
    }
    Is(t) {
        const e = this.ze.getArrayObserver(t);
        e.subscribe(this);
        return e;
    }
    accept(t) {
        if (this.$controller.accept(t) === true) {
            return true;
        }
        return this.view?.accept(t);
    }
}

class DefaultCase extends Case {
    linkToSwitch(t) {
        if (t.defaultCase !== void 0) {
            throw createMappedError(816);
        }
        t.defaultCase = this;
    }
}

(() => {
    const t = [ "value", {
        name: "fallThrough",
        mode: D,
        set(t) {
            switch (t) {
              case "true":
                return true;

              case "false":
                return false;

              default:
                return !!t;
            }
        }
    } ];
    defineAttribute({
        name: "default-case",
        bindables: t,
        isTemplateController: true
    }, DefaultCase);
    defineAttribute({
        name: "case",
        bindables: t,
        isTemplateController: true
    }, Case);
})();

var Ms, Ds, qs;

class PromiseTemplateController {
    constructor() {
        this.preSettledTask = null;
        this.postSettledTask = null;
        this.f = e.resolve(Mt);
        this.l = e.resolve(Ke);
        this.p = e.resolve(lt);
        this.logger = e.resolve(e.ILogger).scopeTo("promise.resolve");
    }
    link(t, e, s, i) {
        this.view = this.f.create(this.$controller).setLocation(this.l);
    }
    attaching(t, s) {
        const i = this.view;
        const n = this.$controller;
        return e.onResolve(i.activate(t, n, this.viewScope = Scope.fromParent(n.scope, {})), (() => this.swap(t)));
    }
    valueChanged(t, e) {
        if (!this.$controller.isActive) {
            return;
        }
        this.swap(null);
    }
    swap(t) {
        const s = this.value;
        if (!e.isPromise(s)) {
            return;
        }
        const i = this.p.domQueue;
        const n = this.fulfilled;
        const r = this.rejected;
        const a = this.pending;
        const h = this.viewScope;
        let c;
        const u = {
            reusable: false
        };
        const $swap = () => {
            void e.onResolveAll(c = (this.preSettledTask = i.queueTask((() => e.onResolveAll(n?.deactivate(t), r?.deactivate(t), a?.activate(t, h))), u)).result.catch((t => {
                if (!(t instanceof l.TaskAbortError)) throw t;
            })), s.then((l => {
                if (this.value !== s) {
                    return;
                }
                const fulfill = () => {
                    this.postSettlePromise = (this.postSettledTask = i.queueTask((() => e.onResolveAll(a?.deactivate(t), r?.deactivate(t), n?.activate(t, h, l))), u)).result;
                };
                if (this.preSettledTask.status === C) {
                    void c.then(fulfill);
                } else {
                    this.preSettledTask.cancel();
                    fulfill();
                }
            }), (l => {
                if (this.value !== s) {
                    return;
                }
                const reject = () => {
                    this.postSettlePromise = (this.postSettledTask = i.queueTask((() => e.onResolveAll(a?.deactivate(t), n?.deactivate(t), r?.activate(t, h, l))), u)).result;
                };
                if (this.preSettledTask.status === C) {
                    void c.then(reject);
                } else {
                    this.preSettledTask.cancel();
                    reject();
                }
            })));
        };
        if (this.postSettledTask?.status === C) {
            void this.postSettlePromise.then($swap);
        } else {
            this.postSettledTask?.cancel();
            $swap();
        }
    }
    detaching(t, e) {
        this.preSettledTask?.cancel();
        this.postSettledTask?.cancel();
        this.preSettledTask = this.postSettledTask = null;
        return this.view.deactivate(t, this.$controller);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
}

PromiseTemplateController.$au = {
    type: ht,
    name: "promise",
    isTemplateController: true,
    bindables: [ "value" ]
};

class PendingTemplateController {
    constructor() {
        this.view = void 0;
        this.f = e.resolve(Mt);
        this.l = e.resolve(Ke);
    }
    link(t, e, s, i) {
        getPromiseController(t).pending = this;
    }
    activate(t, e) {
        let s = this.view;
        if (s === void 0) {
            s = this.view = this.f.create().setLocation(this.l);
        }
        if (s.isActive) {
            return;
        }
        return s.activate(s, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (e === void 0 || !e.isActive) {
            return;
        }
        return e.deactivate(e, this.$controller);
    }
    detaching(t) {
        return this.deactivate(t);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
}

PendingTemplateController.$au = {
    type: ht,
    name: "pending",
    isTemplateController: true,
    bindables: {
        value: {
            mode: q
        }
    }
};

class FulfilledTemplateController {
    constructor() {
        this.view = void 0;
        this.f = e.resolve(Mt);
        this.l = e.resolve(Ke);
    }
    link(t, e, s, i) {
        getPromiseController(t).fulfilled = this;
    }
    activate(t, e, s) {
        this.value = s;
        let i = this.view;
        if (i === void 0) {
            i = this.view = this.f.create().setLocation(this.l);
        }
        if (i.isActive) {
            return;
        }
        return i.activate(i, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (e === void 0 || !e.isActive) {
            return;
        }
        return e.deactivate(e, this.$controller);
    }
    detaching(t, e) {
        return this.deactivate(t);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
}

FulfilledTemplateController.$au = {
    type: ht,
    name: "then",
    isTemplateController: true,
    bindables: {
        value: {
            mode: I
        }
    }
};

class RejectedTemplateController {
    constructor() {
        this.view = void 0;
        this.f = e.resolve(Mt);
        this.l = e.resolve(Ke);
    }
    link(t, e, s, i) {
        getPromiseController(t).rejected = this;
    }
    activate(t, e, s) {
        this.value = s;
        let i = this.view;
        if (i === void 0) {
            i = this.view = this.f.create().setLocation(this.l);
        }
        if (i.isActive) {
            return;
        }
        return i.activate(i, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (e === void 0 || !e.isActive) {
            return;
        }
        return e.deactivate(e, this.$controller);
    }
    detaching(t, e) {
        return this.deactivate(t);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
}

RejectedTemplateController.$au = {
    type: ht,
    name: "catch",
    isTemplateController: true,
    bindables: {
        value: {
            mode: I
        }
    }
};

function getPromiseController(t) {
    const e = t.parent;
    const s = e?.viewModel;
    if (s instanceof PromiseTemplateController) {
        return s;
    }
    throw createMappedError(813);
}

class PromiseAttributePattern {
    "promise.resolve"(t, e) {
        return new i.AttrSyntax(t, e, "promise", "bind");
    }
}

Ms = Symbol.metadata;

PromiseAttributePattern[Ms] = {
    [e.registrableMetadataKey]: i.AttributePattern.create([ {
        pattern: "promise.resolve",
        symbols: ""
    } ], PromiseAttributePattern)
};

class FulfilledAttributePattern {
    then(t, e) {
        return new i.AttrSyntax(t, e, "then", "from-view");
    }
}

Ds = Symbol.metadata;

FulfilledAttributePattern[Ds] = {
    [e.registrableMetadataKey]: i.AttributePattern.create([ {
        pattern: "then",
        symbols: ""
    } ], FulfilledAttributePattern)
};

class RejectedAttributePattern {
    catch(t, e) {
        return new i.AttrSyntax(t, e, "catch", "from-view");
    }
}

qs = Symbol.metadata;

RejectedAttributePattern[qs] = {
    [e.registrableMetadataKey]: i.AttributePattern.create([ {
        pattern: "catch",
        symbols: ""
    } ], RejectedAttributePattern)
};

class Focus {
    constructor() {
        this.Ps = false;
        this.Ee = e.resolve(Ue);
        this.p = e.resolve(lt);
    }
    binding() {
        this.valueChanged();
    }
    valueChanged() {
        if (this.$controller.isActive) {
            this._s();
        } else {
            this.Ps = true;
        }
    }
    attached() {
        if (this.Ps) {
            this.Ps = false;
            this._s();
        }
        this.Ee.addEventListener("focus", this);
        this.Ee.addEventListener("blur", this);
    }
    detaching() {
        const t = this.Ee;
        t.removeEventListener("focus", this);
        t.removeEventListener("blur", this);
    }
    handleEvent(t) {
        if (t.type === "focus") {
            this.value = true;
        } else if (!this.Fs) {
            this.value = false;
        }
    }
    _s() {
        const t = this.Ee;
        const e = this.Fs;
        const s = this.value;
        if (s && !e) {
            t.focus();
        } else if (!s && e) {
            t.blur();
        }
    }
    get Fs() {
        return this.Ee === this.p.document.activeElement;
    }
}

Focus.$au = {
    type: ht,
    name: "focus",
    bindables: {
        value: {
            mode: P
        }
    }
};

class Portal {
    constructor() {
        this.position = "beforeend";
        this.strict = false;
        const t = e.resolve(Mt);
        const s = e.resolve(Ke);
        const i = e.resolve(lt);
        this.p = i;
        this.Vs = i.document.createElement("div");
        (this.view = t.create()).setLocation(this.Os = Dt(i));
        setEffectiveParentNode(this.view.nodes, s);
    }
    attaching(t) {
        if (this.callbackContext == null) {
            this.callbackContext = this.$controller.scope.bindingContext;
        }
        const e = this.Vs = this.Hs();
        this.$s(e, this.position);
        return this.Ns(t, e);
    }
    detaching(t) {
        return this.Ws(t, this.Vs);
    }
    targetChanged() {
        const {$controller: t} = this;
        if (!t.isActive) {
            return;
        }
        const s = this.Hs();
        if (this.Vs === s) {
            return;
        }
        this.Vs = s;
        const i = e.onResolve(this.Ws(null, s), (() => {
            this.$s(s, this.position);
            return this.Ns(null, s);
        }));
        if (e.isPromise(i)) {
            i.catch(rethrow);
        }
    }
    positionChanged() {
        const {$controller: t, Vs: s} = this;
        if (!t.isActive) {
            return;
        }
        const i = e.onResolve(this.Ws(null, s), (() => {
            this.$s(s, this.position);
            return this.Ns(null, s);
        }));
        if (e.isPromise(i)) {
            i.catch(rethrow);
        }
    }
    Ns(t, s) {
        const {activating: i, callbackContext: n, view: r} = this;
        return e.onResolve(i?.call(n, s, r), (() => this.js(t, s)));
    }
    js(t, s) {
        const {$controller: i, view: n} = this;
        if (t === null) {
            n.nodes.insertBefore(this.Os);
        } else {
            return e.onResolve(n.activate(t ?? n, i, i.scope), (() => this.zs(s)));
        }
        return this.zs(s);
    }
    zs(t) {
        const {activated: e, callbackContext: s, view: i} = this;
        return e?.call(s, t, i);
    }
    Ws(t, s) {
        const {deactivating: i, callbackContext: n, view: r} = this;
        return e.onResolve(i?.call(n, s, r), (() => this.Us(t, s)));
    }
    Us(t, s) {
        const {$controller: i, view: n} = this;
        if (t === null) {
            n.nodes.remove();
        } else {
            return e.onResolve(n.deactivate(t, i), (() => this.Gs(s)));
        }
        return this.Gs(s);
    }
    Gs(t) {
        const {deactivated: s, callbackContext: i, view: n} = this;
        return e.onResolve(s?.call(i, t, n), (() => this.Ks()));
    }
    Hs() {
        const t = this.p;
        const s = t.document;
        let i = this.target;
        let n = this.renderContext;
        if (i === "") {
            if (this.strict) {
                throw createMappedError(811);
            }
            return s.body;
        }
        if (e.isString(i)) {
            let r = s;
            if (e.isString(n)) {
                n = s.querySelector(n);
            }
            if (n instanceof t.Node) {
                r = n;
            }
            i = r.querySelector(i);
        }
        if (i instanceof t.Node) {
            return i;
        }
        if (i == null) {
            if (this.strict) {
                throw createMappedError(812);
            }
            return s.body;
        }
        return i;
    }
    Ks() {
        this.Os.remove();
        this.Os.$start.remove();
    }
    $s(t, e) {
        const s = this.Os;
        const i = s.$start;
        const n = t.parentNode;
        const r = [ i, s ];
        switch (e) {
          case "beforeend":
            insertManyBefore(t, null, r);
            break;

          case "afterbegin":
            insertManyBefore(t, t.firstChild, r);
            break;

          case "beforebegin":
            insertManyBefore(n, t, r);
            break;

          case "afterend":
            insertManyBefore(n, t.nextSibling, r);
            break;

          default:
            throw createMappedError(779, e);
        }
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
        this.callbackContext = null;
    }
    accept(t) {
        if (this.view?.accept(t) === true) {
            return true;
        }
    }
}

Portal.$au = {
    type: ht,
    name: "portal",
    isTemplateController: true,
    bindables: [ {
        name: "target",
        primary: true
    }, "position", "activated", "activating", "callbackContext", {
        name: "renderContext",
        callback: "targetChanged"
    }, "strict", "deactivated", "deactivating" ]
};

let Is;

class AuSlot {
    constructor() {
        this.Xs = null;
        this.Qs = null;
        this.Yt = false;
        this.expose = null;
        this.slotchange = null;
        this.Ys = new Set;
        this.ls = null;
        const t = e.resolve(Ne);
        const s = e.resolve(Ke);
        const n = e.resolve(i.IInstruction);
        const r = e.resolve(ue);
        const l = this.name = n.data.name;
        const a = n.projections?.[qt];
        const h = t.instruction?.projections?.[l];
        const c = t.controller.container;
        let u;
        let f;
        if (h == null) {
            f = c.createChild({
                inheritParentResources: true
            });
            u = r.getViewFactory(a ?? (Is ??= CustomElementDefinition.create({
                name: "au-slot-empty-template",
                template: "",
                needsCompile: false
            })), f);
            this.Zs = false;
        } else {
            f = c.createChild();
            f.useResources(t.parent.controller.container);
            registerResolver(f, Ne, new e.InstanceProvider(void 0, t.parent));
            u = r.getViewFactory(h, f);
            this.Zs = true;
            this.Js = c.getAll(_t, false)?.filter((t => t.slotName === "*" || t.slotName === l)) ?? e.emptyArray;
        }
        this.ti = (this.Js ??= e.emptyArray).length > 0;
        this.ei = t;
        this.view = u.create().setLocation(this.l = s);
    }
    get nodes() {
        const t = [];
        const e = this.l;
        let s = e.$start.nextSibling;
        while (s != null && s !== e) {
            if (s.nodeType !== 8) {
                t.push(s);
            }
            s = s.nextSibling;
        }
        return t;
    }
    subscribe(t) {
        this.Ys.add(t);
    }
    unsubscribe(t) {
        this.Ys.delete(t);
    }
    binding(t, e) {
        this.Xs = e.scope;
        while (e.vmKind === "synthetic" && e.parent?.viewModel instanceof AuSlot) {
            e = e.parent.parent;
        }
        const s = e.scope.bindingContext;
        let i;
        if (this.Zs) {
            i = this.ei.controller.scope.parent;
            (this.Qs = Scope.fromParent(i, i.bindingContext)).overrideContext.$host = this.expose ?? s;
        }
    }
    attaching(t, s) {
        return e.onResolve(this.view.activate(t, this.$controller, this.Zs ? this.Qs : this.Xs), (() => {
            if (this.ti) {
                this.Js.forEach((t => t.watch(this)));
                this.Xe();
                this.si();
                this.Yt = true;
            }
        }));
    }
    detaching(t, e) {
        this.Yt = false;
        this.ii();
        this.Js.forEach((t => t.unwatch(this)));
        return this.view.deactivate(t, this.$controller);
    }
    exposeChanged(t) {
        if (this.Zs && this.Qs != null) {
            this.Qs.overrideContext.$host = t;
        }
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
    }
    accept(t) {
        if (this.view?.accept(t) === true) {
            return true;
        }
    }
    Xe() {
        if (this.ls != null) {
            return;
        }
        const t = this.l;
        const e = t.parentElement;
        if (e == null) {
            return;
        }
        (this.ls = createMutationObserver(e, (e => {
            if (isMutationWithinLocation(t, e)) {
                this.si();
            }
        }))).observe(e, {
            childList: true
        });
    }
    ii() {
        this.ls?.disconnect();
        this.ls = null;
    }
    si() {
        const t = this.nodes;
        const e = new Set(this.Ys);
        let s;
        if (this.Yt) {
            this.slotchange?.call(void 0, this.name, t);
        }
        for (s of e) {
            s.handleSlotChange(this, t);
        }
    }
}

AuSlot.$au = {
    type: ss,
    name: "au-slot",
    template: null,
    containerless: true,
    processContent(t, e, s) {
        s.name = t.getAttribute("name") ?? qt;
        let i = t.firstChild;
        let n = null;
        while (i !== null) {
            n = i.nextSibling;
            if (isElement(i) && i.hasAttribute(It)) {
                t.removeChild(i);
            }
            i = n;
        }
    },
    bindables: [ "expose", "slotchange" ]
};

const comparePosition = (t, e) => t.compareDocumentPosition(e);

const isMutationWithinLocation = (t, e) => {
    for (const {addedNodes: s, removedNodes: i, nextSibling: n} of e) {
        let e = 0;
        let r = s.length;
        let l;
        for (;e < r; ++e) {
            l = s[e];
            if (comparePosition(t.$start, l) === 4 && comparePosition(t, l) === 2) {
                return true;
            }
        }
        if (i.length > 0) {
            if (n != null && comparePosition(t.$start, n) === 4 && comparePosition(t, n) === 2) {
                return true;
            }
        }
    }
};

class AuCompose {
    constructor() {
        this.scopeBehavior = "auto";
        this.ni = void 0;
        this.tag = null;
        this.c = e.resolve(e.IContainer);
        this.parent = e.resolve($e);
        this.ri = e.resolve(Ue);
        this.l = e.resolve(Ke);
        this.p = e.resolve(lt);
        this.r = e.resolve(ue);
        this.oi = e.resolve(i.IInstruction);
        this.li = e.resolve(e.transient(CompositionContextFactory, null));
        this.dt = e.resolve(i.ITemplateCompiler);
        this.Z = e.resolve(Ne);
        this.ep = e.resolve(t.IExpressionParser);
        this.oL = e.resolve(s.IObserverLocator);
    }
    get composing() {
        return this.ai;
    }
    get composition() {
        return this.ni;
    }
    attaching(t, s) {
        return this.ai = e.onResolve(this.queue(new ChangeInfo(this.template, this.component, this.model, void 0), t), (t => {
            if (this.li.hi(t)) {
                this.ai = void 0;
            }
        }));
    }
    detaching(t) {
        const s = this.ni;
        const i = this.ai;
        this.li.invalidate();
        this.ni = this.ai = void 0;
        return e.onResolve(i, (() => s?.deactivate(t)));
    }
    propertyChanged(t) {
        if (t === "composing" || t === "composition") return;
        if (t === "model" && this.ni != null) {
            this.ni.update(this.model);
            return;
        }
        if (t === "tag" && this.ni?.controller.vmKind === Le) {
            return;
        }
        this.ai = e.onResolve(this.ai, (() => e.onResolve(this.queue(new ChangeInfo(this.template, this.component, this.model, t), void 0), (t => {
            if (this.li.hi(t)) {
                this.ai = void 0;
            }
        }))));
    }
    queue(t, s) {
        const i = this.li;
        const n = this.ni;
        return e.onResolve(i.create(t), (t => {
            if (i.hi(t)) {
                return e.onResolve(this.compose(t), (r => {
                    if (i.hi(t)) {
                        return e.onResolve(r.activate(s), (() => {
                            if (i.hi(t)) {
                                this.ni = r;
                                return e.onResolve(n?.deactivate(s), (() => t));
                            } else {
                                return e.onResolve(r.controller.deactivate(r.controller, this.$controller), (() => {
                                    r.controller.dispose();
                                    return t;
                                }));
                            }
                        }));
                    }
                    r.controller.dispose();
                    return t;
                }));
            }
            return t;
        }));
    }
    compose(t) {
        const {ci: s, ui: i, fi: n} = t.change;
        const {c: r, $controller: l, l: a, oi: h} = this;
        const c = this.di(this.Z.controller.container, i);
        const u = r.createChild();
        const f = this.p.document.createElement(c == null ? this.tag ?? "div" : c.name);
        a.parentNode.insertBefore(f, a);
        let d;
        if (c == null) {
            d = this.tag == null ? convertToRenderLocation(f) : null;
        } else {
            d = c.containerless ? convertToRenderLocation(f) : null;
        }
        const removeCompositionHost = () => {
            f.remove();
            if (d != null) {
                let t = d.$start.nextSibling;
                let e = null;
                while (t !== null && t !== d) {
                    e = t.nextSibling;
                    t.remove();
                    t = e;
                }
                d.$start?.remove();
                d.remove();
            }
        };
        const p = this.pi(u, typeof i === "string" ? c.Type : i, f, d);
        const compose = () => {
            const i = h.captures ?? e.emptyArray;
            if (c !== null) {
                const s = c.capture;
                const [n, r] = i.reduce(((t, i) => {
                    const n = !(i.target in c.bindables) && (s === true || e.isFunction(s) && !!s(i.target));
                    t[n ? 0 : 1].push(i);
                    return t;
                }), [ [], [] ]);
                const a = Controller.$el(u, p, f, {
                    projections: h.projections,
                    captures: n
                }, c, d);
                this.gi(f, c, r).forEach((t => a.addBinding(t)));
                return new CompositionController(a, (t => a.activate(t ?? a, l, l.scope.parent)), (t => e.onResolve(a.deactivate(t ?? a, l), removeCompositionHost)), (t => p.activate?.(t)), t);
            } else {
                const n = CustomElementDefinition.create({
                    name: os.generateName(),
                    template: s
                });
                const r = this.r.getViewFactory(n, u);
                const a = Controller.$view(r, l);
                const h = this.scopeBehavior === "auto" ? Scope.fromParent(this.parent.scope, p) : Scope.create(p);
                a.setHost(f);
                if (d == null) {
                    this.gi(f, n, i).forEach((t => a.addBinding(t)));
                } else {
                    a.setLocation(d);
                }
                return new CompositionController(a, (t => a.activate(t ?? a, l, h)), (t => e.onResolve(a.deactivate(t ?? a, l), removeCompositionHost)), (t => p.activate?.(t)), t);
            }
        };
        if ("activate" in p) {
            return e.onResolve(p.activate(n), (() => compose()));
        } else {
            return compose();
        }
    }
    pi(t, s, i, n) {
        if (s == null) {
            return new EmptyComponent;
        }
        if (typeof s === "object") {
            return s;
        }
        const r = this.p;
        registerHostNode(t, i, r);
        registerResolver(t, Ke, new e.InstanceProvider("IRenderLocation", n));
        const l = t.invoke(s);
        registerResolver(t, s, new e.InstanceProvider("au-compose.component", l));
        return l;
    }
    di(t, s) {
        if (typeof s === "string") {
            const e = os.find(t, s);
            if (e == null) {
                throw createMappedError(806, s);
            }
            return e;
        }
        const i = e.isFunction(s) ? s : s?.constructor;
        return os.isType(i, void 0) ? os.getDefinition(i, null) : null;
    }
    gi(t, e, s) {
        const i = new HydrationContext(this.$controller, {
            projections: null,
            captures: s
        }, this.Z.parent);
        return SpreadBinding.create(i, t, e, this.r, this.dt, this.p, this.ep, this.oL);
    }
}

AuCompose.$au = {
    type: ss,
    name: "au-compose",
    capture: true,
    containerless: true,
    bindables: [ "template", "component", "model", {
        name: "scopeBehavior",
        set: t => {
            if (t === "scoped" || t === "auto") {
                return t;
            }
            throw createMappedError(805, t);
        }
    }, {
        name: "composing",
        mode: I
    }, {
        name: "composition",
        mode: I
    }, "tag" ]
};

class EmptyComponent {}

class CompositionContextFactory {
    constructor() {
        this.id = 0;
    }
    hi(t) {
        return t.id === this.id;
    }
    create(t) {
        return e.onResolve(t.load(), (t => new CompositionContext(++this.id, t)));
    }
    invalidate() {
        this.id++;
    }
}

class ChangeInfo {
    constructor(t, e, s, i) {
        this.ci = t;
        this.ui = e;
        this.fi = s;
        this.mi = i;
    }
    load() {
        if (e.isPromise(this.ci) || e.isPromise(this.ui)) {
            return Promise.all([ this.ci, this.ui ]).then((([t, e]) => new LoadedChangeInfo(t, e, this.fi, this.mi)));
        } else {
            return new LoadedChangeInfo(this.ci, this.ui, this.fi, this.mi);
        }
    }
}

class LoadedChangeInfo {
    constructor(t, e, s, i) {
        this.ci = t;
        this.ui = e;
        this.fi = s;
        this.mi = i;
    }
}

class CompositionContext {
    constructor(t, e) {
        this.id = t;
        this.change = e;
    }
}

class CompositionController {
    constructor(t, e, s, i, n) {
        this.controller = t;
        this.start = e;
        this.stop = s;
        this.update = i;
        this.context = n;
        this.state = 0;
    }
    activate(t) {
        if (this.state !== 0) {
            throw createMappedError(807, this);
        }
        this.state = 1;
        return this.start(t);
    }
    deactivate(t) {
        switch (this.state) {
          case 1:
            this.state = -1;
            return this.stop(t);

          case -1:
            throw createMappedError(808);

          default:
            this.state = -1;
        }
    }
}

const Ps = /*@__PURE__*/ j("ISanitizer", (t => t.singleton(class {
    sanitize() {
        throw createMappedError(99, "sanitize");
    }
})));

class SanitizeValueConverter {
    constructor() {
        this.xi = e.resolve(Ps);
    }
    toView(t) {
        if (t == null) {
            return null;
        }
        return this.xi.sanitize(t);
    }
}

SanitizeValueConverter.$au = {
    type: pt,
    name: "sanitize"
};

class Show {
    constructor() {
        this.el = e.resolve(Ue);
        this.p = e.resolve(lt);
        this.vi = false;
        this.T = null;
        this.$val = "";
        this.$prio = "";
        this.update = () => {
            this.T = null;
            if (Boolean(this.value) !== this.bi) {
                if (this.bi === this.wi) {
                    this.bi = !this.wi;
                    this.$val = this.el.style.getPropertyValue("display");
                    this.$prio = this.el.style.getPropertyPriority("display");
                    this.el.style.setProperty("display", "none", "important");
                } else {
                    this.bi = this.wi;
                    this.el.style.setProperty("display", this.$val, this.$prio);
                    if (this.el.getAttribute("style") === "") {
                        this.el.removeAttribute("style");
                    }
                }
            }
        };
        const t = e.resolve(i.IInstruction);
        this.bi = this.wi = t.alias !== "hide";
    }
    binding() {
        this.vi = true;
        this.update();
    }
    detaching() {
        this.vi = false;
        this.T?.cancel();
        this.T = null;
    }
    valueChanged() {
        if (this.vi && this.T === null) {
            this.T = this.p.domQueue.queueTask(this.update);
        }
    }
}

Show.$au = {
    type: ht,
    name: "show",
    bindables: [ "value" ],
    aliases: [ "hide" ]
};

const _s = [ us, s.DirtyChecker, NodeObserverLocator ];

const Fs = [ i.RefAttributePattern, i.DotSeparatedAttributePattern, i.EventAttributePattern, Lt ];

const Vs = [ i.AtPrefixedTriggerAttributePattern, i.ColonPrefixedBindAttributePattern ];

const Os = [ i.DefaultBindingCommand, i.OneTimeBindingCommand, i.FromViewBindingCommand, i.ToViewBindingCommand, i.TwoWayBindingCommand, i.ForBindingCommand, i.RefBindingCommand, i.TriggerBindingCommand, i.CaptureBindingCommand, i.ClassBindingCommand, i.StyleBindingCommand, i.AttrBindingCommand, i.SpreadValueBindingCommand ];

const Hs = [ DebounceBindingBehavior, OneTimeBindingBehavior, ToViewBindingBehavior, FromViewBindingBehavior, SignalBindingBehavior, ThrottleBindingBehavior, TwoWayBindingBehavior, SanitizeValueConverter, If, Else, Repeat, With, Switch, Case, DefaultCase, PromiseTemplateController, PendingTemplateController, FulfilledTemplateController, RejectedTemplateController, PromiseAttributePattern, FulfilledAttributePattern, RejectedAttributePattern, AttrBindingBehavior, SelfBindingBehavior, UpdateTriggerBindingBehavior, AuCompose, Portal, Focus, Show, AuSlot ];

const $s = [ Ut, Gt, jt, zt, Ot, Ht, $t, Nt, Wt, Qt, ee, Yt, Zt, Jt, te, Kt, se, ie ];

const Ns = /*@__PURE__*/ createConfiguration(e.noop);

function createConfiguration(t) {
    return {
        optionsProvider: t,
        register(e) {
            const i = {
                coercingOptions: {
                    enableCoercion: false,
                    coerceNullish: false
                }
            };
            t(i);
            return e.register(G(s.ICoercionConfiguration, i.coercingOptions), ..._s, ...Hs, ...Fs, ...Os, ...$s);
        },
        customize(e) {
            return createConfiguration(e ?? t);
        }
    };
}

function children(t, i) {
    if (!children.mixed) {
        children.mixed = true;
        s.subscriberCollection(ChildrenBinding, null);
        lifecycleHooks()(ChildrenLifecycleHooks, null);
    }
    let n;
    const r = H("dependencies");
    function decorator(t, e) {
        switch (e.kind) {
          case "field":
            n.name = e.name;
            break;
        }
        const s = e.metadata[r] ??= [];
        s.push(new ChildrenLifecycleHooks(n ?? {}));
    }
    if (arguments.length > 1) {
        n = {};
        decorator(t, i);
        return;
    } else if (e.isString(t)) {
        n = {
            query: t
        };
        return decorator;
    }
    n = t === void 0 ? {} : t;
    return decorator;
}

children.mixed = false;

class ChildrenBinding {
    constructor(t, e, s, i, n, r) {
        this.yi = void 0;
        this.isBound = false;
        this.obj = e;
        this.cb = s;
        this.K = i;
        this.ki = n;
        this.Ci = r;
        this.ls = createMutationObserver(this.ri = t, (() => {
            this.Bi();
        }));
    }
    getValue() {
        return this.isBound ? this.yi : this.Si();
    }
    setValue(t) {}
    bind() {
        if (this.isBound) {
            return;
        }
        this.isBound = true;
        this.ls.observe(this.ri, {
            childList: true
        });
        this.yi = this.Si();
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        this.isBound = false;
        this.ls.takeRecords();
        this.ls.disconnect();
        this.yi = e.emptyArray;
    }
    Bi() {
        this.yi = this.Si();
        this.cb?.call(this.obj);
        this.subs.notify(this.yi, undefined);
    }
    get() {
        throw createMappedError(99, "get");
    }
    Si() {
        const t = this.K;
        const e = this.ki;
        const s = this.Ci;
        const i = t === "$all" ? this.ri.childNodes : this.ri.querySelectorAll(`:scope > ${t}`);
        const n = i.length;
        const r = [];
        const l = {
            optional: true
        };
        let a;
        let h;
        let c = 0;
        let u;
        while (n > c) {
            u = i[c];
            a = findElementControllerFor(u, l);
            h = a?.viewModel ?? null;
            if (e == null ? true : e(u, h)) {
                r.push(s == null ? h ?? u : s(u, h));
            }
            ++c;
        }
        return r;
    }
}

class ChildrenLifecycleHooks {
    constructor(t) {
        this.X = t;
    }
    register(t) {
        G(ft, this).register(t);
    }
    hydrating(t, e) {
        const s = this.X;
        const i = s.query ?? "*";
        const n = new ChildrenBinding(e.host, t, t[s.callback ?? `${h(s.name)}Changed`], i, s.filter, s.map);
        if (/[\s>]/.test(i)) {
            throw createMappedError(9989, i);
        }
        x(t, s.name, {
            enumerable: true,
            configurable: true,
            get: d((() => n.getValue()), {
                getObserver: () => n
            }),
            set: () => {}
        });
        e.addBinding(n);
    }
}

exports.BindingCommand = i.BindingCommand;

exports.BindingMode = i.BindingMode;

exports.AdoptedStyleSheetsStyles = AdoptedStyleSheetsStyles;

exports.AppRoot = AppRoot;

exports.AppTask = ot;

exports.ArrayLikeHandler = ArrayLikeHandler;

exports.AttrBindingBehavior = AttrBindingBehavior;

exports.AttrMapper = AttrMapper;

exports.AttributeBinding = AttributeBinding;

exports.AttributeBindingRenderer = ee;

exports.AttributeNSAccessor = AttributeNSAccessor;

exports.AuCompose = AuCompose;

exports.AuSlot = AuSlot;

exports.AuSlotsInfo = AuSlotsInfo;

exports.Aurelia = Aurelia;

exports.Bindable = N;

exports.BindableDefinition = BindableDefinition;

exports.BindingBehavior = Z;

exports.BindingBehaviorDefinition = BindingBehaviorDefinition;

exports.BindingContext = BindingContext;

exports.BindingModeBehavior = BindingModeBehavior;

exports.BindingTargetSubscriber = BindingTargetSubscriber;

exports.CSSModulesProcessorRegistry = CSSModulesProcessorRegistry;

exports.Case = Case;

exports.CheckedObserver = CheckedObserver;

exports.ChildrenBinding = ChildrenBinding;

exports.ClassAttributeAccessor = ClassAttributeAccessor;

exports.ComputedWatcher = ComputedWatcher;

exports.ContentBinding = ContentBinding;

exports.Controller = Controller;

exports.CustomAttribute = ut;

exports.CustomAttributeDefinition = CustomAttributeDefinition;

exports.CustomAttributeRenderer = $t;

exports.CustomElement = os;

exports.CustomElementDefinition = CustomElementDefinition;

exports.CustomElementRenderer = Ht;

exports.DataAttributeAccessor = DataAttributeAccessor;

exports.DebounceBindingBehavior = DebounceBindingBehavior;

exports.DefaultBindingLanguage = Os;

exports.DefaultBindingSyntax = Fs;

exports.DefaultCase = DefaultCase;

exports.DefaultComponents = _s;

exports.DefaultRenderers = $s;

exports.DefaultResources = Hs;

exports.Else = Else;

exports.EventModifier = EventModifier;

exports.EventModifierRegistration = Lt;

exports.ExpressionWatcher = ExpressionWatcher;

exports.FlushQueue = FlushQueue;

exports.Focus = Focus;

exports.FragmentNodeSequence = FragmentNodeSequence;

exports.FromViewBindingBehavior = FromViewBindingBehavior;

exports.FulfilledTemplateController = FulfilledTemplateController;

exports.IAppRoot = as;

exports.IAppTask = rt;

exports.IAuSlotWatcher = _t;

exports.IAuSlotsInfo = Pt;

exports.IAurelia = hs;

exports.IController = $e;

exports.IEventModifier = Tt;

exports.IEventTarget = Ge;

exports.IFlushQueue = bt;

exports.IHistory = Je;

exports.IHydrationContext = Ne;

exports.IKeyMapping = Rt;

exports.ILifecycleHooks = ft;

exports.IListenerBindingOptions = Xt;

exports.ILocation = Ze;

exports.IModifiedEventHandlerCreator = Et;

exports.INode = Ue;

exports.IPlatform = lt;

exports.IRenderLocation = Ke;

exports.IRenderer = Vt;

exports.IRendering = ue;

exports.IRepeatableHandler = Cs;

exports.IRepeatableHandlerResolver = ks;

exports.ISVGAnalyzer = cs;

exports.ISanitizer = Ps;

exports.IShadowDOMGlobalStyles = pe;

exports.IShadowDOMStyleFactory = fe;

exports.IShadowDOMStyles = de;

exports.ISignaler = st;

exports.IViewFactory = Mt;

exports.IWindow = Ye;

exports.If = If;

exports.InterpolationBinding = InterpolationBinding;

exports.InterpolationBindingRenderer = zt;

exports.InterpolationPartBinding = InterpolationPartBinding;

exports.IteratorBindingRenderer = Gt;

exports.LetBinding = LetBinding;

exports.LetElementRenderer = Wt;

exports.LifecycleHooks = dt;

exports.LifecycleHooksDefinition = LifecycleHooksDefinition;

exports.LifecycleHooksEntry = LifecycleHooksEntry;

exports.ListenerBinding = ListenerBinding;

exports.ListenerBindingOptions = ListenerBindingOptions;

exports.ListenerBindingRenderer = Qt;

exports.NodeObserverLocator = NodeObserverLocator;

exports.NoopSVGAnalyzer = NoopSVGAnalyzer;

exports.OneTimeBindingBehavior = OneTimeBindingBehavior;

exports.PendingTemplateController = PendingTemplateController;

exports.Portal = Portal;

exports.PromiseTemplateController = PromiseTemplateController;

exports.PropertyBinding = PropertyBinding;

exports.PropertyBindingRenderer = Ut;

exports.RefBinding = RefBinding;

exports.RefBindingRenderer = jt;

exports.RejectedTemplateController = RejectedTemplateController;

exports.Rendering = Rendering;

exports.Repeat = Repeat;

exports.RuntimeTemplateCompilerImplementation = us;

exports.SVGAnalyzer = SVGAnalyzer;

exports.SanitizeValueConverter = SanitizeValueConverter;

exports.Scope = Scope;

exports.SelectValueObserver = SelectValueObserver;

exports.SelfBindingBehavior = SelfBindingBehavior;

exports.SetAttributeRenderer = Yt;

exports.SetClassAttributeRenderer = Zt;

exports.SetPropertyRenderer = Ot;

exports.SetStyleAttributeRenderer = Jt;

exports.ShadowDOMRegistry = ShadowDOMRegistry;

exports.ShortHandBindingSyntax = Vs;

exports.SignalBindingBehavior = SignalBindingBehavior;

exports.SpreadRenderer = se;

exports.StandardConfiguration = Ns;

exports.State = He;

exports.StyleAttributeAccessor = StyleAttributeAccessor;

exports.StyleConfiguration = ge;

exports.StyleElementStyles = StyleElementStyles;

exports.StylePropertyBindingRenderer = te;

exports.Switch = Switch;

exports.TemplateControllerRenderer = Nt;

exports.TextBindingRenderer = Kt;

exports.ThrottleBindingBehavior = ThrottleBindingBehavior;

exports.ToViewBindingBehavior = ToViewBindingBehavior;

exports.TwoWayBindingBehavior = TwoWayBindingBehavior;

exports.UpdateTriggerBindingBehavior = UpdateTriggerBindingBehavior;

exports.ValueAttributeObserver = ValueAttributeObserver;

exports.ValueConverter = mt;

exports.ValueConverterDefinition = ValueConverterDefinition;

exports.ViewFactory = ViewFactory;

exports.Watch = at;

exports.With = With;

exports.alias = alias;

exports.astAssign = E;

exports.astBind = T;

exports.astEvaluate = R;

exports.astUnbind = L;

exports.bindable = bindable;

exports.bindingBehavior = bindingBehavior;

exports.capture = capture;

exports.children = children;

exports.coercer = coercer;

exports.containerless = containerless;

exports.convertToRenderLocation = convertToRenderLocation;

exports.cssModules = cssModules;

exports.customAttribute = customAttribute;

exports.customElement = customElement;

exports.getEffectiveParentNode = getEffectiveParentNode;

exports.getRef = getRef;

exports.isCustomElementController = isCustomElementController;

exports.isCustomElementViewModel = isCustomElementViewModel;

exports.isRenderLocation = isRenderLocation;

exports.lifecycleHooks = lifecycleHooks;

exports.mixinAstEvaluator = vt;

exports.mixinUseScope = xt;

exports.mixingBindingLimited = wt;

exports.processContent = processContent;

exports.registerAliases = registerAliases;

exports.registerHostNode = registerHostNode;

exports.renderer = renderer;

exports.setEffectiveParentNode = setEffectiveParentNode;

exports.setRef = setRef;

exports.shadowCSS = shadowCSS;

exports.slotted = slotted;

exports.templateController = templateController;

exports.useShadowDOM = useShadowDOM;

exports.valueConverter = valueConverter;

exports.watch = watch;
//# sourceMappingURL=index.cjs.map
