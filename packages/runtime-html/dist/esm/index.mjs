import { Protocol as t, getPrototypeChain as e, firstDefined as i, kebabCase as s, noop as n, DI as r, Registration as o, emptyArray as l, all as h, mergeArrays as a, IPlatform as c, IContainer as u, optional as f, InstanceProvider as d, resolveAll as m, onResolve as g, fromDefinitionOrDefault as p, pascalCase as v, fromAnnotationOrTypeOrDefault as x, fromAnnotationOrDefinitionOrTypeOrDefault as w, camelCase as b, toArray as y, ILogger as k, emptyObject as A, IServiceLocator as C, transient as B } from "@aurelia/kernel";

import { Metadata as R, isObject as S } from "@aurelia/metadata";

import { subscriberCollection as I, astEvaluate as T, ISignaler as P, connectable as E, astBind as L, astUnbind as D, astAssign as _, ConnectableSwitcher as U, ProxyObservable as $, IExpressionParser as q, IObserverLocator as M, Scope as F, ICoercionConfiguration as O, AccessScopeExpression as V, PrimitiveLiteralExpression as N, PropertyAccessor as j, INodeObserverLocator as H, getObserverLookup as W, SetterObserver as z, IDirtyChecker as G, createIndexMap as X, applyMutationsToIndices as K, getCollectionObserver as Q, synchronizeIndices as Y, BindingContext as Z } from "@aurelia/runtime";

import { TaskAbortError as J } from "@aurelia/platform";

import { BrowserPlatform as tt } from "@aurelia/platform-browser";

function et(t, e, i, s) {
    var n = arguments.length, r = n < 3 ? e : null === s ? s = Object.getOwnPropertyDescriptor(e, i) : s, o;
    if ("object" === typeof Reflect && "function" === typeof Reflect.decorate) r = Reflect.decorate(t, e, i, s); else for (var l = t.length - 1; l >= 0; l--) if (o = t[l]) r = (n < 3 ? o(r) : n > 3 ? o(e, i, r) : o(e, i)) || r;
    return n > 3 && r && Object.defineProperty(e, i, r), r;
}

function it(t, e) {
    return function(i, s) {
        e(i, s, t);
    };
}

const st = R.getOwn;

const nt = R.hasOwn;

const rt = R.define;

const {annotation: ot, resource: lt} = t;

const ht = ot.keyFor;

const at = lt.keyFor;

const ct = lt.appendTo;

const ut = ot.appendTo;

const ft = ot.getKeys;

const dt = Object;

const mt = dt.prototype;

const gt = () => dt.create(null);

const pt = t => new Error(t);

const vt = mt.hasOwnProperty;

const xt = dt.freeze;

const wt = dt.assign;

const bt = dt.getOwnPropertyNames;

const yt = dt.keys;

const kt = gt();

const At = (t, e, i) => {
    if (true === kt[e]) return true;
    if (!St(e)) return false;
    const s = e.slice(0, 5);
    return kt[e] = "aria-" === s || "data-" === s || i.isStandardSvgAttribute(t, e);
};

const Ct = t => t instanceof Promise;

const Bt = t => t instanceof Array;

const Rt = t => "function" === typeof t;

const St = t => "string" === typeof t;

const It = dt.defineProperty;

const Tt = t => {
    throw t;
};

const Pt = dt.is;

const Et = Reflect.defineProperty;

const Lt = (t, e, i) => {
    Et(t, e, {
        enumerable: false,
        configurable: true,
        writable: true,
        value: i
    });
    return i;
};

function Dt(t, e) {
    let i;
    function s(t, e) {
        if (arguments.length > 1) i.property = e;
        rt(Ut, BindableDefinition.create(e, t, i), t.constructor, e);
        ut(t.constructor, $t.keyFrom(e));
    }
    if (arguments.length > 1) {
        i = {};
        s(t, e);
        return;
    } else if (St(t)) {
        i = {};
        return s;
    }
    i = void 0 === t ? {} : t;
    return s;
}

function _t(t) {
    return t.startsWith(Ut);
}

const Ut = ht("bindable");

const $t = xt({
    name: Ut,
    keyFrom: t => `${Ut}:${t}`,
    from(t, ...e) {
        const i = {};
        const s = Array.isArray;
        function n(e) {
            i[e] = BindableDefinition.create(e, t);
        }
        function r(e, s) {
            i[e] = s instanceof BindableDefinition ? s : BindableDefinition.create(e, t, s);
        }
        function o(t) {
            if (s(t)) t.forEach(n); else if (t instanceof BindableDefinition) i[t.property] = t; else if (void 0 !== t) yt(t).forEach((e => r(e, t[e])));
        }
        e.forEach(o);
        return i;
    },
    for(t) {
        let e;
        const i = {
            add(s) {
                let n;
                let r;
                if (St(s)) {
                    n = s;
                    r = {
                        property: n
                    };
                } else {
                    n = s.property;
                    r = s;
                }
                e = BindableDefinition.create(n, t, r);
                if (!nt(Ut, t, n)) ut(t, $t.keyFrom(n));
                rt(Ut, e, t, n);
                return i;
            },
            mode(t) {
                e.mode = t;
                return i;
            },
            callback(t) {
                e.callback = t;
                return i;
            },
            attribute(t) {
                e.attribute = t;
                return i;
            },
            primary() {
                e.primary = true;
                return i;
            },
            set(t) {
                e.set = t;
                return i;
            }
        };
        return i;
    },
    getAll(t) {
        const i = Ut.length + 1;
        const s = [];
        const n = e(t);
        let r = n.length;
        let o = 0;
        let l;
        let h;
        let a;
        let c;
        while (--r >= 0) {
            a = n[r];
            l = ft(a).filter(_t);
            h = l.length;
            for (c = 0; c < h; ++c) s[o++] = st(Ut, a, l[c].slice(i));
        }
        return s;
    }
});

class BindableDefinition {
    constructor(t, e, i, s, n, r) {
        this.attribute = t;
        this.callback = e;
        this.mode = i;
        this.primary = s;
        this.property = n;
        this.set = r;
    }
    static create(t, e, n = {}) {
        return new BindableDefinition(i(n.attribute, s(t)), i(n.callback, `${t}Changed`), i(n.mode, 2), i(n.primary, false), i(n.property, t), i(n.set, Ft(t, e, n)));
    }
}

function qt(t, e, i) {
    Mt.define(t, e);
}

const Mt = {
    key: ht("coercer"),
    define(t, e) {
        rt(Mt.key, t[e].bind(t), t);
    },
    for(t) {
        return st(Mt.key, t);
    }
};

function Ft(t, e, i = {}) {
    const s = i.type ?? R.get("design:type", e, t) ?? null;
    if (null == s) return n;
    let r;
    switch (s) {
      case Number:
      case Boolean:
      case String:
      case BigInt:
        r = s;
        break;

      default:
        {
            const t = s.coerce;
            r = "function" === typeof t ? t.bind(s) : Mt.for(s) ?? n;
            break;
        }
    }
    return r === n ? r : Ot(r, i.nullable);
}

function Ot(t, e) {
    return function(i, s) {
        if (!s?.enableCoercion) return i;
        return (e ?? (s?.coerceNullish ?? false ? false : true)) && null == i ? i : t(i, s);
    };
}

class BindableObserver {
    get type() {
        return 1;
    }
    constructor(t, e, i, s, r, o) {
        this.set = s;
        this.$controller = r;
        this.i = o;
        this.v = void 0;
        this.ov = void 0;
        const l = t[i];
        const h = t.propertyChanged;
        const a = this.u = Rt(l);
        const c = this.A = Rt(h);
        const u = this.hs = s !== n;
        let f;
        this.o = t;
        this.k = e;
        this.C = c ? h : n;
        this.cb = a ? l : n;
        if (void 0 === this.cb && !c && !u) this.iO = false; else {
            this.iO = true;
            f = t[e];
            this.v = u && void 0 !== f ? s(f, this.i) : f;
            this.B();
        }
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        if (this.hs) t = this.set(t, this.i);
        const e = this.v;
        if (this.iO) {
            if (Pt(t, e)) return;
            this.v = t;
            this.ov = e;
            if (null == this.$controller || this.$controller.isBound) {
                if (this.u) this.cb.call(this.o, t, e);
                if (this.A) this.C.call(this.o, this.k, t, e);
            }
            this.subs.notify(this.v, this.ov);
        } else this.o[this.k] = t;
    }
    subscribe(t) {
        if (false === !this.iO) {
            this.iO = true;
            this.v = this.hs ? this.set(this.o[this.k], this.i) : this.o[this.k];
            this.B();
        }
        this.subs.add(t);
    }
    B() {
        Reflect.defineProperty(this.o, this.k, {
            enumerable: true,
            configurable: true,
            get: () => this.v,
            set: t => {
                this.setValue(t);
            }
        });
    }
}

I(BindableObserver);

const Vt = function(t) {
    function e(t, i, s) {
        r.inject(e)(t, i, s);
    }
    e.$isResolver = true;
    e.resolve = function(e, i) {
        if (i.root === i) return i.get(t);
        return i.has(t, false) ? i.get(t) : i.root.get(t);
    };
    return e;
};

const Nt = function(t) {
    function e(t, i, s) {
        r.inject(e)(t, i, s);
    }
    e.$isResolver = true;
    e.resolve = function(e, i) {
        if (i.root === i) return i.getAll(t, false);
        return i.has(t, false) ? i.getAll(t, false).concat(i.root.getAll(t, false)) : i.root.getAll(t, false);
    };
    return e;
};

const jt = r.createInterface;

const Ht = o.singleton;

const Wt = o.aliasTo;

const zt = o.instance;

o.callback;

const Gt = o.transient;

const Xt = (t, e, i) => t.registerResolver(e, i);

function Kt(...t) {
    return function(e) {
        const i = ht("aliases");
        const s = st(i, e);
        if (void 0 === s) rt(i, t, e); else s.push(...t);
    };
}

function Qt(t, e, i, s) {
    for (let n = 0, r = t.length; n < r; ++n) o.aliasTo(i, e.keyFrom(t[n])).register(s);
}

class CharSpec {
    constructor(t, e, i, s) {
        this.chars = t;
        this.repeat = e;
        this.isSymbol = i;
        this.isInverted = s;
        if (s) switch (t.length) {
          case 0:
            this.has = this.R;
            break;

          case 1:
            this.has = this.I;
            break;

          default:
            this.has = this.T;
        } else switch (t.length) {
          case 0:
            this.has = this.P;
            break;

          case 1:
            this.has = this.L;
            break;

          default:
            this.has = this._;
        }
    }
    equals(t) {
        return this.chars === t.chars && this.repeat === t.repeat && this.isSymbol === t.isSymbol && this.isInverted === t.isInverted;
    }
    _(t) {
        return this.chars.includes(t);
    }
    L(t) {
        return this.chars === t;
    }
    P(t) {
        return false;
    }
    T(t) {
        return !this.chars.includes(t);
    }
    I(t) {
        return this.chars !== t;
    }
    R(t) {
        return true;
    }
}

class Interpretation {
    constructor() {
        this.parts = l;
        this.U = "";
        this.$ = {};
        this.q = {};
    }
    get pattern() {
        const t = this.U;
        if ("" === t) return null; else return t;
    }
    set pattern(t) {
        if (null == t) {
            this.U = "";
            this.parts = l;
        } else {
            this.U = t;
            this.parts = this.q[t];
        }
    }
    append(t, e) {
        const i = this.$;
        if (void 0 === i[t]) i[t] = e; else i[t] += e;
    }
    next(t) {
        const e = this.$;
        let i;
        if (void 0 !== e[t]) {
            i = this.q;
            if (void 0 === i[t]) i[t] = [ e[t] ]; else i[t].push(e[t]);
            e[t] = void 0;
        }
    }
}

class AttrParsingState {
    get U() {
        return this.M ? this.F[0] : null;
    }
    constructor(t, ...e) {
        this.charSpec = t;
        this.O = [];
        this.V = null;
        this.M = false;
        this.F = e;
    }
    findChild(t) {
        const e = this.O;
        const i = e.length;
        let s = null;
        let n = 0;
        for (;n < i; ++n) {
            s = e[n];
            if (t.equals(s.charSpec)) return s;
        }
        return null;
    }
    append(t, e) {
        const i = this.F;
        if (!i.includes(e)) i.push(e);
        let s = this.findChild(t);
        if (null == s) {
            s = new AttrParsingState(t, e);
            this.O.push(s);
            if (t.repeat) s.O.push(s);
        }
        return s;
    }
    findMatches(t, e) {
        const i = [];
        const s = this.O;
        const n = s.length;
        let r = 0;
        let o = null;
        let l = 0;
        let h = 0;
        for (;l < n; ++l) {
            o = s[l];
            if (o.charSpec.has(t)) {
                i.push(o);
                r = o.F.length;
                h = 0;
                if (o.charSpec.isSymbol) for (;h < r; ++h) e.next(o.F[h]); else for (;h < r; ++h) e.append(o.F[h], t);
            }
        }
        return i;
    }
}

class StaticSegment {
    constructor(t) {
        this.text = t;
        const e = this.N = t.length;
        const i = this.j = [];
        let s = 0;
        for (;e > s; ++s) i.push(new CharSpec(t[s], false, false, false));
    }
    eachChar(t) {
        const e = this.N;
        const i = this.j;
        let s = 0;
        for (;e > s; ++s) t(i[s]);
    }
}

class DynamicSegment {
    constructor(t) {
        this.text = "PART";
        this.H = new CharSpec(t, true, false, true);
    }
    eachChar(t) {
        t(this.H);
    }
}

class SymbolSegment {
    constructor(t) {
        this.text = t;
        this.H = new CharSpec(t, false, true, false);
    }
    eachChar(t) {
        t(this.H);
    }
}

class SegmentTypes {
    constructor() {
        this.statics = 0;
        this.dynamics = 0;
        this.symbols = 0;
    }
}

const Yt = jt("ISyntaxInterpreter", (t => t.singleton(SyntaxInterpreter)));

class SyntaxInterpreter {
    constructor() {
        this.W = new AttrParsingState(null);
        this.G = [ this.W ];
    }
    add(t) {
        t = t.slice(0).sort(((t, e) => t.pattern > e.pattern ? 1 : -1));
        const e = t.length;
        let i;
        let s;
        let n;
        let r;
        let o;
        let l;
        let h;
        let a = 0;
        let c;
        while (e > a) {
            i = this.W;
            s = t[a];
            n = s.pattern;
            r = new SegmentTypes;
            o = this.X(s, r);
            l = o.length;
            h = t => i = i.append(t, n);
            for (c = 0; l > c; ++c) o[c].eachChar(h);
            i.V = r;
            i.M = true;
            ++a;
        }
    }
    interpret(t) {
        const e = new Interpretation;
        const i = t.length;
        let s = this.G;
        let n = 0;
        let r;
        for (;n < i; ++n) {
            s = this.K(s, t.charAt(n), e);
            if (0 === s.length) break;
        }
        s = s.filter(Zt);
        if (s.length > 0) {
            s.sort(Jt);
            r = s[0];
            if (!r.charSpec.isSymbol) e.next(r.U);
            e.pattern = r.U;
        }
        return e;
    }
    K(t, e, i) {
        const s = [];
        let n = null;
        const r = t.length;
        let o = 0;
        for (;o < r; ++o) {
            n = t[o];
            s.push(...n.findMatches(e, i));
        }
        return s;
    }
    X(t, e) {
        const i = [];
        const s = t.pattern;
        const n = s.length;
        const r = t.symbols;
        let o = 0;
        let l = 0;
        let h = "";
        while (o < n) {
            h = s.charAt(o);
            if (0 === r.length || !r.includes(h)) if (o === l) if ("P" === h && "PART" === s.slice(o, o + 4)) {
                l = o += 4;
                i.push(new DynamicSegment(r));
                ++e.dynamics;
            } else ++o; else ++o; else if (o !== l) {
                i.push(new StaticSegment(s.slice(l, o)));
                ++e.statics;
                l = o;
            } else {
                i.push(new SymbolSegment(s.slice(l, o + 1)));
                ++e.symbols;
                l = ++o;
            }
        }
        if (l !== o) {
            i.push(new StaticSegment(s.slice(l, o)));
            ++e.statics;
        }
        return i;
    }
}

function Zt(t) {
    return t.M;
}

function Jt(t, e) {
    const i = t.V;
    const s = e.V;
    if (i.statics !== s.statics) return s.statics - i.statics;
    if (i.dynamics !== s.dynamics) return s.dynamics - i.dynamics;
    if (i.symbols !== s.symbols) return s.symbols - i.symbols;
    return 0;
}

class AttrSyntax {
    constructor(t, e, i, s) {
        this.rawName = t;
        this.rawValue = e;
        this.target = i;
        this.command = s;
    }
}

const te = jt("IAttributePattern");

const ee = jt("IAttributeParser", (t => t.singleton(AttributeParser)));

class AttributeParser {
    constructor(t, e) {
        this.Y = {};
        this.Z = t;
        const i = this.F = {};
        const s = e.reduce(((t, e) => {
            const s = re(e.constructor);
            s.forEach((t => i[t.pattern] = e));
            return t.concat(s);
        }), l);
        t.add(s);
    }
    parse(t, e) {
        let i = this.Y[t];
        if (null == i) i = this.Y[t] = this.Z.interpret(t);
        const s = i.pattern;
        if (null == s) return new AttrSyntax(t, e, t, null); else return this.F[s][s](t, e, i.parts);
    }
}

AttributeParser.inject = [ Yt, h(te) ];

function ie(...t) {
    return function e(i) {
        return oe.define(t, i);
    };
}

class AttributePatternResourceDefinition {
    constructor(t) {
        this.Type = t;
        this.name = void 0;
    }
    register(t) {
        Ht(te, this.Type).register(t);
    }
}

const se = at("attribute-pattern");

const ne = "attribute-pattern-definitions";

const re = e => t.annotation.get(e, ne);

const oe = xt({
    name: se,
    definitionAnnotationKey: ne,
    define(e, i) {
        const s = new AttributePatternResourceDefinition(i);
        rt(se, s, i);
        ct(i, se);
        t.annotation.set(i, ne, e);
        ut(i, ne);
        return i;
    },
    getPatternDefinitions: re
});

let le = class DotSeparatedAttributePattern {
    "PART.PART"(t, e, i) {
        return new AttrSyntax(t, e, i[0], i[1]);
    }
    "PART.PART.PART"(t, e, i) {
        return new AttrSyntax(t, e, `${i[0]}.${i[1]}`, i[2]);
    }
};

le = et([ ie({
    pattern: "PART.PART",
    symbols: "."
}, {
    pattern: "PART.PART.PART",
    symbols: "."
}) ], le);

let he = class RefAttributePattern {
    ref(t, e, i) {
        return new AttrSyntax(t, e, "element", "ref");
    }
    "PART.ref"(t, e, i) {
        return new AttrSyntax(t, e, i[0], "ref");
    }
};

he = et([ ie({
    pattern: "ref",
    symbols: ""
}, {
    pattern: "PART.ref",
    symbols: "."
}) ], he);

let ae = class ColonPrefixedBindAttributePattern {
    ":PART"(t, e, i) {
        return new AttrSyntax(t, e, i[0], "bind");
    }
};

ae = et([ ie({
    pattern: ":PART",
    symbols: ":"
}) ], ae);

let ce = class AtPrefixedTriggerAttributePattern {
    "@PART"(t, e, i) {
        return new AttrSyntax(t, e, i[0], "trigger");
    }
};

ce = et([ ie({
    pattern: "@PART",
    symbols: "@"
}) ], ce);

let ue = class SpreadAttributePattern {
    "...$attrs"(t, e, i) {
        return new AttrSyntax(t, e, "", "...$attrs");
    }
};

ue = et([ ie({
    pattern: "...$attrs",
    symbols: ""
}) ], ue);

class AttributeObserver {
    constructor(t, e, i) {
        this.type = 2 | 1 | 4;
        this.v = null;
        this.ov = null;
        this.J = false;
        this.o = t;
        this.tt = e;
        this.et = i;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        this.v = t;
        this.J = t !== this.ov;
        this.it();
    }
    it() {
        if (this.J) {
            this.J = false;
            this.ov = this.v;
            switch (this.et) {
              case "class":
                this.o.classList.toggle(this.tt, !!this.v);
                break;

              case "style":
                {
                    let t = "";
                    let e = this.v;
                    if (St(e) && e.includes("!important")) {
                        t = "important";
                        e = e.replace("!important", "");
                    }
                    this.o.style.setProperty(this.tt, e, t);
                    break;
                }

              default:
                if (null == this.v) this.o.removeAttribute(this.et); else this.o.setAttribute(this.et, String(this.v));
            }
        }
    }
    handleMutation(t) {
        let e = false;
        for (let i = 0, s = t.length; s > i; ++i) {
            const s = t[i];
            if ("attributes" === s.type && s.attributeName === this.tt) {
                e = true;
                break;
            }
        }
        if (e) {
            let t;
            switch (this.et) {
              case "class":
                t = this.o.classList.contains(this.tt);
                break;

              case "style":
                t = this.o.style.getPropertyValue(this.tt);
                break;

              default:
                throw pt(`AUR0651:${this.et}`);
            }
            if (t !== this.v) {
                this.ov = this.v;
                this.v = t;
                this.J = false;
                this.st();
            }
        }
    }
    subscribe(t) {
        if (this.subs.add(t) && 1 === this.subs.count) {
            this.v = this.ov = this.o.getAttribute(this.tt);
            fe(this.o.ownerDocument.defaultView.MutationObserver, this.o, this);
        }
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) de(this.o, this);
    }
    st() {
        pe = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, pe);
    }
}

I(AttributeObserver);

const fe = (t, e, i) => {
    if (void 0 === e.$eMObs) e.$eMObs = new Set;
    if (void 0 === e.$mObs) (e.$mObs = new t(me)).observe(e, {
        attributes: true
    });
    e.$eMObs.add(i);
};

const de = (t, e) => {
    const i = t.$eMObs;
    if (i && i.delete(e)) {
        if (0 === i.size) {
            t.$mObs.disconnect();
            t.$mObs = void 0;
        }
        return true;
    }
    return false;
};

const me = t => {
    t[0].target.$eMObs.forEach(ge, t);
};

function ge(t) {
    t.handleMutation(this);
}

let pe;

function ve(t) {
    return function(e) {
        return be.define(t, e);
    };
}

class BindingBehaviorDefinition {
    constructor(t, e, i, s) {
        this.Type = t;
        this.name = e;
        this.aliases = i;
        this.key = s;
    }
    static create(t, e) {
        let s;
        let n;
        if (St(t)) {
            s = t;
            n = {
                name: s
            };
        } else {
            s = t.name;
            n = t;
        }
        return new BindingBehaviorDefinition(e, i(we(e, "name"), s), a(we(e, "aliases"), n.aliases, e.aliases), be.keyFrom(s));
    }
    register(t) {
        const {Type: e, key: i, aliases: s} = this;
        Ht(i, e).register(t);
        Wt(i, e).register(t);
        Qt(s, be, i, t);
    }
}

const xe = at("binding-behavior");

const we = (t, e) => st(ht(e), t);

const be = xt({
    name: xe,
    keyFrom(t) {
        return `${xe}:${t}`;
    },
    isType(t) {
        return Rt(t) && nt(xe, t);
    },
    define(t, e) {
        const i = BindingBehaviorDefinition.create(t, e);
        rt(xe, i, i.Type);
        rt(xe, i, i);
        ct(e, xe);
        return i.Type;
    },
    getDefinition(t) {
        const e = st(xe, t);
        if (void 0 === e) throw pt(`AUR0151:${t.name}`);
        return e;
    },
    annotate(t, e, i) {
        rt(ht(e), i, t);
    },
    getAnnotation: we
});

function ye(t) {
    return function(e) {
        return Ce.define(t, e);
    };
}

class ValueConverterDefinition {
    constructor(t, e, i, s) {
        this.Type = t;
        this.name = e;
        this.aliases = i;
        this.key = s;
    }
    static create(t, e) {
        let s;
        let n;
        if (St(t)) {
            s = t;
            n = {
                name: s
            };
        } else {
            s = t.name;
            n = t;
        }
        return new ValueConverterDefinition(e, i(Ae(e, "name"), s), a(Ae(e, "aliases"), n.aliases, e.aliases), Ce.keyFrom(s));
    }
    register(t) {
        const {Type: e, key: i, aliases: s} = this;
        o.singleton(i, e).register(t);
        o.aliasTo(i, e).register(t);
        Qt(s, Ce, i, t);
    }
}

const ke = at("value-converter");

const Ae = (t, e) => st(ht(e), t);

const Ce = xt({
    name: ke,
    keyFrom: t => `${ke}:${t}`,
    isType(t) {
        return Rt(t) && nt(ke, t);
    },
    define(t, e) {
        const i = ValueConverterDefinition.create(t, e);
        rt(ke, i, i.Type);
        rt(ke, i, i);
        ct(e, ke);
        return i.Type;
    },
    getDefinition(t) {
        const e = st(ke, t);
        if (void 0 === e) throw pt(`AUR0152:${t.name}`);
        return e;
    },
    annotate(t, e, i) {
        rt(ht(e), i, t);
    },
    getAnnotation: Ae
});

class BindingTargetSubscriber {
    constructor(t, e) {
        this.v = void 0;
        this.b = t;
        this.nt = e;
    }
    flush() {
        this.b.updateSource(this.v);
    }
    handleChange(t, e) {
        const i = this.b;
        if (t !== T(i.ast, i.s, i, null)) {
            this.v = t;
            this.nt.add(this);
        }
    }
}

const Be = t => {
    Lt(t.prototype, "useScope", (function(t) {
        this.s = t;
    }));
};

const Re = (t, e = true) => i => {
    const s = i.prototype;
    if (null != t) Et(s, "strict", {
        enumerable: true,
        get: function() {
            return t;
        }
    });
    Et(s, "strictFnCall", {
        enumerable: true,
        get: function() {
            return e;
        }
    });
    Lt(s, "get", (function(t) {
        return this.l.get(t);
    }));
    Lt(s, "getSignaler", (function() {
        return this.l.root.get(P);
    }));
    Lt(s, "getConverter", (function(t) {
        const e = Ce.keyFrom(t);
        let i = Se.get(this);
        if (null == i) Se.set(this, i = new ResourceLookup);
        return i[e] ?? (i[e] = this.l.get(Vt(e)));
    }));
    Lt(s, "getBehavior", (function(t) {
        const e = be.keyFrom(t);
        let i = Se.get(this);
        if (null == i) Se.set(this, i = new ResourceLookup);
        return i[e] ?? (i[e] = this.l.get(Vt(e)));
    }));
};

const Se = new WeakMap;

class ResourceLookup {}

const Ie = jt("IFlushQueue", (t => t.singleton(FlushQueue)));

class FlushQueue {
    constructor() {
        this.rt = false;
        this.ot = new Set;
    }
    get count() {
        return this.ot.size;
    }
    add(t) {
        this.ot.add(t);
        if (this.rt) return;
        this.rt = true;
        try {
            this.ot.forEach(Te);
        } finally {
            this.rt = false;
        }
    }
    clear() {
        this.ot.clear();
        this.rt = false;
    }
}

function Te(t, e, i) {
    i.delete(t);
    t.flush();
}

const Pe = new WeakSet;

const Ee = (t, e) => {
    Lt(t.prototype, "limit", (function(t) {
        if (Pe.has(this)) throw pt(`AURXXXX: a rate limit has already been applied.`);
        Pe.add(this);
        const i = e(this, t);
        const s = this[i];
        const n = (...t) => s.call(this, ...t);
        const r = "debounce" === t.type ? Le(t, n, this) : De(t, n, this);
        this[i] = r;
        return {
            dispose: () => {
                Pe.delete(this);
                r.dispose();
                delete this[i];
            }
        };
    }));
};

const Le = (t, e, i) => {
    let s;
    let n;
    let r;
    const o = t.queue;
    const l = l => {
        r = l;
        if (i.isBound) {
            n = s;
            s = o.queueTask((() => e(r)), {
                delay: t.delay,
                reusable: false
            });
            n?.cancel();
        } else e(r);
    };
    l.dispose = () => {
        n?.cancel();
        s?.cancel();
    };
    return l;
};

const De = (t, e, i) => {
    let s;
    let n;
    let r = 0;
    let o = 0;
    let l;
    const h = t.queue;
    const a = () => t.now();
    const c = c => {
        l = c;
        if (i.isBound) {
            o = a() - r;
            n = s;
            if (o > t.delay) {
                r = a();
                e(l);
            } else s = h.queueTask((() => {
                r = a();
                e(l);
            }), {
                delay: t.delay - o,
                reusable: false
            });
            n?.cancel();
        } else e(l);
    };
    c.dispose = () => {
        n?.cancel();
        s?.cancel();
    };
    return c;
};

const _e = {
    reusable: false,
    preempt: true
};

class AttributeBinding {
    constructor(t, e, i, s, n, r, o, l, h) {
        this.targetAttribute = o;
        this.targetProperty = l;
        this.mode = h;
        this.isBound = false;
        this.s = void 0;
        this.lt = null;
        this.v = void 0;
        this.boundFn = false;
        this.l = e;
        this.ast = n;
        this.ht = t;
        this.target = r;
        this.oL = i;
        this.ct = s;
    }
    updateTarget(t) {
        this.ut.setValue(t, this.target, this.targetProperty);
    }
    handleChange() {
        if (!this.isBound) return;
        let t;
        this.obs.version++;
        const e = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (e !== this.v) {
            this.v = e;
            const i = 1 !== this.ht.state && (4 & this.ut.type) > 0;
            if (i) {
                t = this.lt;
                this.lt = this.ct.queueTask((() => {
                    this.lt = null;
                    this.updateTarget(e);
                }), _e);
                t?.cancel();
            } else this.updateTarget(e);
        }
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        L(this.ast, t, this);
        this.ut ?? (this.ut = new AttributeObserver(this.target, this.targetProperty, this.targetAttribute));
        if (this.mode & (2 | 1)) this.updateTarget(this.v = T(this.ast, t, this, (2 & this.mode) > 0 ? this : null));
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        D(this.ast, this.s, this);
        this.s = void 0;
        this.v = void 0;
        this.lt?.cancel();
        this.lt = null;
        this.obs.clearAll();
    }
}

Be(AttributeBinding);

Ee(AttributeBinding, (() => "updateTarget"));

E(AttributeBinding);

Re(true)(AttributeBinding);

const Ue = {
    reusable: false,
    preempt: true
};

class InterpolationBinding {
    constructor(t, e, i, s, n, r, o, l) {
        this.ast = n;
        this.target = r;
        this.targetProperty = o;
        this.mode = l;
        this.isBound = false;
        this.s = void 0;
        this.lt = null;
        this.ht = t;
        this.oL = i;
        this.ct = s;
        this.ut = i.getAccessor(r, o);
        const h = n.expressions;
        const a = this.partBindings = Array(h.length);
        const c = h.length;
        let u = 0;
        for (;c > u; ++u) a[u] = new InterpolationPartBinding(h[u], r, o, e, i, this);
    }
    ft() {
        this.updateTarget();
    }
    updateTarget() {
        const t = this.partBindings;
        const e = this.ast.parts;
        const i = t.length;
        let s = "";
        let n = 0;
        if (1 === i) s = e[0] + t[0].v + e[1]; else {
            s = e[0];
            for (;i > n; ++n) s += t[n].v + e[n + 1];
        }
        const r = this.ut;
        const o = 1 !== this.ht.state && (4 & r.type) > 0;
        let l;
        if (o) {
            l = this.lt;
            this.lt = this.ct.queueTask((() => {
                this.lt = null;
                r.setValue(s, this.target, this.targetProperty);
            }), Ue);
            l?.cancel();
            l = null;
        } else r.setValue(s, this.target, this.targetProperty);
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        const e = this.partBindings;
        const i = e.length;
        let s = 0;
        for (;i > s; ++s) e[s].bind(t);
        this.updateTarget();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        this.s = void 0;
        const t = this.partBindings;
        const e = t.length;
        let i = 0;
        for (;e > i; ++i) t[i].unbind();
        this.lt?.cancel();
        this.lt = null;
    }
}

class InterpolationPartBinding {
    constructor(t, e, i, s, n, r) {
        this.ast = t;
        this.target = e;
        this.targetProperty = i;
        this.owner = r;
        this.mode = 2;
        this.task = null;
        this.isBound = false;
        this.v = "";
        this.boundFn = false;
        this.l = s;
        this.oL = n;
    }
    updateTarget() {
        this.owner.ft();
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (t != this.v) {
            this.v = t;
            if (Bt(t)) this.observeCollection(t);
            this.updateTarget();
        }
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        L(this.ast, t, this);
        this.v = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        if (Bt(this.v)) this.observeCollection(this.v);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        D(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

Be(InterpolationPartBinding);

Ee(InterpolationPartBinding, (() => "updateTarget"));

E(InterpolationPartBinding);

Re(true)(InterpolationPartBinding);

class ContentBinding {
    constructor(t, e, i, s, n, r, o, l) {
        this.p = n;
        this.ast = r;
        this.target = o;
        this.strict = l;
        this.isBound = false;
        this.mode = 2;
        this.lt = null;
        this.v = "";
        this.boundFn = false;
        this.l = e;
        this.ht = t;
        this.oL = i;
        this.ct = s;
    }
    updateTarget(t) {
        const e = this.target;
        const i = this.p.Node;
        const s = this.v;
        this.v = t;
        if (s instanceof i) s.parentNode?.removeChild(s);
        if (t instanceof i) {
            e.textContent = "";
            e.parentNode?.insertBefore(t, e);
        } else e.textContent = String(t);
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (t === this.v) {
            this.lt?.cancel();
            this.lt = null;
            return;
        }
        const e = 1 !== this.ht.state;
        if (e) this.dt(t); else this.updateTarget(t);
    }
    handleCollectionChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = this.v = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (Bt(t)) this.observeCollection(t);
        const e = 1 !== this.ht.state;
        if (e) this.dt(t); else this.updateTarget(t);
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        L(this.ast, t, this);
        const e = this.v = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        if (Bt(e)) this.observeCollection(e);
        this.updateTarget(e);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        D(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
        this.lt?.cancel();
        this.lt = null;
    }
    dt(t) {
        const e = this.lt;
        this.lt = this.ct.queueTask((() => {
            this.lt = null;
            this.updateTarget(t);
        }), Ue);
        e?.cancel();
    }
}

Be(ContentBinding);

Ee(ContentBinding, (() => "updateTarget"));

E()(ContentBinding);

Re(void 0, false)(ContentBinding);

class LetBinding {
    constructor(t, e, i, s, n = false) {
        this.ast = i;
        this.targetProperty = s;
        this.isBound = false;
        this.s = void 0;
        this.target = null;
        this.boundFn = false;
        this.l = t;
        this.oL = e;
        this.gt = n;
    }
    updateTarget() {
        this.target[this.targetProperty] = this.v;
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        this.v = T(this.ast, this.s, this, this);
        this.obs.clear();
        this.updateTarget();
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        this.target = this.gt ? t.bindingContext : t.overrideContext;
        L(this.ast, t, this);
        this.v = T(this.ast, this.s, this, this);
        this.updateTarget();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        D(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

Be(LetBinding);

Ee(LetBinding, (() => "updateTarget"));

E(LetBinding);

Re(true)(LetBinding);

class PropertyBinding {
    constructor(t, e, i, s, n, r, o, l) {
        this.ast = n;
        this.target = r;
        this.targetProperty = o;
        this.mode = l;
        this.isBound = false;
        this.s = void 0;
        this.ut = void 0;
        this.lt = null;
        this.vt = null;
        this.boundFn = false;
        this.l = e;
        this.ht = t;
        this.ct = s;
        this.oL = i;
    }
    updateTarget(t) {
        this.ut.setValue(t, this.target, this.targetProperty);
    }
    updateSource(t) {
        _(this.ast, this.s, this, t);
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = T(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        const e = 1 !== this.ht.state && (4 & this.ut.type) > 0;
        if (e) {
            $e = this.lt;
            this.lt = this.ct.queueTask((() => {
                this.updateTarget(t);
                this.lt = null;
            }), qe);
            $e?.cancel();
            $e = null;
        } else this.updateTarget(t);
    }
    handleCollectionChange() {
        this.handleChange();
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        L(this.ast, t, this);
        const e = this.oL;
        const i = this.mode;
        let s = this.ut;
        if (!s) {
            if (4 & i) s = e.getObserver(this.target, this.targetProperty); else s = e.getAccessor(this.target, this.targetProperty);
            this.ut = s;
        }
        const n = (2 & i) > 0;
        if (i & (2 | 1)) this.updateTarget(T(this.ast, this.s, this, n ? this : null));
        if (4 & i) {
            s.subscribe(this.vt ?? (this.vt = new BindingTargetSubscriber(this, this.l.get(Ie))));
            if (!n) this.updateSource(s.getValue(this.target, this.targetProperty));
        }
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        D(this.ast, this.s, this);
        this.s = void 0;
        if (this.vt) {
            this.ut.unsubscribe(this.vt);
            this.vt = null;
        }
        this.lt?.cancel();
        this.lt = null;
        this.obs.clearAll();
    }
    useTargetObserver(t) {
        this.ut?.unsubscribe(this);
        (this.ut = t).subscribe(this);
    }
    useTargetSubscriber(t) {
        if (null != this.vt) throw pt(`AURxxxx: binding already has a target subscriber`);
        this.vt = t;
    }
}

Be(PropertyBinding);

Ee(PropertyBinding, (t => 4 & t.mode ? "updateSource" : "updateTarget"));

E(PropertyBinding);

Re(true, false)(PropertyBinding);

let $e = null;

const qe = {
    reusable: false,
    preempt: true
};

class RefBinding {
    constructor(t, e, i) {
        this.ast = e;
        this.target = i;
        this.isBound = false;
        this.s = void 0;
        this.l = t;
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        L(this.ast, t, this);
        _(this.ast, this.s, this, this.target);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        if (T(this.ast, this.s, this, null) === this.target) _(this.ast, this.s, this, null);
        D(this.ast, this.s, this);
        this.s = void 0;
    }
}

Re(false)(RefBinding);

class ListenerBindingOptions {
    constructor(t, e = false) {
        this.prevent = t;
        this.capture = e;
    }
}

class ListenerBinding {
    constructor(t, e, i, s, n) {
        this.ast = e;
        this.target = i;
        this.targetEvent = s;
        this.isBound = false;
        this.self = false;
        this.boundFn = true;
        this.l = t;
        this.xt = n;
    }
    callSource(t) {
        const e = this.s.overrideContext;
        e.$event = t;
        let i = T(this.ast, this.s, this, null);
        delete e.$event;
        if (Rt(i)) i = i(t);
        if (true !== i && this.xt.prevent) t.preventDefault();
        return i;
    }
    handleEvent(t) {
        if (this.self) if (this.target !== t.composedPath()[0]) return;
        this.callSource(t);
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        L(this.ast, t, this);
        this.target.addEventListener(this.targetEvent, this, this.xt);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        D(this.ast, this.s, this);
        this.s = void 0;
        this.target.removeEventListener(this.targetEvent, this, this.xt);
    }
}

Be(ListenerBinding);

Ee(ListenerBinding, (() => "callSource"));

Re(true, true)(ListenerBinding);

const Me = jt("IAppTask");

class $AppTask {
    constructor(t, e, i) {
        this.c = void 0;
        this.slot = t;
        this.k = e;
        this.cb = i;
    }
    register(t) {
        return this.c = t.register(zt(Me, this));
    }
    run() {
        const t = this.k;
        const e = this.cb;
        return null === t ? e() : e(this.c.get(t));
    }
}

const Fe = xt({
    creating: Oe("creating"),
    hydrating: Oe("hydrating"),
    hydrated: Oe("hydrated"),
    activating: Oe("activating"),
    activated: Oe("activated"),
    deactivating: Oe("deactivating"),
    deactivated: Oe("deactivated")
});

function Oe(t) {
    function e(e, i) {
        if (Rt(i)) return new $AppTask(t, e, i);
        return new $AppTask(t, null, e);
    }
    return e;
}

function Ve(t, e) {
    if (null == t) throw pt(`AUR0772`);
    return function i(s, n, r) {
        const o = null == n;
        const l = o ? s : s.constructor;
        const h = new WatchDefinition(t, o ? e : r.value);
        if (o) {
            if (!Rt(e) && (null == e || !(e in l.prototype))) throw pt(`AUR0773:${String(e)}@${l.name}}`);
        } else if (!Rt(r?.value)) throw pt(`AUR0774:${String(n)}`);
        He.add(l, h);
        if (Qe(l)) Je(l).watches.push(h);
        if (sn(l)) on(l).watches.push(h);
    };
}

class WatchDefinition {
    constructor(t, e) {
        this.expression = t;
        this.callback = e;
    }
}

const Ne = l;

const je = ht("watch");

const He = xt({
    name: je,
    add(t, e) {
        let i = st(je, t);
        if (null == i) rt(je, i = [], t);
        i.push(e);
    },
    getAnnotation(t) {
        return st(je, t) ?? Ne;
    }
});

function We(t) {
    return function(e) {
        return Ze(t, e);
    };
}

function ze(t) {
    return function(e) {
        return Ze(St(t) ? {
            isTemplateController: true,
            name: t
        } : {
            isTemplateController: true,
            ...t
        }, e);
    };
}

class CustomAttributeDefinition {
    get type() {
        return 2;
    }
    constructor(t, e, i, s, n, r, o, l, h, a) {
        this.Type = t;
        this.name = e;
        this.aliases = i;
        this.key = s;
        this.defaultBindingMode = n;
        this.isTemplateController = r;
        this.bindables = o;
        this.noMultiBindings = l;
        this.watches = h;
        this.dependencies = a;
    }
    static create(t, e) {
        let s;
        let n;
        if (St(t)) {
            s = t;
            n = {
                name: s
            };
        } else {
            s = t.name;
            n = t;
        }
        return new CustomAttributeDefinition(e, i(Ke(e, "name"), s), a(Ke(e, "aliases"), n.aliases, e.aliases), Xe(s), i(Ke(e, "defaultBindingMode"), n.defaultBindingMode, e.defaultBindingMode, 2), i(Ke(e, "isTemplateController"), n.isTemplateController, e.isTemplateController, false), $t.from(e, ...$t.getAll(e), Ke(e, "bindables"), e.bindables, n.bindables), i(Ke(e, "noMultiBindings"), n.noMultiBindings, e.noMultiBindings, false), a(He.getAnnotation(e), e.watches), a(Ke(e, "dependencies"), n.dependencies, e.dependencies));
    }
    register(t) {
        const {Type: e, key: i, aliases: s} = this;
        Gt(i, e).register(t);
        Wt(i, e).register(t);
        Qt(s, ti, i, t);
    }
}

const Ge = at("custom-attribute");

const Xe = t => `${Ge}:${t}`;

const Ke = (t, e) => st(ht(e), t);

const Qe = t => Rt(t) && nt(Ge, t);

const Ye = (t, e) => Bs(t, Xe(e)) ?? void 0;

const Ze = (t, e) => {
    const i = CustomAttributeDefinition.create(t, e);
    rt(Ge, i, i.Type);
    rt(Ge, i, i);
    ct(e, Ge);
    return i.Type;
};

const Je = t => {
    const e = st(Ge, t);
    if (void 0 === e) throw pt(`AUR0759:${t.name}`);
    return e;
};

const ti = xt({
    name: Ge,
    keyFrom: Xe,
    isType: Qe,
    for: Ye,
    define: Ze,
    getDefinition: Je,
    annotate(t, e, i) {
        rt(ht(e), i, t);
    },
    getAnnotation: Ke
});

function ei(t, e) {
    let i;
    function s(t, e) {
        if (arguments.length > 1) i.property = e;
        rt(si, ChildrenDefinition.create(e, i), t.constructor, e);
        ut(t.constructor, ni.keyFrom(e));
    }
    if (arguments.length > 1) {
        i = {};
        s(t, e);
        return;
    } else if (St(t)) {
        i = {};
        return s;
    }
    i = void 0 === t ? {} : t;
    return s;
}

function ii(t) {
    return t.startsWith(si);
}

const si = ht("children-observer");

const ni = xt({
    name: si,
    keyFrom: t => `${si}:${t}`,
    from(...t) {
        const e = {};
        function i(t) {
            e[t] = ChildrenDefinition.create(t);
        }
        function s(t, i) {
            e[t] = ChildrenDefinition.create(t, i);
        }
        function n(t) {
            if (Bt(t)) t.forEach(i); else if (t instanceof ChildrenDefinition) e[t.property] = t; else if (void 0 !== t) yt(t).forEach((e => s(e, t)));
        }
        t.forEach(n);
        return e;
    },
    getAll(t) {
        const i = si.length + 1;
        const s = [];
        const n = e(t);
        let r = n.length;
        let o = 0;
        let l;
        let h;
        let a;
        while (--r >= 0) {
            a = n[r];
            l = ft(a).filter(ii);
            h = l.length;
            for (let t = 0; t < h; ++t) s[o++] = st(si, a, l[t].slice(i));
        }
        return s;
    }
});

const ri = {
    childList: true
};

class ChildrenDefinition {
    constructor(t, e, i, s, n, r) {
        this.callback = t;
        this.property = e;
        this.options = i;
        this.query = s;
        this.filter = n;
        this.map = r;
    }
    static create(t, e = {}) {
        return new ChildrenDefinition(i(e.callback, `${t}Changed`), i(e.property, t), e.options ?? ri, e.query, e.filter, e.map);
    }
}

class ChildrenObserver {
    constructor(t, e, i, s, n = oi, r = li, o = hi, l) {
        this.controller = t;
        this.obj = e;
        this.propertyKey = i;
        this.query = n;
        this.filter = r;
        this.map = o;
        this.options = l;
        this.observing = false;
        this.children = void 0;
        this.observer = void 0;
        this.callback = e[s];
        Reflect.defineProperty(this.obj, this.propertyKey, {
            enumerable: true,
            configurable: true,
            get: () => this.getValue(),
            set: () => {}
        });
    }
    getValue() {
        return this.observing ? this.children : this.get();
    }
    setValue(t) {}
    start() {
        if (!this.observing) {
            this.observing = true;
            this.children = this.get();
            (this.observer ?? (this.observer = new this.controller.host.ownerDocument.defaultView.MutationObserver((() => {
                this.wt();
            })))).observe(this.controller.host, this.options);
        }
    }
    stop() {
        if (this.observing) {
            this.observing = false;
            this.observer.disconnect();
            this.children = l;
        }
    }
    wt() {
        this.children = this.get();
        if (void 0 !== this.callback) this.callback.call(this.obj);
        this.subs.notify(this.children, void 0);
    }
    get() {
        return ci(this.controller, this.query, this.filter, this.map);
    }
}

I()(ChildrenObserver);

function oi(t) {
    return t.host.childNodes;
}

function li(t, e, i) {
    return !!i;
}

function hi(t, e, i) {
    return i;
}

const ai = {
    optional: true
};

function ci(t, e, i, s) {
    const n = e(t);
    const r = n.length;
    const o = [];
    let l;
    let h;
    let a;
    let c = 0;
    for (;c < r; ++c) {
        l = n[c];
        h = nn(l, ai);
        a = h?.viewModel ?? null;
        if (i(l, h, a)) o.push(s(l, h, a));
    }
    return o;
}

const ui = c;

const fi = (t, e, i, s) => {
    t.addEventListener(e, i, s);
};

const di = (t, e, i, s) => {
    t.removeEventListener(e, i, s);
};

const mi = t => {
    let e;
    const i = t.prototype;
    Lt(i, "subscribe", (function(t) {
        if (this.subs.add(t) && 1 === this.subs.count) {
            for (e of this.cf.events) fi(this.bt, e, this);
            this.yt = true;
            this.kt?.();
        }
    }));
    Lt(i, "unsubscribe", (function(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) {
            for (e of this.cf.events) di(this.bt, e, this);
            this.yt = false;
            this.At?.();
        }
    }));
    Lt(i, "useConfig", (function(t) {
        this.cf = t;
        if (this.yt) {
            for (e of this.cf.events) di(this.bt, e, this);
            for (e of this.cf.events) fi(this.bt, e, this);
        }
    }));
};

const gi = t => {
    Lt(t.prototype, "subscribe", n);
    Lt(t.prototype, "unsubscribe", n);
};

class ClassAttributeAccessor {
    get doNotCache() {
        return true;
    }
    constructor(t) {
        this.obj = t;
        this.type = 2 | 4;
        this.value = "";
        this.ov = "";
        this.Ct = {};
        this.Bt = 0;
        this.J = false;
    }
    getValue() {
        return this.value;
    }
    setValue(t) {
        this.value = t;
        this.J = t !== this.ov;
        this.it();
    }
    it() {
        if (this.J) {
            this.J = false;
            const t = this.value;
            const e = this.Ct;
            const i = pi(t);
            let s = this.Bt;
            this.ov = t;
            if (i.length > 0) this.Rt(i);
            this.Bt += 1;
            if (0 === s) return;
            s -= 1;
            for (const t in e) {
                if (!vt.call(e, t) || e[t] !== s) continue;
                this.obj.classList.remove(t);
            }
        }
    }
    Rt(t) {
        const e = this.obj;
        const i = t.length;
        let s = 0;
        let n;
        for (;s < i; s++) {
            n = t[s];
            if (0 === n.length) continue;
            this.Ct[n] = this.Bt;
            e.classList.add(n);
        }
    }
}

function pi(t) {
    if (St(t)) return vi(t);
    if ("object" !== typeof t) return l;
    if (t instanceof Array) {
        const e = t.length;
        if (e > 0) {
            const i = [];
            let s = 0;
            for (;e > s; ++s) i.push(...pi(t[s]));
            return i;
        } else return l;
    }
    const e = [];
    let i;
    for (i in t) if (Boolean(t[i])) if (i.includes(" ")) e.push(...vi(i)); else e.push(i);
    return e;
}

function vi(t) {
    const e = t.match(/\S+/g);
    if (null === e) return l;
    return e;
}

gi(ClassAttributeAccessor);

function xi(...t) {
    return new CSSModulesProcessorRegistry(t);
}

class CSSModulesProcessorRegistry {
    constructor(t) {
        this.modules = t;
    }
    register(t) {
        var e;
        const i = wt({}, ...this.modules);
        const s = Ze({
            name: "class",
            bindables: [ "value" ],
            noMultiBindings: true
        }, (e = class CustomAttributeClass {
            constructor(t) {
                this.St = new ClassAttributeAccessor(t);
            }
            binding() {
                this.valueChanged();
            }
            valueChanged() {
                this.St.setValue(this.value?.split(/\s+/g).map((t => i[t] || t)) ?? "");
            }
        }, e.inject = [ Ss ], e));
        t.register(s, zt(Ps, i));
    }
}

function wi(...t) {
    return new ShadowDOMRegistry(t);
}

const bi = jt("IShadowDOMStyleFactory", (t => t.cachedCallback((t => {
    if (AdoptedStyleSheetsStyles.supported(t.get(ui))) return t.get(AdoptedStyleSheetsStylesFactory);
    return t.get(StyleElementStylesFactory);
}))));

class ShadowDOMRegistry {
    constructor(t) {
        this.css = t;
    }
    register(t) {
        const e = t.get(ki);
        const i = t.get(bi);
        t.register(zt(yi, i.createStyles(this.css, e)));
    }
}

class AdoptedStyleSheetsStylesFactory {
    constructor(t) {
        this.p = t;
        this.cache = new Map;
    }
    createStyles(t, e) {
        return new AdoptedStyleSheetsStyles(this.p, t, this.cache, e);
    }
}

AdoptedStyleSheetsStylesFactory.inject = [ ui ];

class StyleElementStylesFactory {
    constructor(t) {
        this.p = t;
    }
    createStyles(t, e) {
        return new StyleElementStyles(this.p, t, e);
    }
}

StyleElementStylesFactory.inject = [ ui ];

const yi = jt("IShadowDOMStyles");

const ki = jt("IShadowDOMGlobalStyles", (t => t.instance({
    applyTo: n
})));

class AdoptedStyleSheetsStyles {
    constructor(t, e, i, s = null) {
        this.sharedStyles = s;
        this.styleSheets = e.map((e => {
            let s;
            if (e instanceof t.CSSStyleSheet) s = e; else {
                s = i.get(e);
                if (void 0 === s) {
                    s = new t.CSSStyleSheet;
                    s.replaceSync(e);
                    i.set(e, s);
                }
            }
            return s;
        }));
    }
    static supported(t) {
        return "adoptedStyleSheets" in t.ShadowRoot.prototype;
    }
    applyTo(t) {
        if (null !== this.sharedStyles) this.sharedStyles.applyTo(t);
        t.adoptedStyleSheets = [ ...t.adoptedStyleSheets, ...this.styleSheets ];
    }
}

class StyleElementStyles {
    constructor(t, e, i = null) {
        this.p = t;
        this.localStyles = e;
        this.sharedStyles = i;
    }
    applyTo(t) {
        const e = this.localStyles;
        const i = this.p;
        for (let s = e.length - 1; s > -1; --s) {
            const n = i.document.createElement("style");
            n.innerHTML = e[s];
            t.prepend(n);
        }
        if (null !== this.sharedStyles) this.sharedStyles.applyTo(t);
    }
}

const Ai = {
    shadowDOM(t) {
        return Fe.creating(u, (e => {
            if (null != t.sharedStyles) {
                const i = e.get(bi);
                e.register(zt(ki, i.createStyles(t.sharedStyles, null)));
            }
        }));
    }
};

const {enter: Ci, exit: Bi} = U;

const {wrap: Ri, unwrap: Si} = $;

class ComputedWatcher {
    get value() {
        return this.v;
    }
    constructor(t, e, i, s, n) {
        this.obj = t;
        this.$get = i;
        this.useProxy = n;
        this.isBound = false;
        this.running = false;
        this.v = void 0;
        this.cb = s;
        this.oL = e;
    }
    handleChange() {
        this.run();
    }
    handleCollectionChange() {
        this.run();
    }
    bind() {
        if (this.isBound) return;
        this.compute();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        this.obs.clearAll();
    }
    run() {
        if (!this.isBound || this.running) return;
        const t = this.obj;
        const e = this.v;
        const i = this.compute();
        if (!Pt(i, e)) this.cb.call(t, i, e, t);
    }
    compute() {
        this.running = true;
        this.obs.version++;
        try {
            Ci(this);
            return this.v = Si(this.$get.call(void 0, this.useProxy ? Ri(this.obj) : this.obj, this));
        } finally {
            this.obs.clear();
            this.running = false;
            Bi(this);
        }
    }
}

class ExpressionWatcher {
    get value() {
        return this.v;
    }
    constructor(t, e, i, s, n) {
        this.scope = t;
        this.l = e;
        this.oL = i;
        this.isBound = false;
        this.boundFn = false;
        this.obj = t.bindingContext;
        this.It = s;
        this.cb = n;
    }
    handleChange(t) {
        const e = this.It;
        const i = this.obj;
        const s = this.v;
        const n = 1 === e.$kind && 1 === this.obs.count;
        if (!n) {
            this.obs.version++;
            t = T(e, this.scope, this, this);
            this.obs.clear();
        }
        if (!Pt(t, s)) {
            this.v = t;
            this.cb.call(i, t, s, i);
        }
    }
    bind() {
        if (this.isBound) return;
        this.obs.version++;
        this.v = T(this.It, this.scope, this, this);
        this.obs.clear();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        this.obs.clearAll();
        this.v = void 0;
    }
}

E(ComputedWatcher);

E(ExpressionWatcher);

Re(true)(ExpressionWatcher);

const Ii = jt("ILifecycleHooks");

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
        const i = new Set;
        let s = e.prototype;
        while (s !== mt) {
            for (const t of bt(s)) if ("constructor" !== t) i.add(t);
            s = Object.getPrototypeOf(s);
        }
        return new LifecycleHooksDefinition(e, i);
    }
    register(t) {
        Ht(Ii, this.Type).register(t);
    }
}

const Ti = new WeakMap;

const Pi = ht("lifecycle-hooks");

const Ei = xt({
    name: Pi,
    define(t, e) {
        const i = LifecycleHooksDefinition.create(t, e);
        rt(Pi, i, e);
        ct(e, Pi);
        return i.Type;
    },
    resolve(t) {
        let e = Ti.get(t);
        if (void 0 === e) {
            Ti.set(t, e = new LifecycleHooksLookupImpl);
            const i = t.root;
            const s = i.id === t.id ? t.getAll(Ii) : t.has(Ii, false) ? i.getAll(Ii).concat(t.getAll(Ii)) : i.getAll(Ii);
            let n;
            let r;
            let o;
            let l;
            let h;
            for (n of s) {
                r = st(Pi, n.constructor);
                o = new LifecycleHooksEntry(r, n);
                for (l of r.propertyNames) {
                    h = e[l];
                    if (void 0 === h) e[l] = [ o ]; else h.push(o);
                }
            }
        }
        return e;
    }
});

class LifecycleHooksLookupImpl {}

function Li() {
    return function t(e) {
        return Ei.define({}, e);
    };
}

const Di = jt("IViewFactory");

class ViewFactory {
    constructor(t, e) {
        this.isCaching = false;
        this.Y = null;
        this.Tt = -1;
        this.name = e.name;
        this.container = t;
        this.def = e;
    }
    setCacheSize(t, e) {
        if (t) {
            if ("*" === t) t = ViewFactory.maxCacheSize; else if (St(t)) t = parseInt(t, 10);
            if (-1 === this.Tt || !e) this.Tt = t;
        }
        if (this.Tt > 0) this.Y = []; else this.Y = null;
        this.isCaching = this.Tt > 0;
    }
    canReturnToCache(t) {
        return null != this.Y && this.Y.length < this.Tt;
    }
    tryReturnToCache(t) {
        if (this.canReturnToCache(t)) {
            this.Y.push(t);
            return true;
        }
        return false;
    }
    create(t) {
        const e = this.Y;
        let i;
        if (null != e && e.length > 0) {
            i = e.pop();
            return i;
        }
        i = Controller.$view(this, t);
        return i;
    }
}

ViewFactory.maxCacheSize = 65535;

const _i = jt("IRendering", (t => t.singleton(Rendering)));

class Rendering {
    get renderers() {
        return this.Pt ?? (this.Pt = this.Et.getAll(bn, false).reduce(((t, e) => {
            t[e.target] = e;
            return t;
        }), gt()));
    }
    constructor(t) {
        this.Lt = new WeakMap;
        this.Dt = new WeakMap;
        const e = t.root;
        this.p = (this.Et = e).get(ui);
        this.ep = e.get(q);
        this.oL = e.get(M);
        this._t = new FragmentNodeSequence(this.p, this.p.document.createDocumentFragment());
    }
    compile(t, e, i) {
        if (false !== t.needsCompile) {
            const s = this.Lt;
            const n = e.get(wn);
            let r = s.get(t);
            if (null == r) s.set(t, r = n.compile(t, e, i)); else e.register(...r.dependencies);
            return r;
        }
        return t;
    }
    getViewFactory(t, e) {
        return new ViewFactory(e, CustomElementDefinition.getOrCreate(t));
    }
    createNodes(t) {
        if (true === t.enhance) return new FragmentNodeSequence(this.p, t.template);
        let e;
        let i = false;
        const s = this.Dt;
        const n = this.p;
        const r = n.document;
        if (s.has(t)) e = s.get(t); else {
            const o = t.template;
            let l;
            if (null === o) e = null; else if (o instanceof n.Node) if ("TEMPLATE" === o.nodeName) {
                e = o.content;
                i = true;
            } else (e = r.createDocumentFragment()).appendChild(o.cloneNode(true)); else {
                l = r.createElement("template");
                if (St(o)) l.innerHTML = o;
                e = l.content;
                i = true;
            }
            s.set(t, e);
        }
        return null == e ? this._t : new FragmentNodeSequence(this.p, i ? r.importNode(e, true) : r.adoptNode(e.cloneNode(true)));
    }
    render(t, e, i, s) {
        const n = i.instructions;
        const r = this.renderers;
        const o = e.length;
        if (e.length !== n.length) throw pt(`AUR0757:${o}<>${n.length}`);
        let l = 0;
        let h = 0;
        let a = 0;
        let c;
        let u;
        let f;
        if (o > 0) while (o > l) {
            c = n[l];
            f = e[l];
            h = 0;
            a = c.length;
            while (a > h) {
                u = c[h];
                r[u.type].render(t, f, u, this.p, this.ep, this.oL);
                ++h;
            }
            ++l;
        }
        if (null != s) {
            c = i.surrogates;
            if ((a = c.length) > 0) {
                h = 0;
                while (a > h) {
                    u = c[h];
                    r[u.type].render(t, s, u, this.p, this.ep, this.oL);
                    ++h;
                }
            }
        }
    }
}

Rendering.inject = [ u ];

var Ui;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["host"] = 1] = "host";
    t[t["shadowRoot"] = 2] = "shadowRoot";
    t[t["location"] = 3] = "location";
})(Ui || (Ui = {}));

const $i = {
    optional: true
};

const qi = new WeakMap;

class Controller {
    get lifecycleHooks() {
        return this.Ut;
    }
    get isActive() {
        return (this.state & (1 | 2)) > 0 && 0 === (4 & this.state);
    }
    get name() {
        if (null === this.parent) switch (this.vmKind) {
          case 1:
            return `[${this.definition.name}]`;

          case 0:
            return this.definition.name;

          case 2:
            return this.viewFactory.name;
        }
        switch (this.vmKind) {
          case 1:
            return `${this.parent.name}>[${this.definition.name}]`;

          case 0:
            return `${this.parent.name}>${this.definition.name}`;

          case 2:
            return this.viewFactory.name === this.parent.definition?.name ? `${this.parent.name}[view]` : `${this.parent.name}[view:${this.viewFactory.name}]`;
        }
    }
    get hooks() {
        return this.$t;
    }
    get viewModel() {
        return this.qt;
    }
    set viewModel(t) {
        this.qt = t;
        this.$t = null == t || 2 === this.vmKind ? HooksDefinition.none : new HooksDefinition(t);
    }
    constructor(t, e, i, s, n, r, o) {
        this.container = t;
        this.vmKind = e;
        this.definition = i;
        this.viewFactory = s;
        this.host = r;
        this.head = null;
        this.tail = null;
        this.next = null;
        this.parent = null;
        this.bindings = null;
        this.children = null;
        this.hasLockedScope = false;
        this.isStrictBinding = false;
        this.scope = null;
        this.isBound = false;
        this.hostController = null;
        this.mountTarget = 0;
        this.shadowRoot = null;
        this.nodes = null;
        this.location = null;
        this.Ut = null;
        this.state = 0;
        this.Mt = false;
        this.Ft = l;
        this.$initiator = null;
        this.$resolve = void 0;
        this.$reject = void 0;
        this.$promise = void 0;
        this.Ot = 0;
        this.Vt = 0;
        this.Nt = 0;
        this.qt = n;
        this.$t = 2 === e ? HooksDefinition.none : new HooksDefinition(n);
        this.location = o;
        this.r = t.root.get(_i);
    }
    static getCached(t) {
        return qi.get(t);
    }
    static getCachedOrThrow(t) {
        const e = Controller.getCached(t);
        if (void 0 === e) throw pt(`AUR0500:${t}`);
        return e;
    }
    static $el(t, e, i, s, n = void 0, r = null) {
        if (qi.has(e)) return qi.get(e);
        n = n ?? on(e.constructor);
        const o = new Controller(t, 0, n, null, e, i, r);
        const l = t.get(f(Yi));
        if (n.dependencies.length > 0) t.register(...n.dependencies);
        Xt(t, Yi, new d("IHydrationContext", new HydrationContext(o, s, l)));
        qi.set(e, o);
        if (null == s || false !== s.hydrate) o.hE(s, l);
        return o;
    }
    static $attr(t, e, i, s) {
        if (qi.has(e)) return qi.get(e);
        s = s ?? Je(e.constructor);
        const n = new Controller(t, 1, s, null, e, i, null);
        if (s.dependencies.length > 0) t.register(...s.dependencies);
        qi.set(e, n);
        n.jt();
        return n;
    }
    static $view(t, e = void 0) {
        const i = new Controller(t.container, 2, null, t, null, null, null);
        i.parent = e ?? null;
        i.Ht();
        return i;
    }
    hE(t, e) {
        const i = this.container;
        const s = this.qt;
        let n = this.definition;
        this.scope = F.create(s, null, true);
        if (n.watches.length > 0) ji(this, i, n, s);
        Fi(this, n, s);
        this.Ft = Oi(this, n, s);
        if (this.$t.hasDefine) {
            const t = s.define(this, e, n);
            if (void 0 !== t && t !== n) n = CustomElementDefinition.getOrCreate(t);
        }
        this.Ut = Ei.resolve(i);
        n.register(i);
        if (null !== n.injectable) Xt(i, n.injectable, new d("definition.injectable", s));
        if (null == t || false !== t.hydrate) {
            this.hS(t);
            this.hC();
        }
    }
    hS(t) {
        if (void 0 !== this.Ut.hydrating) this.Ut.hydrating.forEach(ts, this);
        if (this.$t.hasHydrating) this.qt.hydrating(this);
        const e = this.Wt = this.r.compile(this.definition, this.container, t);
        const {shadowOptions: i, isStrictBinding: s, hasSlots: n, containerless: r} = e;
        let o = this.location;
        this.isStrictBinding = s;
        if (null !== (this.hostController = nn(this.host, $i))) {
            this.host = this.container.root.get(ui).document.createElement(this.definition.name);
            if (r && null == o) o = this.location = _s(this.host);
        }
        Rs(this.host, Ys, this);
        Rs(this.host, this.definition.key, this);
        if (null !== i || n) {
            if (null != o) throw pt(`AUR0501`);
            Rs(this.shadowRoot = this.host.attachShadow(i ?? zi), Ys, this);
            Rs(this.shadowRoot, this.definition.key, this);
            this.mountTarget = 2;
        } else if (null != o) {
            Rs(o, Ys, this);
            Rs(o, this.definition.key, this);
            this.mountTarget = 3;
        } else this.mountTarget = 1;
        this.qt.$controller = this;
        this.nodes = this.r.createNodes(e);
        if (void 0 !== this.Ut.hydrated) this.Ut.hydrated.forEach(es, this);
        if (this.$t.hasHydrated) this.qt.hydrated(this);
    }
    hC() {
        this.r.render(this, this.nodes.findTargets(), this.Wt, this.host);
        if (void 0 !== this.Ut.created) this.Ut.created.forEach(Ji, this);
        if (this.$t.hasCreated) this.qt.created(this);
    }
    jt() {
        const t = this.definition;
        const e = this.qt;
        if (t.watches.length > 0) ji(this, this.container, t, e);
        Fi(this, t, e);
        e.$controller = this;
        this.Ut = Ei.resolve(this.container);
        if (void 0 !== this.Ut.created) this.Ut.created.forEach(Ji, this);
        if (this.$t.hasCreated) this.qt.created(this);
    }
    Ht() {
        this.Wt = this.r.compile(this.viewFactory.def, this.container, null);
        this.isStrictBinding = this.Wt.isStrictBinding;
        this.r.render(this, (this.nodes = this.r.createNodes(this.Wt)).findTargets(), this.Wt, void 0);
    }
    activate(t, e, i) {
        switch (this.state) {
          case 0:
          case 8:
            if (!(null === e || e.isActive)) return;
            this.state = 1;
            break;

          case 2:
            return;

          case 32:
            throw pt(`AUR0502:${this.name}`);

          default:
            throw pt(`AUR0503:${this.name} ${Ki(this.state)}`);
        }
        this.parent = e;
        switch (this.vmKind) {
          case 0:
            this.scope.parent = i ?? null;
            break;

          case 1:
            this.scope = i ?? null;
            break;

          case 2:
            if (void 0 === i || null === i) throw pt(`AUR0504`);
            if (!this.hasLockedScope) this.scope = i;
            break;
        }
        if (this.isStrictBinding) ;
        this.$initiator = t;
        this.zt();
        let s;
        if (2 !== this.vmKind && null != this.Ut.binding) s = m(...this.Ut.binding.map(is, this));
        if (this.$t.hasBinding) s = m(s, this.qt.binding(this.$initiator, this.parent));
        if (Ct(s)) {
            this.Gt();
            s.then((() => {
                this.bind();
            })).catch((t => {
                this.Xt(t);
            }));
            return this.$promise;
        }
        this.bind();
        return this.$promise;
    }
    bind() {
        let t = 0;
        let e = this.Ft.length;
        let i;
        if (e > 0) while (e > t) {
            this.Ft[t].start();
            ++t;
        }
        if (null !== this.bindings) {
            t = 0;
            e = this.bindings.length;
            while (e > t) {
                this.bindings[t].bind(this.scope);
                ++t;
            }
        }
        if (2 !== this.vmKind && null != this.Ut.bound) i = m(...this.Ut.bound.map(ss, this));
        if (this.$t.hasBound) i = m(i, this.qt.bound(this.$initiator, this.parent));
        if (Ct(i)) {
            this.Gt();
            i.then((() => {
                this.isBound = true;
                this.Kt();
            })).catch((t => {
                this.Xt(t);
            }));
            return;
        }
        this.isBound = true;
        this.Kt();
    }
    Qt(...t) {
        switch (this.mountTarget) {
          case 1:
            this.host.append(...t);
            break;

          case 2:
            this.shadowRoot.append(...t);
            break;

          case 3:
            {
                let e = 0;
                for (;e < t.length; ++e) this.location.parentNode.insertBefore(t[e], this.location);
                break;
            }
        }
    }
    Kt() {
        if (null !== this.hostController) switch (this.mountTarget) {
          case 1:
          case 2:
            this.hostController.Qt(this.host);
            break;

          case 3:
            this.hostController.Qt(this.location.$start, this.location);
            break;
        }
        switch (this.mountTarget) {
          case 1:
            this.nodes.appendTo(this.host, null != this.definition && this.definition.enhance);
            break;

          case 2:
            {
                const t = this.container;
                const e = t.has(yi, false) ? t.get(yi) : t.get(ki);
                e.applyTo(this.shadowRoot);
                this.nodes.appendTo(this.shadowRoot);
                break;
            }

          case 3:
            this.nodes.insertBefore(this.location);
            break;
        }
        let t = 0;
        let e;
        if (2 !== this.vmKind && null != this.Ut.attaching) e = m(...this.Ut.attaching.map(ns, this));
        if (this.$t.hasAttaching) e = m(e, this.qt.attaching(this.$initiator, this.parent));
        if (Ct(e)) {
            this.Gt();
            this.zt();
            e.then((() => {
                this.Yt();
            })).catch((t => {
                this.Xt(t);
            }));
        }
        if (null !== this.children) for (;t < this.children.length; ++t) void this.children[t].activate(this.$initiator, this, this.scope);
        this.Yt();
    }
    deactivate(t, e) {
        switch (~16 & this.state) {
          case 2:
            this.state = 4;
            break;

          case 0:
          case 8:
          case 32:
          case 8 | 32:
            return;

          default:
            throw pt(`AUR0505:${this.name} ${Ki(this.state)}`);
        }
        this.$initiator = t;
        if (t === this) this.Zt();
        let i = 0;
        let s;
        if (this.Ft.length) for (;i < this.Ft.length; ++i) this.Ft[i].stop();
        if (null !== this.children) for (i = 0; i < this.children.length; ++i) void this.children[i].deactivate(t, this);
        if (2 !== this.vmKind && null != this.Ut.detaching) s = m(...this.Ut.detaching.map(os, this));
        if (this.$t.hasDetaching) s = m(s, this.qt.detaching(this.$initiator, this.parent));
        if (Ct(s)) {
            this.Gt();
            t.Zt();
            s.then((() => {
                t.Jt();
            })).catch((e => {
                t.Xt(e);
            }));
        }
        if (null === t.head) t.head = this; else t.tail.next = this;
        t.tail = this;
        if (t !== this) return;
        this.Jt();
        return this.$promise;
    }
    removeNodes() {
        switch (this.vmKind) {
          case 0:
          case 2:
            this.nodes.remove();
            this.nodes.unlink();
        }
        if (null !== this.hostController) switch (this.mountTarget) {
          case 1:
          case 2:
            this.host.remove();
            break;

          case 3:
            this.location.$start.remove();
            this.location.remove();
            break;
        }
    }
    unbind() {
        let t = 0;
        if (null !== this.bindings) for (;t < this.bindings.length; ++t) this.bindings[t].unbind();
        this.parent = null;
        switch (this.vmKind) {
          case 1:
            this.scope = null;
            break;

          case 2:
            if (!this.hasLockedScope) this.scope = null;
            if (16 === (16 & this.state) && !this.viewFactory.tryReturnToCache(this) && this.$initiator === this) this.dispose();
            break;

          case 0:
            this.scope.parent = null;
            break;
        }
        this.state = 8;
        this.$initiator = null;
        this.te();
    }
    Gt() {
        if (void 0 === this.$promise) {
            this.$promise = new Promise(((t, e) => {
                this.$resolve = t;
                this.$reject = e;
            }));
            if (this.$initiator !== this) this.parent.Gt();
        }
    }
    te() {
        if (void 0 !== this.$promise) {
            hs = this.$resolve;
            this.$resolve = this.$reject = this.$promise = void 0;
            hs();
            hs = void 0;
        }
    }
    Xt(t) {
        if (void 0 !== this.$promise) {
            as = this.$reject;
            this.$resolve = this.$reject = this.$promise = void 0;
            as(t);
            as = void 0;
        }
        if (this.$initiator !== this) this.parent.Xt(t);
    }
    zt() {
        ++this.Ot;
        if (this.$initiator !== this) this.parent.zt();
    }
    Yt() {
        if (0 === --this.Ot) {
            if (2 !== this.vmKind && null != this.Ut.attached) cs = m(...this.Ut.attached.map(rs, this));
            if (this.$t.hasAttached) cs = m(cs, this.qt.attached(this.$initiator));
            if (Ct(cs)) {
                this.Gt();
                cs.then((() => {
                    this.state = 2;
                    this.te();
                    if (this.$initiator !== this) this.parent.Yt();
                })).catch((t => {
                    this.Xt(t);
                }));
                cs = void 0;
                return;
            }
            cs = void 0;
            this.state = 2;
            this.te();
        }
        if (this.$initiator !== this) this.parent.Yt();
    }
    Zt() {
        ++this.Vt;
    }
    Jt() {
        if (0 === --this.Vt) {
            this.ee();
            this.removeNodes();
            let t = this.$initiator.head;
            let e;
            while (null !== t) {
                if (t !== this) {
                    if (t.debug) t.logger.trace(`detach()`);
                    t.removeNodes();
                }
                if (2 !== t.vmKind && null != t.Ut.unbinding) e = m(...t.Ut.unbinding.map(ls, this));
                if (t.$t.hasUnbinding) {
                    if (t.debug) t.logger.trace("unbinding()");
                    e = m(e, t.viewModel.unbinding(t.$initiator, t.parent));
                }
                if (Ct(e)) {
                    this.Gt();
                    this.ee();
                    e.then((() => {
                        this.ie();
                    })).catch((t => {
                        this.Xt(t);
                    }));
                }
                e = void 0;
                t = t.next;
            }
            this.ie();
        }
    }
    ee() {
        ++this.Nt;
    }
    ie() {
        if (0 === --this.Nt) {
            let t = this.$initiator.head;
            let e = null;
            while (null !== t) {
                if (t !== this) {
                    t.isBound = false;
                    t.unbind();
                }
                e = t.next;
                t.next = null;
                t = e;
            }
            this.head = this.tail = null;
            this.isBound = false;
            this.unbind();
        }
    }
    addBinding(t) {
        if (null === this.bindings) this.bindings = [ t ]; else this.bindings[this.bindings.length] = t;
    }
    addChild(t) {
        if (null === this.children) this.children = [ t ]; else this.children[this.children.length] = t;
    }
    is(t) {
        switch (this.vmKind) {
          case 1:
            return Je(this.qt.constructor).name === t;

          case 0:
            return on(this.qt.constructor).name === t;

          case 2:
            return this.viewFactory.name === t;
        }
    }
    lockScope(t) {
        this.scope = t;
        this.hasLockedScope = true;
    }
    setHost(t) {
        if (0 === this.vmKind) {
            Rs(t, Ys, this);
            Rs(t, this.definition.key, this);
        }
        this.host = t;
        this.mountTarget = 1;
        return this;
    }
    setShadowRoot(t) {
        if (0 === this.vmKind) {
            Rs(t, Ys, this);
            Rs(t, this.definition.key, this);
        }
        this.shadowRoot = t;
        this.mountTarget = 2;
        return this;
    }
    setLocation(t) {
        if (0 === this.vmKind) {
            Rs(t, Ys, this);
            Rs(t, this.definition.key, this);
        }
        this.location = t;
        this.mountTarget = 3;
        return this;
    }
    release() {
        this.state |= 16;
    }
    dispose() {
        if (32 === (32 & this.state)) return;
        this.state |= 32;
        if (this.$t.hasDispose) this.qt.dispose();
        if (null !== this.children) {
            this.children.forEach(Zi);
            this.children = null;
        }
        this.hostController = null;
        this.scope = null;
        this.nodes = null;
        this.location = null;
        this.viewFactory = null;
        if (null !== this.qt) {
            qi.delete(this.qt);
            this.qt = null;
        }
        this.qt = null;
        this.host = null;
        this.shadowRoot = null;
        this.container.disposeResolvers();
    }
    accept(t) {
        if (true === t(this)) return true;
        if (this.$t.hasAccept && true === this.qt.accept(t)) return true;
        if (null !== this.children) {
            const {children: e} = this;
            for (let i = 0, s = e.length; i < s; ++i) if (true === e[i].accept(t)) return true;
        }
    }
}

function Mi(t) {
    let e = t.$observers;
    if (void 0 === e) Reflect.defineProperty(t, "$observers", {
        enumerable: false,
        value: e = {}
    });
    return e;
}

function Fi(t, e, i) {
    const s = e.bindables;
    const n = bt(s);
    const r = n.length;
    if (r > 0) {
        let e;
        let o;
        let l = 0;
        const h = Mi(i);
        const a = t.container;
        const c = a.has(O, true) ? a.get(O) : null;
        for (;l < r; ++l) {
            e = n[l];
            if (void 0 === h[e]) {
                o = s[e];
                h[e] = new BindableObserver(i, e, o.callback, o.set, t, c);
            }
        }
    }
}

function Oi(t, e, i) {
    const s = e.childrenObservers;
    const n = bt(s);
    const r = n.length;
    if (r > 0) {
        const e = Mi(i);
        const o = [];
        let l;
        let h = 0;
        let a;
        for (;h < r; ++h) {
            l = n[h];
            if (null == e[l]) {
                a = s[l];
                o[o.length] = e[l] = new ChildrenObserver(t, i, l, a.callback, a.query, a.filter, a.map, a.options);
            }
        }
        return o;
    }
    return l;
}

const Vi = new Map;

const Ni = t => {
    let e = Vi.get(t);
    if (null == e) {
        e = new V(t, 0);
        Vi.set(t, e);
    }
    return e;
};

function ji(t, e, i, s) {
    const n = e.get(M);
    const r = e.get(q);
    const o = i.watches;
    const l = 0 === t.vmKind ? t.scope : F.create(s, null, true);
    const h = o.length;
    let a;
    let c;
    let u;
    let f = 0;
    for (;h > f; ++f) {
        ({expression: a, callback: c} = o[f]);
        c = Rt(c) ? c : Reflect.get(s, c);
        if (!Rt(c)) throw pt(`AUR0506:${String(c)}`);
        if (Rt(a)) t.addBinding(new ComputedWatcher(s, n, a, c, true)); else {
            u = St(a) ? r.parse(a, 16) : Ni(a);
            t.addBinding(new ExpressionWatcher(l, e, n, u, c));
        }
    }
}

function Hi(t) {
    return t instanceof Controller && 0 === t.vmKind;
}

function Wi(t) {
    return S(t) && sn(t.constructor);
}

class HooksDefinition {
    constructor(t) {
        this.hasDefine = "define" in t;
        this.hasHydrating = "hydrating" in t;
        this.hasHydrated = "hydrated" in t;
        this.hasCreated = "created" in t;
        this.hasBinding = "binding" in t;
        this.hasBound = "bound" in t;
        this.hasAttaching = "attaching" in t;
        this.hasAttached = "attached" in t;
        this.hasDetaching = "detaching" in t;
        this.hasUnbinding = "unbinding" in t;
        this.hasDispose = "dispose" in t;
        this.hasAccept = "accept" in t;
    }
}

HooksDefinition.none = new HooksDefinition({});

const zi = {
    mode: "open"
};

var Gi;

(function(t) {
    t[t["customElement"] = 0] = "customElement";
    t[t["customAttribute"] = 1] = "customAttribute";
    t[t["synthetic"] = 2] = "synthetic";
})(Gi || (Gi = {}));

var Xi;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["activating"] = 1] = "activating";
    t[t["activated"] = 2] = "activated";
    t[t["deactivating"] = 4] = "deactivating";
    t[t["deactivated"] = 8] = "deactivated";
    t[t["released"] = 16] = "released";
    t[t["disposed"] = 32] = "disposed";
})(Xi || (Xi = {}));

function Ki(t) {
    const e = [];
    if (1 === (1 & t)) e.push("activating");
    if (2 === (2 & t)) e.push("activated");
    if (4 === (4 & t)) e.push("deactivating");
    if (8 === (8 & t)) e.push("deactivated");
    if (16 === (16 & t)) e.push("released");
    if (32 === (32 & t)) e.push("disposed");
    return 0 === e.length ? "none" : e.join("|");
}

const Qi = jt("IController");

const Yi = jt("IHydrationContext");

class HydrationContext {
    constructor(t, e, i) {
        this.instruction = e;
        this.parent = i;
        this.controller = t;
    }
}

function Zi(t) {
    t.dispose();
}

function Ji(t) {
    t.instance.created(this.qt, this);
}

function ts(t) {
    t.instance.hydrating(this.qt, this);
}

function es(t) {
    t.instance.hydrated(this.qt, this);
}

function is(t) {
    return t.instance.binding(this.qt, this["$initiator"], this.parent);
}

function ss(t) {
    return t.instance.bound(this.qt, this["$initiator"], this.parent);
}

function ns(t) {
    return t.instance.attaching(this.qt, this["$initiator"], this.parent);
}

function rs(t) {
    return t.instance.attached(this.qt, this["$initiator"]);
}

function os(t) {
    return t.instance.detaching(this.qt, this["$initiator"], this.parent);
}

function ls(t) {
    return t.instance.unbinding(this.qt, this["$initiator"], this.parent);
}

let hs;

let as;

let cs;

const us = jt("IAppRoot");

class AppRoot {
    constructor(t, e, i, s) {
        this.config = t;
        this.platform = e;
        this.container = i;
        this.controller = void 0;
        this.se = void 0;
        this.host = t.host;
        s.prepare(this);
        Xt(i, e.HTMLElement, Xt(i, e.Element, Xt(i, Ss, new d("ElementResolver", t.host))));
        this.se = g(this.ne("creating"), (() => {
            const e = t.component;
            const s = i.createChild();
            let n;
            if (sn(e)) n = this.container.get(e); else n = t.component;
            const r = {
                hydrate: false,
                projections: null
            };
            const o = this.controller = Controller.$el(s, n, this.host, r);
            o.hE(r, null);
            return g(this.ne("hydrating"), (() => {
                o.hS(null);
                return g(this.ne("hydrated"), (() => {
                    o.hC();
                    this.se = void 0;
                }));
            }));
        }));
    }
    activate() {
        return g(this.se, (() => g(this.ne("activating"), (() => g(this.controller.activate(this.controller, null, void 0), (() => this.ne("activated")))))));
    }
    deactivate() {
        return g(this.ne("deactivating"), (() => g(this.controller.deactivate(this.controller, null), (() => this.ne("deactivated")))));
    }
    ne(t) {
        return m(...this.container.getAll(Me).reduce(((e, i) => {
            if (i.slot === t) e.push(i.run());
            return e;
        }), []));
    }
    dispose() {
        this.controller?.dispose();
    }
}

const fs = "au-start";

const ds = "au-end";

const ms = (t, e) => t.document.createElement(e);

const gs = (t, e) => t.document.createComment(e);

const ps = t => {
    const e = gs(t, ds);
    e.$start = gs(t, fs);
    return e;
};

const vs = (t, e) => t.document.createTextNode(e);

const xs = (t, e, i) => t.insertBefore(e, i);

const ws = (t, e, i) => {
    if (null === t) return;
    const s = i.length;
    let n = 0;
    while (s > n) {
        t.insertBefore(i[n], e);
        ++n;
    }
};

const bs = t => t.previousSibling;

const ys = (t, e) => t.content.appendChild(e);

const ks = (t, e) => {
    const i = e.length;
    let s = 0;
    while (i > s) {
        t.content.appendChild(e[s]);
        ++s;
    }
};

const As = t => {
    const e = t.previousSibling;
    let i;
    if (8 === e?.nodeType && "au-end" === e.textContent) {
        i = e;
        if (null == (i.$start = i.previousSibling)) throw Cs();
        t.parentNode?.removeChild(t);
        return i;
    } else throw Cs();
};

const Cs = () => pt(`AURxxxx`);

class Refs {}

function Bs(t, e) {
    return t.$au?.[e] ?? null;
}

function Rs(t, e, i) {
    var s;
    ((s = t).$au ?? (s.$au = new Refs))[e] = i;
}

const Ss = jt("INode");

const Is = jt("IEventTarget", (t => t.cachedCallback((t => {
    if (t.has(us, true)) return t.get(us).host;
    return t.get(ui).document;
}))));

const Ts = jt("IRenderLocation");

const Ps = jt("CssModules");

const Es = new WeakMap;

function Ls(t) {
    if (Es.has(t)) return Es.get(t);
    let e = 0;
    let i = t.nextSibling;
    while (null !== i) {
        if (8 === i.nodeType) switch (i.textContent) {
          case "au-start":
            ++e;
            break;

          case "au-end":
            if (0 === e--) return i;
        }
        i = i.nextSibling;
    }
    if (null === t.parentNode && 11 === t.nodeType) {
        const e = nn(t);
        if (void 0 === e) return null;
        if (2 === e.mountTarget) return Ls(e.host);
    }
    return t.parentNode;
}

function Ds(t, e) {
    if (void 0 !== t.platform && !(t instanceof t.platform.Node)) {
        const i = t.childNodes;
        for (let t = 0, s = i.length; t < s; ++t) Es.set(i[t], e);
    } else Es.set(t, e);
}

function _s(t) {
    if (Us(t)) return t;
    const e = t.ownerDocument.createComment("au-end");
    const i = e.$start = t.ownerDocument.createComment("au-start");
    const s = t.parentNode;
    if (null !== s) {
        s.replaceChild(e, t);
        s.insertBefore(i, e);
    }
    return e;
}

function Us(t) {
    return "au-end" === t.textContent;
}

class FragmentNodeSequence {
    get firstChild() {
        return this.re;
    }
    get lastChild() {
        return this.oe;
    }
    constructor(t, e) {
        this.platform = t;
        this.next = void 0;
        this.le = false;
        this.he = false;
        this.ref = null;
        this.f = e;
        const i = e.querySelectorAll(".au");
        let s = 0;
        let n = i.length;
        let r;
        let o = this.t = Array(n);
        while (n > s) {
            r = i[s];
            if ("AU-M" === r.nodeName) o[s] = As(r); else o[s] = r;
            ++s;
        }
        const l = e.childNodes;
        const h = this.childNodes = Array(n = l.length);
        s = 0;
        while (n > s) {
            h[s] = l[s];
            ++s;
        }
        this.re = e.firstChild;
        this.oe = e.lastChild;
    }
    findTargets() {
        return this.t;
    }
    insertBefore(t) {
        if (this.he && !!this.ref) this.addToLinked(); else {
            const e = t.parentNode;
            if (this.le) {
                let i = this.re;
                let s;
                const n = this.oe;
                while (null != i) {
                    s = i.nextSibling;
                    e.insertBefore(i, t);
                    if (i === n) break;
                    i = s;
                }
            } else {
                this.le = true;
                t.parentNode.insertBefore(this.f, t);
            }
        }
    }
    appendTo(t, e = false) {
        if (this.le) {
            let e = this.re;
            let i;
            const s = this.oe;
            while (null != e) {
                i = e.nextSibling;
                t.appendChild(e);
                if (e === s) break;
                e = i;
            }
        } else {
            this.le = true;
            if (!e) t.appendChild(this.f);
        }
    }
    remove() {
        if (this.le) {
            this.le = false;
            const t = this.f;
            const e = this.oe;
            let i;
            let s = this.re;
            while (null !== s) {
                i = s.nextSibling;
                t.appendChild(s);
                if (s === e) break;
                s = i;
            }
        }
    }
    addToLinked() {
        const t = this.ref;
        const e = t.parentNode;
        if (this.le) {
            let i = this.re;
            let s;
            const n = this.oe;
            while (null != i) {
                s = i.nextSibling;
                e.insertBefore(i, t);
                if (i === n) break;
                i = s;
            }
        } else {
            this.le = true;
            e.insertBefore(this.f, t);
        }
    }
    unlink() {
        this.he = false;
        this.next = void 0;
        this.ref = void 0;
    }
    link(t) {
        this.he = true;
        if (Us(t)) this.ref = t; else {
            this.next = t;
            this.ae();
        }
    }
    ae() {
        if (void 0 !== this.next) this.ref = this.next.firstChild; else this.ref = void 0;
    }
}

const $s = jt("IWindow", (t => t.callback((t => t.get(ui).window))));

const qs = jt("ILocation", (t => t.callback((t => t.get($s).location))));

const Ms = jt("IHistory", (t => t.callback((t => t.get($s).history))));

function Fs(t) {
    return function(e) {
        return en(t, e);
    };
}

function Os(t) {
    if (void 0 === t) return function(t) {
        tn(t, "shadowOptions", {
            mode: "open"
        });
    };
    if (!Rt(t)) return function(e) {
        tn(e, "shadowOptions", t);
    };
    tn(t, "shadowOptions", {
        mode: "open"
    });
}

function Vs(t) {
    if (void 0 === t) return function(t) {
        Ns(t);
    };
    Ns(t);
}

function Ns(t) {
    const e = st(Ys, t);
    if (void 0 === e) {
        tn(t, "containerless", true);
        return;
    }
    e.containerless = true;
}

function js(t) {
    if (void 0 === t) return function(t) {
        tn(t, "isStrictBinding", true);
    };
    tn(t, "isStrictBinding", true);
}

const Hs = new WeakMap;

class CustomElementDefinition {
    get type() {
        return 1;
    }
    constructor(t, e, i, s, n, r, o, l, h, a, c, u, f, d, m, g, p, v, x, w, b) {
        this.Type = t;
        this.name = e;
        this.aliases = i;
        this.key = s;
        this.cache = n;
        this.capture = r;
        this.template = o;
        this.instructions = l;
        this.dependencies = h;
        this.injectable = a;
        this.needsCompile = c;
        this.surrogates = u;
        this.bindables = f;
        this.childrenObservers = d;
        this.containerless = m;
        this.isStrictBinding = g;
        this.shadowOptions = p;
        this.hasSlots = v;
        this.enhance = x;
        this.watches = w;
        this.processContent = b;
    }
    static create(t, e = null) {
        if (null === e) {
            const i = t;
            if (St(i)) throw pt(`AUR0761:${t}`);
            const s = p("name", i, Js);
            if (Rt(i.Type)) e = i.Type; else e = hn(v(s));
            return new CustomElementDefinition(e, s, a(i.aliases), p("key", i, (() => Zs(s))), p("cache", i, zs), p("capture", i, Xs), p("template", i, Gs), a(i.instructions), a(i.dependencies), p("injectable", i, Gs), p("needsCompile", i, Ks), a(i.surrogates), $t.from(e, i.bindables), ni.from(i.childrenObservers), p("containerless", i, Xs), p("isStrictBinding", i, Xs), p("shadowOptions", i, Gs), p("hasSlots", i, Xs), p("enhance", i, Xs), p("watches", i, Qs), x("processContent", e, Gs));
        }
        if (St(t)) return new CustomElementDefinition(e, t, a(rn(e, "aliases"), e.aliases), Zs(t), x("cache", e, zs), x("capture", e, Xs), x("template", e, Gs), a(rn(e, "instructions"), e.instructions), a(rn(e, "dependencies"), e.dependencies), x("injectable", e, Gs), x("needsCompile", e, Ks), a(rn(e, "surrogates"), e.surrogates), $t.from(e, ...$t.getAll(e), rn(e, "bindables"), e.bindables), ni.from(...ni.getAll(e), rn(e, "childrenObservers"), e.childrenObservers), x("containerless", e, Xs), x("isStrictBinding", e, Xs), x("shadowOptions", e, Gs), x("hasSlots", e, Xs), x("enhance", e, Xs), a(He.getAnnotation(e), e.watches), x("processContent", e, Gs));
        const i = p("name", t, Js);
        return new CustomElementDefinition(e, i, a(rn(e, "aliases"), t.aliases, e.aliases), Zs(i), w("cache", t, e, zs), w("capture", t, e, Xs), w("template", t, e, Gs), a(rn(e, "instructions"), t.instructions, e.instructions), a(rn(e, "dependencies"), t.dependencies, e.dependencies), w("injectable", t, e, Gs), w("needsCompile", t, e, Ks), a(rn(e, "surrogates"), t.surrogates, e.surrogates), $t.from(e, ...$t.getAll(e), rn(e, "bindables"), e.bindables, t.bindables), ni.from(...ni.getAll(e), rn(e, "childrenObservers"), e.childrenObservers, t.childrenObservers), w("containerless", t, e, Xs), w("isStrictBinding", t, e, Xs), w("shadowOptions", t, e, Gs), w("hasSlots", t, e, Xs), w("enhance", t, e, Xs), a(t.watches, He.getAnnotation(e), e.watches), w("processContent", t, e, Gs));
    }
    static getOrCreate(t) {
        if (t instanceof CustomElementDefinition) return t;
        if (Hs.has(t)) return Hs.get(t);
        const e = CustomElementDefinition.create(t);
        Hs.set(t, e);
        rt(Ys, e, e.Type);
        return e;
    }
    register(t) {
        const {Type: e, key: i, aliases: s} = this;
        if (!t.has(i, false)) {
            Gt(i, e).register(t);
            Wt(i, e).register(t);
            Qt(s, an, i, t);
        }
    }
}

const Ws = {
    name: void 0,
    searchParents: false,
    optional: false
};

const zs = () => 0;

const Gs = () => null;

const Xs = () => false;

const Ks = () => true;

const Qs = () => l;

const Ys = at("custom-element");

const Zs = t => `${Ys}:${t}`;

const Js = (() => {
    let t = 0;
    return () => `unnamed-${++t}`;
})();

const tn = (t, e, i) => {
    rt(ht(e), i, t);
};

const en = (t, e) => {
    const i = CustomElementDefinition.create(t, e);
    rt(Ys, i, i.Type);
    rt(Ys, i, i);
    ct(i.Type, Ys);
    return i.Type;
};

const sn = t => Rt(t) && nt(Ys, t);

const nn = (t, e = Ws) => {
    if (void 0 === e.name && true !== e.searchParents) {
        const i = Bs(t, Ys);
        if (null === i) {
            if (true === e.optional) return null;
            throw pt(`AUR0762`);
        }
        return i;
    }
    if (void 0 !== e.name) {
        if (true !== e.searchParents) {
            const i = Bs(t, Ys);
            if (null === i) throw pt(`AUR0763`);
            if (i.is(e.name)) return i;
            return;
        }
        let i = t;
        let s = false;
        while (null !== i) {
            const t = Bs(i, Ys);
            if (null !== t) {
                s = true;
                if (t.is(e.name)) return t;
            }
            i = Ls(i);
        }
        if (s) return;
        throw pt(`AUR0764`);
    }
    let i = t;
    while (null !== i) {
        const t = Bs(i, Ys);
        if (null !== t) return t;
        i = Ls(i);
    }
    throw pt(`AUR0765`);
};

const rn = (t, e) => st(ht(e), t);

const on = t => {
    const e = st(Ys, t);
    if (void 0 === e) throw pt(`AUR0760:${t.name}`);
    return e;
};

const ln = () => {
    const t = function(e, i, s) {
        const n = r.getOrCreateAnnotationParamTypes(e);
        n[s] = t;
        return e;
    };
    t.register = function(e) {
        return {
            resolve(e, i) {
                if (i.has(t, true)) return i.get(t); else return null;
            }
        };
    };
    return t;
};

const hn = function() {
    const t = {
        value: "",
        writable: false,
        enumerable: false,
        configurable: true
    };
    const e = {};
    return function(i, s = e) {
        const n = class {};
        t.value = i;
        Reflect.defineProperty(n, "name", t);
        if (s !== e) wt(n.prototype, s);
        return n;
    };
}();

const an = xt({
    name: Ys,
    keyFrom: Zs,
    isType: sn,
    for: nn,
    define: en,
    getDefinition: on,
    annotate: tn,
    getAnnotation: rn,
    generateName: Js,
    createInjectable: ln,
    generateType: hn
});

const cn = ht("processContent");

function un(t) {
    return void 0 === t ? function(t, e, i) {
        rt(cn, fn(t, e), t);
    } : function(e) {
        t = fn(e, t);
        const i = st(Ys, e);
        if (void 0 !== i) i.processContent = t; else rt(cn, t, e);
        return e;
    };
}

function fn(t, e) {
    if (St(e)) e = t[e];
    if (!Rt(e)) throw pt(`AUR0766:${typeof e}`);
    return e;
}

function dn(t) {
    return function(e) {
        const i = Rt(t) ? t : true;
        tn(e, "capture", i);
        if (sn(e)) on(e).capture = i;
    };
}

const mn = jt("IProjections");

const gn = jt("IAuSlotsInfo");

class AuSlotsInfo {
    constructor(t) {
        this.projectedSlots = t;
    }
}

var pn;

(function(t) {
    t["hydrateElement"] = "ra";
    t["hydrateAttribute"] = "rb";
    t["hydrateTemplateController"] = "rc";
    t["hydrateLetElement"] = "rd";
    t["setProperty"] = "re";
    t["interpolation"] = "rf";
    t["propertyBinding"] = "rg";
    t["letBinding"] = "ri";
    t["refBinding"] = "rj";
    t["iteratorBinding"] = "rk";
    t["multiAttr"] = "rl";
    t["textBinding"] = "ha";
    t["listenerBinding"] = "hb";
    t["attributeBinding"] = "hc";
    t["stylePropertyBinding"] = "hd";
    t["setAttribute"] = "he";
    t["setClassAttribute"] = "hf";
    t["setStyleAttribute"] = "hg";
    t["spreadBinding"] = "hs";
    t["spreadElementProp"] = "hp";
})(pn || (pn = {}));

const vn = jt("Instruction");

function xn(t) {
    const e = t.type;
    return St(e) && 2 === e.length;
}

class InterpolationInstruction {
    constructor(t, e) {
        this.from = t;
        this.to = e;
        this.type = "rf";
    }
}

class PropertyBindingInstruction {
    constructor(t, e, i) {
        this.from = t;
        this.to = e;
        this.mode = i;
        this.type = "rg";
    }
}

class IteratorBindingInstruction {
    constructor(t, e, i) {
        this.forOf = t;
        this.to = e;
        this.props = i;
        this.type = "rk";
    }
}

class RefBindingInstruction {
    constructor(t, e) {
        this.from = t;
        this.to = e;
        this.type = "rj";
    }
}

class SetPropertyInstruction {
    constructor(t, e) {
        this.value = t;
        this.to = e;
        this.type = "re";
    }
}

class MultiAttrInstruction {
    constructor(t, e, i) {
        this.value = t;
        this.to = e;
        this.command = i;
        this.type = "rl";
    }
}

class HydrateElementInstruction {
    constructor(t, e, i, s, n, r) {
        this.res = t;
        this.alias = e;
        this.props = i;
        this.projections = s;
        this.containerless = n;
        this.captures = r;
        this.type = "ra";
        this.auSlot = null;
    }
}

class HydrateAttributeInstruction {
    constructor(t, e, i) {
        this.res = t;
        this.alias = e;
        this.props = i;
        this.type = "rb";
    }
}

class HydrateTemplateController {
    constructor(t, e, i, s) {
        this.def = t;
        this.res = e;
        this.alias = i;
        this.props = s;
        this.type = "rc";
    }
}

class HydrateLetElementInstruction {
    constructor(t, e) {
        this.instructions = t;
        this.toBindingContext = e;
        this.type = "rd";
    }
}

class LetBindingInstruction {
    constructor(t, e) {
        this.from = t;
        this.to = e;
        this.type = "ri";
    }
}

class TextBindingInstruction {
    constructor(t, e) {
        this.from = t;
        this.strict = e;
        this.type = "ha";
    }
}

class ListenerBindingInstruction {
    constructor(t, e, i, s) {
        this.from = t;
        this.to = e;
        this.preventDefault = i;
        this.capture = s;
        this.type = "hb";
    }
}

class StylePropertyBindingInstruction {
    constructor(t, e) {
        this.from = t;
        this.to = e;
        this.type = "hd";
    }
}

class SetAttributeInstruction {
    constructor(t, e) {
        this.value = t;
        this.to = e;
        this.type = "he";
    }
}

class SetClassAttributeInstruction {
    constructor(t) {
        this.value = t;
        this.type = "hf";
    }
}

class SetStyleAttributeInstruction {
    constructor(t) {
        this.value = t;
        this.type = "hg";
    }
}

class AttributeBindingInstruction {
    constructor(t, e, i) {
        this.attr = t;
        this.from = e;
        this.to = i;
        this.type = "hc";
    }
}

class SpreadBindingInstruction {
    constructor() {
        this.type = "hs";
    }
}

class SpreadElementPropBindingInstruction {
    constructor(t) {
        this.instructions = t;
        this.type = "hp";
    }
}

const wn = jt("ITemplateCompiler");

const bn = jt("IRenderer");

function yn(t) {
    return function e(i) {
        i.register = function(t) {
            Ht(bn, this).register(t);
        };
        It(i.prototype, "target", {
            configurable: true,
            get: function() {
                return t;
            }
        });
        return i;
    };
}

function kn(t, e, i) {
    if (St(e)) return t.parse(e, i);
    return e;
}

function An(t) {
    if (null != t.viewModel) return t.viewModel;
    return t;
}

function Cn(t, e) {
    if ("element" === e) return t;
    switch (e) {
      case "controller":
        return nn(t);

      case "view":
        throw pt(`AUR0750`);

      case "view-model":
        return nn(t).viewModel;

      default:
        {
            const i = Ye(t, e);
            if (void 0 !== i) return i.viewModel;
            const s = nn(t, {
                name: e
            });
            if (void 0 === s) throw pt(`AUR0751:${e}`);
            return s.viewModel;
        }
    }
}

let Bn = class SetPropertyRenderer {
    render(t, e, i) {
        const s = An(e);
        if (void 0 !== s.$observers?.[i.to]) s.$observers[i.to].setValue(i.value); else s[i.to] = i.value;
    }
};

Bn = et([ yn("re") ], Bn);

let Rn = class CustomElementRenderer {
    static get inject() {
        return [ _i ];
    }
    constructor(t) {
        this.r = t;
    }
    render(t, e, i, s, n, r) {
        let o;
        let l;
        let h;
        let a;
        const c = i.res;
        const u = i.projections;
        const f = t.container;
        switch (typeof c) {
          case "string":
            o = f.find(an, c);
            if (null == o) throw pt(`AUR0752:${c}@${t["name"]}`);
            break;

          default:
            o = c;
        }
        const m = i.containerless || o.containerless;
        const g = m ? _s(e) : null;
        const p = Xn(s, t, e, i, g, null == u ? void 0 : new AuSlotsInfo(yt(u)));
        l = o.Type;
        h = p.invoke(l);
        Xt(p, l, new d(o.key, h));
        a = Controller.$el(p, h, e, i, o, g);
        Rs(e, o.key, a);
        const v = this.r.renderers;
        const x = i.props;
        const w = x.length;
        let b = 0;
        let y;
        while (w > b) {
            y = x[b];
            v[y.type].render(t, a, y, s, n, r);
            ++b;
        }
        t.addChild(a);
    }
};

Rn = et([ yn("ra") ], Rn);

let Sn = class CustomAttributeRenderer {
    static get inject() {
        return [ _i ];
    }
    constructor(t) {
        this.r = t;
    }
    render(t, e, i, s, n, r) {
        let o = t.container;
        let l;
        switch (typeof i.res) {
          case "string":
            l = o.find(ti, i.res);
            if (null == l) throw pt(`AUR0753:${i.res}@${t["name"]}`);
            break;

          default:
            l = i.res;
        }
        const h = Kn(s, l, t, e, i, void 0, void 0);
        const a = Controller.$attr(h.ctn, h.vm, e, l);
        Rs(e, l.key, a);
        const c = this.r.renderers;
        const u = i.props;
        const f = u.length;
        let d = 0;
        let m;
        while (f > d) {
            m = u[d];
            c[m.type].render(t, a, m, s, n, r);
            ++d;
        }
        t.addChild(a);
    }
};

Sn = et([ yn("rb") ], Sn);

let In = class TemplateControllerRenderer {
    static get inject() {
        return [ _i, ui ];
    }
    constructor(t, e) {
        this.r = t;
        this.p = e;
    }
    render(t, e, i, s, n, r) {
        let o = t.container;
        let l;
        switch (typeof i.res) {
          case "string":
            l = o.find(ti, i.res);
            if (null == l) throw pt(`AUR0754:${i.res}@${t["name"]}`);
            break;

          default:
            l = i.res;
        }
        const h = this.r.getViewFactory(i.def, o);
        const a = _s(e);
        const c = Kn(this.p, l, t, e, i, h, a);
        const u = Controller.$attr(c.ctn, c.vm, e, l);
        Rs(a, l.key, u);
        c.vm.link?.(t, u, e, i);
        const f = this.r.renderers;
        const d = i.props;
        const m = d.length;
        let g = 0;
        let p;
        while (m > g) {
            p = d[g];
            f[p.type].render(t, u, p, s, n, r);
            ++g;
        }
        t.addChild(u);
    }
};

In = et([ yn("rc") ], In);

let Tn = class LetElementRenderer {
    render(t, e, i, s, n, r) {
        e.remove();
        const o = i.instructions;
        const l = i.toBindingContext;
        const h = t.container;
        const a = o.length;
        let c;
        let u;
        let f = 0;
        while (a > f) {
            c = o[f];
            u = kn(n, c.from, 16);
            t.addBinding(new LetBinding(h, r, u, c.to, l));
            ++f;
        }
    }
};

Tn = et([ yn("rd") ], Tn);

let Pn = class RefBindingRenderer {
    render(t, e, i, s, n) {
        t.addBinding(new RefBinding(t.container, kn(n, i.from, 16), Cn(e, i.to)));
    }
};

Pn = et([ yn("rj") ], Pn);

let En = class InterpolationBindingRenderer {
    render(t, e, i, s, n, r) {
        t.addBinding(new InterpolationBinding(t, t.container, r, s.domWriteQueue, kn(n, i.from, 1), An(e), i.to, 2));
    }
};

En = et([ yn("rf") ], En);

let Ln = class PropertyBindingRenderer {
    render(t, e, i, s, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, s.domWriteQueue, kn(n, i.from, 16), An(e), i.to, i.mode));
    }
};

Ln = et([ yn("rg") ], Ln);

let Dn = class IteratorBindingRenderer {
    render(t, e, i, s, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, s.domWriteQueue, kn(n, i.forOf, 2), An(e), i.to, 2));
    }
};

Dn = et([ yn("rk") ], Dn);

let _n = class TextBindingRenderer {
    render(t, e, i, s, n, r) {
        t.addBinding(new ContentBinding(t, t.container, r, s.domWriteQueue, s, kn(n, i.from, 16), xs(e.parentNode, vs(s, ""), e), i.strict));
    }
};

_n = et([ yn("ha") ], _n);

let Un = class ListenerBindingRenderer {
    render(t, e, i, s, n) {
        t.addBinding(new ListenerBinding(t.container, kn(n, i.from, 8), e, i.to, new ListenerBindingOptions(i.preventDefault, i.capture)));
    }
};

Un = et([ yn("hb") ], Un);

let $n = class SetAttributeRenderer {
    render(t, e, i) {
        e.setAttribute(i.to, i.value);
    }
};

$n = et([ yn("he") ], $n);

let qn = class SetClassAttributeRenderer {
    render(t, e, i) {
        Nn(e.classList, i.value);
    }
};

qn = et([ yn("hf") ], qn);

let Mn = class SetStyleAttributeRenderer {
    render(t, e, i) {
        e.style.cssText += i.value;
    }
};

Mn = et([ yn("hg") ], Mn);

let Fn = class StylePropertyBindingRenderer {
    render(t, e, i, s, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, s.domWriteQueue, kn(n, i.from, 16), e.style, i.to, 2));
    }
};

Fn = et([ yn("hd") ], Fn);

let On = class AttributeBindingRenderer {
    render(t, e, i, s, n, r) {
        const o = t.container;
        const l = o.has(Ps, false) ? o.get(Ps) : null;
        t.addBinding(new AttributeBinding(t, o, r, s.domWriteQueue, kn(n, i.from, 16), e, i.attr, null == l ? i.to : i.to.split(/\s/g).map((t => l[t] ?? t)).join(" "), 2));
    }
};

On = et([ yn("hc") ], On);

let Vn = class SpreadRenderer {
    static get inject() {
        return [ wn, _i ];
    }
    constructor(t, e) {
        this.ce = t;
        this.r = e;
    }
    render(t, e, i, s, n, r) {
        const o = t.container;
        const h = o.get(Yi);
        const a = this.r.renderers;
        const c = t => {
            let e = t;
            let i = h;
            while (null != i && e > 0) {
                i = i.parent;
                --e;
            }
            if (null == i) throw pt("No scope context for spread binding.");
            return i;
        };
        const u = i => {
            const o = c(i);
            const h = jn(o);
            const f = this.ce.compileSpread(o.controller.definition, o.instruction?.captures ?? l, o.controller.container, e);
            let d;
            for (d of f) switch (d.type) {
              case "hs":
                u(i + 1);
                break;

              case "hp":
                a[d.instructions.type].render(h, nn(e), d.instructions, s, n, r);
                break;

              default:
                a[d.type].render(h, e, d, s, n, r);
            }
            t.addBinding(h);
        };
        u(0);
    }
};

Vn = et([ yn("hs") ], Vn);

class SpreadBinding {
    get container() {
        return this.locator;
    }
    get definition() {
        return this.ctrl.definition;
    }
    get isStrictBinding() {
        return this.ctrl.isStrictBinding;
    }
    get state() {
        return this.ctrl.state;
    }
    constructor(t, e) {
        this.ue = t;
        this.fe = e;
        this.isBound = false;
        this.ctrl = e.controller;
        this.locator = this.ctrl.container;
    }
    get(t) {
        return this.locator.get(t);
    }
    bind(t) {
        if (this.isBound) return;
        this.isBound = true;
        const e = this.scope = this.fe.controller.scope.parent ?? void 0;
        if (null == e) throw pt("Invalid spreading. Context scope is null/undefined");
        this.ue.forEach((t => t.bind(e)));
    }
    unbind() {
        this.ue.forEach((t => t.unbind()));
        this.isBound = false;
    }
    addBinding(t) {
        this.ue.push(t);
    }
    addChild(t) {
        if (1 !== t.vmKind) throw pt("Spread binding does not support spreading custom attributes/template controllers");
        this.ctrl.addChild(t);
    }
    limit() {
        throw pt("not implemented");
    }
    useScope() {
        throw pt("not implemented");
    }
}

function Nn(t, e) {
    const i = e.length;
    let s = 0;
    for (let n = 0; n < i; ++n) if (32 === e.charCodeAt(n)) {
        if (n !== s) t.add(e.slice(s, n));
        s = n + 1;
    } else if (n + 1 === i) t.add(e.slice(s));
}

const jn = t => new SpreadBinding([], t);

const Hn = "IController";

const Wn = "IInstruction";

const zn = "IRenderLocation";

const Gn = "IAuSlotsInfo";

function Xn(t, e, i, s, n, r) {
    const o = e.container.createChild();
    Xt(o, t.HTMLElement, Xt(o, t.Element, Xt(o, Ss, new d("ElementResolver", i))));
    Xt(o, Qi, new d(Hn, e));
    Xt(o, vn, new d(Wn, s));
    Xt(o, Ts, null == n ? Qn : new RenderLocationProvider(n));
    Xt(o, Di, Yn);
    Xt(o, gn, null == r ? Zn : new d(Gn, r));
    return o;
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
        if (null === t) throw pt(`AUR7055`);
        if (!St(t.name) || 0 === t.name.length) throw pt(`AUR0756`);
        return t;
    }
}

function Kn(t, e, i, s, n, r, o, l) {
    const h = i.container.createChild();
    Xt(h, t.HTMLElement, Xt(h, t.Element, Xt(h, Ss, new d("ElementResolver", s))));
    i = i instanceof Controller ? i : i.ctrl;
    Xt(h, Qi, new d(Hn, i));
    Xt(h, vn, new d(Wn, n));
    Xt(h, Ts, null == o ? Qn : new d(zn, o));
    Xt(h, Di, null == r ? Yn : new ViewFactoryProvider(r));
    Xt(h, gn, null == l ? Zn : new d(Gn, l));
    return {
        vm: h.invoke(e.Type),
        ctn: h
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

const Qn = new RenderLocationProvider(null);

const Yn = new ViewFactoryProvider(null);

const Zn = new d(Gn, new AuSlotsInfo(l));

var Jn;

(function(t) {
    t[t["None"] = 0] = "None";
    t[t["IgnoreAttr"] = 1] = "IgnoreAttr";
})(Jn || (Jn = {}));

function tr(t) {
    return function(e) {
        return nr.define(t, e);
    };
}

class BindingCommandDefinition {
    constructor(t, e, i, s, n) {
        this.Type = t;
        this.name = e;
        this.aliases = i;
        this.key = s;
        this.type = n;
    }
    static create(t, e) {
        let s;
        let n;
        if (St(t)) {
            s = t;
            n = {
                name: s
            };
        } else {
            s = t.name;
            n = t;
        }
        return new BindingCommandDefinition(e, i(sr(e, "name"), s), a(sr(e, "aliases"), n.aliases, e.aliases), ir(s), i(sr(e, "type"), n.type, e.type, null));
    }
    register(t) {
        const {Type: e, key: i, aliases: s} = this;
        Ht(i, e).register(t);
        Wt(i, e).register(t);
        Qt(s, nr, i, t);
    }
}

const er = at("binding-command");

const ir = t => `${er}:${t}`;

const sr = (t, e) => st(ht(e), t);

const nr = xt({
    name: er,
    keyFrom: ir,
    define(t, e) {
        const i = BindingCommandDefinition.create(t, e);
        rt(er, i, i.Type);
        rt(er, i, i);
        ct(e, er);
        return i.Type;
    },
    getAnnotation: sr
});

let rr = class OneTimeBindingCommand {
    get type() {
        return 0;
    }
    build(t, e, i) {
        const s = t.attr;
        let n = s.target;
        let r = t.attr.rawValue;
        if (null == t.bindable) n = i.map(t.node, n) ?? b(n); else {
            if ("" === r && 1 === t.def.type) r = b(n);
            n = t.bindable.property;
        }
        return new PropertyBindingInstruction(e.parse(r, 16), n, 1);
    }
};

rr = et([ tr("one-time") ], rr);

let or = class ToViewBindingCommand {
    get type() {
        return 0;
    }
    build(t, e, i) {
        const s = t.attr;
        let n = s.target;
        let r = t.attr.rawValue;
        if (null == t.bindable) n = i.map(t.node, n) ?? b(n); else {
            if ("" === r && 1 === t.def.type) r = b(n);
            n = t.bindable.property;
        }
        return new PropertyBindingInstruction(e.parse(r, 16), n, 2);
    }
};

or = et([ tr("to-view") ], or);

let lr = class FromViewBindingCommand {
    get type() {
        return 0;
    }
    build(t, e, i) {
        const s = t.attr;
        let n = s.target;
        let r = s.rawValue;
        if (null == t.bindable) n = i.map(t.node, n) ?? b(n); else {
            if ("" === r && 1 === t.def.type) r = b(n);
            n = t.bindable.property;
        }
        return new PropertyBindingInstruction(e.parse(r, 16), n, 4);
    }
};

lr = et([ tr("from-view") ], lr);

let hr = class TwoWayBindingCommand {
    get type() {
        return 0;
    }
    build(t, e, i) {
        const s = t.attr;
        let n = s.target;
        let r = s.rawValue;
        if (null == t.bindable) n = i.map(t.node, n) ?? b(n); else {
            if ("" === r && 1 === t.def.type) r = b(n);
            n = t.bindable.property;
        }
        return new PropertyBindingInstruction(e.parse(r, 16), n, 6);
    }
};

hr = et([ tr("two-way") ], hr);

let ar = class DefaultBindingCommand {
    get type() {
        return 0;
    }
    build(t, e, i) {
        const s = t.attr;
        const n = t.bindable;
        let r;
        let o;
        let l = s.target;
        let h = s.rawValue;
        if (null == n) {
            o = i.isTwoWay(t.node, l) ? 6 : 2;
            l = i.map(t.node, l) ?? b(l);
        } else {
            if ("" === h && 1 === t.def.type) h = b(l);
            r = t.def.defaultBindingMode;
            o = 8 === n.mode || null == n.mode ? null == r || 8 === r ? 2 : r : n.mode;
            l = n.property;
        }
        return new PropertyBindingInstruction(e.parse(h, 16), l, o);
    }
};

ar = et([ tr("bind") ], ar);

let cr = class ForBindingCommand {
    get type() {
        return 0;
    }
    static get inject() {
        return [ ee ];
    }
    constructor(t) {
        this.de = t;
    }
    build(t, e) {
        const i = null === t.bindable ? b(t.attr.target) : t.bindable.property;
        const s = e.parse(t.attr.rawValue, 2);
        let n = l;
        if (s.semiIdx > -1) {
            const e = t.attr.rawValue.slice(s.semiIdx + 1);
            const i = e.indexOf(":");
            if (i > -1) {
                const t = e.slice(0, i).trim();
                const s = e.slice(i + 1).trim();
                const r = this.de.parse(t, s);
                n = [ new MultiAttrInstruction(s, r.target, r.command) ];
            }
        }
        return new IteratorBindingInstruction(s, i, n);
    }
};

cr = et([ tr("for") ], cr);

let ur = class TriggerBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new ListenerBindingInstruction(e.parse(t.attr.rawValue, 8), t.attr.target, true, false);
    }
};

ur = et([ tr("trigger") ], ur);

let fr = class CaptureBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new ListenerBindingInstruction(e.parse(t.attr.rawValue, 8), t.attr.target, false, true);
    }
};

fr = et([ tr("capture") ], fr);

let dr = class AttrBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new AttributeBindingInstruction(t.attr.target, e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

dr = et([ tr("attr") ], dr);

let mr = class StyleBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new AttributeBindingInstruction("style", e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

mr = et([ tr("style") ], mr);

let gr = class ClassBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new AttributeBindingInstruction("class", e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

gr = et([ tr("class") ], gr);

let pr = class RefBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new RefBindingInstruction(e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

pr = et([ tr("ref") ], pr);

let vr = class SpreadBindingCommand {
    get type() {
        return 1;
    }
    build(t) {
        return new SpreadBindingInstruction;
    }
};

vr = et([ tr("...$attrs") ], vr);

const xr = jt("ISVGAnalyzer", (t => t.singleton(NoopSVGAnalyzer)));

const wr = t => {
    const e = gt();
    t = St(t) ? t.split(" ") : t;
    let i;
    for (i of t) e[i] = true;
    return e;
};

class NoopSVGAnalyzer {
    isStandardSvgAttribute(t, e) {
        return false;
    }
}

class SVGAnalyzer {
    static register(t) {
        return Ht(xr, this).register(t);
    }
    constructor(t) {
        this.me = wt(gt(), {
            a: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage target transform xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            altGlyph: wr("class dx dy externalResourcesRequired format glyphRef id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            altglyph: gt(),
            altGlyphDef: wr("id xml:base xml:lang xml:space"),
            altglyphdef: gt(),
            altGlyphItem: wr("id xml:base xml:lang xml:space"),
            altglyphitem: gt(),
            animate: wr("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateColor: wr("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateMotion: wr("accumulate additive begin by calcMode dur end externalResourcesRequired fill from id keyPoints keySplines keyTimes max min onbegin onend onload onrepeat origin path repeatCount repeatDur requiredExtensions requiredFeatures restart rotate systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateTransform: wr("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to type values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            circle: wr("class cx cy externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup r requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            clipPath: wr("class clipPathUnits externalResourcesRequired id requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            "color-profile": wr("id local name rendering-intent xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            cursor: wr("externalResourcesRequired id requiredExtensions requiredFeatures systemLanguage x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            defs: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            desc: wr("class id style xml:base xml:lang xml:space"),
            ellipse: wr("class cx cy externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rx ry style systemLanguage transform xml:base xml:lang xml:space"),
            feBlend: wr("class height id in in2 mode result style width x xml:base xml:lang xml:space y"),
            feColorMatrix: wr("class height id in result style type values width x xml:base xml:lang xml:space y"),
            feComponentTransfer: wr("class height id in result style width x xml:base xml:lang xml:space y"),
            feComposite: wr("class height id in in2 k1 k2 k3 k4 operator result style width x xml:base xml:lang xml:space y"),
            feConvolveMatrix: wr("bias class divisor edgeMode height id in kernelMatrix kernelUnitLength order preserveAlpha result style targetX targetY width x xml:base xml:lang xml:space y"),
            feDiffuseLighting: wr("class diffuseConstant height id in kernelUnitLength result style surfaceScale width x xml:base xml:lang xml:space y"),
            feDisplacementMap: wr("class height id in in2 result scale style width x xChannelSelector xml:base xml:lang xml:space y yChannelSelector"),
            feDistantLight: wr("azimuth elevation id xml:base xml:lang xml:space"),
            feFlood: wr("class height id result style width x xml:base xml:lang xml:space y"),
            feFuncA: wr("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncB: wr("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncG: wr("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncR: wr("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feGaussianBlur: wr("class height id in result stdDeviation style width x xml:base xml:lang xml:space y"),
            feImage: wr("class externalResourcesRequired height id preserveAspectRatio result style width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            feMerge: wr("class height id result style width x xml:base xml:lang xml:space y"),
            feMergeNode: wr("id xml:base xml:lang xml:space"),
            feMorphology: wr("class height id in operator radius result style width x xml:base xml:lang xml:space y"),
            feOffset: wr("class dx dy height id in result style width x xml:base xml:lang xml:space y"),
            fePointLight: wr("id x xml:base xml:lang xml:space y z"),
            feSpecularLighting: wr("class height id in kernelUnitLength result specularConstant specularExponent style surfaceScale width x xml:base xml:lang xml:space y"),
            feSpotLight: wr("id limitingConeAngle pointsAtX pointsAtY pointsAtZ specularExponent x xml:base xml:lang xml:space y z"),
            feTile: wr("class height id in result style width x xml:base xml:lang xml:space y"),
            feTurbulence: wr("baseFrequency class height id numOctaves result seed stitchTiles style type width x xml:base xml:lang xml:space y"),
            filter: wr("class externalResourcesRequired filterRes filterUnits height id primitiveUnits style width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            font: wr("class externalResourcesRequired horiz-adv-x horiz-origin-x horiz-origin-y id style vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            "font-face": wr("accent-height alphabetic ascent bbox cap-height descent font-family font-size font-stretch font-style font-variant font-weight hanging id ideographic mathematical overline-position overline-thickness panose-1 slope stemh stemv strikethrough-position strikethrough-thickness underline-position underline-thickness unicode-range units-per-em v-alphabetic v-hanging v-ideographic v-mathematical widths x-height xml:base xml:lang xml:space"),
            "font-face-format": wr("id string xml:base xml:lang xml:space"),
            "font-face-name": wr("id name xml:base xml:lang xml:space"),
            "font-face-src": wr("id xml:base xml:lang xml:space"),
            "font-face-uri": wr("id xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            foreignObject: wr("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform width x xml:base xml:lang xml:space y"),
            g: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            glyph: wr("arabic-form class d glyph-name horiz-adv-x id lang orientation style unicode vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            glyphRef: wr("class dx dy format glyphRef id style x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            glyphref: gt(),
            hkern: wr("g1 g2 id k u1 u2 xml:base xml:lang xml:space"),
            image: wr("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage transform width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            line: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform x1 x2 xml:base xml:lang xml:space y1 y2"),
            linearGradient: wr("class externalResourcesRequired gradientTransform gradientUnits id spreadMethod style x1 x2 xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space y1 y2"),
            marker: wr("class externalResourcesRequired id markerHeight markerUnits markerWidth orient preserveAspectRatio refX refY style viewBox xml:base xml:lang xml:space"),
            mask: wr("class externalResourcesRequired height id maskContentUnits maskUnits requiredExtensions requiredFeatures style systemLanguage width x xml:base xml:lang xml:space y"),
            metadata: wr("id xml:base xml:lang xml:space"),
            "missing-glyph": wr("class d horiz-adv-x id style vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            mpath: wr("externalResourcesRequired id xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            path: wr("class d externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup pathLength requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            pattern: wr("class externalResourcesRequired height id patternContentUnits patternTransform patternUnits preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage viewBox width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            polygon: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup points requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            polyline: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup points requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            radialGradient: wr("class cx cy externalResourcesRequired fx fy gradientTransform gradientUnits id r spreadMethod style xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space"),
            rect: wr("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rx ry style systemLanguage transform width x xml:base xml:lang xml:space y"),
            script: wr("externalResourcesRequired id type xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            set: wr("attributeName attributeType begin dur end externalResourcesRequired fill id max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            stop: wr("class id offset style xml:base xml:lang xml:space"),
            style: wr("id media title type xml:base xml:lang xml:space"),
            svg: wr("baseProfile class contentScriptType contentStyleType externalResourcesRequired height id onabort onactivate onclick onerror onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup onresize onscroll onunload onzoom preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage version viewBox width x xml:base xml:lang xml:space y zoomAndPan"),
            switch: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            symbol: wr("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup preserveAspectRatio style viewBox xml:base xml:lang xml:space"),
            text: wr("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength transform x xml:base xml:lang xml:space y"),
            textPath: wr("class externalResourcesRequired id lengthAdjust method onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures spacing startOffset style systemLanguage textLength xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space"),
            title: wr("class id style xml:base xml:lang xml:space"),
            tref: wr("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength x xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space y"),
            tspan: wr("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength x xml:base xml:lang xml:space y"),
            use: wr("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            view: wr("externalResourcesRequired id preserveAspectRatio viewBox viewTarget xml:base xml:lang xml:space zoomAndPan"),
            vkern: wr("g1 g2 id k u1 u2 xml:base xml:lang xml:space")
        });
        this.ge = wr("a altGlyph animate animateColor circle clipPath defs ellipse feBlend feColorMatrix feComponentTransfer feComposite feConvolveMatrix feDiffuseLighting feDisplacementMap feFlood feGaussianBlur feImage feMerge feMorphology feOffset feSpecularLighting feTile feTurbulence filter font foreignObject g glyph glyphRef image line linearGradient marker mask missing-glyph path pattern polygon polyline radialGradient rect stop svg switch symbol text textPath tref tspan use");
        this.pe = wr("alignment-baseline baseline-shift clip-path clip-rule clip color-interpolation-filters color-interpolation color-profile color-rendering color cursor direction display dominant-baseline enable-background fill-opacity fill-rule fill filter flood-color flood-opacity font-family font-size-adjust font-size font-stretch font-style font-variant font-weight glyph-orientation-horizontal glyph-orientation-vertical image-rendering kerning letter-spacing lighting-color marker-end marker-mid marker-start mask opacity overflow pointer-events shape-rendering stop-color stop-opacity stroke-dasharray stroke-dashoffset stroke-linecap stroke-linejoin stroke-miterlimit stroke-opacity stroke-width stroke text-anchor text-decoration text-rendering unicode-bidi visibility word-spacing writing-mode");
        this.SVGElement = t.globalThis.SVGElement;
        const e = t.document.createElement("div");
        e.innerHTML = "<svg><altGlyph /></svg>";
        if ("altglyph" === e.firstElementChild.nodeName) {
            const t = this.me;
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
        if (!(t instanceof this.SVGElement)) return false;
        return true === this.ge[t.nodeName] && true === this.pe[e] || true === this.me[t.nodeName]?.[e];
    }
}

SVGAnalyzer.inject = [ ui ];

const br = jt("IAttrMapper", (t => t.singleton(AttrMapper)));

class AttrMapper {
    static get inject() {
        return [ xr ];
    }
    constructor(t) {
        this.svg = t;
        this.fns = [];
        this.ve = gt();
        this.xe = gt();
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
        var e;
        let i;
        let s;
        let n;
        let r;
        for (n in t) {
            i = t[n];
            s = (e = this.ve)[n] ?? (e[n] = gt());
            for (r in i) {
                if (void 0 !== s[r]) throw kr(r, n);
                s[r] = i[r];
            }
        }
    }
    useGlobalMapping(t) {
        const e = this.xe;
        for (const i in t) {
            if (void 0 !== e[i]) throw kr(i, "*");
            e[i] = t[i];
        }
    }
    useTwoWay(t) {
        this.fns.push(t);
    }
    isTwoWay(t, e) {
        return yr(t, e) || this.fns.length > 0 && this.fns.some((i => i(t, e)));
    }
    map(t, e) {
        return this.ve[t.nodeName]?.[e] ?? this.xe[e] ?? (At(t, e, this.svg) ? e : null);
    }
}

function yr(t, e) {
    switch (t.nodeName) {
      case "INPUT":
        switch (t.type) {
          case "checkbox":
          case "radio":
            return "checked" === e;

          default:
            return "value" === e || "files" === e || "value-as-number" === e || "value-as-date" === e;
        }

      case "TEXTAREA":
      case "SELECT":
        return "value" === e;

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

function kr(t, e) {
    return pt(`Attribute ${t} has been already registered for ${"*" === e ? "all elements" : `<${e}/>`}`);
}

const Ar = jt("ITemplateElementFactory", (t => t.singleton(TemplateElementFactory)));

const Cr = {};

class TemplateElementFactory {
    constructor(t) {
        this.p = t;
        this.we = Br(this.p);
    }
    createTemplate(t) {
        if (St(t)) {
            let e = Cr[t];
            if (void 0 === e) {
                const i = this.we;
                i.innerHTML = t;
                const s = i.content.firstElementChild;
                if (null == s || "TEMPLATE" !== s.nodeName || null != s.nextElementSibling) {
                    this.we = Br(this.p);
                    e = i;
                } else {
                    i.content.removeChild(s);
                    e = s;
                }
                Cr[t] = e;
            }
            return e.cloneNode(true);
        }
        if ("TEMPLATE" !== t.nodeName) {
            const e = Br(this.p);
            e.content.appendChild(t);
            return e;
        }
        t.parentNode?.removeChild(t);
        return t.cloneNode(true);
    }
}

TemplateElementFactory.inject = [ ui ];

const Br = t => t.document.createElement("template");

class TemplateCompiler {
    constructor() {
        this.debug = false;
        this.resolveResources = true;
    }
    static register(t) {
        return Ht(wn, this).register(t);
    }
    compile(t, e, i) {
        const s = CustomElementDefinition.getOrCreate(t);
        if (null === s.template || void 0 === s.template) return s;
        if (false === s.needsCompile) return s;
        i ?? (i = Ur);
        const n = new CompilationContext(t, e, i, null, null, void 0);
        const r = St(s.template) || !t.enhance ? n.be.createTemplate(s.template) : s.template;
        const o = r.nodeName === Ir && null != r.content;
        const h = o ? r.content : r;
        const a = e.get(Nt(Wr));
        const c = a.length;
        let u = 0;
        if (c > 0) while (c > u) {
            a[u].compiling?.(r);
            ++u;
        }
        if (r.hasAttribute(Nr)) throw pt(`AUR0701`);
        this.ye(h, n);
        this.ke(h, n);
        return CustomElementDefinition.create({
            ...t,
            name: t.name || Js(),
            dependencies: (t.dependencies ?? l).concat(n.deps ?? l),
            instructions: n.rows,
            surrogates: o ? this.Ae(r, n) : l,
            template: r,
            hasSlots: n.hasSlot,
            needsCompile: false
        });
    }
    compileSpread(t, e, i, s) {
        const n = new CompilationContext(t, i, Ur, null, null, void 0);
        const r = [];
        const o = n.Ce(s.nodeName.toLowerCase());
        const l = null !== o;
        const h = n.ep;
        const a = e.length;
        let c = 0;
        let u;
        let f = null;
        let d;
        let m;
        let g;
        let p;
        let v;
        let x = null;
        let w;
        let y;
        let k;
        let A;
        for (;a > c; ++c) {
            u = e[c];
            k = u.target;
            A = u.rawValue;
            x = n.Be(u);
            if (null !== x && (1 & x.type) > 0) {
                qr.node = s;
                qr.attr = u;
                qr.bindable = null;
                qr.def = null;
                r.push(x.build(qr, n.ep, n.m));
                continue;
            }
            f = n.Re(k);
            if (null !== f) {
                if (f.isTemplateController) throw pt(`AUR0703:${k}`);
                g = BindablesInfo.from(f, true);
                y = false === f.noMultiBindings && null === x && Dr(A);
                if (y) m = this.Se(s, A, f, n); else {
                    v = g.primary;
                    if (null === x) {
                        w = h.parse(A, 1);
                        m = [ null === w ? new SetPropertyInstruction(A, v.property) : new InterpolationInstruction(w, v.property) ];
                    } else {
                        qr.node = s;
                        qr.attr = u;
                        qr.bindable = v;
                        qr.def = f;
                        m = [ x.build(qr, n.ep, n.m) ];
                    }
                }
                (d ?? (d = [])).push(new HydrateAttributeInstruction(this.resolveResources ? f : f.name, null != f.aliases && f.aliases.includes(k) ? k : void 0, m));
                continue;
            }
            if (null === x) {
                w = h.parse(A, 1);
                if (l) {
                    g = BindablesInfo.from(o, false);
                    p = g.attrs[k];
                    if (void 0 !== p) {
                        w = h.parse(A, 1);
                        r.push(new SpreadElementPropBindingInstruction(null == w ? new SetPropertyInstruction(A, p.property) : new InterpolationInstruction(w, p.property)));
                        continue;
                    }
                }
                if (null != w) r.push(new InterpolationInstruction(w, n.m.map(s, k) ?? b(k))); else switch (k) {
                  case "class":
                    r.push(new SetClassAttributeInstruction(A));
                    break;

                  case "style":
                    r.push(new SetStyleAttributeInstruction(A));
                    break;

                  default:
                    r.push(new SetAttributeInstruction(A, k));
                }
            } else {
                if (l) {
                    g = BindablesInfo.from(o, false);
                    p = g.attrs[k];
                    if (void 0 !== p) {
                        qr.node = s;
                        qr.attr = u;
                        qr.bindable = p;
                        qr.def = o;
                        r.push(new SpreadElementPropBindingInstruction(x.build(qr, n.ep, n.m)));
                        continue;
                    }
                }
                qr.node = s;
                qr.attr = u;
                qr.bindable = null;
                qr.def = null;
                r.push(x.build(qr, n.ep, n.m));
            }
        }
        _r();
        if (null != d) return d.concat(r);
        return r;
    }
    Ae(t, e) {
        const i = [];
        const s = t.attributes;
        const n = e.ep;
        let r = s.length;
        let o = 0;
        let l;
        let h;
        let a;
        let c;
        let u = null;
        let f;
        let d;
        let m;
        let g;
        let p = null;
        let v;
        let x;
        let w;
        let y;
        for (;r > o; ++o) {
            l = s[o];
            h = l.name;
            a = l.value;
            c = e.de.parse(h, a);
            w = c.target;
            y = c.rawValue;
            if (Mr[w]) throw pt(`AUR0702:${h}`);
            p = e.Be(c);
            if (null !== p && (1 & p.type) > 0) {
                qr.node = t;
                qr.attr = c;
                qr.bindable = null;
                qr.def = null;
                i.push(p.build(qr, e.ep, e.m));
                continue;
            }
            u = e.Re(w);
            if (null !== u) {
                if (u.isTemplateController) throw pt(`AUR0703:${w}`);
                m = BindablesInfo.from(u, true);
                x = false === u.noMultiBindings && null === p && Dr(y);
                if (x) d = this.Se(t, y, u, e); else {
                    g = m.primary;
                    if (null === p) {
                        v = n.parse(y, 1);
                        d = [ null === v ? new SetPropertyInstruction(y, g.property) : new InterpolationInstruction(v, g.property) ];
                    } else {
                        qr.node = t;
                        qr.attr = c;
                        qr.bindable = g;
                        qr.def = u;
                        d = [ p.build(qr, e.ep, e.m) ];
                    }
                }
                t.removeAttribute(h);
                --o;
                --r;
                (f ?? (f = [])).push(new HydrateAttributeInstruction(this.resolveResources ? u : u.name, null != u.aliases && u.aliases.includes(w) ? w : void 0, d));
                continue;
            }
            if (null === p) {
                v = n.parse(y, 1);
                if (null != v) {
                    t.removeAttribute(h);
                    --o;
                    --r;
                    i.push(new InterpolationInstruction(v, e.m.map(t, w) ?? b(w)));
                } else switch (h) {
                  case "class":
                    i.push(new SetClassAttributeInstruction(y));
                    break;

                  case "style":
                    i.push(new SetStyleAttributeInstruction(y));
                    break;

                  default:
                    i.push(new SetAttributeInstruction(y, h));
                }
            } else {
                qr.node = t;
                qr.attr = c;
                qr.bindable = null;
                qr.def = null;
                i.push(p.build(qr, e.ep, e.m));
            }
        }
        _r();
        if (null != f) return f.concat(i);
        return i;
    }
    ke(t, e) {
        switch (t.nodeType) {
          case 1:
            switch (t.nodeName) {
              case "LET":
                return this.Ie(t, e);

              default:
                return this.Te(t, e);
            }

          case 3:
            return this.Pe(t, e);

          case 11:
            {
                let i = t.firstChild;
                while (null !== i) i = this.ke(i, e);
                break;
            }
        }
        return t.nextSibling;
    }
    Ie(t, e) {
        const i = t.attributes;
        const s = i.length;
        const n = [];
        const r = e.ep;
        let o = false;
        let l = 0;
        let h;
        let a;
        let c;
        let u;
        let f;
        let d;
        let m;
        let g;
        for (;s > l; ++l) {
            h = i[l];
            c = h.name;
            u = h.value;
            if ("to-binding-context" === c) {
                o = true;
                continue;
            }
            a = e.de.parse(c, u);
            d = a.target;
            m = a.rawValue;
            f = e.Be(a);
            if (null !== f) {
                if ("bind" === a.command) n.push(new LetBindingInstruction(r.parse(m, 16), b(d))); else throw pt(`AUR0704:${a.command}`);
                continue;
            }
            g = r.parse(m, 1);
            n.push(new LetBindingInstruction(null === g ? new N(m) : g, b(d)));
        }
        e.rows.push([ new HydrateLetElementInstruction(n, o) ]);
        return this.Ee(t).nextSibling;
    }
    Te(t, e) {
        var i, s, r, o;
        const h = t.nextSibling;
        const a = (t.getAttribute("as-element") ?? t.nodeName).toLowerCase();
        const c = e.Ce(a);
        const u = null !== c;
        const f = u && null != c.shadowOptions;
        const d = c?.capture;
        const m = null != d && "boolean" !== typeof d;
        const g = d ? [] : l;
        const p = e.ep;
        const v = this.debug ? n : () => {
            t.removeAttribute(C);
            --k;
            --y;
        };
        let x = t.attributes;
        let w;
        let y = x.length;
        let k = 0;
        let A;
        let C;
        let B;
        let R;
        let S;
        let I;
        let T = null;
        let P = false;
        let E;
        let L;
        let D;
        let _;
        let U;
        let $;
        let q;
        let M = null;
        let F;
        let O;
        let V;
        let N;
        let j = true;
        let H = false;
        let W = false;
        if ("slot" === a) {
            if (null == e.root.def.shadowOptions) throw pt(`AUR0717:${e.root.def.name}`);
            e.root.hasSlot = true;
        }
        if (u) {
            j = c.processContent?.call(c.Type, t, e.p);
            x = t.attributes;
            y = x.length;
        }
        if (e.root.def.enhance && t.classList.contains("au")) throw pt(`AUR0705`);
        for (;y > k; ++k) {
            A = x[k];
            C = A.name;
            B = A.value;
            switch (C) {
              case "as-element":
              case "containerless":
                v();
                if (!H) H = "containerless" === C;
                continue;
            }
            R = e.de.parse(C, B);
            M = e.Be(R);
            V = R.target;
            N = R.rawValue;
            if (d && (!m || m && d(V))) {
                if (null != M && 1 & M.type) {
                    v();
                    g.push(R);
                    continue;
                }
                W = V !== Yr && "slot" !== V;
                if (W) {
                    F = BindablesInfo.from(c, false);
                    if (null == F.attrs[V] && !e.Re(V)?.isTemplateController) {
                        v();
                        g.push(R);
                        continue;
                    }
                }
            }
            if (null !== M && 1 & M.type) {
                qr.node = t;
                qr.attr = R;
                qr.bindable = null;
                qr.def = null;
                (S ?? (S = [])).push(M.build(qr, e.ep, e.m));
                v();
                continue;
            }
            T = e.Re(V);
            if (null !== T) {
                F = BindablesInfo.from(T, true);
                P = false === T.noMultiBindings && null === M && Dr(N);
                if (P) D = this.Se(t, N, T, e); else {
                    O = F.primary;
                    if (null === M) {
                        $ = p.parse(N, 1);
                        D = [ null === $ ? new SetPropertyInstruction(N, O.property) : new InterpolationInstruction($, O.property) ];
                    } else {
                        qr.node = t;
                        qr.attr = R;
                        qr.bindable = O;
                        qr.def = T;
                        D = [ M.build(qr, e.ep, e.m) ];
                    }
                }
                v();
                if (T.isTemplateController) (_ ?? (_ = [])).push(new HydrateTemplateController($r, this.resolveResources ? T : T.name, void 0, D)); else (L ?? (L = [])).push(new HydrateAttributeInstruction(this.resolveResources ? T : T.name, null != T.aliases && T.aliases.includes(V) ? V : void 0, D));
                continue;
            }
            if (null === M) {
                if (u) {
                    F = BindablesInfo.from(c, false);
                    E = F.attrs[V];
                    if (void 0 !== E) {
                        $ = p.parse(N, 1);
                        (I ?? (I = [])).push(null == $ ? new SetPropertyInstruction(N, E.property) : new InterpolationInstruction($, E.property));
                        v();
                        continue;
                    }
                }
                $ = p.parse(N, 1);
                if (null != $) {
                    v();
                    (S ?? (S = [])).push(new InterpolationInstruction($, e.m.map(t, V) ?? b(V)));
                }
                continue;
            }
            v();
            if (u) {
                F = BindablesInfo.from(c, false);
                E = F.attrs[V];
                if (void 0 !== E) {
                    qr.node = t;
                    qr.attr = R;
                    qr.bindable = E;
                    qr.def = c;
                    (I ?? (I = [])).push(M.build(qr, e.ep, e.m));
                    continue;
                }
            }
            qr.node = t;
            qr.attr = R;
            qr.bindable = null;
            qr.def = null;
            (S ?? (S = [])).push(M.build(qr, e.ep, e.m));
        }
        _r();
        if (this.Le(t) && null != S && S.length > 1) this.De(t, S);
        if (u) {
            q = new HydrateElementInstruction(this.resolveResources ? c : c.name, void 0, I ?? l, null, H, g);
            if (a === Yr) {
                const i = t.getAttribute("name") || Qr;
                const s = e.t();
                const n = e._e();
                let r = t.firstChild;
                while (null !== r) {
                    if (1 === r.nodeType && r.hasAttribute(Yr)) t.removeChild(r); else ys(s, r);
                    r = t.firstChild;
                }
                this.ke(s.content, n);
                q.auSlot = {
                    name: i,
                    fallback: CustomElementDefinition.create({
                        name: Js(),
                        template: s,
                        instructions: n.rows,
                        needsCompile: false
                    })
                };
                t = this.Ue(t, e);
            }
        }
        if (null != S || null != q || null != L) {
            w = l.concat(q ?? l, L ?? l, S ?? l);
            this.Ee(t);
        }
        let z;
        if (null != _) {
            y = _.length - 1;
            k = y;
            U = _[k];
            let n;
            if (Er(t)) {
                n = e.t();
                ks(n, [ e.$e(Tr), e.$e(Pr), this.Ee(e.h(Sr)) ]);
            } else {
                this.Ue(t, e);
                if ("TEMPLATE" === t.nodeName) n = t; else {
                    n = e.t();
                    ys(n, t);
                }
            }
            const r = n;
            const o = e._e(null == w ? [] : [ w ]);
            let l;
            let h;
            let d;
            let m;
            let g;
            let p;
            let v;
            let x;
            let b = 0, A = 0;
            let C = t.firstChild;
            let B = false;
            if (false !== j) while (null !== C) {
                h = 1 === C.nodeType ? C.getAttribute(Yr) : null;
                if (null !== h) C.removeAttribute(Yr);
                if (u) {
                    l = C.nextSibling;
                    if (!f) {
                        B = 3 === C.nodeType && "" === C.textContent.trim();
                        if (!B) ((i = m ?? (m = {}))[s = h || Qr] ?? (i[s] = [])).push(C);
                        t.removeChild(C);
                    }
                    C = l;
                } else {
                    if (null !== h) {
                        h = h || Qr;
                        throw pt(`AUR0706:${a}[${h}]`);
                    }
                    C = C.nextSibling;
                }
            }
            if (null != m) {
                d = {};
                for (h in m) {
                    n = e.t();
                    g = m[h];
                    for (b = 0, A = g.length; A > b; ++b) {
                        p = g[b];
                        if ("TEMPLATE" === p.nodeName) if (p.attributes.length > 0) ys(n, p); else ys(n, p.content); else ys(n, p);
                    }
                    x = e._e();
                    this.ke(n.content, x);
                    d[h] = CustomElementDefinition.create({
                        name: Js(),
                        template: n,
                        instructions: x.rows,
                        needsCompile: false,
                        isStrictBinding: e.root.def.isStrictBinding
                    });
                }
                q.projections = d;
            }
            if (u && (H || c.containerless)) this.Ue(t, e);
            z = !u || !c.containerless && !H && false !== j;
            if (z) if (t.nodeName === Ir) this.ke(t.content, o); else {
                C = t.firstChild;
                while (null !== C) C = this.ke(C, o);
            }
            U.def = CustomElementDefinition.create({
                name: Js(),
                template: r,
                instructions: o.rows,
                needsCompile: false,
                isStrictBinding: e.root.def.isStrictBinding
            });
            while (k-- > 0) {
                U = _[k];
                n = e.t();
                v = this.Ee(e.h(Sr));
                ks(n, [ e.$e(Tr), e.$e(Pr), v ]);
                U.def = CustomElementDefinition.create({
                    name: Js(),
                    template: n,
                    needsCompile: false,
                    instructions: [ [ _[k + 1] ] ],
                    isStrictBinding: e.root.def.isStrictBinding
                });
            }
            e.rows.push([ U ]);
        } else {
            if (null != w) e.rows.push(w);
            let i = t.firstChild;
            let s;
            let n;
            let l = null;
            let h;
            let d;
            let m;
            let g;
            let p;
            let v = false;
            let x = 0, b = 0;
            if (false !== j) while (null !== i) {
                n = 1 === i.nodeType ? i.getAttribute(Yr) : null;
                if (null !== n) i.removeAttribute(Yr);
                if (u) {
                    s = i.nextSibling;
                    if (!f) {
                        v = 3 === i.nodeType && "" === i.textContent.trim();
                        if (!v) ((r = h ?? (h = {}))[o = n || Qr] ?? (r[o] = [])).push(i);
                        t.removeChild(i);
                    }
                    i = s;
                } else {
                    if (null !== n) {
                        n = n || Qr;
                        throw pt(`AUR0706:${a}[${n}]`);
                    }
                    i = i.nextSibling;
                }
            }
            if (null != h) {
                l = {};
                for (n in h) {
                    g = e.t();
                    d = h[n];
                    for (x = 0, b = d.length; b > x; ++x) {
                        m = d[x];
                        if (m.nodeName === Ir) if (m.attributes.length > 0) ys(g, m); else ys(g, m.content); else ys(g, m);
                    }
                    p = e._e();
                    this.ke(g.content, p);
                    l[n] = CustomElementDefinition.create({
                        name: Js(),
                        template: g,
                        instructions: p.rows,
                        needsCompile: false,
                        isStrictBinding: e.root.def.isStrictBinding
                    });
                }
                q.projections = l;
            }
            if (u && (H || c.containerless)) this.Ue(t, e);
            z = !u || !c.containerless && !H && false !== j;
            if (z && t.childNodes.length > 0) {
                i = t.firstChild;
                while (null !== i) i = this.ke(i, e);
            }
        }
        return h;
    }
    Pe(t, e) {
        const i = t.parentNode;
        const s = e.ep.parse(t.textContent, 1);
        const n = t.nextSibling;
        let r;
        let o;
        let l;
        let h;
        let a;
        if (null !== s) {
            ({parts: r, expressions: o} = s);
            if (a = r[0]) xs(i, e.qe(a), t);
            for (l = 0, h = o.length; h > l; ++l) {
                ws(i, t, [ e.$e(Tr), e.$e(Pr), this.Ee(e.h(Sr)) ]);
                if (a = r[l + 1]) xs(i, e.qe(a), t);
                e.rows.push([ new TextBindingInstruction(o[l], e.root.def.isStrictBinding) ]);
            }
            i.removeChild(t);
        }
        return n;
    }
    Se(t, e, i, s) {
        const n = BindablesInfo.from(i, true);
        const r = e.length;
        const o = [];
        let l;
        let h;
        let a = 0;
        let c = 0;
        let u;
        let f;
        let d;
        let m;
        for (let g = 0; g < r; ++g) {
            c = e.charCodeAt(g);
            if (92 === c) ++g; else if (58 === c) {
                l = e.slice(a, g);
                while (e.charCodeAt(++g) <= 32) ;
                a = g;
                for (;g < r; ++g) {
                    c = e.charCodeAt(g);
                    if (92 === c) ++g; else if (59 === c) {
                        h = e.slice(a, g);
                        break;
                    }
                }
                if (void 0 === h) h = e.slice(a);
                f = s.de.parse(l, h);
                d = s.Be(f);
                m = n.attrs[f.target];
                if (null == m) throw pt(`AUR0707:${i.name}.${f.target}`);
                if (null === d) {
                    u = s.ep.parse(h, 1);
                    o.push(null === u ? new SetPropertyInstruction(h, m.property) : new InterpolationInstruction(u, m.property));
                } else {
                    qr.node = t;
                    qr.attr = f;
                    qr.bindable = m;
                    qr.def = i;
                    o.push(d.build(qr, s.ep, s.m));
                }
                while (g < r && e.charCodeAt(++g) <= 32) ;
                a = g;
                l = void 0;
                h = void 0;
            }
        }
        _r();
        return o;
    }
    ye(t, e) {
        const i = t;
        const s = y(i.querySelectorAll("template[as-custom-element]"));
        const n = s.length;
        if (0 === n) return;
        if (n === i.childElementCount) throw pt(`AUR0708`);
        const r = new Set;
        const o = [];
        for (const t of s) {
            if (t.parentNode !== i) throw pt(`AUR0709`);
            const s = jr(t, r);
            const n = class LocalTemplate {};
            const l = t.content;
            const h = y(l.querySelectorAll("bindable"));
            const a = $t.for(n);
            const c = new Set;
            const u = new Set;
            for (const t of h) {
                if (t.parentNode !== l) throw pt(`AUR0710`);
                const e = t.getAttribute("property");
                if (null === e) throw pt(`AUR0711`);
                const i = t.getAttribute("attribute");
                if (null !== i && u.has(i) || c.has(e)) throw pt(`AUR0712:${e}+${i}`); else {
                    if (null !== i) u.add(i);
                    c.add(e);
                }
                a.add({
                    property: e,
                    attribute: i ?? void 0,
                    mode: Hr(t)
                });
                const s = t.getAttributeNames().filter((t => !Vr.includes(t)));
                if (s.length > 0) ;
                l.removeChild(t);
            }
            o.push(n);
            e.Me(en({
                name: s,
                template: t
            }, n));
            i.removeChild(t);
        }
        let h = 0;
        const a = o.length;
        for (;a > h; ++h) on(o[h]).dependencies.push(...e.def.dependencies ?? l, ...e.deps ?? l);
    }
    Le(t) {
        return "INPUT" === t.nodeName && 1 === Fr[t.type];
    }
    De(t, e) {
        switch (t.nodeName) {
          case "INPUT":
            {
                const t = e;
                let i;
                let s;
                let n = 0;
                let r;
                for (let e = 0; e < t.length && n < 3; e++) {
                    r = t[e];
                    switch (r.to) {
                      case "model":
                      case "value":
                      case "matcher":
                        i = e;
                        n++;
                        break;

                      case "checked":
                        s = e;
                        n++;
                        break;
                    }
                }
                if (void 0 !== s && void 0 !== i && s < i) [t[i], t[s]] = [ t[s], t[i] ];
            }
        }
    }
    Fe(t) {
        return t.nodeName === Sr && Lr(Rr = bs(t)) && Rr.textContent === Pr && Lr(Rr = bs(Rr)) && Rr.textContent === Tr;
    }
    Ee(t) {
        t.classList.add("au");
        return t;
    }
    Ue(t, e) {
        if (Er(t)) return t;
        const i = t.parentNode;
        const s = this.Ee(e.h(Sr));
        ws(i, t, [ e.$e(Tr), e.$e(Pr), s ]);
        i.removeChild(t);
        return s;
    }
}

let Rr;

const Sr = "AU-M";

const Ir = "TEMPLATE";

const Tr = "au-start";

const Pr = "au-end";

const Er = t => t.nodeName === Sr && Lr(Rr = bs(t)) && Rr.textContent === Pr && Lr(Rr = bs(Rr)) && Rr.textContent === Tr;

const Lr = t => 8 === t?.nodeType;

class CompilationContext {
    constructor(t, e, i, s, n, r) {
        this.hasSlot = false;
        this.Oe = gt();
        const o = null !== s;
        this.c = e;
        this.root = null === n ? this : n;
        this.def = t;
        this.ci = i;
        this.parent = s;
        this.be = o ? s.be : e.get(Ar);
        this.de = o ? s.de : e.get(ee);
        this.ep = o ? s.ep : e.get(q);
        this.m = o ? s.m : e.get(br);
        this.Ve = o ? s.Ve : e.get(k);
        this.p = o ? s.p : e.get(ui);
        this.localEls = o ? s.localEls : new Set;
        this.rows = r ?? [];
    }
    Me(t) {
        var e;
        ((e = this.root).deps ?? (e.deps = [])).push(t);
        this.root.c.register(t);
    }
    qe(t) {
        return vs(this.p, t);
    }
    $e(t) {
        return gs(this.p, t);
    }
    h(t) {
        const e = ms(this.p, t);
        if ("template" === t) this.p.document.adoptNode(e.content);
        return e;
    }
    t() {
        return this.h("template");
    }
    Ce(t) {
        return this.c.find(an, t);
    }
    Re(t) {
        return this.c.find(ti, t);
    }
    _e(t) {
        return new CompilationContext(this.def, this.c, this.ci, this, this.root, t);
    }
    Be(t) {
        if (this.root !== this) return this.root.Be(t);
        const e = t.command;
        if (null === e) return null;
        let i = this.Oe[e];
        if (void 0 === i) {
            i = this.c.create(nr, e);
            if (null === i) throw pt(`AUR0713:${e}`);
            this.Oe[e] = i;
        }
        return i;
    }
}

const Dr = t => {
    const e = t.length;
    let i = 0;
    let s = 0;
    while (e > s) {
        i = t.charCodeAt(s);
        if (92 === i) ++s; else if (58 === i) return true; else if (36 === i && 123 === t.charCodeAt(s + 1)) return false;
        ++s;
    }
    return false;
};

const _r = () => {
    qr.node = qr.attr = qr.bindable = qr.def = null;
};

const Ur = {
    projections: null
};

const $r = {
    name: "unnamed"
};

const qr = {
    node: null,
    attr: null,
    bindable: null,
    def: null
};

const Mr = wt(gt(), {
    id: true,
    name: true,
    "au-slot": true,
    "as-element": true
});

const Fr = {
    checkbox: 1,
    radio: 1
};

const Or = new WeakMap;

class BindablesInfo {
    static from(t, e) {
        let i = Or.get(t);
        if (null == i) {
            const s = t.bindables;
            const n = gt();
            const r = e ? void 0 === t.defaultBindingMode ? 8 : t.defaultBindingMode : 8;
            let o;
            let l;
            let h = false;
            let a;
            let c;
            for (l in s) {
                o = s[l];
                c = o.attribute;
                if (true === o.primary) {
                    if (h) throw pt(`AUR0714:${t.name}`);
                    h = true;
                    a = o;
                } else if (!h && null == a) a = o;
                n[c] = BindableDefinition.create(l, t.Type, o);
            }
            if (null == o && e) a = n.value = BindableDefinition.create("value", t.Type, {
                mode: r
            });
            Or.set(t, i = new BindablesInfo(n, s, a));
        }
        return i;
    }
    constructor(t, e, i) {
        this.attrs = t;
        this.bindables = e;
        this.primary = i;
    }
}

const Vr = xt([ "property", "attribute", "mode" ]);

const Nr = "as-custom-element";

const jr = (t, e) => {
    const i = t.getAttribute(Nr);
    if (null === i || "" === i) throw pt(`AUR0715`);
    if (e.has(i)) throw pt(`AUR0716:${i}`); else {
        e.add(i);
        t.removeAttribute(Nr);
    }
    return i;
};

const Hr = t => {
    switch (t.getAttribute("mode")) {
      case "oneTime":
        return 1;

      case "toView":
        return 2;

      case "fromView":
        return 4;

      case "twoWay":
        return 6;

      case "default":
      default:
        return 8;
    }
};

const Wr = jt("ITemplateCompilerHooks");

const zr = new WeakMap;

const Gr = at("compiler-hooks");

const Xr = xt({
    name: Gr,
    define(t) {
        let e = zr.get(t);
        if (void 0 === e) {
            zr.set(t, e = new TemplateCompilerHooksDefinition(t));
            rt(Gr, e, t);
            ct(t, Gr);
        }
        return t;
    }
});

class TemplateCompilerHooksDefinition {
    get name() {
        return "";
    }
    constructor(t) {
        this.Type = t;
    }
    register(t) {
        t.register(Ht(Wr, this.Type));
    }
}

const Kr = t => {
    return void 0 === t ? e : e(t);
    function e(t) {
        return Xr.define(t);
    }
};

const Qr = "default";

const Yr = "au-slot";

const Zr = new Map;

class BindingModeBehavior {
    bind(t, e) {
        Zr.set(e, e.mode);
        e.mode = this.mode;
    }
    unbind(t, e) {
        e.mode = Zr.get(e);
        Zr.delete(e);
    }
}

class OneTimeBindingBehavior extends BindingModeBehavior {
    get mode() {
        return 1;
    }
}

class ToViewBindingBehavior extends BindingModeBehavior {
    get mode() {
        return 2;
    }
}

class FromViewBindingBehavior extends BindingModeBehavior {
    get mode() {
        return 4;
    }
}

class TwoWayBindingBehavior extends BindingModeBehavior {
    get mode() {
        return 6;
    }
}

ve("oneTime")(OneTimeBindingBehavior);

ve("toView")(ToViewBindingBehavior);

ve("fromView")(FromViewBindingBehavior);

ve("twoWay")(TwoWayBindingBehavior);

const Jr = new WeakMap;

const to = 200;

class DebounceBindingBehavior {
    constructor(t) {
        this.p = t;
    }
    bind(t, e, i) {
        i = Number(i);
        const s = {
            type: "debounce",
            delay: i > 0 ? i : to,
            now: this.p.performanceNow,
            queue: this.p.taskQueue
        };
        const n = e.limit?.(s);
        if (null == n) ; else Jr.set(e, n);
    }
    unbind(t, e) {
        Jr.get(e)?.dispose();
        Jr.delete(e);
    }
}

DebounceBindingBehavior.inject = [ c ];

ve("debounce")(DebounceBindingBehavior);

class SignalBindingBehavior {
    constructor(t) {
        this.Ne = new Map;
        this.je = t;
    }
    bind(t, e, ...i) {
        if (!("handleChange" in e)) throw pt(`AUR0817`);
        if (0 === i.length) throw pt(`AUR0818`);
        this.Ne.set(e, i);
        let s;
        for (s of i) this.je.addSignalListener(s, e);
    }
    unbind(t, e) {
        const i = this.Ne.get(e);
        this.Ne.delete(e);
        let s;
        for (s of i) this.je.removeSignalListener(s, e);
    }
}

SignalBindingBehavior.inject = [ P ];

ve("signal")(SignalBindingBehavior);

const eo = new WeakMap;

const io = 200;

class ThrottleBindingBehavior {
    constructor(t) {
        this.He = t.performanceNow;
        this.ct = t.taskQueue;
    }
    bind(t, e, i) {
        i = Number(i);
        const s = {
            type: "throttle",
            delay: i > 0 ? i : io,
            now: this.He,
            queue: this.ct
        };
        const n = e.limit?.(s);
        if (null == n) ; else eo.set(e, n);
    }
    unbind(t, e) {
        eo.get(e)?.dispose();
        eo.delete(e);
    }
}

ThrottleBindingBehavior.inject = [ c ];

ve("throttle")(ThrottleBindingBehavior);

class DataAttributeAccessor {
    constructor() {
        this.type = 2 | 4;
    }
    getValue(t, e) {
        return t.getAttribute(e);
    }
    setValue(t, e, i) {
        if (null == t) e.removeAttribute(i); else e.setAttribute(i, t);
    }
}

gi(DataAttributeAccessor);

const so = new DataAttributeAccessor;

class AttrBindingBehavior {
    bind(t, e) {
        if (!(e instanceof PropertyBinding)) throw pt(`AURxxxx`);
        e.useTargetObserver(so);
    }
}

ve("attr")(AttrBindingBehavior);

class SelfBindingBehavior {
    bind(t, e) {
        if (!(e instanceof ListenerBinding)) throw pt(`AUR0801`);
        e.self = true;
    }
    unbind(t, e) {
        e.self = false;
    }
}

ve("self")(SelfBindingBehavior);

const no = gt();

class AttributeNSAccessor {
    static forNs(t) {
        return no[t] ?? (no[t] = new AttributeNSAccessor(t));
    }
    constructor(t) {
        this.ns = t;
        this.type = 2 | 4;
    }
    getValue(t, e) {
        return t.getAttributeNS(this.ns, e);
    }
    setValue(t, e, i) {
        if (null == t) e.removeAttributeNS(this.ns, i); else e.setAttributeNS(this.ns, i, t);
    }
}

gi(AttributeNSAccessor);

function ro(t, e) {
    return t === e;
}

class CheckedObserver {
    constructor(t, e, i, s) {
        this.type = 2 | 1 | 4;
        this.v = void 0;
        this.ov = void 0;
        this.We = void 0;
        this.ze = void 0;
        this.yt = false;
        this.bt = t;
        this.oL = s;
        this.cf = i;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        const e = this.v;
        if (t === e) return;
        this.v = t;
        this.ov = e;
        this.Ge();
        this.Xe();
        this.st();
    }
    handleCollectionChange() {
        this.Xe();
    }
    handleChange(t, e) {
        this.Xe();
    }
    Xe() {
        const t = this.v;
        const e = this.bt;
        const i = vt.call(e, "model") ? e.model : e.value;
        const s = "radio" === e.type;
        const n = void 0 !== e.matcher ? e.matcher : ro;
        if (s) e.checked = !!n(t, i); else if (true === t) e.checked = true; else {
            let s = false;
            if (Bt(t)) s = -1 !== t.findIndex((t => !!n(t, i))); else if (t instanceof Set) {
                for (const e of t) if (n(e, i)) {
                    s = true;
                    break;
                }
            } else if (t instanceof Map) for (const e of t) {
                const t = e[0];
                const r = e[1];
                if (n(t, i) && true === r) {
                    s = true;
                    break;
                }
            }
            e.checked = s;
        }
    }
    handleEvent() {
        let t = this.ov = this.v;
        const e = this.bt;
        const i = vt.call(e, "model") ? e.model : e.value;
        const s = e.checked;
        const n = void 0 !== e.matcher ? e.matcher : ro;
        if ("checkbox" === e.type) {
            if (Bt(t)) {
                const e = t.findIndex((t => !!n(t, i)));
                if (s && -1 === e) t.push(i); else if (!s && -1 !== e) t.splice(e, 1);
                return;
            } else if (t instanceof Set) {
                const e = {};
                let r = e;
                for (const e of t) if (true === n(e, i)) {
                    r = e;
                    break;
                }
                if (s && r === e) t.add(i); else if (!s && r !== e) t.delete(r);
                return;
            } else if (t instanceof Map) {
                let e;
                for (const s of t) {
                    const t = s[0];
                    if (true === n(t, i)) {
                        e = t;
                        break;
                    }
                }
                t.set(e, s);
                return;
            }
            t = s;
        } else if (s) t = i; else return;
        this.v = t;
        this.st();
    }
    kt() {
        this.Ge();
    }
    At() {
        this.We?.unsubscribe(this);
        this.ze?.unsubscribe(this);
        this.We = this.ze = void 0;
    }
    st() {
        oo = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, oo);
    }
    Ge() {
        const t = this.bt;
        (this.ze ?? (this.ze = t.$observers?.model ?? t.$observers?.value))?.subscribe(this);
        this.We?.unsubscribe(this);
        this.We = void 0;
        if ("checkbox" === t.type) (this.We = wo(this.v, this.oL))?.subscribe(this);
    }
}

mi(CheckedObserver);

I(CheckedObserver);

let oo;

const lo = {
    childList: true,
    subtree: true,
    characterData: true
};

function ho(t, e) {
    return t === e;
}

class SelectValueObserver {
    constructor(t, e, i, s) {
        this.type = 2 | 1 | 4;
        this.v = void 0;
        this.ov = void 0;
        this.J = false;
        this.Ke = void 0;
        this.Qe = void 0;
        this.iO = false;
        this.yt = false;
        this.bt = t;
        this.oL = s;
        this.cf = i;
    }
    getValue() {
        return this.iO ? this.v : this.bt.multiple ? ao(this.bt.options) : this.bt.value;
    }
    setValue(t) {
        this.ov = this.v;
        this.v = t;
        this.J = t !== this.ov;
        this.Ye(t instanceof Array ? t : null);
        this.it();
    }
    it() {
        if (this.J) {
            this.J = false;
            this.syncOptions();
        }
    }
    handleCollectionChange() {
        this.syncOptions();
    }
    syncOptions() {
        const t = this.v;
        const e = this.bt;
        const i = Bt(t);
        const s = e.matcher ?? ho;
        const n = e.options;
        let r = n.length;
        while (r-- > 0) {
            const e = n[r];
            const o = vt.call(e, "model") ? e.model : e.value;
            if (i) {
                e.selected = -1 !== t.findIndex((t => !!s(o, t)));
                continue;
            }
            e.selected = !!s(o, t);
        }
    }
    syncValue() {
        const t = this.bt;
        const e = t.options;
        const i = e.length;
        const s = this.v;
        let n = 0;
        if (t.multiple) {
            if (!(s instanceof Array)) return true;
            let r;
            const o = t.matcher || ho;
            const l = [];
            while (n < i) {
                r = e[n];
                if (r.selected) l.push(vt.call(r, "model") ? r.model : r.value);
                ++n;
            }
            let h;
            n = 0;
            while (n < s.length) {
                h = s[n];
                if (-1 === l.findIndex((t => !!o(h, t)))) s.splice(n, 1); else ++n;
            }
            n = 0;
            while (n < l.length) {
                h = l[n];
                if (-1 === s.findIndex((t => !!o(h, t)))) s.push(h);
                ++n;
            }
            return false;
        }
        let r = null;
        let o;
        while (n < i) {
            o = e[n];
            if (o.selected) {
                r = vt.call(o, "model") ? o.model : o.value;
                break;
            }
            ++n;
        }
        this.ov = this.v;
        this.v = r;
        return true;
    }
    kt() {
        (this.Qe = new this.bt.ownerDocument.defaultView.MutationObserver(this.Ze.bind(this))).observe(this.bt, lo);
        this.Ye(this.v instanceof Array ? this.v : null);
        this.iO = true;
    }
    At() {
        this.Qe.disconnect();
        this.Ke?.unsubscribe(this);
        this.Qe = this.Ke = void 0;
        this.iO = false;
    }
    Ye(t) {
        this.Ke?.unsubscribe(this);
        this.Ke = void 0;
        if (null != t) {
            if (!this.bt.multiple) throw pt(`AUR0654`);
            (this.Ke = this.oL.getArrayObserver(t)).subscribe(this);
        }
    }
    handleEvent() {
        const t = this.syncValue();
        if (t) this.st();
    }
    Ze(t) {
        this.syncOptions();
        const e = this.syncValue();
        if (e) this.st();
    }
    st() {
        co = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, co);
    }
}

mi(SelectValueObserver);

I(SelectValueObserver);

function ao(t) {
    const e = [];
    if (0 === t.length) return e;
    const i = t.length;
    let s = 0;
    let n;
    while (i > s) {
        n = t[s];
        if (n.selected) e[e.length] = vt.call(n, "model") ? n.model : n.value;
        ++s;
    }
    return e;
}

let co;

const uo = "--";

class StyleAttributeAccessor {
    constructor(t) {
        this.obj = t;
        this.type = 2 | 4;
        this.v = "";
        this.ov = "";
        this.styles = {};
        this.version = 0;
        this.J = false;
    }
    getValue() {
        return this.obj.style.cssText;
    }
    setValue(t) {
        this.v = t;
        this.J = t !== this.ov;
        this.it();
    }
    Je(t) {
        const e = [];
        const i = /url\([^)]+$/;
        let s = 0;
        let n = "";
        let r;
        let o;
        let l;
        let h;
        while (s < t.length) {
            r = t.indexOf(";", s);
            if (-1 === r) r = t.length;
            n += t.substring(s, r);
            s = r + 1;
            if (i.test(n)) {
                n += ";";
                continue;
            }
            o = n.indexOf(":");
            l = n.substring(0, o).trim();
            h = n.substring(o + 1).trim();
            e.push([ l, h ]);
            n = "";
        }
        return e;
    }
    ti(t) {
        let e;
        let i;
        const n = [];
        for (i in t) {
            e = t[i];
            if (null == e) continue;
            if (St(e)) {
                if (i.startsWith(uo)) {
                    n.push([ i, e ]);
                    continue;
                }
                n.push([ s(i), e ]);
                continue;
            }
            n.push(...this.ei(e));
        }
        return n;
    }
    ii(t) {
        const e = t.length;
        if (e > 0) {
            const i = [];
            let s = 0;
            for (;e > s; ++s) i.push(...this.ei(t[s]));
            return i;
        }
        return l;
    }
    ei(t) {
        if (St(t)) return this.Je(t);
        if (t instanceof Array) return this.ii(t);
        if (t instanceof Object) return this.ti(t);
        return l;
    }
    it() {
        if (this.J) {
            this.J = false;
            const t = this.v;
            const e = this.styles;
            const i = this.ei(t);
            let s;
            let n = this.version;
            this.ov = t;
            let r;
            let o;
            let l;
            let h = 0;
            const a = i.length;
            for (;h < a; ++h) {
                r = i[h];
                o = r[0];
                l = r[1];
                this.setProperty(o, l);
                e[o] = n;
            }
            this.styles = e;
            this.version += 1;
            if (0 === n) return;
            n -= 1;
            for (s in e) {
                if (!vt.call(e, s) || e[s] !== n) continue;
                this.obj.style.removeProperty(s);
            }
        }
    }
    setProperty(t, e) {
        let i = "";
        if (null != e && Rt(e.indexOf) && e.includes("!important")) {
            i = "important";
            e = e.replace("!important", "");
        }
        this.obj.style.setProperty(t, e, i);
    }
    bind() {
        this.v = this.ov = this.obj.style.cssText;
    }
}

gi(StyleAttributeAccessor);

class ValueAttributeObserver {
    constructor(t, e, i) {
        this.type = 2 | 1 | 4;
        this.v = "";
        this.ov = "";
        this.J = false;
        this.yt = false;
        this.bt = t;
        this.k = e;
        this.cf = i;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        if (Pt(t, this.v)) return;
        this.ov = this.v;
        this.v = t;
        this.J = true;
        if (!this.cf.readonly) this.it();
    }
    it() {
        if (this.J) {
            this.J = false;
            this.bt[this.k] = this.v ?? this.cf.default;
            this.st();
        }
    }
    handleEvent() {
        this.ov = this.v;
        this.v = this.bt[this.k];
        if (this.ov !== this.v) {
            this.J = false;
            this.st();
        }
    }
    kt() {
        this.v = this.ov = this.bt[this.k];
    }
    st() {
        fo = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, fo);
    }
}

mi(ValueAttributeObserver);

I(ValueAttributeObserver);

let fo;

const mo = "http://www.w3.org/1999/xlink";

const go = "http://www.w3.org/XML/1998/namespace";

const po = "http://www.w3.org/2000/xmlns/";

const vo = wt(gt(), {
    "xlink:actuate": [ "actuate", mo ],
    "xlink:arcrole": [ "arcrole", mo ],
    "xlink:href": [ "href", mo ],
    "xlink:role": [ "role", mo ],
    "xlink:show": [ "show", mo ],
    "xlink:title": [ "title", mo ],
    "xlink:type": [ "type", mo ],
    "xml:lang": [ "lang", go ],
    "xml:space": [ "space", go ],
    xmlns: [ "xmlns", po ],
    "xmlns:xlink": [ "xlink", po ]
});

const xo = new j;

xo.type = 2 | 4;

class NodeObserverLocator {
    constructor(t, e, i, s) {
        this.locator = t;
        this.platform = e;
        this.dirtyChecker = i;
        this.svgAnalyzer = s;
        this.allowDirtyCheck = true;
        this.si = gt();
        this.ni = gt();
        this.ri = gt();
        this.oi = gt();
        const n = [ "change", "input" ];
        const r = {
            events: n,
            default: ""
        };
        this.useConfig({
            INPUT: {
                value: r,
                valueAsNumber: {
                    events: n,
                    default: 0
                },
                checked: {
                    type: CheckedObserver,
                    events: n
                },
                files: {
                    events: n,
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
                value: r
            }
        });
        const o = {
            events: [ "change", "input", "blur", "keyup", "paste" ],
            default: ""
        };
        const l = {
            events: [ "scroll" ],
            default: 0
        };
        this.useConfigGlobal({
            scrollTop: l,
            scrollLeft: l,
            textContent: o,
            innerHTML: o
        });
        this.overrideAccessorGlobal("css", "style", "class");
        this.overrideAccessor({
            INPUT: [ "value", "checked", "model" ],
            SELECT: [ "value" ],
            TEXTAREA: [ "value" ]
        });
    }
    static register(t) {
        Wt(H, NodeObserverLocator).register(t);
        Ht(H, NodeObserverLocator).register(t);
    }
    handles(t, e) {
        return t instanceof this.platform.Node;
    }
    useConfig(t, e, i) {
        const s = this.si;
        let n;
        if (St(t)) {
            n = s[t] ?? (s[t] = gt());
            if (null == n[e]) n[e] = i; else bo(t, e);
        } else for (const i in t) {
            n = s[i] ?? (s[i] = gt());
            const r = t[i];
            for (e in r) if (null == n[e]) n[e] = r[e]; else bo(i, e);
        }
    }
    useConfigGlobal(t, e) {
        const i = this.ni;
        if ("object" === typeof t) for (const e in t) if (null == i[e]) i[e] = t[e]; else bo("*", e); else if (null == i[t]) i[t] = e; else bo("*", t);
    }
    getAccessor(t, e, i) {
        if (e in this.oi || e in (this.ri[t.tagName] ?? A)) return this.getObserver(t, e, i);
        switch (e) {
          case "src":
          case "href":
          case "role":
          case "minLength":
          case "maxLength":
          case "placeholder":
          case "size":
          case "pattern":
          case "title":
            return so;

          default:
            {
                const i = vo[e];
                if (void 0 !== i) return AttributeNSAccessor.forNs(i[1]);
                if (At(t, e, this.svgAnalyzer)) return so;
                return xo;
            }
        }
    }
    overrideAccessor(t, e) {
        var i, s;
        let n;
        if (St(t)) {
            n = (i = this.ri)[t] ?? (i[t] = gt());
            n[e] = true;
        } else for (const e in t) for (const i of t[e]) {
            n = (s = this.ri)[e] ?? (s[e] = gt());
            n[i] = true;
        }
    }
    overrideAccessorGlobal(...t) {
        for (const e of t) this.oi[e] = true;
    }
    getNodeObserverConfig(t, e) {
        return this.si[t.tagName]?.[e] ?? this.ni[e];
    }
    getNodeObserver(t, e, i) {
        const s = this.si[t.tagName]?.[e] ?? this.ni[e];
        let n;
        if (null != s) {
            n = new (s.type ?? ValueAttributeObserver)(t, e, s, i, this.locator);
            if (!n.doNotCache) W(t)[e] = n;
            return n;
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
        const s = this.getNodeObserver(t, e, i);
        if (null != s) return s;
        const n = vo[e];
        if (void 0 !== n) return AttributeNSAccessor.forNs(n[1]);
        if (At(t, e, this.svgAnalyzer)) return so;
        if (e in t.constructor.prototype) {
            if (this.allowDirtyCheck) return this.dirtyChecker.createProperty(t, e);
            throw pt(`AUR0652:${String(e)}`);
        } else return new z(t, e);
    }
}

NodeObserverLocator.inject = [ C, ui, G, xr ];

function wo(t, e) {
    if (t instanceof Array) return e.getArrayObserver(t);
    if (t instanceof Map) return e.getMapObserver(t);
    if (t instanceof Set) return e.getSetObserver(t);
}

function bo(t, e) {
    throw pt(`AUR0653:${String(e)}@${t}`);
}

class UpdateTriggerBindingBehavior {
    constructor(t, e) {
        if (!(e instanceof NodeObserverLocator)) throw pt("AURxxxx: updateTrigger binding behavior only works with the default implementation of Aurelia HTML observation. Implement your own node observation + updateTrigger");
        this.oL = t;
        this.li = e;
    }
    bind(t, e, ...i) {
        if (0 === i.length) throw pt(`AUR0802`);
        if (!(e instanceof PropertyBinding) || !(4 & e.mode)) throw pt(`AUR0803`);
        const s = this.li.getNodeObserverConfig(e.target, e.targetProperty);
        if (null == s) throw pt(`AURxxxx`);
        const n = this.li.getNodeObserver(e.target, e.targetProperty, this.oL);
        n.useConfig({
            readonly: s.readonly,
            default: s.default,
            events: i
        });
        e.useTargetObserver(n);
    }
}

UpdateTriggerBindingBehavior.inject = [ M, H ];

ve("updateTrigger")(UpdateTriggerBindingBehavior);

class Focus {
    constructor(t, e) {
        this.hi = false;
        this.ai = t;
        this.p = e;
    }
    binding() {
        this.valueChanged();
    }
    valueChanged() {
        if (this.$controller.isActive) this.ui(); else this.hi = true;
    }
    attached() {
        if (this.hi) {
            this.hi = false;
            this.ui();
        }
        this.ai.addEventListener("focus", this);
        this.ai.addEventListener("blur", this);
    }
    afterDetachChildren() {
        const t = this.ai;
        t.removeEventListener("focus", this);
        t.removeEventListener("blur", this);
    }
    handleEvent(t) {
        if ("focus" === t.type) this.value = true; else if (!this.fi) this.value = false;
    }
    ui() {
        const t = this.ai;
        const e = this.fi;
        const i = this.value;
        if (i && !e) t.focus(); else if (!i && e) t.blur();
    }
    get fi() {
        return this.ai === this.p.document.activeElement;
    }
}

Focus.inject = [ Ss, ui ];

et([ Dt({
    mode: 6
}) ], Focus.prototype, "value", void 0);

We("focus")(Focus);

let yo = class Show {
    constructor(t, e, i) {
        this.el = t;
        this.p = e;
        this.di = false;
        this.lt = null;
        this.$val = "";
        this.$prio = "";
        this.update = () => {
            this.lt = null;
            if (Boolean(this.value) !== this.mi) if (this.mi === this.gi) {
                this.mi = !this.gi;
                this.$val = this.el.style.getPropertyValue("display");
                this.$prio = this.el.style.getPropertyPriority("display");
                this.el.style.setProperty("display", "none", "important");
            } else {
                this.mi = this.gi;
                this.el.style.setProperty("display", this.$val, this.$prio);
                if ("" === this.el.getAttribute("style")) this.el.removeAttribute("style");
            }
        };
        this.mi = this.gi = "hide" !== i.alias;
    }
    binding() {
        this.di = true;
        this.update();
    }
    detaching() {
        this.di = false;
        this.lt?.cancel();
        this.lt = null;
    }
    valueChanged() {
        if (this.di && null === this.lt) this.lt = this.p.domWriteQueue.queueTask(this.update);
    }
};

et([ Dt ], yo.prototype, "value", void 0);

yo = et([ it(0, Ss), it(1, ui), it(2, vn) ], yo);

Kt("hide")(yo);

We("show")(yo);

class Portal {
    constructor(t, e, i) {
        this.position = "beforeend";
        this.strict = false;
        this.p = i;
        this.pi = i.document.createElement("div");
        (this.view = t.create()).setLocation(this.vi = ps(i));
        Ds(this.view.nodes, e);
    }
    attaching(t) {
        if (null == this.callbackContext) this.callbackContext = this.$controller.scope.bindingContext;
        const e = this.pi = this.xi();
        this.wi(e, this.position);
        return this.bi(t, e);
    }
    detaching(t) {
        return this.yi(t, this.pi);
    }
    targetChanged() {
        const {$controller: t} = this;
        if (!t.isActive) return;
        const e = this.xi();
        if (this.pi === e) return;
        this.pi = e;
        const i = g(this.yi(null, e), (() => {
            this.wi(e, this.position);
            return this.bi(null, e);
        }));
        if (Ct(i)) i.catch(Tt);
    }
    positionChanged() {
        const {$controller: t, pi: e} = this;
        if (!t.isActive) return;
        const i = g(this.yi(null, e), (() => {
            this.wi(e, this.position);
            return this.bi(null, e);
        }));
        if (Ct(i)) i.catch(Tt);
    }
    bi(t, e) {
        const {activating: i, callbackContext: s, view: n} = this;
        return g(i?.call(s, e, n), (() => this.ki(t, e)));
    }
    ki(t, e) {
        const {$controller: i, view: s} = this;
        if (null === t) s.nodes.insertBefore(this.vi); else return g(s.activate(t ?? s, i, i.scope), (() => this.Ai(e)));
        return this.Ai(e);
    }
    Ai(t) {
        const {activated: e, callbackContext: i, view: s} = this;
        return e?.call(i, t, s);
    }
    yi(t, e) {
        const {deactivating: i, callbackContext: s, view: n} = this;
        return g(i?.call(s, e, n), (() => this.Ci(t, e)));
    }
    Ci(t, e) {
        const {$controller: i, view: s} = this;
        if (null === t) s.nodes.remove(); else return g(s.deactivate(t, i), (() => this.Bi(e)));
        return this.Bi(e);
    }
    Bi(t) {
        const {deactivated: e, callbackContext: i, view: s} = this;
        return e?.call(i, t, s);
    }
    xi() {
        const t = this.p;
        const e = t.document;
        let i = this.target;
        let s = this.renderContext;
        if ("" === i) {
            if (this.strict) throw pt(`AUR0811`);
            return e.body;
        }
        if (St(i)) {
            let n = e;
            if (St(s)) s = e.querySelector(s);
            if (s instanceof t.Node) n = s;
            i = n.querySelector(i);
        }
        if (i instanceof t.Node) return i;
        if (null == i) {
            if (this.strict) throw pt(`AUR0812`);
            return e.body;
        }
        return i;
    }
    wi(t, e) {
        const i = this.vi;
        const s = i.$start;
        const n = t.parentNode;
        const r = [ s, i ];
        switch (e) {
          case "beforeend":
            ws(t, null, r);
            break;

          case "afterbegin":
            ws(t, t.firstChild, r);
            break;

          case "beforebegin":
            ws(n, t, r);
            break;

          case "afterend":
            ws(n, t.nextSibling, r);
            break;

          default:
            throw new Error("Invalid portal insertion position");
        }
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
        this.callbackContext = null;
    }
    accept(t) {
        if (true === this.view?.accept(t)) return true;
    }
}

Portal.inject = [ Di, Ts, ui ];

et([ Dt({
    primary: true
}) ], Portal.prototype, "target", void 0);

et([ Dt() ], Portal.prototype, "position", void 0);

et([ Dt({
    callback: "targetChanged"
}) ], Portal.prototype, "renderContext", void 0);

et([ Dt() ], Portal.prototype, "strict", void 0);

et([ Dt() ], Portal.prototype, "deactivating", void 0);

et([ Dt() ], Portal.prototype, "activating", void 0);

et([ Dt() ], Portal.prototype, "deactivated", void 0);

et([ Dt() ], Portal.prototype, "activated", void 0);

et([ Dt() ], Portal.prototype, "callbackContext", void 0);

ze("portal")(Portal);

class If {
    constructor(t, e) {
        this.elseFactory = void 0;
        this.elseView = void 0;
        this.ifView = void 0;
        this.view = void 0;
        this.value = false;
        this.cache = true;
        this.pending = void 0;
        this.Ri = false;
        this.Si = 0;
        this.Ii = t;
        this.l = e;
    }
    attaching(t, e) {
        let i;
        const s = this.$controller;
        const n = this.Si++;
        const r = () => !this.Ri && this.Si === n + 1;
        return g(this.pending, (() => {
            if (!r()) return;
            this.pending = void 0;
            if (this.value) i = this.view = this.ifView = this.cache && null != this.ifView ? this.ifView : this.Ii.create(); else i = this.view = this.elseView = this.cache && null != this.elseView ? this.elseView : this.elseFactory?.create();
            if (null == i) return;
            i.setLocation(this.l);
            this.pending = g(i.activate(t, s, s.scope), (() => {
                if (r()) this.pending = void 0;
            }));
        }));
    }
    detaching(t, e) {
        this.Ri = true;
        return g(this.pending, (() => {
            this.Ri = false;
            this.pending = void 0;
            void this.view?.deactivate(t, this.$controller);
        }));
    }
    valueChanged(t, e) {
        if (!this.$controller.isActive) return;
        t = !!t;
        e = !!e;
        if (t === e) return;
        const i = this.view;
        const s = this.$controller;
        const n = this.Si++;
        const r = () => !this.Ri && this.Si === n + 1;
        let o;
        return g(this.pending, (() => this.pending = g(i?.deactivate(i, s), (() => {
            if (!r()) return;
            if (t) o = this.view = this.ifView = this.cache && null != this.ifView ? this.ifView : this.Ii.create(); else o = this.view = this.elseView = this.cache && null != this.elseView ? this.elseView : this.elseFactory?.create();
            if (null == o) return;
            o.setLocation(this.l);
            return g(o.activate(o, s, s.scope), (() => {
                if (r()) this.pending = void 0;
            }));
        }))));
    }
    dispose() {
        this.ifView?.dispose();
        this.elseView?.dispose();
        this.ifView = this.elseView = this.view = void 0;
    }
    accept(t) {
        if (true === this.view?.accept(t)) return true;
    }
}

If.inject = [ Di, Ts ];

et([ Dt ], If.prototype, "value", void 0);

et([ Dt({
    set: t => "" === t || !!t && "false" !== t
}) ], If.prototype, "cache", void 0);

ze("if")(If);

class Else {
    constructor(t) {
        this.f = t;
    }
    link(t, e, i, s) {
        const n = t.children;
        const r = n[n.length - 1];
        if (r instanceof If) r.elseFactory = this.f; else if (r.viewModel instanceof If) r.viewModel.elseFactory = this.f; else throw pt(`AUR0810`);
    }
}

Else.inject = [ Di ];

ze({
    name: "else"
})(Else);

function ko(t) {
    t.dispose();
}

const Ao = [ 18, 17 ];

class Repeat {
    constructor(t, e, i, s, n) {
        this.views = [];
        this.key = null;
        this.Ti = new Map;
        this.Pi = new Map;
        this.Ei = void 0;
        this.Li = false;
        this.Di = false;
        this._i = null;
        this.Ui = void 0;
        this.$i = false;
        const r = t.props[0].props[0];
        if (void 0 !== r) {
            const {to: t, value: i, command: s} = r;
            if ("key" === t) if (null === s) this.key = i; else if ("bind" === s) this.key = e.parse(i, 16); else throw pt(`AUR775:${s}`); else throw pt(`AUR776:${t}`);
        }
        this.l = i;
        this.qi = s;
        this.f = n;
    }
    binding(t, e) {
        const i = this.qi.bindings;
        const s = i.length;
        let n;
        let r;
        let o = 0;
        for (;s > o; ++o) {
            n = i[o];
            if (n.target === this && "items" === n.targetProperty) {
                r = this.forOf = n.ast;
                this.Mi = n;
                let t = r.iterable;
                while (null != t && Ao.includes(t.$kind)) {
                    t = t.expression;
                    this.Li = true;
                }
                this._i = t;
                break;
            }
        }
        this.Fi();
        const l = r.declaration;
        if (!(this.$i = 24 === l.$kind || 25 === l.$kind)) this.local = T(l, this.$controller.scope, n, null);
    }
    attaching(t, e) {
        this.Oi();
        return this.Vi(t);
    }
    detaching(t, e) {
        this.Fi();
        return this.Ni(t);
    }
    unbinding(t, e) {
        this.Pi.clear();
        this.Ti.clear();
    }
    itemsChanged() {
        if (!this.$controller.isActive) return;
        this.Fi();
        this.Oi();
        this.ji(this.items, void 0);
    }
    handleCollectionChange(t, e) {
        const i = this.$controller;
        if (!i.isActive) return;
        if (this.Li) {
            if (this.Di) return;
            this.Di = true;
            this.items = T(this.forOf.iterable, i.scope, this.Mi, null);
            this.Di = false;
            return;
        }
        this.Oi();
        this.ji(t, e);
    }
    ji(t, e) {
        const i = this.views;
        const s = i.length;
        const n = this.key;
        const r = null !== n;
        if (r || void 0 === e) {
            const t = this.local;
            const o = this.Ui;
            const l = o.length;
            const h = this.forOf;
            const a = h.declaration;
            const c = this.Mi;
            const u = this.$i;
            e = X(l);
            let f = 0;
            if (0 === s) for (;f < l; ++f) e[f] = -2; else if (0 === l) if (u) for (f = 0; f < s; ++f) {
                e.deletedIndices.push(f);
                e.deletedItems.push(T(a, i[f].scope, c, null));
            } else for (f = 0; f < s; ++f) {
                e.deletedIndices.push(f);
                e.deletedItems.push(i[f].scope.bindingContext[t]);
            } else {
                const d = Array(s);
                if (u) for (f = 0; f < s; ++f) d[f] = T(a, i[f].scope, c, null); else for (f = 0; f < s; ++f) d[f] = i[f].scope.bindingContext[t];
                let m;
                let g;
                let p;
                let v;
                let x = 0;
                const w = s - 1;
                const b = l - 1;
                const y = new Map;
                const k = new Map;
                const A = this.Ti;
                const C = this.Pi;
                const B = this.$controller.scope;
                f = 0;
                t: {
                    while (true) {
                        m = d[f];
                        g = o[f];
                        p = r ? qo(A, n, m, Mo(C, d[f], h, B, c, t, u), c) : m;
                        v = r ? qo(A, n, g, Mo(C, o[f], h, B, c, t, u), c) : g;
                        if (p !== v) {
                            A.set(m, p);
                            A.set(g, v);
                            break;
                        }
                        ++f;
                        if (f > w || f > b) break t;
                    }
                    if (w !== b) break t;
                    x = b;
                    while (true) {
                        m = d[x];
                        g = o[x];
                        p = r ? qo(A, n, m, Mo(C, m, h, B, c, t, u), c) : m;
                        v = r ? qo(A, n, g, Mo(C, g, h, B, c, t, u), c) : g;
                        if (p !== v) {
                            A.set(m, p);
                            A.set(g, v);
                            break;
                        }
                        --x;
                        if (f > x) break t;
                    }
                }
                const R = f;
                const S = f;
                for (f = S; f <= b; ++f) {
                    if (A.has(g = o[f])) v = A.get(g); else {
                        v = r ? qo(A, n, g, Mo(C, g, h, B, c, t, u), c) : g;
                        A.set(g, v);
                    }
                    k.set(v, f);
                }
                for (f = R; f <= w; ++f) {
                    if (A.has(m = d[f])) p = A.get(m); else p = r ? qo(A, n, m, i[f].scope, c) : m;
                    y.set(p, f);
                    if (k.has(p)) e[k.get(p)] = f; else {
                        e.deletedIndices.push(f);
                        e.deletedItems.push(m);
                    }
                }
                for (f = S; f <= b; ++f) if (!y.has(A.get(o[f]))) e[f] = -2;
                y.clear();
                k.clear();
            }
        }
        if (void 0 === e) {
            const t = g(this.Ni(null), (() => this.Vi(null)));
            if (Ct(t)) t.catch(Tt);
        } else {
            const t = K(e);
            if (t.deletedIndices.length > 0) {
                const e = g(this.Hi(t), (() => this.Wi(s, t)));
                if (Ct(e)) e.catch(Tt);
            } else this.Wi(s, t);
        }
    }
    Fi() {
        const t = this.$controller.scope;
        let e = this.zi;
        let i = this.Li;
        let s;
        if (i) {
            e = this.zi = T(this._i, t, this.Mi, null) ?? null;
            i = this.Li = !Pt(this.items, e);
        }
        const n = this.Ei;
        if (this.$controller.isActive) {
            s = this.Ei = Q(i ? e : this.items);
            if (n !== s) {
                n?.unsubscribe(this);
                s?.subscribe(this);
            }
        } else {
            n?.unsubscribe(this);
            this.Ei = void 0;
        }
    }
    Oi() {
        const {items: t} = this;
        if (Bt(t)) {
            this.Ui = t;
            return;
        }
        const e = [];
        Lo(t, ((t, i) => {
            e[i] = t;
        }));
        this.Ui = e;
    }
    Vi(t) {
        let e;
        let i;
        let s;
        let n;
        const {$controller: r, f: o, local: l, l: h, items: a, Pi: c, Mi: u, forOf: f, $i: d} = this;
        const m = r.scope;
        const g = Eo(a);
        const p = this.views = Array(g);
        Lo(a, ((a, v) => {
            s = p[v] = o.create().setLocation(h);
            s.nodes.unlink();
            n = Mo(c, a, f, m, u, l, d);
            To(n.overrideContext, v, g);
            i = s.activate(t ?? s, r, n);
            if (Ct(i)) (e ?? (e = [])).push(i);
        }));
        if (void 0 !== e) return 1 === e.length ? e[0] : Promise.all(e);
    }
    Ni(t) {
        let e;
        let i;
        let s;
        let n = 0;
        const {views: r, $controller: o} = this;
        const l = r.length;
        for (;l > n; ++n) {
            s = r[n];
            s.release();
            i = s.deactivate(t ?? s, o);
            if (Ct(i)) (e ?? (e = [])).push(i);
        }
        if (void 0 !== e) return 1 === e.length ? e[0] : Promise.all(e);
    }
    Hi(t) {
        let e;
        let i;
        let s;
        const {$controller: n, views: r} = this;
        const o = t.deletedIndices;
        const l = o.length;
        let h = 0;
        for (;l > h; ++h) {
            s = r[o[h]];
            s.release();
            i = s.deactivate(s, n);
            if (Ct(i)) (e ?? (e = [])).push(i);
        }
        h = 0;
        let a = 0;
        for (;l > h; ++h) {
            a = o[h] - h;
            r.splice(a, 1);
        }
        if (void 0 !== e) return 1 === e.length ? e[0] : Promise.all(e);
    }
    Wi(t, e) {
        let i;
        let s;
        let n;
        let r;
        let o = 0;
        const {$controller: l, f: h, local: a, Ui: c, l: u, views: f, $i: d, Mi: m, Pi: g, forOf: p} = this;
        const v = e.length;
        for (;v > o; ++o) if (-2 === e[o]) {
            n = h.create();
            f.splice(o, 0, n);
        }
        if (f.length !== v) throw Io(f.length, v);
        const x = l.scope;
        const w = e.length;
        Y(f, e);
        const b = So(e);
        const y = b.length;
        const k = p.declaration;
        let A;
        let C = y - 1;
        o = w - 1;
        for (;o >= 0; --o) {
            n = f[o];
            A = f[o + 1];
            n.nodes.link(A?.nodes ?? u);
            if (-2 === e[o]) {
                r = Mo(g, c[o], p, x, m, a, d);
                To(r.overrideContext, o, w);
                n.setLocation(u);
                s = n.activate(n, l, r);
                if (Ct(s)) (i ?? (i = [])).push(s);
            } else if (C < 0 || 1 === y || o !== b[C]) {
                if (d) _(k, n.scope, m, c[o]); else n.scope.bindingContext[a] = c[o];
                To(n.scope.overrideContext, o, w);
                n.nodes.insertBefore(n.location);
            } else {
                if (d) _(k, n.scope, m, c[o]); else n.scope.bindingContext[a] = c[o];
                if (t !== w) To(n.scope.overrideContext, o, w);
                --C;
            }
        }
        if (void 0 !== i) return 1 === i.length ? i[0] : Promise.all(i);
    }
    dispose() {
        this.views.forEach(ko);
        this.views = void 0;
    }
    accept(t) {
        const {views: e} = this;
        if (void 0 !== e) for (let i = 0, s = e.length; i < s; ++i) if (true === e[i].accept(t)) return true;
    }
}

Repeat.inject = [ vn, q, Ts, Qi, Di ];

et([ Dt ], Repeat.prototype, "items", void 0);

ze("repeat")(Repeat);

let Co = 16;

let Bo = new Int32Array(Co);

let Ro = new Int32Array(Co);

function So(t) {
    const e = t.length;
    if (e > Co) {
        Co = e;
        Bo = new Int32Array(e);
        Ro = new Int32Array(e);
    }
    let i = 0;
    let s = 0;
    let n = 0;
    let r = 0;
    let o = 0;
    let l = 0;
    let h = 0;
    let a = 0;
    for (;r < e; r++) {
        s = t[r];
        if (-2 !== s) {
            o = Bo[i];
            n = t[o];
            if (-2 !== n && n < s) {
                Ro[r] = o;
                Bo[++i] = r;
                continue;
            }
            l = 0;
            h = i;
            while (l < h) {
                a = l + h >> 1;
                n = t[Bo[a]];
                if (-2 !== n && n < s) l = a + 1; else h = a;
            }
            n = t[Bo[l]];
            if (s < n || -2 === n) {
                if (l > 0) Ro[r] = Bo[l - 1];
                Bo[l] = r;
            }
        }
    }
    r = ++i;
    const c = new Int32Array(r);
    s = Bo[i - 1];
    while (i-- > 0) {
        c[i] = s;
        s = Ro[s];
    }
    while (r-- > 0) Bo[r] = 0;
    return c;
}

const Io = (t, e) => pt(`AUR0814:${t}!=${e}`);

const To = (t, e, i) => {
    const s = 0 === e;
    const n = e === i - 1;
    const r = e % 2 === 0;
    t.$index = e;
    t.$first = s;
    t.$last = n;
    t.$middle = !s && !n;
    t.$even = r;
    t.$odd = !r;
    t.$length = i;
};

const Po = mt.toString;

const Eo = t => {
    switch (Po.call(t)) {
      case "[object Array]":
        return t.length;

      case "[object Map]":
        return t.size;

      case "[object Set]":
        return t.size;

      case "[object Number]":
        return t;

      case "[object Null]":
        return 0;

      case "[object Undefined]":
        return 0;

      default:
        throw pt(`Cannot count ${Po.call(t)}`);
    }
};

const Lo = (t, e) => {
    switch (Po.call(t)) {
      case "[object Array]":
        return Do(t, e);

      case "[object Map]":
        return _o(t, e);

      case "[object Set]":
        return Uo(t, e);

      case "[object Number]":
        return $o(t, e);

      case "[object Null]":
        return;

      case "[object Undefined]":
        return;

      default:
        throw pt(`Cannot iterate over ${Po.call(t)}`);
    }
};

const Do = (t, e) => {
    const i = t.length;
    let s = 0;
    for (;s < i; ++s) e(t[s], s, t);
};

const _o = (t, e) => {
    let i = -0;
    let s;
    for (s of t.entries()) e(s, i++, t);
};

const Uo = (t, e) => {
    let i = 0;
    let s;
    for (s of t.keys()) e(s, i++, t);
};

const $o = (t, e) => {
    let i = 0;
    for (;i < t; ++i) e(i, i, t);
};

const qo = (t, e, i, s, n) => {
    let r = t.get(i);
    if (void 0 === r) {
        if ("string" === typeof e) r = i[e]; else r = T(e, s, n, null);
        t.set(i, r);
    }
    return r;
};

const Mo = (t, e, i, s, n, r, o) => {
    let l = t.get(e);
    if (void 0 === l) {
        if (o) _(i.declaration, l = F.fromParent(s, new Z), n, e); else l = F.fromParent(s, new Z(r, e));
        t.set(e, l);
    }
    return l;
};

class With {
    constructor(t, e) {
        this.view = t.create().setLocation(e);
    }
    valueChanged(t, e) {
        const i = this.$controller;
        const s = this.view.bindings;
        let n;
        let r = 0, o = 0;
        if (i.isActive && null != s) {
            n = F.fromParent(i.scope, void 0 === t ? {} : t);
            for (o = s.length; o > r; ++r) s[r].bind(n);
        }
    }
    attaching(t, e) {
        const {$controller: i, value: s} = this;
        const n = F.fromParent(i.scope, void 0 === s ? {} : s);
        return this.view.activate(t, i, n);
    }
    detaching(t, e) {
        return this.view.deactivate(t, this.$controller);
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
    }
    accept(t) {
        if (true === this.view?.accept(t)) return true;
    }
}

With.inject = [ Di, Ts ];

et([ Dt ], With.prototype, "value", void 0);

ze("with")(With);

let Fo = class Switch {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.cases = [];
        this.activeCases = [];
        this.promise = void 0;
    }
    link(t, e, i, s) {
        this.view = this.f.create(this.$controller).setLocation(this.l);
    }
    attaching(t, e) {
        const i = this.view;
        const s = this.$controller;
        this.queue((() => i.activate(t, s, s.scope)));
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
        if (!this.$controller.isActive) return;
        this.queue((() => this.swap(null, this.value)));
    }
    caseChanged(t) {
        this.queue((() => this.Gi(t)));
    }
    Gi(t) {
        const e = t.isMatch(this.value);
        const i = this.activeCases;
        const s = i.length;
        if (!e) {
            if (s > 0 && i[0].id === t.id) return this.Xi(null);
            return;
        }
        if (s > 0 && i[0].id < t.id) return;
        const n = [];
        let r = t.fallThrough;
        if (!r) n.push(t); else {
            const e = this.cases;
            const i = e.indexOf(t);
            for (let t = i, s = e.length; t < s && r; t++) {
                const i = e[t];
                n.push(i);
                r = i.fallThrough;
            }
        }
        return g(this.Xi(null, n), (() => {
            this.activeCases = n;
            return this.Ki(null);
        }));
    }
    swap(t, e) {
        const i = [];
        let s = false;
        for (const t of this.cases) {
            if (s || t.isMatch(e)) {
                i.push(t);
                s = t.fallThrough;
            }
            if (i.length > 0 && !s) break;
        }
        const n = this.defaultCase;
        if (0 === i.length && void 0 !== n) i.push(n);
        return g(this.activeCases.length > 0 ? this.Xi(t, i) : void 0, (() => {
            this.activeCases = i;
            if (0 === i.length) return;
            return this.Ki(t);
        }));
    }
    Ki(t) {
        const e = this.$controller;
        if (!e.isActive) return;
        const i = this.activeCases;
        const s = i.length;
        if (0 === s) return;
        const n = e.scope;
        if (1 === s) return i[0].activate(t, n);
        return m(...i.map((e => e.activate(t, n))));
    }
    Xi(t, e = []) {
        const i = this.activeCases;
        const s = i.length;
        if (0 === s) return;
        if (1 === s) {
            const s = i[0];
            if (!e.includes(s)) {
                i.length = 0;
                return s.deactivate(t);
            }
            return;
        }
        return g(m(...i.reduce(((i, s) => {
            if (!e.includes(s)) i.push(s.deactivate(t));
            return i;
        }), [])), (() => {
            i.length = 0;
        }));
    }
    queue(t) {
        const e = this.promise;
        let i;
        i = this.promise = g(g(e, t), (() => {
            if (this.promise === i) this.promise = void 0;
        }));
    }
    accept(t) {
        if (true === this.$controller.accept(t)) return true;
        if (this.activeCases.some((e => e.accept(t)))) return true;
    }
};

et([ Dt ], Fo.prototype, "value", void 0);

Fo = et([ ze("switch"), it(0, Di), it(1, Ts) ], Fo);

let Oo = 0;

let Vo = class Case {
    constructor(t, e, i, s) {
        this.f = t;
        this.Qi = e;
        this.l = i;
        this.id = ++Oo;
        this.fallThrough = false;
        this.view = void 0;
        this.Yi = s.config.level <= 1;
        this.Ve = s.scopeTo(`${this.constructor.name}-#${this.id}`);
    }
    link(t, e, i, s) {
        const n = t.parent;
        const r = n?.viewModel;
        if (r instanceof Fo) {
            this.$switch = r;
            this.linkToSwitch(r);
        } else throw pt(`AUR0815`);
    }
    detaching(t, e) {
        return this.deactivate(t);
    }
    isMatch(t) {
        this.Ve.debug("isMatch()");
        const e = this.value;
        if (Bt(e)) {
            if (void 0 === this.Ei) this.Ei = this.Zi(e);
            return e.includes(t);
        }
        return e === t;
    }
    valueChanged(t, e) {
        if (Bt(t)) {
            this.Ei?.unsubscribe(this);
            this.Ei = this.Zi(t);
        } else if (void 0 !== this.Ei) this.Ei.unsubscribe(this);
        this.$switch.caseChanged(this);
    }
    handleCollectionChange() {
        this.$switch.caseChanged(this);
    }
    activate(t, e) {
        let i = this.view;
        if (void 0 === i) i = this.view = this.f.create().setLocation(this.l);
        if (i.isActive) return;
        return i.activate(t ?? i, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (void 0 === e || !e.isActive) return;
        return e.deactivate(t ?? e, this.$controller);
    }
    dispose() {
        this.Ei?.unsubscribe(this);
        this.view?.dispose();
        this.view = void 0;
    }
    linkToSwitch(t) {
        t.cases.push(this);
    }
    Zi(t) {
        const e = this.Qi.getArrayObserver(t);
        e.subscribe(this);
        return e;
    }
    accept(t) {
        if (true === this.$controller.accept(t)) return true;
        return this.view?.accept(t);
    }
};

Vo.inject = [ Di, M, Ts, k ];

et([ Dt ], Vo.prototype, "value", void 0);

et([ Dt({
    set: t => {
        switch (t) {
          case "true":
            return true;

          case "false":
            return false;

          default:
            return !!t;
        }
    },
    mode: 1
}) ], Vo.prototype, "fallThrough", void 0);

Vo = et([ ze("case") ], Vo);

let No = class DefaultCase extends Vo {
    linkToSwitch(t) {
        if (void 0 !== t.defaultCase) throw pt(`AUR0816`);
        t.defaultCase = this;
    }
};

No = et([ ze("default-case") ], No);

let jo = class PromiseTemplateController {
    constructor(t, e, i, s) {
        this.f = t;
        this.l = e;
        this.p = i;
        this.preSettledTask = null;
        this.postSettledTask = null;
        this.logger = s.scopeTo("promise.resolve");
    }
    link(t, e, i, s) {
        this.view = this.f.create(this.$controller).setLocation(this.l);
    }
    attaching(t, e) {
        const i = this.view;
        const s = this.$controller;
        return g(i.activate(t, s, this.viewScope = F.fromParent(s.scope, {})), (() => this.swap(t)));
    }
    valueChanged(t, e) {
        if (!this.$controller.isActive) return;
        this.swap(null);
    }
    swap(t) {
        const e = this.value;
        if (!Ct(e)) {
            this.logger.warn(`The value '${String(e)}' is not a promise. No change will be done.`);
            return;
        }
        const i = this.p.domWriteQueue;
        const s = this.fulfilled;
        const n = this.rejected;
        const r = this.pending;
        const o = this.viewScope;
        let l;
        const h = {
            reusable: false
        };
        const a = () => {
            void m(l = (this.preSettledTask = i.queueTask((() => m(s?.deactivate(t), n?.deactivate(t), r?.activate(t, o))), h)).result.catch((t => {
                if (!(t instanceof J)) throw t;
            })), e.then((a => {
                if (this.value !== e) return;
                const c = () => {
                    this.postSettlePromise = (this.postSettledTask = i.queueTask((() => m(r?.deactivate(t), n?.deactivate(t), s?.activate(t, o, a))), h)).result;
                };
                if (1 === this.preSettledTask.status) void l.then(c); else {
                    this.preSettledTask.cancel();
                    c();
                }
            }), (a => {
                if (this.value !== e) return;
                const c = () => {
                    this.postSettlePromise = (this.postSettledTask = i.queueTask((() => m(r?.deactivate(t), s?.deactivate(t), n?.activate(t, o, a))), h)).result;
                };
                if (1 === this.preSettledTask.status) void l.then(c); else {
                    this.preSettledTask.cancel();
                    c();
                }
            })));
        };
        if (1 === this.postSettledTask?.status) void this.postSettlePromise.then(a); else {
            this.postSettledTask?.cancel();
            a();
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
};

et([ Dt ], jo.prototype, "value", void 0);

jo = et([ ze("promise"), it(0, Di), it(1, Ts), it(2, ui), it(3, k) ], jo);

let Ho = class PendingTemplateController {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.view = void 0;
    }
    link(t, e, i, s) {
        Go(t).pending = this;
    }
    activate(t, e) {
        let i = this.view;
        if (void 0 === i) i = this.view = this.f.create().setLocation(this.l);
        if (i.isActive) return;
        return i.activate(i, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (void 0 === e || !e.isActive) return;
        return e.deactivate(e, this.$controller);
    }
    detaching(t) {
        return this.deactivate(t);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

et([ Dt({
    mode: 2
}) ], Ho.prototype, "value", void 0);

Ho = et([ ze("pending"), it(0, Di), it(1, Ts) ], Ho);

let Wo = class FulfilledTemplateController {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.view = void 0;
    }
    link(t, e, i, s) {
        Go(t).fulfilled = this;
    }
    activate(t, e, i) {
        this.value = i;
        let s = this.view;
        if (void 0 === s) s = this.view = this.f.create().setLocation(this.l);
        if (s.isActive) return;
        return s.activate(s, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (void 0 === e || !e.isActive) return;
        return e.deactivate(e, this.$controller);
    }
    detaching(t, e) {
        return this.deactivate(t);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

et([ Dt({
    mode: 4
}) ], Wo.prototype, "value", void 0);

Wo = et([ ze("then"), it(0, Di), it(1, Ts) ], Wo);

let zo = class RejectedTemplateController {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.view = void 0;
    }
    link(t, e, i, s) {
        Go(t).rejected = this;
    }
    activate(t, e, i) {
        this.value = i;
        let s = this.view;
        if (void 0 === s) s = this.view = this.f.create().setLocation(this.l);
        if (s.isActive) return;
        return s.activate(s, this.$controller, e);
    }
    deactivate(t) {
        const e = this.view;
        if (void 0 === e || !e.isActive) return;
        return e.deactivate(e, this.$controller);
    }
    detaching(t, e) {
        return this.deactivate(t);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

et([ Dt({
    mode: 4
}) ], zo.prototype, "value", void 0);

zo = et([ ze("catch"), it(0, Di), it(1, Ts) ], zo);

function Go(t) {
    const e = t.parent;
    const i = e?.viewModel;
    if (i instanceof jo) return i;
    throw pt(`AUR0813`);
}

let Xo = class PromiseAttributePattern {
    "promise.resolve"(t, e, i) {
        return new AttrSyntax(t, e, "promise", "bind");
    }
};

Xo = et([ ie({
    pattern: "promise.resolve",
    symbols: ""
}) ], Xo);

let Ko = class FulfilledAttributePattern {
    then(t, e, i) {
        return new AttrSyntax(t, e, "then", "from-view");
    }
};

Ko = et([ ie({
    pattern: "then",
    symbols: ""
}) ], Ko);

let Qo = class RejectedAttributePattern {
    catch(t, e, i) {
        return new AttrSyntax(t, e, "catch", "from-view");
    }
};

Qo = et([ ie({
    pattern: "catch",
    symbols: ""
}) ], Qo);

class AuCompose {
    static get inject() {
        return [ u, Qi, Ss, Ts, ui, vn, B(CompositionContextFactory) ];
    }
    get pending() {
        return this.Ji;
    }
    get composition() {
        return this.ts;
    }
    constructor(t, e, i, s, n, r, o) {
        this.c = t;
        this.parent = e;
        this.host = i;
        this.l = s;
        this.p = n;
        this.scopeBehavior = "auto";
        this.ts = void 0;
        this.r = t.get(_i);
        this.es = r;
        this.ss = o;
    }
    attaching(t, e) {
        return this.Ji = g(this.queue(new ChangeInfo(this.template, this.component, this.model, void 0), t), (t => {
            if (this.ss.isCurrent(t)) this.Ji = void 0;
        }));
    }
    detaching(t) {
        const e = this.ts;
        const i = this.Ji;
        this.ss.invalidate();
        this.ts = this.Ji = void 0;
        return g(i, (() => e?.deactivate(t)));
    }
    propertyChanged(t) {
        if ("model" === t && null != this.ts) {
            this.ts.update(this.model);
            return;
        }
        this.Ji = g(this.Ji, (() => g(this.queue(new ChangeInfo(this.template, this.component, this.model, t), void 0), (t => {
            if (this.ss.isCurrent(t)) this.Ji = void 0;
        }))));
    }
    queue(t, e) {
        const i = this.ss;
        const s = this.ts;
        return g(i.create(t), (t => {
            if (i.isCurrent(t)) return g(this.compose(t), (n => {
                if (i.isCurrent(t)) return g(n.activate(e), (() => {
                    if (i.isCurrent(t)) {
                        this.ts = n;
                        return g(s?.deactivate(e), (() => t));
                    } else return g(n.controller.deactivate(n.controller, this.$controller), (() => {
                        n.controller.dispose();
                        return t;
                    }));
                }));
                n.controller.dispose();
                return t;
            }));
            return t;
        }));
    }
    compose(t) {
        let e;
        let i;
        let s;
        const {we: n, rs: r, os: o} = t.change;
        const {c: l, host: h, $controller: a, l: c} = this;
        const u = this.getDef(r);
        const f = l.createChild();
        const d = null == c ? h.parentNode : c.parentNode;
        if (null !== u) {
            if (u.containerless) throw pt(`AUR0806`);
            if (null == c) {
                i = h;
                s = () => {};
            } else {
                i = d.insertBefore(this.p.document.createElement(u.name), c);
                s = () => {
                    i.remove();
                };
            }
            e = this.ls(f, r, i);
        } else {
            i = null == c ? h : c;
            e = this.ls(f, r, i);
        }
        const m = () => {
            if (null !== u) {
                const n = Controller.$el(f, e, i, {
                    projections: this.es.projections
                }, u);
                return new CompositionController(n, (t => n.activate(t ?? n, a, a.scope.parent)), (t => g(n.deactivate(t ?? n, a), s)), (t => e.activate?.(t)), t);
            } else {
                const s = CustomElementDefinition.create({
                    name: an.generateName(),
                    template: n
                });
                const r = this.r.getViewFactory(s, f);
                const o = Controller.$view(r, a);
                const l = "auto" === this.scopeBehavior ? F.fromParent(this.parent.scope, e) : F.create(e);
                if (Us(i)) o.setLocation(i); else o.setHost(i);
                return new CompositionController(o, (t => o.activate(t ?? o, a, l)), (t => o.deactivate(t ?? o, a)), (t => e.activate?.(t)), t);
            }
        };
        if ("activate" in e) return g(e.activate(o), (() => m())); else return m();
    }
    ls(t, e, i) {
        if (null == e) return new EmptyComponent;
        if ("object" === typeof e) return e;
        const s = this.p;
        const n = Us(i);
        Xt(t, s.Element, Xt(t, Ss, new d("ElementResolver", n ? null : i)));
        Xt(t, Ts, new d("IRenderLocation", n ? i : null));
        const r = t.invoke(e);
        Xt(t, e, new d("au-compose.component", r));
        return r;
    }
    getDef(t) {
        const e = Rt(t) ? t : t?.constructor;
        return an.isType(e) ? an.getDefinition(e) : null;
    }
}

et([ Dt ], AuCompose.prototype, "template", void 0);

et([ Dt ], AuCompose.prototype, "component", void 0);

et([ Dt ], AuCompose.prototype, "model", void 0);

et([ Dt({
    set: t => {
        if ("scoped" === t || "auto" === t) return t;
        throw pt(`AUR0805`);
    }
}) ], AuCompose.prototype, "scopeBehavior", void 0);

Fs("au-compose")(AuCompose);

class EmptyComponent {}

class CompositionContextFactory {
    constructor() {
        this.id = 0;
    }
    isCurrent(t) {
        return t.id === this.id;
    }
    create(t) {
        return g(t.load(), (t => new CompositionContext(++this.id, t)));
    }
    invalidate() {
        this.id++;
    }
}

class ChangeInfo {
    constructor(t, e, i, s) {
        this.we = t;
        this.rs = e;
        this.os = i;
        this.cs = s;
    }
    load() {
        if (Ct(this.we) || Ct(this.rs)) return Promise.all([ this.we, this.rs ]).then((([t, e]) => new LoadedChangeInfo(t, e, this.os, this.cs))); else return new LoadedChangeInfo(this.we, this.rs, this.os, this.cs);
    }
}

class LoadedChangeInfo {
    constructor(t, e, i, s) {
        this.we = t;
        this.rs = e;
        this.os = i;
        this.cs = s;
    }
}

class CompositionContext {
    constructor(t, e) {
        this.id = t;
        this.change = e;
    }
}

class CompositionController {
    constructor(t, e, i, s, n) {
        this.controller = t;
        this.start = e;
        this.stop = i;
        this.update = s;
        this.context = n;
        this.state = 0;
    }
    activate(t) {
        if (0 !== this.state) throw pt(`AUR0807:${this.controller.name}`);
        this.state = 1;
        return this.start(t);
    }
    deactivate(t) {
        switch (this.state) {
          case 1:
            this.state = -1;
            return this.stop(t);

          case -1:
            throw pt(`AUR0808`);

          default:
            this.state = -1;
        }
    }
}

let Yo = class AuSlot {
    static get inject() {
        return [ Ts, vn, Yi, _i ];
    }
    constructor(t, e, i, s) {
        this.us = null;
        this.ds = null;
        let n;
        let r;
        const o = e.auSlot;
        const l = i.instruction?.projections?.[o.name];
        if (null == l) {
            n = s.getViewFactory(o.fallback, i.controller.container);
            this.gs = false;
        } else {
            r = i.parent.controller.container.createChild();
            Xt(r, i.controller.definition.Type, new d(void 0, i.controller.viewModel));
            n = s.getViewFactory(l, r);
            this.gs = true;
        }
        this.ps = i;
        this.view = n.create().setLocation(t);
    }
    binding(t, e) {
        this.us = this.$controller.scope.parent;
        let i;
        if (this.gs) {
            i = this.ps.controller.scope.parent;
            (this.ds = F.fromParent(i, i.bindingContext)).overrideContext.$host = this.expose ?? this.us.bindingContext;
        }
    }
    attaching(t, e) {
        return this.view.activate(t, this.$controller, this.gs ? this.ds : this.us);
    }
    detaching(t, e) {
        return this.view.deactivate(t, this.$controller);
    }
    exposeChanged(t) {
        if (this.gs && null != this.ds) this.ds.overrideContext.$host = t;
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
    }
    accept(t) {
        if (true === this.view?.accept(t)) return true;
    }
};

et([ Dt ], Yo.prototype, "expose", void 0);

Yo = et([ Fs({
    name: "au-slot",
    template: null,
    containerless: true
}) ], Yo);

const Zo = jt("ISanitizer", (t => t.singleton(class {
    sanitize() {
        throw pt('"sanitize" method not implemented');
    }
})));

let Jo = class SanitizeValueConverter {
    constructor(t) {
        this.vs = t;
    }
    toView(t) {
        if (null == t) return null;
        return this.vs.sanitize(t);
    }
};

Jo = et([ it(0, Zo) ], Jo);

ye("sanitize")(Jo);

const tl = DebounceBindingBehavior;

const el = OneTimeBindingBehavior;

const il = ToViewBindingBehavior;

const sl = FromViewBindingBehavior;

const nl = SignalBindingBehavior;

const rl = ThrottleBindingBehavior;

const ol = TwoWayBindingBehavior;

const ll = TemplateCompiler;

const hl = NodeObserverLocator;

const al = [ ll, hl ];

const cl = SVGAnalyzer;

const ul = ce;

const fl = ae;

const dl = he;

const ml = le;

const gl = ue;

const pl = [ dl, ml, gl ];

const vl = [ ul, fl ];

const xl = ar;

const wl = cr;

const bl = lr;

const yl = rr;

const kl = or;

const Al = hr;

const Cl = pr;

const Bl = ur;

const Rl = fr;

const Sl = dr;

const Il = gr;

const Tl = mr;

const Pl = vr;

const El = [ xl, yl, bl, kl, Al, wl, Cl, Bl, Rl, Il, Tl, Sl, Pl ];

const Ll = Jo;

const Dl = If;

const _l = Else;

const Ul = Repeat;

const $l = With;

const ql = Fo;

const Ml = Vo;

const Fl = No;

const Ol = jo;

const Vl = Ho;

const Nl = Wo;

const jl = zo;

const Hl = Xo;

const Wl = Ko;

const zl = Qo;

const Gl = SelfBindingBehavior;

const Xl = UpdateTriggerBindingBehavior;

const Kl = AuCompose;

const Ql = Portal;

const Yl = Focus;

const Zl = yo;

const Jl = [ tl, el, il, sl, nl, rl, ol, Ll, Dl, _l, Ul, $l, ql, Ml, Fl, Ol, Vl, Nl, jl, Hl, Wl, zl, AttrBindingBehavior, Gl, Xl, Kl, Ql, Yl, Zl, Yo ];

const th = [ Ln, Dn, Pn, En, Bn, Rn, Sn, In, Tn, Un, On, $n, qn, Mn, Fn, _n, Vn ];

const eh = ih(n);

function ih(t) {
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
            return e.register(zt(O, i.coercingOptions), ...al, ...Jl, ...pl, ...El, ...th);
        },
        customize(e) {
            return ih(e ?? t);
        }
    };
}

const sh = jt("IAurelia");

class Aurelia {
    get isRunning() {
        return this.ir;
    }
    get isStarting() {
        return this.xs;
    }
    get isStopping() {
        return this.ws;
    }
    get root() {
        if (null == this.bs) {
            if (null == this.next) throw pt(`AUR0767`);
            return this.next;
        }
        return this.bs;
    }
    constructor(t = r.createContainer()) {
        this.container = t;
        this.ir = false;
        this.xs = false;
        this.ws = false;
        this.bs = void 0;
        this.next = void 0;
        this.ys = void 0;
        this.ks = void 0;
        if (t.has(sh, true)) throw pt(`AUR0768`);
        Xt(t, sh, new d("IAurelia", this));
        Xt(t, us, this.As = new d("IAppRoot"));
    }
    register(...t) {
        this.container.register(...t);
        return this;
    }
    app(t) {
        this.next = new AppRoot(t, this.Cs(t.host), this.container, this.As);
        return this;
    }
    enhance(t, e) {
        const i = t.container ?? this.container.createChild();
        const s = t.host;
        const n = this.Cs(s);
        const r = t.component;
        let o;
        if (Rt(r)) {
            Xt(i, n.HTMLElement, Xt(i, n.Element, Xt(i, Ss, new d("ElementResolver", s))));
            o = i.invoke(r);
        } else o = r;
        Xt(i, Is, new d("IEventTarget", s));
        e = e ?? null;
        const l = Controller.$el(i, o, s, null, CustomElementDefinition.create({
            name: Js(),
            template: s,
            enhance: true
        }));
        return g(l.activate(l, e), (() => l));
    }
    async waitForIdle() {
        const t = this.root.platform;
        await t.domWriteQueue.yield();
        await t.domReadQueue.yield();
        await t.taskQueue.yield();
    }
    Cs(t) {
        let e;
        if (!this.container.has(ui, false)) {
            if (null === t.ownerDocument.defaultView) throw pt(`AUR0769`);
            e = new tt(t.ownerDocument.defaultView);
            this.container.register(zt(ui, e));
        } else e = this.container.get(ui);
        return e;
    }
    start(t = this.next) {
        if (null == t) throw pt(`AUR0770`);
        if (Ct(this.ys)) return this.ys;
        return this.ys = g(this.stop(), (() => {
            Reflect.set(t.host, "$aurelia", this);
            this.As.prepare(this.bs = t);
            this.xs = true;
            return g(t.activate(), (() => {
                this.ir = true;
                this.xs = false;
                this.ys = void 0;
                this.Bs(t, "au-started", t.host);
            }));
        }));
    }
    stop(t = false) {
        if (Ct(this.ks)) return this.ks;
        if (true === this.ir) {
            const e = this.bs;
            this.ir = false;
            this.ws = true;
            return this.ks = g(e.deactivate(), (() => {
                Reflect.deleteProperty(e.host, "$aurelia");
                if (t) e.dispose();
                this.bs = void 0;
                this.As.dispose();
                this.ws = false;
                this.Bs(e, "au-stopped", e.host);
            }));
        }
    }
    dispose() {
        if (this.ir || this.ws) throw pt(`AUR0771`);
        this.container.dispose();
    }
    Bs(t, e, i) {
        const s = new t.platform.window.CustomEvent(e, {
            detail: this,
            bubbles: true,
            cancelable: true
        });
        i.dispatchEvent(s);
    }
}

var nh;

(function(t) {
    t[t["oneTime"] = 1] = "oneTime";
    t[t["toView"] = 2] = "toView";
    t[t["fromView"] = 4] = "fromView";
    t[t["twoWay"] = 6] = "twoWay";
    t[t["default"] = 8] = "default";
})(nh || (nh = {}));

var rh;

(function(t) {
    t[t["Element"] = 1] = "Element";
    t[t["Attribute"] = 2] = "Attribute";
})(rh || (rh = {}));

export { AdoptedStyleSheetsStyles, AppRoot, Fe as AppTask, ce as AtPrefixedTriggerAttributePattern, ul as AtPrefixedTriggerAttributePatternRegistration, AttrBindingBehavior, dr as AttrBindingCommand, Sl as AttrBindingCommandRegistration, AttrSyntax, AttributeBinding, AttributeBindingInstruction, On as AttributeBindingRenderer, AttributeNSAccessor, oe as AttributePattern, AuCompose, Yo as AuSlot, AuSlotsInfo, Aurelia, $t as Bindable, BindableDefinition, BindableObserver, BindablesInfo, be as BindingBehavior, BindingBehaviorDefinition, nr as BindingCommand, BindingCommandDefinition, nh as BindingMode, BindingModeBehavior, BindingTargetSubscriber, CSSModulesProcessorRegistry, fr as CaptureBindingCommand, Rl as CaptureBindingCommandRegistration, Vo as Case, CheckedObserver, ni as Children, ChildrenDefinition, ChildrenObserver, ClassAttributeAccessor, gr as ClassBindingCommand, Il as ClassBindingCommandRegistration, ae as ColonPrefixedBindAttributePattern, fl as ColonPrefixedBindAttributePatternRegistration, Jn as CommandType, ComputedWatcher, ContentBinding, Controller, ti as CustomAttribute, CustomAttributeDefinition, Sn as CustomAttributeRenderer, an as CustomElement, CustomElementDefinition, Rn as CustomElementRenderer, DataAttributeAccessor, DebounceBindingBehavior, tl as DebounceBindingBehaviorRegistration, ar as DefaultBindingCommand, xl as DefaultBindingCommandRegistration, El as DefaultBindingLanguage, pl as DefaultBindingSyntax, No as DefaultCase, al as DefaultComponents, th as DefaultRenderers, Jl as DefaultResources, rh as DefinitionType, le as DotSeparatedAttributePattern, ml as DotSeparatedAttributePatternRegistration, Else, _l as ElseRegistration, ExpressionWatcher, FlushQueue, Focus, cr as ForBindingCommand, wl as ForBindingCommandRegistration, FragmentNodeSequence, FromViewBindingBehavior, sl as FromViewBindingBehaviorRegistration, lr as FromViewBindingCommand, bl as FromViewBindingCommandRegistration, Wo as FulfilledTemplateController, HooksDefinition, HydrateAttributeInstruction, HydrateElementInstruction, HydrateLetElementInstruction, HydrateTemplateController, us as IAppRoot, Me as IAppTask, br as IAttrMapper, ee as IAttributeParser, te as IAttributePattern, gn as IAuSlotsInfo, sh as IAurelia, Qi as IController, Is as IEventTarget, Ie as IFlushQueue, Ms as IHistory, Yi as IHydrationContext, vn as IInstruction, Ii as ILifecycleHooks, qs as ILocation, Ss as INode, hl as INodeObserverLocatorRegistration, ui as IPlatform, mn as IProjections, Ts as IRenderLocation, bn as IRenderer, _i as IRendering, xr as ISVGAnalyzer, Zo as ISanitizer, ki as IShadowDOMGlobalStyles, yi as IShadowDOMStyles, Yt as ISyntaxInterpreter, wn as ITemplateCompiler, Wr as ITemplateCompilerHooks, ll as ITemplateCompilerRegistration, Ar as ITemplateElementFactory, Di as IViewFactory, $s as IWindow, If, Dl as IfRegistration, pn as InstructionType, InterpolationBinding, En as InterpolationBindingRenderer, InterpolationInstruction, InterpolationPartBinding, Interpretation, IteratorBindingInstruction, Dn as IteratorBindingRenderer, LetBinding, LetBindingInstruction, Tn as LetElementRenderer, Ei as LifecycleHooks, LifecycleHooksDefinition, LifecycleHooksEntry, ListenerBinding, ListenerBindingInstruction, ListenerBindingOptions, Un as ListenerBindingRenderer, MultiAttrInstruction, NodeObserverLocator, NoopSVGAnalyzer, OneTimeBindingBehavior, el as OneTimeBindingBehaviorRegistration, rr as OneTimeBindingCommand, yl as OneTimeBindingCommandRegistration, Ho as PendingTemplateController, Portal, jo as PromiseTemplateController, PropertyBinding, PropertyBindingInstruction, Ln as PropertyBindingRenderer, he as RefAttributePattern, dl as RefAttributePatternRegistration, RefBinding, Cl as RefBindingCommandRegistration, RefBindingInstruction, Pn as RefBindingRenderer, zo as RejectedTemplateController, Rendering, Repeat, Ul as RepeatRegistration, SVGAnalyzer, cl as SVGAnalyzerRegistration, Jo as SanitizeValueConverter, Ll as SanitizeValueConverterRegistration, SelectValueObserver, SelfBindingBehavior, Gl as SelfBindingBehaviorRegistration, SetAttributeInstruction, $n as SetAttributeRenderer, SetClassAttributeInstruction, qn as SetClassAttributeRenderer, SetPropertyInstruction, Bn as SetPropertyRenderer, SetStyleAttributeInstruction, Mn as SetStyleAttributeRenderer, ShadowDOMRegistry, vl as ShortHandBindingSyntax, SignalBindingBehavior, nl as SignalBindingBehaviorRegistration, SpreadBindingInstruction, SpreadElementPropBindingInstruction, Vn as SpreadRenderer, eh as StandardConfiguration, Xi as State, StyleAttributeAccessor, mr as StyleBindingCommand, Tl as StyleBindingCommandRegistration, Ai as StyleConfiguration, StyleElementStyles, StylePropertyBindingInstruction, Fn as StylePropertyBindingRenderer, Fo as Switch, TemplateCompiler, Xr as TemplateCompilerHooks, In as TemplateControllerRenderer, TextBindingInstruction, _n as TextBindingRenderer, ThrottleBindingBehavior, rl as ThrottleBindingBehaviorRegistration, ToViewBindingBehavior, il as ToViewBindingBehaviorRegistration, or as ToViewBindingCommand, kl as ToViewBindingCommandRegistration, ur as TriggerBindingCommand, Bl as TriggerBindingCommandRegistration, TwoWayBindingBehavior, ol as TwoWayBindingBehaviorRegistration, hr as TwoWayBindingCommand, Al as TwoWayBindingCommandRegistration, UpdateTriggerBindingBehavior, Xl as UpdateTriggerBindingBehaviorRegistration, ValueAttributeObserver, Ce as ValueConverter, ValueConverterDefinition, ViewFactory, Gi as ViewModelKind, He as Watch, With, $l as WithRegistration, Kt as alias, Nt as allResources, ie as attributePattern, Dt as bindable, ve as bindingBehavior, tr as bindingCommand, dn as capture, ei as children, qt as coercer, Vs as containerless, _s as convertToRenderLocation, xi as cssModules, We as customAttribute, Fs as customElement, Ls as getEffectiveParentNode, Bs as getRef, Hi as isCustomElementController, Wi as isCustomElementViewModel, xn as isInstruction, Us as isRenderLocation, Li as lifecycleHooks, Re as mixinAstEvaluator, Be as mixinUseScope, Ee as mixingBindingLimited, un as processContent, Qt as registerAliases, yn as renderer, Ds as setEffectiveParentNode, Rs as setRef, wi as shadowCSS, js as strict, Kr as templateCompilerHooks, ze as templateController, Os as useShadowDOM, ye as valueConverter, Ve as watch };
//# sourceMappingURL=index.mjs.map
