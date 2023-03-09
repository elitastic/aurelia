"use strict";

Object.defineProperty(exports, "__esModule", {
    value: true
});

var t = require("@aurelia/kernel");

var e = require("@aurelia/metadata");

var s = require("@aurelia/runtime");

var i = require("@aurelia/platform");

var n = require("@aurelia/platform-browser");

function r(t, e, s, i) {
    var n = arguments.length, r = n < 3 ? e : null === i ? i = Object.getOwnPropertyDescriptor(e, s) : i, o;
    if ("object" === typeof Reflect && "function" === typeof Reflect.decorate) r = Reflect.decorate(t, e, s, i); else for (var l = t.length - 1; l >= 0; l--) if (o = t[l]) r = (n < 3 ? o(r) : n > 3 ? o(e, s, r) : o(e, s)) || r;
    return n > 3 && r && Object.defineProperty(e, s, r), r;
}

function o(t, e) {
    return function(s, i) {
        e(s, i, t);
    };
}

const l = e.Metadata.getOwn;

const h = e.Metadata.hasOwn;

const a = e.Metadata.define;

const {annotation: c, resource: u} = t.Protocol;

const f = c.keyFor;

const d = u.keyFor;

const p = u.appendTo;

const x = c.appendTo;

const m = c.getKeys;

const g = Object;

const v = g.prototype;

const w = () => g.create(null);

const b = t => new Error(t);

const y = v.hasOwnProperty;

const k = g.freeze;

const A = g.assign;

const C = g.getOwnPropertyNames;

const B = g.keys;

const R = w();

const S = (t, e, s) => {
    if (true === R[e]) return true;
    if (!E(e)) return false;
    const i = e.slice(0, 5);
    return R[e] = "aria-" === i || "data-" === i || s.isStandardSvgAttribute(t, e);
};

const I = t => t instanceof Promise;

const T = t => t instanceof Array;

const P = t => "function" === typeof t;

const E = t => "string" === typeof t;

const L = g.defineProperty;

const D = t => {
    throw t;
};

const _ = g.is;

const q = Reflect.defineProperty;

const U = (t, e, s) => {
    q(t, e, {
        enumerable: false,
        configurable: true,
        writable: true,
        value: s
    });
    return s;
};

function $(t, e) {
    let s;
    function i(t, e) {
        if (arguments.length > 1) s.property = e;
        a(F, BindableDefinition.create(e, t, s), t.constructor, e);
        x(t.constructor, O.keyFrom(e));
    }
    if (arguments.length > 1) {
        s = {};
        i(t, e);
        return;
    } else if (E(t)) {
        s = {};
        return i;
    }
    s = void 0 === t ? {} : t;
    return i;
}

function M(t) {
    return t.startsWith(F);
}

const F = f("bindable");

const O = k({
    name: F,
    keyFrom: t => `${F}:${t}`,
    from(t, ...e) {
        const s = {};
        const i = Array.isArray;
        function n(e) {
            s[e] = BindableDefinition.create(e, t);
        }
        function r(e, i) {
            s[e] = i instanceof BindableDefinition ? i : BindableDefinition.create(e, t, i);
        }
        function o(t) {
            if (i(t)) t.forEach(n); else if (t instanceof BindableDefinition) s[t.property] = t; else if (void 0 !== t) B(t).forEach((e => r(e, t[e])));
        }
        e.forEach(o);
        return s;
    },
    for(t) {
        let e;
        const s = {
            add(i) {
                let n;
                let r;
                if (E(i)) {
                    n = i;
                    r = {
                        property: n
                    };
                } else {
                    n = i.property;
                    r = i;
                }
                e = BindableDefinition.create(n, t, r);
                if (!h(F, t, n)) x(t, O.keyFrom(n));
                a(F, e, t, n);
                return s;
            },
            mode(t) {
                e.mode = t;
                return s;
            },
            callback(t) {
                e.callback = t;
                return s;
            },
            attribute(t) {
                e.attribute = t;
                return s;
            },
            primary() {
                e.primary = true;
                return s;
            },
            set(t) {
                e.set = t;
                return s;
            }
        };
        return s;
    },
    getAll(e) {
        const s = F.length + 1;
        const i = [];
        const n = t.getPrototypeChain(e);
        let r = n.length;
        let o = 0;
        let h;
        let a;
        let c;
        let u;
        while (--r >= 0) {
            c = n[r];
            h = m(c).filter(M);
            a = h.length;
            for (u = 0; u < a; ++u) i[o++] = l(F, c, h[u].slice(s));
        }
        return i;
    }
});

class BindableDefinition {
    constructor(t, e, s, i, n, r) {
        this.attribute = t;
        this.callback = e;
        this.mode = s;
        this.primary = i;
        this.property = n;
        this.set = r;
    }
    static create(e, s, i = {}) {
        return new BindableDefinition(t.firstDefined(i.attribute, t.kebabCase(e)), t.firstDefined(i.callback, `${e}Changed`), t.firstDefined(i.mode, 2), t.firstDefined(i.primary, false), t.firstDefined(i.property, e), t.firstDefined(i.set, j(e, s, i)));
    }
}

function V(t, e, s) {
    N.define(t, e);
}

const N = {
    key: f("coercer"),
    define(t, e) {
        a(N.key, t[e].bind(t), t);
    },
    for(t) {
        return l(N.key, t);
    }
};

function j(s, i, n = {}) {
    const r = n.type ?? e.Metadata.get("design:type", i, s) ?? null;
    if (null == r) return t.noop;
    let o;
    switch (r) {
      case Number:
      case Boolean:
      case String:
      case BigInt:
        o = r;
        break;

      default:
        {
            const e = r.coerce;
            o = "function" === typeof e ? e.bind(r) : N.for(r) ?? t.noop;
            break;
        }
    }
    return o === t.noop ? o : H(o, n.nullable);
}

function H(t, e) {
    return function(s, i) {
        if (!i?.enableCoercion) return s;
        return (e ?? (i?.coerceNullish ?? false ? false : true)) && null == s ? s : t(s, i);
    };
}

class BindableObserver {
    get type() {
        return 1;
    }
    constructor(e, s, i, n, r, o) {
        this.set = n;
        this.$controller = r;
        this.i = o;
        this.v = void 0;
        this.ov = void 0;
        const l = e[i];
        const h = e.propertyChanged;
        const a = this.u = P(l);
        const c = this.A = P(h);
        const u = this.hs = n !== t.noop;
        let f;
        this.o = e;
        this.k = s;
        this.C = c ? h : t.noop;
        this.cb = a ? l : t.noop;
        if (void 0 === this.cb && !c && !u) this.iO = false; else {
            this.iO = true;
            f = e[s];
            this.v = u && void 0 !== f ? n(f, this.i) : f;
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
            if (_(t, e)) return;
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

s.subscriberCollection(BindableObserver);

const W = function(e) {
    function s(e, i, n) {
        t.DI.inject(s)(e, i, n);
    }
    s.$isResolver = true;
    s.resolve = function(t, s) {
        if (s.root === s) return s.get(e);
        return s.has(e, false) ? s.get(e) : s.root.get(e);
    };
    return s;
};

const z = function(e) {
    function s(e, i, n) {
        t.DI.inject(s)(e, i, n);
    }
    s.$isResolver = true;
    s.resolve = function(t, s) {
        if (s.root === s) return s.getAll(e, false);
        return s.has(e, false) ? s.getAll(e, false).concat(s.root.getAll(e, false)) : s.root.getAll(e, false);
    };
    return s;
};

const G = t.DI.createInterface;

const X = t.Registration.singleton;

const K = t.Registration.aliasTo;

const Q = t.Registration.instance;

t.Registration.callback;

const Y = t.Registration.transient;

const Z = (t, e, s) => t.registerResolver(e, s);

function J(...t) {
    return function(e) {
        const s = f("aliases");
        const i = l(s, e);
        if (void 0 === i) a(s, t, e); else i.push(...t);
    };
}

function tt(e, s, i, n) {
    for (let r = 0, o = e.length; r < o; ++r) t.Registration.aliasTo(i, s.keyFrom(e[r])).register(n);
}

class CharSpec {
    constructor(t, e, s, i) {
        this.chars = t;
        this.repeat = e;
        this.isSymbol = s;
        this.isInverted = i;
        if (i) switch (t.length) {
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
        this.parts = t.emptyArray;
        this.q = "";
        this.U = {};
        this.$ = {};
    }
    get pattern() {
        const t = this.q;
        if ("" === t) return null; else return t;
    }
    set pattern(e) {
        if (null == e) {
            this.q = "";
            this.parts = t.emptyArray;
        } else {
            this.q = e;
            this.parts = this.$[e];
        }
    }
    append(t, e) {
        const s = this.U;
        if (void 0 === s[t]) s[t] = e; else s[t] += e;
    }
    next(t) {
        const e = this.U;
        let s;
        if (void 0 !== e[t]) {
            s = this.$;
            if (void 0 === s[t]) s[t] = [ e[t] ]; else s[t].push(e[t]);
            e[t] = void 0;
        }
    }
}

class AttrParsingState {
    get q() {
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
        const s = e.length;
        let i = null;
        let n = 0;
        for (;n < s; ++n) {
            i = e[n];
            if (t.equals(i.charSpec)) return i;
        }
        return null;
    }
    append(t, e) {
        const s = this.F;
        if (!s.includes(e)) s.push(e);
        let i = this.findChild(t);
        if (null == i) {
            i = new AttrParsingState(t, e);
            this.O.push(i);
            if (t.repeat) i.O.push(i);
        }
        return i;
    }
    findMatches(t, e) {
        const s = [];
        const i = this.O;
        const n = i.length;
        let r = 0;
        let o = null;
        let l = 0;
        let h = 0;
        for (;l < n; ++l) {
            o = i[l];
            if (o.charSpec.has(t)) {
                s.push(o);
                r = o.F.length;
                h = 0;
                if (o.charSpec.isSymbol) for (;h < r; ++h) e.next(o.F[h]); else for (;h < r; ++h) e.append(o.F[h], t);
            }
        }
        return s;
    }
}

class StaticSegment {
    constructor(t) {
        this.text = t;
        const e = this.N = t.length;
        const s = this.j = [];
        let i = 0;
        for (;e > i; ++i) s.push(new CharSpec(t[i], false, false, false));
    }
    eachChar(t) {
        const e = this.N;
        const s = this.j;
        let i = 0;
        for (;e > i; ++i) t(s[i]);
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

const et = G("ISyntaxInterpreter", (t => t.singleton(SyntaxInterpreter)));

class SyntaxInterpreter {
    constructor() {
        this.W = new AttrParsingState(null);
        this.G = [ this.W ];
    }
    add(t) {
        t = t.slice(0).sort(((t, e) => t.pattern > e.pattern ? 1 : -1));
        const e = t.length;
        let s;
        let i;
        let n;
        let r;
        let o;
        let l;
        let h;
        let a = 0;
        let c;
        while (e > a) {
            s = this.W;
            i = t[a];
            n = i.pattern;
            r = new SegmentTypes;
            o = this.X(i, r);
            l = o.length;
            h = t => s = s.append(t, n);
            for (c = 0; l > c; ++c) o[c].eachChar(h);
            s.V = r;
            s.M = true;
            ++a;
        }
    }
    interpret(t) {
        const e = new Interpretation;
        const s = t.length;
        let i = this.G;
        let n = 0;
        let r;
        for (;n < s; ++n) {
            i = this.K(i, t.charAt(n), e);
            if (0 === i.length) break;
        }
        i = i.filter(st);
        if (i.length > 0) {
            i.sort(it);
            r = i[0];
            if (!r.charSpec.isSymbol) e.next(r.q);
            e.pattern = r.q;
        }
        return e;
    }
    K(t, e, s) {
        const i = [];
        let n = null;
        const r = t.length;
        let o = 0;
        for (;o < r; ++o) {
            n = t[o];
            i.push(...n.findMatches(e, s));
        }
        return i;
    }
    X(t, e) {
        const s = [];
        const i = t.pattern;
        const n = i.length;
        const r = t.symbols;
        let o = 0;
        let l = 0;
        let h = "";
        while (o < n) {
            h = i.charAt(o);
            if (0 === r.length || !r.includes(h)) if (o === l) if ("P" === h && "PART" === i.slice(o, o + 4)) {
                l = o += 4;
                s.push(new DynamicSegment(r));
                ++e.dynamics;
            } else ++o; else ++o; else if (o !== l) {
                s.push(new StaticSegment(i.slice(l, o)));
                ++e.statics;
                l = o;
            } else {
                s.push(new SymbolSegment(i.slice(l, o + 1)));
                ++e.symbols;
                l = ++o;
            }
        }
        if (l !== o) {
            s.push(new StaticSegment(i.slice(l, o)));
            ++e.statics;
        }
        return s;
    }
}

function st(t) {
    return t.M;
}

function it(t, e) {
    const s = t.V;
    const i = e.V;
    if (s.statics !== i.statics) return i.statics - s.statics;
    if (s.dynamics !== i.dynamics) return i.dynamics - s.dynamics;
    if (s.symbols !== i.symbols) return i.symbols - s.symbols;
    return 0;
}

class AttrSyntax {
    constructor(t, e, s, i) {
        this.rawName = t;
        this.rawValue = e;
        this.target = s;
        this.command = i;
    }
}

const nt = G("IAttributePattern");

const rt = G("IAttributeParser", (t => t.singleton(AttributeParser)));

class AttributeParser {
    constructor(e, s) {
        this.Y = {};
        this.Z = e;
        const i = this.F = {};
        const n = s.reduce(((t, e) => {
            const s = at(e.constructor);
            s.forEach((t => i[t.pattern] = e));
            return t.concat(s);
        }), t.emptyArray);
        e.add(n);
    }
    parse(t, e) {
        let s = this.Y[t];
        if (null == s) s = this.Y[t] = this.Z.interpret(t);
        const i = s.pattern;
        if (null == i) return new AttrSyntax(t, e, t, null); else return this.F[i][i](t, e, s.parts);
    }
}

AttributeParser.inject = [ et, t.all(nt) ];

function ot(...t) {
    return function e(s) {
        return ct.define(t, s);
    };
}

class AttributePatternResourceDefinition {
    constructor(t) {
        this.Type = t;
        this.name = void 0;
    }
    register(t) {
        X(nt, this.Type).register(t);
    }
}

const lt = d("attribute-pattern");

const ht = "attribute-pattern-definitions";

const at = e => t.Protocol.annotation.get(e, ht);

const ct = k({
    name: lt,
    definitionAnnotationKey: ht,
    define(e, s) {
        const i = new AttributePatternResourceDefinition(s);
        a(lt, i, s);
        p(s, lt);
        t.Protocol.annotation.set(s, ht, e);
        x(s, ht);
        return s;
    },
    getPatternDefinitions: at
});

exports.DotSeparatedAttributePattern = class DotSeparatedAttributePattern {
    "PART.PART"(t, e, s) {
        return new AttrSyntax(t, e, s[0], s[1]);
    }
    "PART.PART.PART"(t, e, s) {
        return new AttrSyntax(t, e, `${s[0]}.${s[1]}`, s[2]);
    }
};

exports.DotSeparatedAttributePattern = r([ ot({
    pattern: "PART.PART",
    symbols: "."
}, {
    pattern: "PART.PART.PART",
    symbols: "."
}) ], exports.DotSeparatedAttributePattern);

exports.RefAttributePattern = class RefAttributePattern {
    ref(t, e, s) {
        return new AttrSyntax(t, e, "element", "ref");
    }
    "PART.ref"(t, e, s) {
        return new AttrSyntax(t, e, s[0], "ref");
    }
};

exports.RefAttributePattern = r([ ot({
    pattern: "ref",
    symbols: ""
}, {
    pattern: "PART.ref",
    symbols: "."
}) ], exports.RefAttributePattern);

exports.ColonPrefixedBindAttributePattern = class ColonPrefixedBindAttributePattern {
    ":PART"(t, e, s) {
        return new AttrSyntax(t, e, s[0], "bind");
    }
};

exports.ColonPrefixedBindAttributePattern = r([ ot({
    pattern: ":PART",
    symbols: ":"
}) ], exports.ColonPrefixedBindAttributePattern);

exports.AtPrefixedTriggerAttributePattern = class AtPrefixedTriggerAttributePattern {
    "@PART"(t, e, s) {
        return new AttrSyntax(t, e, s[0], "trigger");
    }
};

exports.AtPrefixedTriggerAttributePattern = r([ ot({
    pattern: "@PART",
    symbols: "@"
}) ], exports.AtPrefixedTriggerAttributePattern);

let ut = class SpreadAttributePattern {
    "...$attrs"(t, e, s) {
        return new AttrSyntax(t, e, "", "...$attrs");
    }
};

ut = r([ ot({
    pattern: "...$attrs",
    symbols: ""
}) ], ut);

class AttributeObserver {
    constructor(t, e, s) {
        this.type = 2 | 1 | 4;
        this.v = null;
        this.ov = null;
        this.J = false;
        this.o = t;
        this.tt = e;
        this.et = s;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        this.v = t;
        this.J = t !== this.ov;
        this.st();
    }
    st() {
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
                    if (E(e) && e.includes("!important")) {
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
        for (let s = 0, i = t.length; i > s; ++s) {
            const i = t[s];
            if ("attributes" === i.type && i.attributeName === this.tt) {
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
                throw b(`AUR0651:${this.et}`);
            }
            if (t !== this.v) {
                this.ov = this.v;
                this.v = t;
                this.J = false;
                this.it();
            }
        }
    }
    subscribe(t) {
        if (this.subs.add(t) && 1 === this.subs.count) {
            this.v = this.ov = this.o.getAttribute(this.tt);
            ft(this.o.ownerDocument.defaultView.MutationObserver, this.o, this);
        }
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) dt(this.o, this);
    }
    it() {
        mt = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, mt);
    }
}

s.subscriberCollection(AttributeObserver);

const ft = (t, e, s) => {
    if (void 0 === e.$eMObs) e.$eMObs = new Set;
    if (void 0 === e.$mObs) (e.$mObs = new t(pt)).observe(e, {
        attributes: true
    });
    e.$eMObs.add(s);
};

const dt = (t, e) => {
    const s = t.$eMObs;
    if (s && s.delete(e)) {
        if (0 === s.size) {
            t.$mObs.disconnect();
            t.$mObs = void 0;
        }
        return true;
    }
    return false;
};

const pt = t => {
    t[0].target.$eMObs.forEach(xt, t);
};

function xt(t) {
    t.handleMutation(this);
}

let mt;

function gt(t) {
    return function(e) {
        return bt.define(t, e);
    };
}

class BindingBehaviorDefinition {
    constructor(t, e, s, i) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
    }
    static create(e, s) {
        let i;
        let n;
        if (E(e)) {
            i = e;
            n = {
                name: i
            };
        } else {
            i = e.name;
            n = e;
        }
        return new BindingBehaviorDefinition(s, t.firstDefined(wt(s, "name"), i), t.mergeArrays(wt(s, "aliases"), n.aliases, s.aliases), bt.keyFrom(i));
    }
    register(t) {
        const {Type: e, key: s, aliases: i} = this;
        X(s, e).register(t);
        K(s, e).register(t);
        tt(i, bt, s, t);
    }
}

const vt = d("binding-behavior");

const wt = (t, e) => l(f(e), t);

const bt = k({
    name: vt,
    keyFrom(t) {
        return `${vt}:${t}`;
    },
    isType(t) {
        return P(t) && h(vt, t);
    },
    define(t, e) {
        const s = BindingBehaviorDefinition.create(t, e);
        a(vt, s, s.Type);
        a(vt, s, s);
        p(e, vt);
        return s.Type;
    },
    getDefinition(t) {
        const e = l(vt, t);
        if (void 0 === e) throw b(`AUR0151:${t.name}`);
        return e;
    },
    annotate(t, e, s) {
        a(f(e), s, t);
    },
    getAnnotation: wt
});

function yt(t) {
    return function(e) {
        return Ct.define(t, e);
    };
}

class ValueConverterDefinition {
    constructor(t, e, s, i) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
    }
    static create(e, s) {
        let i;
        let n;
        if (E(e)) {
            i = e;
            n = {
                name: i
            };
        } else {
            i = e.name;
            n = e;
        }
        return new ValueConverterDefinition(s, t.firstDefined(At(s, "name"), i), t.mergeArrays(At(s, "aliases"), n.aliases, s.aliases), Ct.keyFrom(i));
    }
    register(e) {
        const {Type: s, key: i, aliases: n} = this;
        t.Registration.singleton(i, s).register(e);
        t.Registration.aliasTo(i, s).register(e);
        tt(n, Ct, i, e);
    }
}

const kt = d("value-converter");

const At = (t, e) => l(f(e), t);

const Ct = k({
    name: kt,
    keyFrom: t => `${kt}:${t}`,
    isType(t) {
        return P(t) && h(kt, t);
    },
    define(t, e) {
        const s = ValueConverterDefinition.create(t, e);
        a(kt, s, s.Type);
        a(kt, s, s);
        p(e, kt);
        return s.Type;
    },
    getDefinition(t) {
        const e = l(kt, t);
        if (void 0 === e) throw b(`AUR0152:${t.name}`);
        return e;
    },
    annotate(t, e, s) {
        a(f(e), s, t);
    },
    getAnnotation: At
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
        if (t !== s.astEvaluate(i.ast, i.s, i, null)) {
            this.v = t;
            this.nt.add(this);
        }
    }
}

const Bt = t => {
    U(t.prototype, "useScope", (function(t) {
        this.s = t;
    }));
};

const Rt = (t, e = true) => i => {
    const n = i.prototype;
    if (null != t) q(n, "strict", {
        enumerable: true,
        get: function() {
            return t;
        }
    });
    q(n, "strictFnCall", {
        enumerable: true,
        get: function() {
            return e;
        }
    });
    U(n, "get", (function(t) {
        return this.l.get(t);
    }));
    U(n, "getSignaler", (function() {
        return this.l.root.get(s.ISignaler);
    }));
    U(n, "getConverter", (function(t) {
        const e = Ct.keyFrom(t);
        let s = St.get(this);
        if (null == s) St.set(this, s = new ResourceLookup);
        return s[e] ?? (s[e] = this.l.get(W(e)));
    }));
    U(n, "getBehavior", (function(t) {
        const e = bt.keyFrom(t);
        let s = St.get(this);
        if (null == s) St.set(this, s = new ResourceLookup);
        return s[e] ?? (s[e] = this.l.get(W(e)));
    }));
};

const St = new WeakMap;

class ResourceLookup {}

const It = G("IFlushQueue", (t => t.singleton(FlushQueue)));

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
            this.ot.forEach(Tt);
        } finally {
            this.rt = false;
        }
    }
    clear() {
        this.ot.clear();
        this.rt = false;
    }
}

function Tt(t, e, s) {
    s.delete(t);
    t.flush();
}

const Pt = new WeakSet;

const Et = (t, e) => {
    U(t.prototype, "limit", (function(t) {
        if (Pt.has(this)) throw b(`AURXXXX: a rate limit has already been applied.`);
        Pt.add(this);
        const s = e(this, t);
        const i = this[s];
        const n = (...t) => i.call(this, ...t);
        const r = "debounce" === t.type ? Lt(t, n, this) : Dt(t, n, this);
        this[s] = r;
        return {
            dispose: () => {
                Pt.delete(this);
                r.dispose();
                delete this[s];
            }
        };
    }));
};

const Lt = (t, e, s) => {
    let i;
    let n;
    let r;
    const o = t.queue;
    const l = l => {
        r = l;
        if (s.isBound) {
            n = i;
            i = o.queueTask((() => e(r)), {
                delay: t.delay,
                reusable: false
            });
            n?.cancel();
        } else e(r);
    };
    l.dispose = () => {
        n?.cancel();
        i?.cancel();
    };
    return l;
};

const Dt = (t, e, s) => {
    let i;
    let n;
    let r = 0;
    let o = 0;
    let l;
    const h = t.queue;
    const a = () => t.now();
    const c = c => {
        l = c;
        if (s.isBound) {
            o = a() - r;
            n = i;
            if (o > t.delay) {
                r = a();
                e(l);
            } else i = h.queueTask((() => {
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
        i?.cancel();
    };
    return c;
};

const _t = {
    reusable: false,
    preempt: true
};

class AttributeBinding {
    constructor(t, e, s, i, n, r, o, l, h) {
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
        this.oL = s;
        this.ct = i;
    }
    updateTarget(t) {
        this.ut.setValue(t, this.target, this.targetProperty);
    }
    handleChange() {
        if (!this.isBound) return;
        let t;
        this.obs.version++;
        const e = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (e !== this.v) {
            this.v = e;
            const s = 1 !== this.ht.state && (4 & this.ut.type) > 0;
            if (s) {
                t = this.lt;
                this.lt = this.ct.queueTask((() => {
                    this.lt = null;
                    this.updateTarget(e);
                }), _t);
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
        s.astBind(this.ast, t, this);
        this.ut ?? (this.ut = new AttributeObserver(this.target, this.targetProperty, this.targetAttribute));
        if (this.mode & (2 | 1)) this.updateTarget(this.v = s.astEvaluate(this.ast, t, this, (2 & this.mode) > 0 ? this : null));
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        s.astUnbind(this.ast, this.s, this);
        this.s = void 0;
        this.v = void 0;
        this.lt?.cancel();
        this.lt = null;
        this.obs.clearAll();
    }
}

Bt(AttributeBinding);

Et(AttributeBinding, (() => "updateTarget"));

s.connectable(AttributeBinding);

Rt(true)(AttributeBinding);

const qt = {
    reusable: false,
    preempt: true
};

class InterpolationBinding {
    constructor(t, e, s, i, n, r, o, l) {
        this.ast = n;
        this.target = r;
        this.targetProperty = o;
        this.mode = l;
        this.isBound = false;
        this.s = void 0;
        this.lt = null;
        this.ht = t;
        this.oL = s;
        this.ct = i;
        this.ut = s.getAccessor(r, o);
        const h = n.expressions;
        const a = this.partBindings = Array(h.length);
        const c = h.length;
        let u = 0;
        for (;c > u; ++u) a[u] = new InterpolationPartBinding(h[u], r, o, e, s, this);
    }
    ft() {
        this.updateTarget();
    }
    updateTarget() {
        const t = this.partBindings;
        const e = this.ast.parts;
        const s = t.length;
        let i = "";
        let n = 0;
        if (1 === s) i = e[0] + t[0].v + e[1]; else {
            i = e[0];
            for (;s > n; ++n) i += t[n].v + e[n + 1];
        }
        const r = this.ut;
        const o = 1 !== this.ht.state && (4 & r.type) > 0;
        let l;
        if (o) {
            l = this.lt;
            this.lt = this.ct.queueTask((() => {
                this.lt = null;
                r.setValue(i, this.target, this.targetProperty);
            }), qt);
            l?.cancel();
            l = null;
        } else r.setValue(i, this.target, this.targetProperty);
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        const e = this.partBindings;
        const s = e.length;
        let i = 0;
        for (;s > i; ++i) e[i].bind(t);
        this.updateTarget();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        this.s = void 0;
        const t = this.partBindings;
        const e = t.length;
        let s = 0;
        for (;e > s; ++s) t[s].unbind();
        this.lt?.cancel();
        this.lt = null;
    }
}

class InterpolationPartBinding {
    constructor(t, e, s, i, n, r) {
        this.ast = t;
        this.target = e;
        this.targetProperty = s;
        this.owner = r;
        this.mode = 2;
        this.task = null;
        this.isBound = false;
        this.v = "";
        this.boundFn = false;
        this.l = i;
        this.oL = n;
    }
    updateTarget() {
        this.owner.ft();
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (t != this.v) {
            this.v = t;
            if (T(t)) this.observeCollection(t);
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
        s.astBind(this.ast, t, this);
        this.v = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        if (T(this.v)) this.observeCollection(this.v);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        s.astUnbind(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

Bt(InterpolationPartBinding);

Et(InterpolationPartBinding, (() => "updateTarget"));

s.connectable(InterpolationPartBinding);

Rt(true)(InterpolationPartBinding);

class ContentBinding {
    constructor(t, e, s, i, n, r, o, l) {
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
        this.oL = s;
        this.ct = i;
    }
    updateTarget(t) {
        const e = this.target;
        const s = this.p.Node;
        const i = this.v;
        this.v = t;
        if (i instanceof s) i.parentNode?.removeChild(i);
        if (t instanceof s) {
            e.textContent = "";
            e.parentNode?.insertBefore(t, e);
        } else e.textContent = String(t);
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
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
        const t = this.v = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        if (T(t)) this.observeCollection(t);
        const e = 1 !== this.ht.state;
        if (e) this.dt(t); else this.updateTarget(t);
    }
    bind(t) {
        if (this.isBound) {
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        s.astBind(this.ast, t, this);
        const e = this.v = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        if (T(e)) this.observeCollection(e);
        this.updateTarget(e);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        s.astUnbind(this.ast, this.s, this);
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
        }), qt);
        e?.cancel();
    }
}

Bt(ContentBinding);

Et(ContentBinding, (() => "updateTarget"));

s.connectable()(ContentBinding);

Rt(void 0, false)(ContentBinding);

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
        this.xt = n;
    }
    updateTarget() {
        this.target[this.targetProperty] = this.v;
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        this.v = s.astEvaluate(this.ast, this.s, this, this);
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
        this.target = this.xt ? t.bindingContext : t.overrideContext;
        s.astBind(this.ast, t, this);
        this.v = s.astEvaluate(this.ast, this.s, this, this);
        this.updateTarget();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        s.astUnbind(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

Bt(LetBinding);

Et(LetBinding, (() => "updateTarget"));

s.connectable(LetBinding);

Rt(true)(LetBinding);

class PropertyBinding {
    constructor(t, e, s, i, n, r, o, l) {
        this.ast = n;
        this.target = r;
        this.targetProperty = o;
        this.mode = l;
        this.isBound = false;
        this.s = void 0;
        this.ut = void 0;
        this.lt = null;
        this.gt = null;
        this.boundFn = false;
        this.l = e;
        this.ht = t;
        this.ct = i;
        this.oL = s;
    }
    updateTarget(t) {
        this.ut.setValue(t, this.target, this.targetProperty);
    }
    updateSource(t) {
        s.astAssign(this.ast, this.s, this, t);
    }
    handleChange() {
        if (!this.isBound) return;
        this.obs.version++;
        const t = s.astEvaluate(this.ast, this.s, this, (2 & this.mode) > 0 ? this : null);
        this.obs.clear();
        const e = 1 !== this.ht.state && (4 & this.ut.type) > 0;
        if (e) {
            Ut = this.lt;
            this.lt = this.ct.queueTask((() => {
                this.updateTarget(t);
                this.lt = null;
            }), $t);
            Ut?.cancel();
            Ut = null;
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
        s.astBind(this.ast, t, this);
        const e = this.oL;
        const i = this.mode;
        let n = this.ut;
        if (!n) {
            if (4 & i) n = e.getObserver(this.target, this.targetProperty); else n = e.getAccessor(this.target, this.targetProperty);
            this.ut = n;
        }
        const r = (2 & i) > 0;
        if (i & (2 | 1)) this.updateTarget(s.astEvaluate(this.ast, this.s, this, r ? this : null));
        if (4 & i) {
            n.subscribe(this.gt ?? (this.gt = new BindingTargetSubscriber(this, this.l.get(It))));
            if (!r) this.updateSource(n.getValue(this.target, this.targetProperty));
        }
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        s.astUnbind(this.ast, this.s, this);
        this.s = void 0;
        if (this.gt) {
            this.ut.unsubscribe(this.gt);
            this.gt = null;
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
        if (null != this.gt) throw b(`AURxxxx: binding already has a target subscriber`);
        this.gt = t;
    }
}

Bt(PropertyBinding);

Et(PropertyBinding, (t => 4 & t.mode ? "updateSource" : "updateTarget"));

s.connectable(PropertyBinding);

Rt(true, false)(PropertyBinding);

let Ut = null;

const $t = {
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
            if (this.s === t) return;
            this.unbind();
        }
        this.s = t;
        s.astBind(this.ast, t, this);
        s.astAssign(this.ast, this.s, this, this.target);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        if (s.astEvaluate(this.ast, this.s, this, null) === this.target) s.astAssign(this.ast, this.s, this, null);
        s.astUnbind(this.ast, this.s, this);
        this.s = void 0;
    }
}

Rt(false)(RefBinding);

class ListenerBindingOptions {
    constructor(t, e = false) {
        this.prevent = t;
        this.capture = e;
    }
}

class ListenerBinding {
    constructor(t, e, s, i, n) {
        this.ast = e;
        this.target = s;
        this.targetEvent = i;
        this.isBound = false;
        this.self = false;
        this.boundFn = true;
        this.l = t;
        this.vt = n;
    }
    callSource(t) {
        const e = this.s.overrideContext;
        e.$event = t;
        let i = s.astEvaluate(this.ast, this.s, this, null);
        delete e.$event;
        if (P(i)) i = i(t);
        if (true !== i && this.vt.prevent) t.preventDefault();
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
        s.astBind(this.ast, t, this);
        this.target.addEventListener(this.targetEvent, this, this.vt);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) return;
        this.isBound = false;
        s.astUnbind(this.ast, this.s, this);
        this.s = void 0;
        this.target.removeEventListener(this.targetEvent, this, this.vt);
    }
}

Bt(ListenerBinding);

Et(ListenerBinding, (() => "callSource"));

Rt(true, true)(ListenerBinding);

const Mt = G("IAppTask");

class $AppTask {
    constructor(t, e, s) {
        this.c = void 0;
        this.slot = t;
        this.k = e;
        this.cb = s;
    }
    register(t) {
        return this.c = t.register(Q(Mt, this));
    }
    run() {
        const t = this.k;
        const e = this.cb;
        return null === t ? e() : e(this.c.get(t));
    }
}

const Ft = k({
    creating: Ot("creating"),
    hydrating: Ot("hydrating"),
    hydrated: Ot("hydrated"),
    activating: Ot("activating"),
    activated: Ot("activated"),
    deactivating: Ot("deactivating"),
    deactivated: Ot("deactivated")
});

function Ot(t) {
    function e(e, s) {
        if (P(s)) return new $AppTask(t, e, s);
        return new $AppTask(t, null, e);
    }
    return e;
}

function Vt(t, e) {
    if (null == t) throw b(`AUR0772`);
    return function s(i, n, r) {
        const o = null == n;
        const l = o ? i : i.constructor;
        const h = new WatchDefinition(t, o ? e : r.value);
        if (o) {
            if (!P(e) && (null == e || !(e in l.prototype))) throw b(`AUR0773:${String(e)}@${l.name}}`);
        } else if (!P(r?.value)) throw b(`AUR0774:${String(n)}`);
        Ht.add(l, h);
        if (Qt(l)) Jt(l).watches.push(h);
        if (ti(l)) ii(l).watches.push(h);
    };
}

class WatchDefinition {
    constructor(t, e) {
        this.expression = t;
        this.callback = e;
    }
}

const Nt = t.emptyArray;

const jt = f("watch");

const Ht = k({
    name: jt,
    add(t, e) {
        let s = l(jt, t);
        if (null == s) a(jt, s = [], t);
        s.push(e);
    },
    getAnnotation(t) {
        return l(jt, t) ?? Nt;
    }
});

function Wt(t) {
    return function(e) {
        return Zt(t, e);
    };
}

function zt(t) {
    return function(e) {
        return Zt(E(t) ? {
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
    constructor(t, e, s, i, n, r, o, l, h, a) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
        this.defaultBindingMode = n;
        this.isTemplateController = r;
        this.bindables = o;
        this.noMultiBindings = l;
        this.watches = h;
        this.dependencies = a;
    }
    static create(e, s) {
        let i;
        let n;
        if (E(e)) {
            i = e;
            n = {
                name: i
            };
        } else {
            i = e.name;
            n = e;
        }
        return new CustomAttributeDefinition(s, t.firstDefined(Kt(s, "name"), i), t.mergeArrays(Kt(s, "aliases"), n.aliases, s.aliases), Xt(i), t.firstDefined(Kt(s, "defaultBindingMode"), n.defaultBindingMode, s.defaultBindingMode, 2), t.firstDefined(Kt(s, "isTemplateController"), n.isTemplateController, s.isTemplateController, false), O.from(s, ...O.getAll(s), Kt(s, "bindables"), s.bindables, n.bindables), t.firstDefined(Kt(s, "noMultiBindings"), n.noMultiBindings, s.noMultiBindings, false), t.mergeArrays(Ht.getAnnotation(s), s.watches), t.mergeArrays(Kt(s, "dependencies"), n.dependencies, s.dependencies));
    }
    register(t) {
        const {Type: e, key: s, aliases: i} = this;
        Y(s, e).register(t);
        K(s, e).register(t);
        tt(i, te, s, t);
    }
}

const Gt = d("custom-attribute");

const Xt = t => `${Gt}:${t}`;

const Kt = (t, e) => l(f(e), t);

const Qt = t => P(t) && h(Gt, t);

const Yt = (t, e) => As(t, Xt(e)) ?? void 0;

const Zt = (t, e) => {
    const s = CustomAttributeDefinition.create(t, e);
    a(Gt, s, s.Type);
    a(Gt, s, s);
    p(e, Gt);
    return s.Type;
};

const Jt = t => {
    const e = l(Gt, t);
    if (void 0 === e) throw b(`AUR0759:${t.name}`);
    return e;
};

const te = k({
    name: Gt,
    keyFrom: Xt,
    isType: Qt,
    for: Yt,
    define: Zt,
    getDefinition: Jt,
    annotate(t, e, s) {
        a(f(e), s, t);
    },
    getAnnotation: Kt
});

function ee(t, e) {
    let s;
    function i(t, e) {
        if (arguments.length > 1) s.property = e;
        a(ie, ChildrenDefinition.create(e, s), t.constructor, e);
        x(t.constructor, ne.keyFrom(e));
    }
    if (arguments.length > 1) {
        s = {};
        i(t, e);
        return;
    } else if (E(t)) {
        s = {};
        return i;
    }
    s = void 0 === t ? {} : t;
    return i;
}

function se(t) {
    return t.startsWith(ie);
}

const ie = f("children-observer");

const ne = k({
    name: ie,
    keyFrom: t => `${ie}:${t}`,
    from(...t) {
        const e = {};
        function s(t) {
            e[t] = ChildrenDefinition.create(t);
        }
        function i(t, s) {
            e[t] = ChildrenDefinition.create(t, s);
        }
        function n(t) {
            if (T(t)) t.forEach(s); else if (t instanceof ChildrenDefinition) e[t.property] = t; else if (void 0 !== t) B(t).forEach((e => i(e, t)));
        }
        t.forEach(n);
        return e;
    },
    getAll(e) {
        const s = ie.length + 1;
        const i = [];
        const n = t.getPrototypeChain(e);
        let r = n.length;
        let o = 0;
        let h;
        let a;
        let c;
        while (--r >= 0) {
            c = n[r];
            h = m(c).filter(se);
            a = h.length;
            for (let t = 0; t < a; ++t) i[o++] = l(ie, c, h[t].slice(s));
        }
        return i;
    }
});

const re = {
    childList: true
};

class ChildrenDefinition {
    constructor(t, e, s, i, n, r) {
        this.callback = t;
        this.property = e;
        this.options = s;
        this.query = i;
        this.filter = n;
        this.map = r;
    }
    static create(e, s = {}) {
        return new ChildrenDefinition(t.firstDefined(s.callback, `${e}Changed`), t.firstDefined(s.property, e), s.options ?? re, s.query, s.filter, s.map);
    }
}

class ChildrenObserver {
    constructor(t, e, s, i, n = oe, r = le, o = he, l) {
        this.controller = t;
        this.obj = e;
        this.propertyKey = s;
        this.query = n;
        this.filter = r;
        this.map = o;
        this.options = l;
        this.observing = false;
        this.children = void 0;
        this.observer = void 0;
        this.callback = e[i];
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
            this.children = t.emptyArray;
        }
    }
    wt() {
        this.children = this.get();
        if (void 0 !== this.callback) this.callback.call(this.obj);
        this.subs.notify(this.children, void 0);
    }
    get() {
        return ce(this.controller, this.query, this.filter, this.map);
    }
}

s.subscriberCollection()(ChildrenObserver);

function oe(t) {
    return t.host.childNodes;
}

function le(t, e, s) {
    return !!s;
}

function he(t, e, s) {
    return s;
}

const ae = {
    optional: true
};

function ce(t, e, s, i) {
    const n = e(t);
    const r = n.length;
    const o = [];
    let l;
    let h;
    let a;
    let c = 0;
    for (;c < r; ++c) {
        l = n[c];
        h = ei(l, ae);
        a = h?.viewModel ?? null;
        if (s(l, h, a)) o.push(i(l, h, a));
    }
    return o;
}

const ue = t.IPlatform;

const fe = (t, e, s, i) => {
    t.addEventListener(e, s, i);
};

const de = (t, e, s, i) => {
    t.removeEventListener(e, s, i);
};

const pe = t => {
    let e;
    const s = t.prototype;
    U(s, "subscribe", (function(t) {
        if (this.subs.add(t) && 1 === this.subs.count) {
            for (e of this.cf.events) fe(this.bt, e, this);
            this.yt = true;
            this.kt?.();
        }
    }));
    U(s, "unsubscribe", (function(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) {
            for (e of this.cf.events) de(this.bt, e, this);
            this.yt = false;
            this.At?.();
        }
    }));
    U(s, "useConfig", (function(t) {
        this.cf = t;
        if (this.yt) {
            for (e of this.cf.events) de(this.bt, e, this);
            for (e of this.cf.events) fe(this.bt, e, this);
        }
    }));
};

const xe = e => {
    U(e.prototype, "subscribe", t.noop);
    U(e.prototype, "unsubscribe", t.noop);
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
        this.st();
    }
    st() {
        if (this.J) {
            this.J = false;
            const t = this.value;
            const e = this.Ct;
            const s = me(t);
            let i = this.Bt;
            this.ov = t;
            if (s.length > 0) this.Rt(s);
            this.Bt += 1;
            if (0 === i) return;
            i -= 1;
            for (const t in e) {
                if (!y.call(e, t) || e[t] !== i) continue;
                this.obj.classList.remove(t);
            }
        }
    }
    Rt(t) {
        const e = this.obj;
        const s = t.length;
        let i = 0;
        let n;
        for (;i < s; i++) {
            n = t[i];
            if (0 === n.length) continue;
            this.Ct[n] = this.Bt;
            e.classList.add(n);
        }
    }
}

function me(e) {
    if (E(e)) return ge(e);
    if ("object" !== typeof e) return t.emptyArray;
    if (e instanceof Array) {
        const s = e.length;
        if (s > 0) {
            const t = [];
            let i = 0;
            for (;s > i; ++i) t.push(...me(e[i]));
            return t;
        } else return t.emptyArray;
    }
    const s = [];
    let i;
    for (i in e) if (Boolean(e[i])) if (i.includes(" ")) s.push(...ge(i)); else s.push(i);
    return s;
}

function ge(e) {
    const s = e.match(/\S+/g);
    if (null === s) return t.emptyArray;
    return s;
}

xe(ClassAttributeAccessor);

function ve(...t) {
    return new CSSModulesProcessorRegistry(t);
}

class CSSModulesProcessorRegistry {
    constructor(t) {
        this.modules = t;
    }
    register(t) {
        var e;
        const s = A({}, ...this.modules);
        const i = Zt({
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
                this.St.setValue(this.value?.split(/\s+/g).map((t => s[t] || t)) ?? "");
            }
        }, e.inject = [ Bs ], e));
        t.register(i, Q(Is, s));
    }
}

function we(...t) {
    return new ShadowDOMRegistry(t);
}

const be = G("IShadowDOMStyleFactory", (t => t.cachedCallback((t => {
    if (AdoptedStyleSheetsStyles.supported(t.get(ue))) return t.get(AdoptedStyleSheetsStylesFactory);
    return t.get(StyleElementStylesFactory);
}))));

class ShadowDOMRegistry {
    constructor(t) {
        this.css = t;
    }
    register(t) {
        const e = t.get(ke);
        const s = t.get(be);
        t.register(Q(ye, s.createStyles(this.css, e)));
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

AdoptedStyleSheetsStylesFactory.inject = [ ue ];

class StyleElementStylesFactory {
    constructor(t) {
        this.p = t;
    }
    createStyles(t, e) {
        return new StyleElementStyles(this.p, t, e);
    }
}

StyleElementStylesFactory.inject = [ ue ];

const ye = G("IShadowDOMStyles");

const ke = G("IShadowDOMGlobalStyles", (e => e.instance({
    applyTo: t.noop
})));

class AdoptedStyleSheetsStyles {
    constructor(t, e, s, i = null) {
        this.sharedStyles = i;
        this.styleSheets = e.map((e => {
            let i;
            if (e instanceof t.CSSStyleSheet) i = e; else {
                i = s.get(e);
                if (void 0 === i) {
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
        if (null !== this.sharedStyles) this.sharedStyles.applyTo(t);
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
        if (null !== this.sharedStyles) this.sharedStyles.applyTo(t);
    }
}

const Ae = {
    shadowDOM(e) {
        return Ft.creating(t.IContainer, (t => {
            if (null != e.sharedStyles) {
                const s = t.get(be);
                t.register(Q(ke, s.createStyles(e.sharedStyles, null)));
            }
        }));
    }
};

const {enter: Ce, exit: Be} = s.ConnectableSwitcher;

const {wrap: Re, unwrap: Se} = s.ProxyObservable;

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
        const s = this.compute();
        if (!_(s, e)) this.cb.call(t, s, e, t);
    }
    compute() {
        this.running = true;
        this.obs.version++;
        try {
            Ce(this);
            return this.v = Se(this.$get.call(void 0, this.useProxy ? Re(this.obj) : this.obj, this));
        } finally {
            this.obs.clear();
            this.running = false;
            Be(this);
        }
    }
}

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
        this.It = i;
        this.cb = n;
    }
    handleChange(t) {
        const e = this.It;
        const i = this.obj;
        const n = this.v;
        const r = 1 === e.$kind && 1 === this.obs.count;
        if (!r) {
            this.obs.version++;
            t = s.astEvaluate(e, this.scope, this, this);
            this.obs.clear();
        }
        if (!_(t, n)) {
            this.v = t;
            this.cb.call(i, t, n, i);
        }
    }
    bind() {
        if (this.isBound) return;
        this.obs.version++;
        this.v = s.astEvaluate(this.It, this.scope, this, this);
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

s.connectable(ComputedWatcher);

s.connectable(ExpressionWatcher);

Rt(true)(ExpressionWatcher);

const Ie = G("ILifecycleHooks");

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
        while (i !== v) {
            for (const t of C(i)) if ("constructor" !== t) s.add(t);
            i = Object.getPrototypeOf(i);
        }
        return new LifecycleHooksDefinition(e, s);
    }
    register(t) {
        X(Ie, this.Type).register(t);
    }
}

const Te = new WeakMap;

const Pe = f("lifecycle-hooks");

const Ee = k({
    name: Pe,
    define(t, e) {
        const s = LifecycleHooksDefinition.create(t, e);
        a(Pe, s, e);
        p(e, Pe);
        return s.Type;
    },
    resolve(t) {
        let e = Te.get(t);
        if (void 0 === e) {
            Te.set(t, e = new LifecycleHooksLookupImpl);
            const s = t.root;
            const i = s.id === t.id ? t.getAll(Ie) : t.has(Ie, false) ? s.getAll(Ie).concat(t.getAll(Ie)) : s.getAll(Ie);
            let n;
            let r;
            let o;
            let h;
            let a;
            for (n of i) {
                r = l(Pe, n.constructor);
                o = new LifecycleHooksEntry(r, n);
                for (h of r.propertyNames) {
                    a = e[h];
                    if (void 0 === a) e[h] = [ o ]; else a.push(o);
                }
            }
        }
        return e;
    }
});

class LifecycleHooksLookupImpl {}

function Le() {
    return function t(e) {
        return Ee.define({}, e);
    };
}

const De = G("IViewFactory");

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
            if ("*" === t) t = ViewFactory.maxCacheSize; else if (E(t)) t = parseInt(t, 10);
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
        let s;
        if (null != e && e.length > 0) {
            s = e.pop();
            return s;
        }
        s = Controller.$view(this, t);
        return s;
    }
}

ViewFactory.maxCacheSize = 65535;

const _e = G("IRendering", (t => t.singleton(Rendering)));

class Rendering {
    get renderers() {
        return this.Pt ?? (this.Pt = this.Et.getAll(mi, false).reduce(((t, e) => {
            t[e.target] = e;
            return t;
        }), w()));
    }
    constructor(t) {
        this.Lt = new WeakMap;
        this.Dt = new WeakMap;
        const e = t.root;
        this.p = (this.Et = e).get(ue);
        this.ep = e.get(s.IExpressionParser);
        this.oL = e.get(s.IObserverLocator);
        this._t = new FragmentNodeSequence(this.p, this.p.document.createDocumentFragment());
    }
    compile(t, e, s) {
        if (false !== t.needsCompile) {
            const i = this.Lt;
            const n = e.get(xi);
            let r = i.get(t);
            if (null == r) i.set(t, r = n.compile(t, e, s)); else e.register(...r.dependencies);
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
        let s = false;
        const i = this.Dt;
        const n = this.p;
        const r = n.document;
        if (i.has(t)) e = i.get(t); else {
            const o = t.template;
            let l;
            if (null === o) e = null; else if (o instanceof n.Node) if ("TEMPLATE" === o.nodeName) {
                e = o.content;
                s = true;
            } else (e = r.createDocumentFragment()).appendChild(o.cloneNode(true)); else {
                l = r.createElement("template");
                if (E(o)) l.innerHTML = o;
                e = l.content;
                s = true;
            }
            i.set(t, e);
        }
        return null == e ? this._t : new FragmentNodeSequence(this.p, s ? r.importNode(e, true) : r.adoptNode(e.cloneNode(true)));
    }
    render(t, e, s, i) {
        const n = s.instructions;
        const r = this.renderers;
        const o = e.length;
        if (e.length !== n.length) throw b(`AUR0757:${o}<>${n.length}`);
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
        if (null != i) {
            c = s.surrogates;
            if ((a = c.length) > 0) {
                h = 0;
                while (a > h) {
                    u = c[h];
                    r[u.type].render(t, i, u, this.p, this.ep, this.oL);
                    ++h;
                }
            }
        }
    }
}

Rendering.inject = [ t.IContainer ];

exports.LifecycleFlags = void 0;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["fromBind"] = 1] = "fromBind";
    t[t["fromUnbind"] = 2] = "fromUnbind";
    t[t["dispose"] = 4] = "dispose";
})(exports.LifecycleFlags || (exports.LifecycleFlags = {}));

var qe;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["host"] = 1] = "host";
    t[t["shadowRoot"] = 2] = "shadowRoot";
    t[t["location"] = 3] = "location";
})(qe || (qe = {}));

const Ue = {
    optional: true
};

const $e = new WeakMap;

class Controller {
    get lifecycleHooks() {
        return this.qt;
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
        return this.Ut;
    }
    get viewModel() {
        return this.$t;
    }
    set viewModel(t) {
        this.$t = t;
        this.Ut = null == t || 2 === this.vmKind ? HooksDefinition.none : new HooksDefinition(t);
    }
    constructor(e, s, i, n, r, o, l) {
        this.container = e;
        this.vmKind = s;
        this.definition = i;
        this.viewFactory = n;
        this.host = o;
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
        this.qt = null;
        this.state = 0;
        this.Mt = false;
        this.Ft = t.emptyArray;
        this.flags = 0;
        this.$initiator = null;
        this.$flags = 0;
        this.$resolve = void 0;
        this.$reject = void 0;
        this.$promise = void 0;
        this.Ot = 0;
        this.Vt = 0;
        this.Nt = 0;
        this.$t = r;
        this.Ut = 2 === s ? HooksDefinition.none : new HooksDefinition(r);
        this.location = l;
        this.r = e.root.get(_e);
    }
    static getCached(t) {
        return $e.get(t);
    }
    static getCachedOrThrow(t) {
        const e = Controller.getCached(t);
        if (void 0 === e) throw b(`AUR0500:${t}`);
        return e;
    }
    static $el(e, s, i, n, r = void 0, o = null) {
        if ($e.has(s)) return $e.get(s);
        r = r ?? ii(s.constructor);
        const l = new Controller(e, 0, r, null, s, i, o);
        const h = e.get(t.optional(Ke));
        if (r.dependencies.length > 0) e.register(...r.dependencies);
        Z(e, Ke, new t.InstanceProvider("IHydrationContext", new HydrationContext(l, n, h)));
        $e.set(s, l);
        if (null == n || false !== n.hydrate) l.hE(n, h);
        return l;
    }
    static $attr(t, e, s, i) {
        if ($e.has(e)) return $e.get(e);
        i = i ?? Jt(e.constructor);
        const n = new Controller(t, 1, i, null, e, s, null);
        if (i.dependencies.length > 0) t.register(...i.dependencies);
        $e.set(e, n);
        n.jt();
        return n;
    }
    static $view(t, e = void 0) {
        const s = new Controller(t.container, 2, null, t, null, null, null);
        s.parent = e ?? null;
        s.Ht();
        return s;
    }
    hE(e, i) {
        const n = this.container;
        const r = this.flags;
        const o = this.$t;
        let l = this.definition;
        this.scope = s.Scope.create(o, null, true);
        if (l.watches.length > 0) je(this, n, l, o);
        Fe(this, l, r, o);
        this.Ft = Oe(this, l, o);
        if (this.Ut.hasDefine) {
            const t = o.define(this, i, l);
            if (void 0 !== t && t !== l) l = CustomElementDefinition.getOrCreate(t);
        }
        this.qt = Ee.resolve(n);
        l.register(n);
        if (null !== l.injectable) Z(n, l.injectable, new t.InstanceProvider("definition.injectable", o));
        if (null == e || false !== e.hydrate) {
            this.hS(e);
            this.hC();
        }
    }
    hS(t) {
        if (void 0 !== this.qt.hydrating) this.qt.hydrating.forEach(Ze, this);
        if (this.Ut.hasHydrating) this.$t.hydrating(this);
        const e = this.Wt = this.r.compile(this.definition, this.container, t);
        const {shadowOptions: s, isStrictBinding: i, hasSlots: n, containerless: r} = e;
        let o = this.location;
        this.isStrictBinding = i;
        if (null !== (this.hostController = ei(this.host, Ue))) {
            this.host = this.container.root.get(ue).document.createElement(this.definition.name);
            if (r && null == o) o = this.location = Ls(this.host);
        }
        Cs(this.host, Ks, this);
        Cs(this.host, this.definition.key, this);
        if (null !== s || n) {
            if (null != o) throw b(`AUR0501`);
            Cs(this.shadowRoot = this.host.attachShadow(s ?? ze), Ks, this);
            Cs(this.shadowRoot, this.definition.key, this);
            this.mountTarget = 2;
        } else if (null != o) {
            Cs(o, Ks, this);
            Cs(o, this.definition.key, this);
            this.mountTarget = 3;
        } else this.mountTarget = 1;
        this.$t.$controller = this;
        this.nodes = this.r.createNodes(e);
        if (void 0 !== this.qt.hydrated) this.qt.hydrated.forEach(Je, this);
        if (this.Ut.hasHydrated) this.$t.hydrated(this);
    }
    hC() {
        this.r.render(this, this.nodes.findTargets(), this.Wt, this.host);
        if (void 0 !== this.qt.created) this.qt.created.forEach(Ye, this);
        if (this.Ut.hasCreated) this.$t.created(this);
    }
    jt() {
        const t = this.definition;
        const e = this.$t;
        if (t.watches.length > 0) je(this, this.container, t, e);
        Fe(this, t, this.flags, e);
        e.$controller = this;
        this.qt = Ee.resolve(this.container);
        if (void 0 !== this.qt.created) this.qt.created.forEach(Ye, this);
        if (this.Ut.hasCreated) this.$t.created(this);
    }
    Ht() {
        this.Wt = this.r.compile(this.viewFactory.def, this.container, null);
        this.isStrictBinding = this.Wt.isStrictBinding;
        this.r.render(this, (this.nodes = this.r.createNodes(this.Wt)).findTargets(), this.Wt, void 0);
    }
    activate(e, s, i, n) {
        switch (this.state) {
          case 0:
          case 8:
            if (!(null === s || s.isActive)) return;
            this.state = 1;
            break;

          case 2:
            return;

          case 32:
            throw b(`AUR0502:${this.name}`);

          default:
            throw b(`AUR0503:${this.name} ${Ge(this.state)}`);
        }
        this.parent = s;
        i |= 1;
        switch (this.vmKind) {
          case 0:
            this.scope.parent = n ?? null;
            break;

          case 1:
            this.scope = n ?? null;
            break;

          case 2:
            if (void 0 === n || null === n) throw b(`AUR0504`);
            if (!this.hasLockedScope) this.scope = n;
            break;
        }
        if (this.isStrictBinding) ;
        this.$initiator = e;
        this.$flags = i;
        this.zt();
        let r;
        if (2 !== this.vmKind && null != this.qt.binding) r = t.resolveAll(...this.qt.binding.map(ts, this));
        if (this.Ut.hasBinding) r = t.resolveAll(r, this.$t.binding(this.$initiator, this.parent, this.$flags));
        if (I(r)) {
            this.Gt();
            r.then((() => {
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
        let e = 0;
        let s = this.Ft.length;
        let i;
        if (s > 0) while (s > e) {
            this.Ft[e].start();
            ++e;
        }
        if (null !== this.bindings) {
            e = 0;
            s = this.bindings.length;
            while (s > e) {
                this.bindings[e].bind(this.scope);
                ++e;
            }
        }
        if (2 !== this.vmKind && null != this.qt.bound) i = t.resolveAll(...this.qt.bound.map(es, this));
        if (this.Ut.hasBound) i = t.resolveAll(i, this.$t.bound(this.$initiator, this.parent, this.$flags));
        if (I(i)) {
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
                const e = t.has(ye, false) ? t.get(ye) : t.get(ke);
                e.applyTo(this.shadowRoot);
                this.nodes.appendTo(this.shadowRoot);
                break;
            }

          case 3:
            this.nodes.insertBefore(this.location);
            break;
        }
        let e = 0;
        let s;
        if (2 !== this.vmKind && null != this.qt.attaching) s = t.resolveAll(...this.qt.attaching.map(ss, this));
        if (this.Ut.hasAttaching) s = t.resolveAll(s, this.$t.attaching(this.$initiator, this.parent, this.$flags));
        if (I(s)) {
            this.Gt();
            this.zt();
            s.then((() => {
                this.Yt();
            })).catch((t => {
                this.Xt(t);
            }));
        }
        if (null !== this.children) for (;e < this.children.length; ++e) void this.children[e].activate(this.$initiator, this, this.$flags, this.scope);
        this.Yt();
    }
    deactivate(e, s, i) {
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
            throw b(`AUR0505:${this.name} ${Ge(this.state)}`);
        }
        this.$initiator = e;
        this.$flags = i;
        if (e === this) this.Zt();
        let n = 0;
        let r;
        if (this.Ft.length) for (;n < this.Ft.length; ++n) this.Ft[n].stop();
        if (null !== this.children) for (n = 0; n < this.children.length; ++n) void this.children[n].deactivate(e, this, i);
        if (2 !== this.vmKind && null != this.qt.detaching) r = t.resolveAll(...this.qt.detaching.map(ns, this));
        if (this.Ut.hasDetaching) r = t.resolveAll(r, this.$t.detaching(this.$initiator, this.parent, this.$flags));
        if (I(r)) {
            this.Gt();
            e.Zt();
            r.then((() => {
                e.Jt();
            })).catch((t => {
                e.Xt(t);
            }));
        }
        if (null === e.head) e.head = this; else e.tail.next = this;
        e.tail = this;
        if (e !== this) return;
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
        const t = 2 | this.$flags;
        let e = 0;
        if (null !== this.bindings) for (;e < this.bindings.length; ++e) this.bindings[e].unbind();
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
        if (4 === (4 & t) && this.$initiator === this) this.dispose();
        this.state = 32 & this.state | 8;
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
            os = this.$resolve;
            this.$resolve = this.$reject = this.$promise = void 0;
            os();
            os = void 0;
        }
    }
    Xt(t) {
        if (void 0 !== this.$promise) {
            ls = this.$reject;
            this.$resolve = this.$reject = this.$promise = void 0;
            ls(t);
            ls = void 0;
        }
        if (this.$initiator !== this) this.parent.Xt(t);
    }
    zt() {
        ++this.Ot;
        if (this.$initiator !== this) this.parent.zt();
    }
    Yt() {
        if (0 === --this.Ot) {
            if (2 !== this.vmKind && null != this.qt.attached) hs = t.resolveAll(...this.qt.attached.map(is, this));
            if (this.Ut.hasAttached) hs = t.resolveAll(hs, this.$t.attached(this.$initiator, this.$flags));
            if (I(hs)) {
                this.Gt();
                hs.then((() => {
                    this.state = 2;
                    this.te();
                    if (this.$initiator !== this) this.parent.Yt();
                })).catch((t => {
                    this.Xt(t);
                }));
                hs = void 0;
                return;
            }
            hs = void 0;
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
            let e = this.$initiator.head;
            let s;
            while (null !== e) {
                if (e !== this) {
                    if (e.debug) e.logger.trace(`detach()`);
                    e.removeNodes();
                }
                if (2 !== e.vmKind && null != e.qt.unbinding) s = t.resolveAll(...e.qt.unbinding.map(rs, this));
                if (e.Ut.hasUnbinding) {
                    if (e.debug) e.logger.trace("unbinding()");
                    s = t.resolveAll(s, e.viewModel.unbinding(e.$initiator, e.parent, e.$flags));
                }
                if (I(s)) {
                    this.Gt();
                    this.ee();
                    s.then((() => {
                        this.se();
                    })).catch((t => {
                        this.Xt(t);
                    }));
                }
                s = void 0;
                e = e.next;
            }
            this.se();
        }
    }
    ee() {
        ++this.Nt;
    }
    se() {
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
            return Jt(this.$t.constructor).name === t;

          case 0:
            return ii(this.$t.constructor).name === t;

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
            Cs(t, Ks, this);
            Cs(t, this.definition.key, this);
        }
        this.host = t;
        this.mountTarget = 1;
        return this;
    }
    setShadowRoot(t) {
        if (0 === this.vmKind) {
            Cs(t, Ks, this);
            Cs(t, this.definition.key, this);
        }
        this.shadowRoot = t;
        this.mountTarget = 2;
        return this;
    }
    setLocation(t) {
        if (0 === this.vmKind) {
            Cs(t, Ks, this);
            Cs(t, this.definition.key, this);
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
        if (this.Ut.hasDispose) this.$t.dispose();
        if (null !== this.children) {
            this.children.forEach(Qe);
            this.children = null;
        }
        this.hostController = null;
        this.scope = null;
        this.nodes = null;
        this.location = null;
        this.viewFactory = null;
        if (null !== this.$t) {
            $e.delete(this.$t);
            this.$t = null;
        }
        this.$t = null;
        this.host = null;
        this.shadowRoot = null;
        this.container.disposeResolvers();
    }
    accept(t) {
        if (true === t(this)) return true;
        if (this.Ut.hasAccept && true === this.$t.accept(t)) return true;
        if (null !== this.children) {
            const {children: e} = this;
            for (let s = 0, i = e.length; s < i; ++s) if (true === e[s].accept(t)) return true;
        }
    }
}

function Me(t) {
    let e = t.$observers;
    if (void 0 === e) Reflect.defineProperty(t, "$observers", {
        enumerable: false,
        value: e = {}
    });
    return e;
}

function Fe(t, e, i, n) {
    const r = e.bindables;
    const o = C(r);
    const l = o.length;
    if (l > 0) {
        let e;
        let i;
        let h = 0;
        const a = Me(n);
        const c = t.container;
        const u = c.has(s.ICoercionConfiguration, true) ? c.get(s.ICoercionConfiguration) : null;
        for (;h < l; ++h) {
            e = o[h];
            if (void 0 === a[e]) {
                i = r[e];
                a[e] = new BindableObserver(n, e, i.callback, i.set, t, u);
            }
        }
    }
}

function Oe(e, s, i) {
    const n = s.childrenObservers;
    const r = C(n);
    const o = r.length;
    if (o > 0) {
        const t = Me(i);
        const s = [];
        let l;
        let h = 0;
        let a;
        for (;h < o; ++h) {
            l = r[h];
            if (null == t[l]) {
                a = n[l];
                s[s.length] = t[l] = new ChildrenObserver(e, i, l, a.callback, a.query, a.filter, a.map, a.options);
            }
        }
        return s;
    }
    return t.emptyArray;
}

const Ve = new Map;

const Ne = t => {
    let e = Ve.get(t);
    if (null == e) {
        e = new s.AccessScopeExpression(t, 0);
        Ve.set(t, e);
    }
    return e;
};

function je(t, e, i, n) {
    const r = e.get(s.IObserverLocator);
    const o = e.get(s.IExpressionParser);
    const l = i.watches;
    const h = 0 === t.vmKind ? t.scope : s.Scope.create(n, null, true);
    const a = l.length;
    let c;
    let u;
    let f;
    let d = 0;
    for (;a > d; ++d) {
        ({expression: c, callback: u} = l[d]);
        u = P(u) ? u : Reflect.get(n, u);
        if (!P(u)) throw b(`AUR0506:${String(u)}`);
        if (P(c)) t.addBinding(new ComputedWatcher(n, r, c, u, true)); else {
            f = E(c) ? o.parse(c, 16) : Ne(c);
            t.addBinding(new ExpressionWatcher(h, e, r, f, u));
        }
    }
}

function He(t) {
    return t instanceof Controller && 0 === t.vmKind;
}

function We(t) {
    return e.isObject(t) && ti(t.constructor);
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

const ze = {
    mode: "open"
};

exports.ViewModelKind = void 0;

(function(t) {
    t[t["customElement"] = 0] = "customElement";
    t[t["customAttribute"] = 1] = "customAttribute";
    t[t["synthetic"] = 2] = "synthetic";
})(exports.ViewModelKind || (exports.ViewModelKind = {}));

exports.State = void 0;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["activating"] = 1] = "activating";
    t[t["activated"] = 2] = "activated";
    t[t["deactivating"] = 4] = "deactivating";
    t[t["deactivated"] = 8] = "deactivated";
    t[t["released"] = 16] = "released";
    t[t["disposed"] = 32] = "disposed";
})(exports.State || (exports.State = {}));

function Ge(t) {
    const e = [];
    if (1 === (1 & t)) e.push("activating");
    if (2 === (2 & t)) e.push("activated");
    if (4 === (4 & t)) e.push("deactivating");
    if (8 === (8 & t)) e.push("deactivated");
    if (16 === (16 & t)) e.push("released");
    if (32 === (32 & t)) e.push("disposed");
    return 0 === e.length ? "none" : e.join("|");
}

const Xe = G("IController");

const Ke = G("IHydrationContext");

class HydrationContext {
    constructor(t, e, s) {
        this.instruction = e;
        this.parent = s;
        this.controller = t;
    }
}

function Qe(t) {
    t.dispose();
}

function Ye(t) {
    t.instance.created(this.$t, this);
}

function Ze(t) {
    t.instance.hydrating(this.$t, this);
}

function Je(t) {
    t.instance.hydrated(this.$t, this);
}

function ts(t) {
    return t.instance.binding(this.$t, this["$initiator"], this.parent, this["$flags"]);
}

function es(t) {
    return t.instance.bound(this.$t, this["$initiator"], this.parent, this["$flags"]);
}

function ss(t) {
    return t.instance.attaching(this.$t, this["$initiator"], this.parent, this["$flags"]);
}

function is(t) {
    return t.instance.attached(this.$t, this["$initiator"], this["$flags"]);
}

function ns(t) {
    return t.instance.detaching(this.$t, this["$initiator"], this.parent, this["$flags"]);
}

function rs(t) {
    return t.instance.unbinding(this.$t, this["$initiator"], this.parent, this["$flags"]);
}

let os;

let ls;

let hs;

const as = G("IAppRoot");

class AppRoot {
    constructor(e, s, i, n) {
        this.config = e;
        this.platform = s;
        this.container = i;
        this.controller = void 0;
        this.ie = void 0;
        this.host = e.host;
        n.prepare(this);
        Z(i, s.HTMLElement, Z(i, s.Element, Z(i, Bs, new t.InstanceProvider("ElementResolver", e.host))));
        this.ie = t.onResolve(this.ne("creating"), (() => {
            const s = e.component;
            const n = i.createChild();
            let r;
            if (ti(s)) r = this.container.get(s); else r = e.component;
            const o = {
                hydrate: false,
                projections: null
            };
            const l = this.controller = Controller.$el(n, r, this.host, o);
            l.hE(o, null);
            return t.onResolve(this.ne("hydrating"), (() => {
                l.hS(null);
                return t.onResolve(this.ne("hydrated"), (() => {
                    l.hC();
                    this.ie = void 0;
                }));
            }));
        }));
    }
    activate() {
        return t.onResolve(this.ie, (() => t.onResolve(this.ne("activating"), (() => t.onResolve(this.controller.activate(this.controller, null, 1, void 0), (() => this.ne("activated")))))));
    }
    deactivate() {
        return t.onResolve(this.ne("deactivating"), (() => t.onResolve(this.controller.deactivate(this.controller, null, 0), (() => this.ne("deactivated")))));
    }
    ne(e) {
        return t.resolveAll(...this.container.getAll(Mt).reduce(((t, s) => {
            if (s.slot === e) t.push(s.run());
            return t;
        }), []));
    }
    dispose() {
        this.controller?.dispose();
    }
}

const cs = "au-start";

const us = "au-end";

const fs = (t, e) => t.document.createElement(e);

const ds = (t, e) => t.document.createComment(e);

const ps = t => {
    const e = ds(t, us);
    e.$start = ds(t, cs);
    return e;
};

const xs = (t, e) => t.document.createTextNode(e);

const ms = (t, e, s) => t.insertBefore(e, s);

const gs = (t, e, s) => {
    if (null === t) return;
    const i = s.length;
    let n = 0;
    while (i > n) {
        t.insertBefore(s[n], e);
        ++n;
    }
};

const vs = t => t.previousSibling;

const ws = (t, e) => t.content.appendChild(e);

const bs = (t, e) => {
    const s = e.length;
    let i = 0;
    while (s > i) {
        t.content.appendChild(e[i]);
        ++i;
    }
};

const ys = t => {
    const e = t.previousSibling;
    let s;
    if (8 === e?.nodeType && "au-end" === e.textContent) {
        s = e;
        if (null == (s.$start = s.previousSibling)) throw ks();
        t.parentNode?.removeChild(t);
        return s;
    } else throw ks();
};

const ks = () => b(`AURxxxx`);

class Refs {}

function As(t, e) {
    return t.$au?.[e] ?? null;
}

function Cs(t, e, s) {
    var i;
    ((i = t).$au ?? (i.$au = new Refs))[e] = s;
}

const Bs = G("INode");

const Rs = G("IEventTarget", (t => t.cachedCallback((t => {
    if (t.has(as, true)) return t.get(as).host;
    return t.get(ue).document;
}))));

const Ss = G("IRenderLocation");

const Is = G("CssModules");

const Ts = new WeakMap;

function Ps(t) {
    if (Ts.has(t)) return Ts.get(t);
    let e = 0;
    let s = t.nextSibling;
    while (null !== s) {
        if (8 === s.nodeType) switch (s.textContent) {
          case "au-start":
            ++e;
            break;

          case "au-end":
            if (0 === e--) return s;
        }
        s = s.nextSibling;
    }
    if (null === t.parentNode && 11 === t.nodeType) {
        const e = ei(t);
        if (void 0 === e) return null;
        if (2 === e.mountTarget) return Ps(e.host);
    }
    return t.parentNode;
}

function Es(t, e) {
    if (void 0 !== t.platform && !(t instanceof t.platform.Node)) {
        const s = t.childNodes;
        for (let t = 0, i = s.length; t < i; ++t) Ts.set(s[t], e);
    } else Ts.set(t, e);
}

function Ls(t) {
    if (Ds(t)) return t;
    const e = t.ownerDocument.createComment("au-end");
    const s = e.$start = t.ownerDocument.createComment("au-start");
    const i = t.parentNode;
    if (null !== i) {
        i.replaceChild(e, t);
        i.insertBefore(s, e);
    }
    return e;
}

function Ds(t) {
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
        const s = e.querySelectorAll(".au");
        let i = 0;
        let n = s.length;
        let r;
        let o = this.t = Array(n);
        while (n > i) {
            r = s[i];
            if ("AU-M" === r.nodeName) o[i] = ys(r); else o[i] = r;
            ++i;
        }
        const l = e.childNodes;
        const h = this.childNodes = Array(n = l.length);
        i = 0;
        while (n > i) {
            h[i] = l[i];
            ++i;
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
                let s = this.re;
                let i;
                const n = this.oe;
                while (null != s) {
                    i = s.nextSibling;
                    e.insertBefore(s, t);
                    if (s === n) break;
                    s = i;
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
            let s;
            const i = this.oe;
            while (null != e) {
                s = e.nextSibling;
                t.appendChild(e);
                if (e === i) break;
                e = s;
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
            let s;
            let i = this.re;
            while (null !== i) {
                s = i.nextSibling;
                t.appendChild(i);
                if (i === e) break;
                i = s;
            }
        }
    }
    addToLinked() {
        const t = this.ref;
        const e = t.parentNode;
        if (this.le) {
            let s = this.re;
            let i;
            const n = this.oe;
            while (null != s) {
                i = s.nextSibling;
                e.insertBefore(s, t);
                if (s === n) break;
                s = i;
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
        if (Ds(t)) this.ref = t; else {
            this.next = t;
            this.ae();
        }
    }
    ae() {
        if (void 0 !== this.next) this.ref = this.next.firstChild; else this.ref = void 0;
    }
}

const _s = G("IWindow", (t => t.callback((t => t.get(ue).window))));

const qs = G("ILocation", (t => t.callback((t => t.get(_s).location))));

const Us = G("IHistory", (t => t.callback((t => t.get(_s).history))));

function $s(t) {
    return function(e) {
        return Js(t, e);
    };
}

function Ms(t) {
    if (void 0 === t) return function(t) {
        Zs(t, "shadowOptions", {
            mode: "open"
        });
    };
    if (!P(t)) return function(e) {
        Zs(e, "shadowOptions", t);
    };
    Zs(t, "shadowOptions", {
        mode: "open"
    });
}

function Fs(t) {
    if (void 0 === t) return function(t) {
        Os(t);
    };
    Os(t);
}

function Os(t) {
    const e = l(Ks, t);
    if (void 0 === e) {
        Zs(t, "containerless", true);
        return;
    }
    e.containerless = true;
}

function Vs(t) {
    if (void 0 === t) return function(t) {
        Zs(t, "isStrictBinding", true);
    };
    Zs(t, "isStrictBinding", true);
}

const Ns = new WeakMap;

class CustomElementDefinition {
    get type() {
        return 1;
    }
    constructor(t, e, s, i, n, r, o, l, h, a, c, u, f, d, p, x, m, g, v, w, b) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
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
        this.containerless = p;
        this.isStrictBinding = x;
        this.shadowOptions = m;
        this.hasSlots = g;
        this.enhance = v;
        this.watches = w;
        this.processContent = b;
    }
    static create(e, s = null) {
        if (null === s) {
            const i = e;
            if (E(i)) throw b(`AUR0761:${e}`);
            const n = t.fromDefinitionOrDefault("name", i, Ys);
            if (P(i.Type)) s = i.Type; else s = ri(t.pascalCase(n));
            return new CustomElementDefinition(s, n, t.mergeArrays(i.aliases), t.fromDefinitionOrDefault("key", i, (() => Qs(n))), t.fromDefinitionOrDefault("cache", i, Hs), t.fromDefinitionOrDefault("capture", i, zs), t.fromDefinitionOrDefault("template", i, Ws), t.mergeArrays(i.instructions), t.mergeArrays(i.dependencies), t.fromDefinitionOrDefault("injectable", i, Ws), t.fromDefinitionOrDefault("needsCompile", i, Gs), t.mergeArrays(i.surrogates), O.from(s, i.bindables), ne.from(i.childrenObservers), t.fromDefinitionOrDefault("containerless", i, zs), t.fromDefinitionOrDefault("isStrictBinding", i, zs), t.fromDefinitionOrDefault("shadowOptions", i, Ws), t.fromDefinitionOrDefault("hasSlots", i, zs), t.fromDefinitionOrDefault("enhance", i, zs), t.fromDefinitionOrDefault("watches", i, Xs), t.fromAnnotationOrTypeOrDefault("processContent", s, Ws));
        }
        if (E(e)) return new CustomElementDefinition(s, e, t.mergeArrays(si(s, "aliases"), s.aliases), Qs(e), t.fromAnnotationOrTypeOrDefault("cache", s, Hs), t.fromAnnotationOrTypeOrDefault("capture", s, zs), t.fromAnnotationOrTypeOrDefault("template", s, Ws), t.mergeArrays(si(s, "instructions"), s.instructions), t.mergeArrays(si(s, "dependencies"), s.dependencies), t.fromAnnotationOrTypeOrDefault("injectable", s, Ws), t.fromAnnotationOrTypeOrDefault("needsCompile", s, Gs), t.mergeArrays(si(s, "surrogates"), s.surrogates), O.from(s, ...O.getAll(s), si(s, "bindables"), s.bindables), ne.from(...ne.getAll(s), si(s, "childrenObservers"), s.childrenObservers), t.fromAnnotationOrTypeOrDefault("containerless", s, zs), t.fromAnnotationOrTypeOrDefault("isStrictBinding", s, zs), t.fromAnnotationOrTypeOrDefault("shadowOptions", s, Ws), t.fromAnnotationOrTypeOrDefault("hasSlots", s, zs), t.fromAnnotationOrTypeOrDefault("enhance", s, zs), t.mergeArrays(Ht.getAnnotation(s), s.watches), t.fromAnnotationOrTypeOrDefault("processContent", s, Ws));
        const i = t.fromDefinitionOrDefault("name", e, Ys);
        return new CustomElementDefinition(s, i, t.mergeArrays(si(s, "aliases"), e.aliases, s.aliases), Qs(i), t.fromAnnotationOrDefinitionOrTypeOrDefault("cache", e, s, Hs), t.fromAnnotationOrDefinitionOrTypeOrDefault("capture", e, s, zs), t.fromAnnotationOrDefinitionOrTypeOrDefault("template", e, s, Ws), t.mergeArrays(si(s, "instructions"), e.instructions, s.instructions), t.mergeArrays(si(s, "dependencies"), e.dependencies, s.dependencies), t.fromAnnotationOrDefinitionOrTypeOrDefault("injectable", e, s, Ws), t.fromAnnotationOrDefinitionOrTypeOrDefault("needsCompile", e, s, Gs), t.mergeArrays(si(s, "surrogates"), e.surrogates, s.surrogates), O.from(s, ...O.getAll(s), si(s, "bindables"), s.bindables, e.bindables), ne.from(...ne.getAll(s), si(s, "childrenObservers"), s.childrenObservers, e.childrenObservers), t.fromAnnotationOrDefinitionOrTypeOrDefault("containerless", e, s, zs), t.fromAnnotationOrDefinitionOrTypeOrDefault("isStrictBinding", e, s, zs), t.fromAnnotationOrDefinitionOrTypeOrDefault("shadowOptions", e, s, Ws), t.fromAnnotationOrDefinitionOrTypeOrDefault("hasSlots", e, s, zs), t.fromAnnotationOrDefinitionOrTypeOrDefault("enhance", e, s, zs), t.mergeArrays(e.watches, Ht.getAnnotation(s), s.watches), t.fromAnnotationOrDefinitionOrTypeOrDefault("processContent", e, s, Ws));
    }
    static getOrCreate(t) {
        if (t instanceof CustomElementDefinition) return t;
        if (Ns.has(t)) return Ns.get(t);
        const e = CustomElementDefinition.create(t);
        Ns.set(t, e);
        a(Ks, e, e.Type);
        return e;
    }
    register(t) {
        const {Type: e, key: s, aliases: i} = this;
        if (!t.has(s, false)) {
            Y(s, e).register(t);
            K(s, e).register(t);
            tt(i, oi, s, t);
        }
    }
}

const js = {
    name: void 0,
    searchParents: false,
    optional: false
};

const Hs = () => 0;

const Ws = () => null;

const zs = () => false;

const Gs = () => true;

const Xs = () => t.emptyArray;

const Ks = d("custom-element");

const Qs = t => `${Ks}:${t}`;

const Ys = (() => {
    let t = 0;
    return () => `unnamed-${++t}`;
})();

const Zs = (t, e, s) => {
    a(f(e), s, t);
};

const Js = (t, e) => {
    const s = CustomElementDefinition.create(t, e);
    a(Ks, s, s.Type);
    a(Ks, s, s);
    p(s.Type, Ks);
    return s.Type;
};

const ti = t => P(t) && h(Ks, t);

const ei = (t, e = js) => {
    if (void 0 === e.name && true !== e.searchParents) {
        const s = As(t, Ks);
        if (null === s) {
            if (true === e.optional) return null;
            throw b(`AUR0762`);
        }
        return s;
    }
    if (void 0 !== e.name) {
        if (true !== e.searchParents) {
            const s = As(t, Ks);
            if (null === s) throw b(`AUR0763`);
            if (s.is(e.name)) return s;
            return;
        }
        let s = t;
        let i = false;
        while (null !== s) {
            const t = As(s, Ks);
            if (null !== t) {
                i = true;
                if (t.is(e.name)) return t;
            }
            s = Ps(s);
        }
        if (i) return;
        throw b(`AUR0764`);
    }
    let s = t;
    while (null !== s) {
        const t = As(s, Ks);
        if (null !== t) return t;
        s = Ps(s);
    }
    throw b(`AUR0765`);
};

const si = (t, e) => l(f(e), t);

const ii = t => {
    const e = l(Ks, t);
    if (void 0 === e) throw b(`AUR0760:${t.name}`);
    return e;
};

const ni = () => {
    const e = function(s, i, n) {
        const r = t.DI.getOrCreateAnnotationParamTypes(s);
        r[n] = e;
        return s;
    };
    e.register = function(t) {
        return {
            resolve(t, s) {
                if (s.has(e, true)) return s.get(e); else return null;
            }
        };
    };
    return e;
};

const ri = function() {
    const t = {
        value: "",
        writable: false,
        enumerable: false,
        configurable: true
    };
    const e = {};
    return function(s, i = e) {
        const n = class {};
        t.value = s;
        Reflect.defineProperty(n, "name", t);
        if (i !== e) A(n.prototype, i);
        return n;
    };
}();

const oi = k({
    name: Ks,
    keyFrom: Qs,
    isType: ti,
    for: ei,
    define: Js,
    getDefinition: ii,
    annotate: Zs,
    getAnnotation: si,
    generateName: Ys,
    createInjectable: ni,
    generateType: ri
});

const li = f("processContent");

function hi(t) {
    return void 0 === t ? function(t, e, s) {
        a(li, ai(t, e), t);
    } : function(e) {
        t = ai(e, t);
        const s = l(Ks, e);
        if (void 0 !== s) s.processContent = t; else a(li, t, e);
        return e;
    };
}

function ai(t, e) {
    if (E(e)) e = t[e];
    if (!P(e)) throw b(`AUR0766:${typeof e}`);
    return e;
}

function ci(t) {
    return function(e) {
        const s = P(t) ? t : true;
        Zs(e, "capture", s);
        if (ti(e)) ii(e).capture = s;
    };
}

const ui = G("IProjections");

const fi = G("IAuSlotsInfo");

class AuSlotsInfo {
    constructor(t) {
        this.projectedSlots = t;
    }
}

exports.InstructionType = void 0;

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
})(exports.InstructionType || (exports.InstructionType = {}));

const di = G("Instruction");

function pi(t) {
    const e = t.type;
    return E(e) && 2 === e.length;
}

class InterpolationInstruction {
    constructor(t, e) {
        this.from = t;
        this.to = e;
        this.type = "rf";
    }
}

class PropertyBindingInstruction {
    constructor(t, e, s) {
        this.from = t;
        this.to = e;
        this.mode = s;
        this.type = "rg";
    }
}

class IteratorBindingInstruction {
    constructor(t, e, s) {
        this.forOf = t;
        this.to = e;
        this.props = s;
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
    constructor(t, e, s) {
        this.value = t;
        this.to = e;
        this.command = s;
        this.type = "rl";
    }
}

class HydrateElementInstruction {
    constructor(t, e, s, i, n, r) {
        this.res = t;
        this.alias = e;
        this.props = s;
        this.projections = i;
        this.containerless = n;
        this.captures = r;
        this.type = "ra";
        this.auSlot = null;
    }
}

class HydrateAttributeInstruction {
    constructor(t, e, s) {
        this.res = t;
        this.alias = e;
        this.props = s;
        this.type = "rb";
    }
}

class HydrateTemplateController {
    constructor(t, e, s, i) {
        this.def = t;
        this.res = e;
        this.alias = s;
        this.props = i;
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
    constructor(t, e, s, i) {
        this.from = t;
        this.to = e;
        this.preventDefault = s;
        this.capture = i;
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
    constructor(t, e, s) {
        this.attr = t;
        this.from = e;
        this.to = s;
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

const xi = G("ITemplateCompiler");

const mi = G("IRenderer");

function gi(t) {
    return function e(s) {
        s.register = function(t) {
            X(mi, this).register(t);
        };
        L(s.prototype, "target", {
            configurable: true,
            get: function() {
                return t;
            }
        });
        return s;
    };
}

function vi(t, e, s) {
    if (E(e)) return t.parse(e, s);
    return e;
}

function wi(t) {
    if (null != t.viewModel) return t.viewModel;
    return t;
}

function bi(t, e) {
    if ("element" === e) return t;
    switch (e) {
      case "controller":
        return ei(t);

      case "view":
        throw b(`AUR0750`);

      case "view-model":
        return ei(t).viewModel;

      default:
        {
            const s = Yt(t, e);
            if (void 0 !== s) return s.viewModel;
            const i = ei(t, {
                name: e
            });
            if (void 0 === i) throw b(`AUR0751:${e}`);
            return i.viewModel;
        }
    }
}

exports.SetPropertyRenderer = class SetPropertyRenderer {
    render(t, e, s) {
        const i = wi(e);
        if (void 0 !== i.$observers?.[s.to]) i.$observers[s.to].setValue(s.value); else i[s.to] = s.value;
    }
};

exports.SetPropertyRenderer = r([ gi("re") ], exports.SetPropertyRenderer);

exports.CustomElementRenderer = class CustomElementRenderer {
    static get inject() {
        return [ _e ];
    }
    constructor(t) {
        this.r = t;
    }
    render(e, s, i, n, r, o) {
        let l;
        let h;
        let a;
        let c;
        const u = i.res;
        const f = i.projections;
        const d = e.container;
        switch (typeof u) {
          case "string":
            l = d.find(oi, u);
            if (null == l) throw b(`AUR0752:${u}@${e["name"]}`);
            break;

          default:
            l = u;
        }
        const p = i.containerless || l.containerless;
        const x = p ? Ls(s) : null;
        const m = Si(n, e, s, i, x, null == f ? void 0 : new AuSlotsInfo(B(f)));
        h = l.Type;
        a = m.invoke(h);
        Z(m, h, new t.InstanceProvider(l.key, a));
        c = Controller.$el(m, a, s, i, l, x);
        Cs(s, l.key, c);
        const g = this.r.renderers;
        const v = i.props;
        const w = v.length;
        let y = 0;
        let k;
        while (w > y) {
            k = v[y];
            g[k.type].render(e, c, k, n, r, o);
            ++y;
        }
        e.addChild(c);
    }
};

exports.CustomElementRenderer = r([ gi("ra") ], exports.CustomElementRenderer);

exports.CustomAttributeRenderer = class CustomAttributeRenderer {
    static get inject() {
        return [ _e ];
    }
    constructor(t) {
        this.r = t;
    }
    render(t, e, s, i, n, r) {
        let o = t.container;
        let l;
        switch (typeof s.res) {
          case "string":
            l = o.find(te, s.res);
            if (null == l) throw b(`AUR0753:${s.res}@${t["name"]}`);
            break;

          default:
            l = s.res;
        }
        const h = Ii(i, l, t, e, s, void 0, void 0);
        const a = Controller.$attr(h.ctn, h.vm, e, l);
        Cs(e, l.key, a);
        const c = this.r.renderers;
        const u = s.props;
        const f = u.length;
        let d = 0;
        let p;
        while (f > d) {
            p = u[d];
            c[p.type].render(t, a, p, i, n, r);
            ++d;
        }
        t.addChild(a);
    }
};

exports.CustomAttributeRenderer = r([ gi("rb") ], exports.CustomAttributeRenderer);

exports.TemplateControllerRenderer = class TemplateControllerRenderer {
    static get inject() {
        return [ _e, ue ];
    }
    constructor(t, e) {
        this.r = t;
        this.p = e;
    }
    render(t, e, s, i, n, r) {
        let o = t.container;
        let l;
        switch (typeof s.res) {
          case "string":
            l = o.find(te, s.res);
            if (null == l) throw b(`AUR0754:${s.res}@${t["name"]}`);
            break;

          default:
            l = s.res;
        }
        const h = this.r.getViewFactory(s.def, o);
        const a = Ls(e);
        const c = Ii(this.p, l, t, e, s, h, a);
        const u = Controller.$attr(c.ctn, c.vm, e, l);
        Cs(a, l.key, u);
        c.vm.link?.(t, u, e, s);
        const f = this.r.renderers;
        const d = s.props;
        const p = d.length;
        let x = 0;
        let m;
        while (p > x) {
            m = d[x];
            f[m.type].render(t, u, m, i, n, r);
            ++x;
        }
        t.addChild(u);
    }
};

exports.TemplateControllerRenderer = r([ gi("rc") ], exports.TemplateControllerRenderer);

exports.LetElementRenderer = class LetElementRenderer {
    render(t, e, s, i, n, r) {
        e.remove();
        const o = s.instructions;
        const l = s.toBindingContext;
        const h = t.container;
        const a = o.length;
        let c;
        let u;
        let f = 0;
        while (a > f) {
            c = o[f];
            u = vi(n, c.from, 16);
            t.addBinding(new LetBinding(h, r, u, c.to, l));
            ++f;
        }
    }
};

exports.LetElementRenderer = r([ gi("rd") ], exports.LetElementRenderer);

exports.RefBindingRenderer = class RefBindingRenderer {
    render(t, e, s, i, n) {
        t.addBinding(new RefBinding(t.container, vi(n, s.from, 16), bi(e, s.to)));
    }
};

exports.RefBindingRenderer = r([ gi("rj") ], exports.RefBindingRenderer);

exports.InterpolationBindingRenderer = class InterpolationBindingRenderer {
    render(t, e, s, i, n, r) {
        t.addBinding(new InterpolationBinding(t, t.container, r, i.domWriteQueue, vi(n, s.from, 1), wi(e), s.to, 2));
    }
};

exports.InterpolationBindingRenderer = r([ gi("rf") ], exports.InterpolationBindingRenderer);

exports.PropertyBindingRenderer = class PropertyBindingRenderer {
    render(t, e, s, i, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, i.domWriteQueue, vi(n, s.from, 16), wi(e), s.to, s.mode));
    }
};

exports.PropertyBindingRenderer = r([ gi("rg") ], exports.PropertyBindingRenderer);

exports.IteratorBindingRenderer = class IteratorBindingRenderer {
    render(t, e, s, i, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, i.domWriteQueue, vi(n, s.forOf, 2), wi(e), s.to, 2));
    }
};

exports.IteratorBindingRenderer = r([ gi("rk") ], exports.IteratorBindingRenderer);

exports.TextBindingRenderer = class TextBindingRenderer {
    render(t, e, s, i, n, r) {
        t.addBinding(new ContentBinding(t, t.container, r, i.domWriteQueue, i, vi(n, s.from, 16), ms(e.parentNode, xs(i, ""), e), s.strict));
    }
};

exports.TextBindingRenderer = r([ gi("ha") ], exports.TextBindingRenderer);

exports.ListenerBindingRenderer = class ListenerBindingRenderer {
    render(t, e, s, i, n) {
        t.addBinding(new ListenerBinding(t.container, vi(n, s.from, 8), e, s.to, new ListenerBindingOptions(s.preventDefault, s.capture)));
    }
};

exports.ListenerBindingRenderer = r([ gi("hb") ], exports.ListenerBindingRenderer);

exports.SetAttributeRenderer = class SetAttributeRenderer {
    render(t, e, s) {
        e.setAttribute(s.to, s.value);
    }
};

exports.SetAttributeRenderer = r([ gi("he") ], exports.SetAttributeRenderer);

exports.SetClassAttributeRenderer = class SetClassAttributeRenderer {
    render(t, e, s) {
        yi(e.classList, s.value);
    }
};

exports.SetClassAttributeRenderer = r([ gi("hf") ], exports.SetClassAttributeRenderer);

exports.SetStyleAttributeRenderer = class SetStyleAttributeRenderer {
    render(t, e, s) {
        e.style.cssText += s.value;
    }
};

exports.SetStyleAttributeRenderer = r([ gi("hg") ], exports.SetStyleAttributeRenderer);

exports.StylePropertyBindingRenderer = class StylePropertyBindingRenderer {
    render(t, e, s, i, n, r) {
        t.addBinding(new PropertyBinding(t, t.container, r, i.domWriteQueue, vi(n, s.from, 16), e.style, s.to, 2));
    }
};

exports.StylePropertyBindingRenderer = r([ gi("hd") ], exports.StylePropertyBindingRenderer);

exports.AttributeBindingRenderer = class AttributeBindingRenderer {
    render(t, e, s, i, n, r) {
        const o = t.container;
        const l = o.has(Is, false) ? o.get(Is) : null;
        t.addBinding(new AttributeBinding(t, o, r, i.domWriteQueue, vi(n, s.from, 16), e, s.attr, null == l ? s.to : s.to.split(/\s/g).map((t => l[t] ?? t)).join(" "), 2));
    }
};

exports.AttributeBindingRenderer = r([ gi("hc") ], exports.AttributeBindingRenderer);

exports.SpreadRenderer = class SpreadRenderer {
    static get inject() {
        return [ xi, _e ];
    }
    constructor(t, e) {
        this.ce = t;
        this.r = e;
    }
    render(e, s, i, n, r, o) {
        const l = e.container;
        const h = l.get(Ke);
        const a = this.r.renderers;
        const c = t => {
            let e = t;
            let s = h;
            while (null != s && e > 0) {
                s = s.parent;
                --e;
            }
            if (null == s) throw b("No scope context for spread binding.");
            return s;
        };
        const u = i => {
            const l = c(i);
            const h = ki(l);
            const f = this.ce.compileSpread(l.controller.definition, l.instruction?.captures ?? t.emptyArray, l.controller.container, s);
            let d;
            for (d of f) switch (d.type) {
              case "hs":
                u(i + 1);
                break;

              case "hp":
                a[d.instructions.type].render(h, ei(s), d.instructions, n, r, o);
                break;

              default:
                a[d.type].render(h, s, d, n, r, o);
            }
            e.addBinding(h);
        };
        u(0);
    }
};

exports.SpreadRenderer = r([ gi("hs") ], exports.SpreadRenderer);

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
        if (null == e) throw b("Invalid spreading. Context scope is null/undefined");
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
        if (1 !== t.vmKind) throw b("Spread binding does not support spreading custom attributes/template controllers");
        this.ctrl.addChild(t);
    }
    limit() {
        throw b("not implemented");
    }
    useScope() {
        throw b("not implemented");
    }
}

function yi(t, e) {
    const s = e.length;
    let i = 0;
    for (let n = 0; n < s; ++n) if (32 === e.charCodeAt(n)) {
        if (n !== i) t.add(e.slice(i, n));
        i = n + 1;
    } else if (n + 1 === s) t.add(e.slice(i));
}

const ki = t => new SpreadBinding([], t);

const Ai = "IController";

const Ci = "IInstruction";

const Bi = "IRenderLocation";

const Ri = "IAuSlotsInfo";

function Si(e, s, i, n, r, o) {
    const l = s.container.createChild();
    Z(l, e.HTMLElement, Z(l, e.Element, Z(l, Bs, new t.InstanceProvider("ElementResolver", i))));
    Z(l, Xe, new t.InstanceProvider(Ai, s));
    Z(l, di, new t.InstanceProvider(Ci, n));
    Z(l, Ss, null == r ? Ti : new RenderLocationProvider(r));
    Z(l, De, Pi);
    Z(l, fi, null == o ? Ei : new t.InstanceProvider(Ri, o));
    return l;
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
        if (null === t) throw b(`AUR7055`);
        if (!E(t.name) || 0 === t.name.length) throw b(`AUR0756`);
        return t;
    }
}

function Ii(e, s, i, n, r, o, l, h) {
    const a = i.container.createChild();
    Z(a, e.HTMLElement, Z(a, e.Element, Z(a, Bs, new t.InstanceProvider("ElementResolver", n))));
    i = i instanceof Controller ? i : i.ctrl;
    Z(a, Xe, new t.InstanceProvider(Ai, i));
    Z(a, di, new t.InstanceProvider(Ci, r));
    Z(a, Ss, null == l ? Ti : new t.InstanceProvider(Bi, l));
    Z(a, De, null == o ? Pi : new ViewFactoryProvider(o));
    Z(a, fi, null == h ? Ei : new t.InstanceProvider(Ri, h));
    return {
        vm: a.invoke(s.Type),
        ctn: a
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

const Ti = new RenderLocationProvider(null);

const Pi = new ViewFactoryProvider(null);

const Ei = new t.InstanceProvider(Ri, new AuSlotsInfo(t.emptyArray));

exports.CommandType = void 0;

(function(t) {
    t[t["None"] = 0] = "None";
    t[t["IgnoreAttr"] = 1] = "IgnoreAttr";
})(exports.CommandType || (exports.CommandType = {}));

function Li(t) {
    return function(e) {
        return Ui.define(t, e);
    };
}

class BindingCommandDefinition {
    constructor(t, e, s, i, n) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = i;
        this.type = n;
    }
    static create(e, s) {
        let i;
        let n;
        if (E(e)) {
            i = e;
            n = {
                name: i
            };
        } else {
            i = e.name;
            n = e;
        }
        return new BindingCommandDefinition(s, t.firstDefined(qi(s, "name"), i), t.mergeArrays(qi(s, "aliases"), n.aliases, s.aliases), _i(i), t.firstDefined(qi(s, "type"), n.type, s.type, null));
    }
    register(t) {
        const {Type: e, key: s, aliases: i} = this;
        X(s, e).register(t);
        K(s, e).register(t);
        tt(i, Ui, s, t);
    }
}

const Di = d("binding-command");

const _i = t => `${Di}:${t}`;

const qi = (t, e) => l(f(e), t);

const Ui = k({
    name: Di,
    keyFrom: _i,
    define(t, e) {
        const s = BindingCommandDefinition.create(t, e);
        a(Di, s, s.Type);
        a(Di, s, s);
        p(e, Di);
        return s.Type;
    },
    getAnnotation: qi
});

exports.OneTimeBindingCommand = class OneTimeBindingCommand {
    get type() {
        return 0;
    }
    build(e, s, i) {
        const n = e.attr;
        let r = n.target;
        let o = e.attr.rawValue;
        if (null == e.bindable) r = i.map(e.node, r) ?? t.camelCase(r); else {
            if ("" === o && 1 === e.def.type) o = t.camelCase(r);
            r = e.bindable.property;
        }
        return new PropertyBindingInstruction(s.parse(o, 16), r, 1);
    }
};

exports.OneTimeBindingCommand = r([ Li("one-time") ], exports.OneTimeBindingCommand);

exports.ToViewBindingCommand = class ToViewBindingCommand {
    get type() {
        return 0;
    }
    build(e, s, i) {
        const n = e.attr;
        let r = n.target;
        let o = e.attr.rawValue;
        if (null == e.bindable) r = i.map(e.node, r) ?? t.camelCase(r); else {
            if ("" === o && 1 === e.def.type) o = t.camelCase(r);
            r = e.bindable.property;
        }
        return new PropertyBindingInstruction(s.parse(o, 16), r, 2);
    }
};

exports.ToViewBindingCommand = r([ Li("to-view") ], exports.ToViewBindingCommand);

exports.FromViewBindingCommand = class FromViewBindingCommand {
    get type() {
        return 0;
    }
    build(e, s, i) {
        const n = e.attr;
        let r = n.target;
        let o = n.rawValue;
        if (null == e.bindable) r = i.map(e.node, r) ?? t.camelCase(r); else {
            if ("" === o && 1 === e.def.type) o = t.camelCase(r);
            r = e.bindable.property;
        }
        return new PropertyBindingInstruction(s.parse(o, 16), r, 4);
    }
};

exports.FromViewBindingCommand = r([ Li("from-view") ], exports.FromViewBindingCommand);

exports.TwoWayBindingCommand = class TwoWayBindingCommand {
    get type() {
        return 0;
    }
    build(e, s, i) {
        const n = e.attr;
        let r = n.target;
        let o = n.rawValue;
        if (null == e.bindable) r = i.map(e.node, r) ?? t.camelCase(r); else {
            if ("" === o && 1 === e.def.type) o = t.camelCase(r);
            r = e.bindable.property;
        }
        return new PropertyBindingInstruction(s.parse(o, 16), r, 6);
    }
};

exports.TwoWayBindingCommand = r([ Li("two-way") ], exports.TwoWayBindingCommand);

exports.DefaultBindingCommand = class DefaultBindingCommand {
    get type() {
        return 0;
    }
    build(e, s, i) {
        const n = e.attr;
        const r = e.bindable;
        let o;
        let l;
        let h = n.target;
        let a = n.rawValue;
        if (null == r) {
            l = i.isTwoWay(e.node, h) ? 6 : 2;
            h = i.map(e.node, h) ?? t.camelCase(h);
        } else {
            if ("" === a && 1 === e.def.type) a = t.camelCase(h);
            o = e.def.defaultBindingMode;
            l = 8 === r.mode || null == r.mode ? null == o || 8 === o ? 2 : o : r.mode;
            h = r.property;
        }
        return new PropertyBindingInstruction(s.parse(a, 16), h, l);
    }
};

exports.DefaultBindingCommand = r([ Li("bind") ], exports.DefaultBindingCommand);

exports.ForBindingCommand = class ForBindingCommand {
    get type() {
        return 0;
    }
    static get inject() {
        return [ rt ];
    }
    constructor(t) {
        this.de = t;
    }
    build(e, s) {
        const i = null === e.bindable ? t.camelCase(e.attr.target) : e.bindable.property;
        const n = s.parse(e.attr.rawValue, 2);
        let r = t.emptyArray;
        if (n.semiIdx > -1) {
            const t = e.attr.rawValue.slice(n.semiIdx + 1);
            const s = t.indexOf(":");
            if (s > -1) {
                const e = t.slice(0, s).trim();
                const i = t.slice(s + 1).trim();
                const n = this.de.parse(e, i);
                r = [ new MultiAttrInstruction(i, n.target, n.command) ];
            }
        }
        return new IteratorBindingInstruction(n, i, r);
    }
};

exports.ForBindingCommand = r([ Li("for") ], exports.ForBindingCommand);

exports.TriggerBindingCommand = class TriggerBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new ListenerBindingInstruction(e.parse(t.attr.rawValue, 8), t.attr.target, true, false);
    }
};

exports.TriggerBindingCommand = r([ Li("trigger") ], exports.TriggerBindingCommand);

exports.CaptureBindingCommand = class CaptureBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new ListenerBindingInstruction(e.parse(t.attr.rawValue, 8), t.attr.target, false, true);
    }
};

exports.CaptureBindingCommand = r([ Li("capture") ], exports.CaptureBindingCommand);

exports.AttrBindingCommand = class AttrBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new AttributeBindingInstruction(t.attr.target, e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

exports.AttrBindingCommand = r([ Li("attr") ], exports.AttrBindingCommand);

exports.StyleBindingCommand = class StyleBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new AttributeBindingInstruction("style", e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

exports.StyleBindingCommand = r([ Li("style") ], exports.StyleBindingCommand);

exports.ClassBindingCommand = class ClassBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new AttributeBindingInstruction("class", e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

exports.ClassBindingCommand = r([ Li("class") ], exports.ClassBindingCommand);

let $i = class RefBindingCommand {
    get type() {
        return 1;
    }
    build(t, e) {
        return new RefBindingInstruction(e.parse(t.attr.rawValue, 16), t.attr.target);
    }
};

$i = r([ Li("ref") ], $i);

let Mi = class SpreadBindingCommand {
    get type() {
        return 1;
    }
    build(t) {
        return new SpreadBindingInstruction;
    }
};

Mi = r([ Li("...$attrs") ], Mi);

const Fi = G("ISVGAnalyzer", (t => t.singleton(NoopSVGAnalyzer)));

const Oi = t => {
    const e = w();
    t = E(t) ? t.split(" ") : t;
    let s;
    for (s of t) e[s] = true;
    return e;
};

class NoopSVGAnalyzer {
    isStandardSvgAttribute(t, e) {
        return false;
    }
}

class SVGAnalyzer {
    static register(t) {
        return X(Fi, this).register(t);
    }
    constructor(t) {
        this.pe = A(w(), {
            a: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage target transform xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            altGlyph: Oi("class dx dy externalResourcesRequired format glyphRef id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            altglyph: w(),
            altGlyphDef: Oi("id xml:base xml:lang xml:space"),
            altglyphdef: w(),
            altGlyphItem: Oi("id xml:base xml:lang xml:space"),
            altglyphitem: w(),
            animate: Oi("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateColor: Oi("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateMotion: Oi("accumulate additive begin by calcMode dur end externalResourcesRequired fill from id keyPoints keySplines keyTimes max min onbegin onend onload onrepeat origin path repeatCount repeatDur requiredExtensions requiredFeatures restart rotate systemLanguage to values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            animateTransform: Oi("accumulate additive attributeName attributeType begin by calcMode dur end externalResourcesRequired fill from id keySplines keyTimes max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to type values xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            circle: Oi("class cx cy externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup r requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            clipPath: Oi("class clipPathUnits externalResourcesRequired id requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            "color-profile": Oi("id local name rendering-intent xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            cursor: Oi("externalResourcesRequired id requiredExtensions requiredFeatures systemLanguage x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            defs: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            desc: Oi("class id style xml:base xml:lang xml:space"),
            ellipse: Oi("class cx cy externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rx ry style systemLanguage transform xml:base xml:lang xml:space"),
            feBlend: Oi("class height id in in2 mode result style width x xml:base xml:lang xml:space y"),
            feColorMatrix: Oi("class height id in result style type values width x xml:base xml:lang xml:space y"),
            feComponentTransfer: Oi("class height id in result style width x xml:base xml:lang xml:space y"),
            feComposite: Oi("class height id in in2 k1 k2 k3 k4 operator result style width x xml:base xml:lang xml:space y"),
            feConvolveMatrix: Oi("bias class divisor edgeMode height id in kernelMatrix kernelUnitLength order preserveAlpha result style targetX targetY width x xml:base xml:lang xml:space y"),
            feDiffuseLighting: Oi("class diffuseConstant height id in kernelUnitLength result style surfaceScale width x xml:base xml:lang xml:space y"),
            feDisplacementMap: Oi("class height id in in2 result scale style width x xChannelSelector xml:base xml:lang xml:space y yChannelSelector"),
            feDistantLight: Oi("azimuth elevation id xml:base xml:lang xml:space"),
            feFlood: Oi("class height id result style width x xml:base xml:lang xml:space y"),
            feFuncA: Oi("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncB: Oi("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncG: Oi("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feFuncR: Oi("amplitude exponent id intercept offset slope tableValues type xml:base xml:lang xml:space"),
            feGaussianBlur: Oi("class height id in result stdDeviation style width x xml:base xml:lang xml:space y"),
            feImage: Oi("class externalResourcesRequired height id preserveAspectRatio result style width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            feMerge: Oi("class height id result style width x xml:base xml:lang xml:space y"),
            feMergeNode: Oi("id xml:base xml:lang xml:space"),
            feMorphology: Oi("class height id in operator radius result style width x xml:base xml:lang xml:space y"),
            feOffset: Oi("class dx dy height id in result style width x xml:base xml:lang xml:space y"),
            fePointLight: Oi("id x xml:base xml:lang xml:space y z"),
            feSpecularLighting: Oi("class height id in kernelUnitLength result specularConstant specularExponent style surfaceScale width x xml:base xml:lang xml:space y"),
            feSpotLight: Oi("id limitingConeAngle pointsAtX pointsAtY pointsAtZ specularExponent x xml:base xml:lang xml:space y z"),
            feTile: Oi("class height id in result style width x xml:base xml:lang xml:space y"),
            feTurbulence: Oi("baseFrequency class height id numOctaves result seed stitchTiles style type width x xml:base xml:lang xml:space y"),
            filter: Oi("class externalResourcesRequired filterRes filterUnits height id primitiveUnits style width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            font: Oi("class externalResourcesRequired horiz-adv-x horiz-origin-x horiz-origin-y id style vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            "font-face": Oi("accent-height alphabetic ascent bbox cap-height descent font-family font-size font-stretch font-style font-variant font-weight hanging id ideographic mathematical overline-position overline-thickness panose-1 slope stemh stemv strikethrough-position strikethrough-thickness underline-position underline-thickness unicode-range units-per-em v-alphabetic v-hanging v-ideographic v-mathematical widths x-height xml:base xml:lang xml:space"),
            "font-face-format": Oi("id string xml:base xml:lang xml:space"),
            "font-face-name": Oi("id name xml:base xml:lang xml:space"),
            "font-face-src": Oi("id xml:base xml:lang xml:space"),
            "font-face-uri": Oi("id xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            foreignObject: Oi("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform width x xml:base xml:lang xml:space y"),
            g: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            glyph: Oi("arabic-form class d glyph-name horiz-adv-x id lang orientation style unicode vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            glyphRef: Oi("class dx dy format glyphRef id style x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            glyphref: w(),
            hkern: Oi("g1 g2 id k u1 u2 xml:base xml:lang xml:space"),
            image: Oi("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage transform width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            line: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform x1 x2 xml:base xml:lang xml:space y1 y2"),
            linearGradient: Oi("class externalResourcesRequired gradientTransform gradientUnits id spreadMethod style x1 x2 xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space y1 y2"),
            marker: Oi("class externalResourcesRequired id markerHeight markerUnits markerWidth orient preserveAspectRatio refX refY style viewBox xml:base xml:lang xml:space"),
            mask: Oi("class externalResourcesRequired height id maskContentUnits maskUnits requiredExtensions requiredFeatures style systemLanguage width x xml:base xml:lang xml:space y"),
            metadata: Oi("id xml:base xml:lang xml:space"),
            "missing-glyph": Oi("class d horiz-adv-x id style vert-adv-y vert-origin-x vert-origin-y xml:base xml:lang xml:space"),
            mpath: Oi("externalResourcesRequired id xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            path: Oi("class d externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup pathLength requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            pattern: Oi("class externalResourcesRequired height id patternContentUnits patternTransform patternUnits preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage viewBox width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            polygon: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup points requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            polyline: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup points requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            radialGradient: Oi("class cx cy externalResourcesRequired fx fy gradientTransform gradientUnits id r spreadMethod style xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space"),
            rect: Oi("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rx ry style systemLanguage transform width x xml:base xml:lang xml:space y"),
            script: Oi("externalResourcesRequired id type xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            set: Oi("attributeName attributeType begin dur end externalResourcesRequired fill id max min onbegin onend onload onrepeat repeatCount repeatDur requiredExtensions requiredFeatures restart systemLanguage to xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space"),
            stop: Oi("class id offset style xml:base xml:lang xml:space"),
            style: Oi("id media title type xml:base xml:lang xml:space"),
            svg: Oi("baseProfile class contentScriptType contentStyleType externalResourcesRequired height id onabort onactivate onclick onerror onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup onresize onscroll onunload onzoom preserveAspectRatio requiredExtensions requiredFeatures style systemLanguage version viewBox width x xml:base xml:lang xml:space y zoomAndPan"),
            switch: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform xml:base xml:lang xml:space"),
            symbol: Oi("class externalResourcesRequired id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup preserveAspectRatio style viewBox xml:base xml:lang xml:space"),
            text: Oi("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength transform x xml:base xml:lang xml:space y"),
            textPath: Oi("class externalResourcesRequired id lengthAdjust method onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures spacing startOffset style systemLanguage textLength xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space"),
            title: Oi("class id style xml:base xml:lang xml:space"),
            tref: Oi("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength x xlink:arcrole xlink:href xlink:role xlink:title xlink:type xml:base xml:lang xml:space y"),
            tspan: Oi("class dx dy externalResourcesRequired id lengthAdjust onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures rotate style systemLanguage textLength x xml:base xml:lang xml:space y"),
            use: Oi("class externalResourcesRequired height id onactivate onclick onfocusin onfocusout onload onmousedown onmousemove onmouseout onmouseover onmouseup requiredExtensions requiredFeatures style systemLanguage transform width x xlink:actuate xlink:arcrole xlink:href xlink:role xlink:show xlink:title xlink:type xml:base xml:lang xml:space y"),
            view: Oi("externalResourcesRequired id preserveAspectRatio viewBox viewTarget xml:base xml:lang xml:space zoomAndPan"),
            vkern: Oi("g1 g2 id k u1 u2 xml:base xml:lang xml:space")
        });
        this.xe = Oi("a altGlyph animate animateColor circle clipPath defs ellipse feBlend feColorMatrix feComponentTransfer feComposite feConvolveMatrix feDiffuseLighting feDisplacementMap feFlood feGaussianBlur feImage feMerge feMorphology feOffset feSpecularLighting feTile feTurbulence filter font foreignObject g glyph glyphRef image line linearGradient marker mask missing-glyph path pattern polygon polyline radialGradient rect stop svg switch symbol text textPath tref tspan use");
        this.me = Oi("alignment-baseline baseline-shift clip-path clip-rule clip color-interpolation-filters color-interpolation color-profile color-rendering color cursor direction display dominant-baseline enable-background fill-opacity fill-rule fill filter flood-color flood-opacity font-family font-size-adjust font-size font-stretch font-style font-variant font-weight glyph-orientation-horizontal glyph-orientation-vertical image-rendering kerning letter-spacing lighting-color marker-end marker-mid marker-start mask opacity overflow pointer-events shape-rendering stop-color stop-opacity stroke-dasharray stroke-dashoffset stroke-linecap stroke-linejoin stroke-miterlimit stroke-opacity stroke-width stroke text-anchor text-decoration text-rendering unicode-bidi visibility word-spacing writing-mode");
        this.SVGElement = t.globalThis.SVGElement;
        const e = t.document.createElement("div");
        e.innerHTML = "<svg><altGlyph /></svg>";
        if ("altglyph" === e.firstElementChild.nodeName) {
            const t = this.pe;
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
        return true === this.xe[t.nodeName] && true === this.me[e] || true === this.pe[t.nodeName]?.[e];
    }
}

SVGAnalyzer.inject = [ ue ];

const Vi = G("IAttrMapper", (t => t.singleton(AttrMapper)));

class AttrMapper {
    static get inject() {
        return [ Fi ];
    }
    constructor(t) {
        this.svg = t;
        this.fns = [];
        this.ge = w();
        this.ve = w();
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
        let s;
        let i;
        let n;
        let r;
        for (n in t) {
            s = t[n];
            i = (e = this.ge)[n] ?? (e[n] = w());
            for (r in s) {
                if (void 0 !== i[r]) throw ji(r, n);
                i[r] = s[r];
            }
        }
    }
    useGlobalMapping(t) {
        const e = this.ve;
        for (const s in t) {
            if (void 0 !== e[s]) throw ji(s, "*");
            e[s] = t[s];
        }
    }
    useTwoWay(t) {
        this.fns.push(t);
    }
    isTwoWay(t, e) {
        return Ni(t, e) || this.fns.length > 0 && this.fns.some((s => s(t, e)));
    }
    map(t, e) {
        return this.ge[t.nodeName]?.[e] ?? this.ve[e] ?? (S(t, e, this.svg) ? e : null);
    }
}

function Ni(t, e) {
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

function ji(t, e) {
    return b(`Attribute ${t} has been already registered for ${"*" === e ? "all elements" : `<${e}/>`}`);
}

const Hi = G("ITemplateElementFactory", (t => t.singleton(TemplateElementFactory)));

const Wi = {};

class TemplateElementFactory {
    constructor(t) {
        this.p = t;
        this.we = zi(this.p);
    }
    createTemplate(t) {
        if (E(t)) {
            let e = Wi[t];
            if (void 0 === e) {
                const s = this.we;
                s.innerHTML = t;
                const i = s.content.firstElementChild;
                if (null == i || "TEMPLATE" !== i.nodeName || null != i.nextElementSibling) {
                    this.we = zi(this.p);
                    e = s;
                } else {
                    s.content.removeChild(i);
                    e = i;
                }
                Wi[t] = e;
            }
            return e.cloneNode(true);
        }
        if ("TEMPLATE" !== t.nodeName) {
            const e = zi(this.p);
            e.content.appendChild(t);
            return e;
        }
        t.parentNode?.removeChild(t);
        return t.cloneNode(true);
    }
}

TemplateElementFactory.inject = [ ue ];

const zi = t => t.document.createElement("template");

class TemplateCompiler {
    constructor() {
        this.debug = false;
        this.resolveResources = true;
    }
    static register(t) {
        return X(xi, this).register(t);
    }
    compile(e, s, i) {
        const n = CustomElementDefinition.getOrCreate(e);
        if (null === n.template || void 0 === n.template) return n;
        if (false === n.needsCompile) return n;
        i ?? (i = sn);
        const r = new CompilationContext(e, s, i, null, null, void 0);
        const o = E(n.template) || !e.enhance ? r.be.createTemplate(n.template) : n.template;
        const l = o.nodeName === Ki && null != o.content;
        const h = l ? o.content : o;
        const a = s.get(z(dn));
        const c = a.length;
        let u = 0;
        if (c > 0) while (c > u) {
            a[u].compiling?.(o);
            ++u;
        }
        if (o.hasAttribute(cn)) throw b(`AUR0701`);
        this.ye(h, r);
        this.ke(h, r);
        return CustomElementDefinition.create({
            ...e,
            name: e.name || Ys(),
            dependencies: (e.dependencies ?? t.emptyArray).concat(r.deps ?? t.emptyArray),
            instructions: r.rows,
            surrogates: l ? this.Ae(o, r) : t.emptyArray,
            template: o,
            hasSlots: r.hasSlot,
            needsCompile: false
        });
    }
    compileSpread(e, s, i, n) {
        const r = new CompilationContext(e, i, sn, null, null, void 0);
        const o = [];
        const l = r.Ce(n.nodeName.toLowerCase());
        const h = null !== l;
        const a = r.ep;
        const c = s.length;
        let u = 0;
        let f;
        let d = null;
        let p;
        let x;
        let m;
        let g;
        let v;
        let w = null;
        let y;
        let k;
        let A;
        let C;
        for (;c > u; ++u) {
            f = s[u];
            A = f.target;
            C = f.rawValue;
            w = r.Be(f);
            if (null !== w && (1 & w.type) > 0) {
                rn.node = n;
                rn.attr = f;
                rn.bindable = null;
                rn.def = null;
                o.push(w.build(rn, r.ep, r.m));
                continue;
            }
            d = r.Re(A);
            if (null !== d) {
                if (d.isTemplateController) throw b(`AUR0703:${A}`);
                m = BindablesInfo.from(d, true);
                k = false === d.noMultiBindings && null === w && tn(C);
                if (k) x = this.Se(n, C, d, r); else {
                    v = m.primary;
                    if (null === w) {
                        y = a.parse(C, 1);
                        x = [ null === y ? new SetPropertyInstruction(C, v.property) : new InterpolationInstruction(y, v.property) ];
                    } else {
                        rn.node = n;
                        rn.attr = f;
                        rn.bindable = v;
                        rn.def = d;
                        x = [ w.build(rn, r.ep, r.m) ];
                    }
                }
                (p ?? (p = [])).push(new HydrateAttributeInstruction(this.resolveResources ? d : d.name, null != d.aliases && d.aliases.includes(A) ? A : void 0, x));
                continue;
            }
            if (null === w) {
                y = a.parse(C, 1);
                if (h) {
                    m = BindablesInfo.from(l, false);
                    g = m.attrs[A];
                    if (void 0 !== g) {
                        y = a.parse(C, 1);
                        o.push(new SpreadElementPropBindingInstruction(null == y ? new SetPropertyInstruction(C, g.property) : new InterpolationInstruction(y, g.property)));
                        continue;
                    }
                }
                if (null != y) o.push(new InterpolationInstruction(y, r.m.map(n, A) ?? t.camelCase(A))); else switch (A) {
                  case "class":
                    o.push(new SetClassAttributeInstruction(C));
                    break;

                  case "style":
                    o.push(new SetStyleAttributeInstruction(C));
                    break;

                  default:
                    o.push(new SetAttributeInstruction(C, A));
                }
            } else {
                if (h) {
                    m = BindablesInfo.from(l, false);
                    g = m.attrs[A];
                    if (void 0 !== g) {
                        rn.node = n;
                        rn.attr = f;
                        rn.bindable = g;
                        rn.def = l;
                        o.push(new SpreadElementPropBindingInstruction(w.build(rn, r.ep, r.m)));
                        continue;
                    }
                }
                rn.node = n;
                rn.attr = f;
                rn.bindable = null;
                rn.def = null;
                o.push(w.build(rn, r.ep, r.m));
            }
        }
        en();
        if (null != p) return p.concat(o);
        return o;
    }
    Ae(e, s) {
        const i = [];
        const n = e.attributes;
        const r = s.ep;
        let o = n.length;
        let l = 0;
        let h;
        let a;
        let c;
        let u;
        let f = null;
        let d;
        let p;
        let x;
        let m;
        let g = null;
        let v;
        let w;
        let y;
        let k;
        for (;o > l; ++l) {
            h = n[l];
            a = h.name;
            c = h.value;
            u = s.de.parse(a, c);
            y = u.target;
            k = u.rawValue;
            if (on[y]) throw b(`AUR0702:${a}`);
            g = s.Be(u);
            if (null !== g && (1 & g.type) > 0) {
                rn.node = e;
                rn.attr = u;
                rn.bindable = null;
                rn.def = null;
                i.push(g.build(rn, s.ep, s.m));
                continue;
            }
            f = s.Re(y);
            if (null !== f) {
                if (f.isTemplateController) throw b(`AUR0703:${y}`);
                x = BindablesInfo.from(f, true);
                w = false === f.noMultiBindings && null === g && tn(k);
                if (w) p = this.Se(e, k, f, s); else {
                    m = x.primary;
                    if (null === g) {
                        v = r.parse(k, 1);
                        p = [ null === v ? new SetPropertyInstruction(k, m.property) : new InterpolationInstruction(v, m.property) ];
                    } else {
                        rn.node = e;
                        rn.attr = u;
                        rn.bindable = m;
                        rn.def = f;
                        p = [ g.build(rn, s.ep, s.m) ];
                    }
                }
                e.removeAttribute(a);
                --l;
                --o;
                (d ?? (d = [])).push(new HydrateAttributeInstruction(this.resolveResources ? f : f.name, null != f.aliases && f.aliases.includes(y) ? y : void 0, p));
                continue;
            }
            if (null === g) {
                v = r.parse(k, 1);
                if (null != v) {
                    e.removeAttribute(a);
                    --l;
                    --o;
                    i.push(new InterpolationInstruction(v, s.m.map(e, y) ?? t.camelCase(y)));
                } else switch (a) {
                  case "class":
                    i.push(new SetClassAttributeInstruction(k));
                    break;

                  case "style":
                    i.push(new SetStyleAttributeInstruction(k));
                    break;

                  default:
                    i.push(new SetAttributeInstruction(k, a));
                }
            } else {
                rn.node = e;
                rn.attr = u;
                rn.bindable = null;
                rn.def = null;
                i.push(g.build(rn, s.ep, s.m));
            }
        }
        en();
        if (null != d) return d.concat(i);
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
                let s = t.firstChild;
                while (null !== s) s = this.ke(s, e);
                break;
            }
        }
        return t.nextSibling;
    }
    Ie(e, i) {
        const n = e.attributes;
        const r = n.length;
        const o = [];
        const l = i.ep;
        let h = false;
        let a = 0;
        let c;
        let u;
        let f;
        let d;
        let p;
        let x;
        let m;
        let g;
        for (;r > a; ++a) {
            c = n[a];
            f = c.name;
            d = c.value;
            if ("to-binding-context" === f) {
                h = true;
                continue;
            }
            u = i.de.parse(f, d);
            x = u.target;
            m = u.rawValue;
            p = i.Be(u);
            if (null !== p) {
                if ("bind" === u.command) o.push(new LetBindingInstruction(l.parse(m, 16), t.camelCase(x))); else throw b(`AUR0704:${u.command}`);
                continue;
            }
            g = l.parse(m, 1);
            o.push(new LetBindingInstruction(null === g ? new s.PrimitiveLiteralExpression(m) : g, t.camelCase(x)));
        }
        i.rows.push([ new HydrateLetElementInstruction(o, h) ]);
        return this.Ee(e).nextSibling;
    }
    Te(e, s) {
        var i, n, r, o;
        const l = e.nextSibling;
        const h = (e.getAttribute("as-element") ?? e.nodeName).toLowerCase();
        const a = s.Ce(h);
        const c = null !== a;
        const u = c && null != a.shadowOptions;
        const f = a?.capture;
        const d = null != f && "boolean" !== typeof f;
        const p = f ? [] : t.emptyArray;
        const x = s.ep;
        const m = this.debug ? t.noop : () => {
            e.removeAttribute(A);
            --y;
            --w;
        };
        let g = e.attributes;
        let v;
        let w = g.length;
        let y = 0;
        let k;
        let A;
        let C;
        let B;
        let R;
        let S;
        let I = null;
        let T = false;
        let P;
        let E;
        let L;
        let D;
        let _;
        let q;
        let U;
        let $ = null;
        let M;
        let F;
        let O;
        let V;
        let N = true;
        let j = false;
        let H = false;
        if ("slot" === h) {
            if (null == s.root.def.shadowOptions) throw b(`AUR0717:${s.root.def.name}`);
            s.root.hasSlot = true;
        }
        if (c) {
            N = a.processContent?.call(a.Type, e, s.p);
            g = e.attributes;
            w = g.length;
        }
        if (s.root.def.enhance && e.classList.contains("au")) throw b(`AUR0705`);
        for (;w > y; ++y) {
            k = g[y];
            A = k.name;
            C = k.value;
            switch (A) {
              case "as-element":
              case "containerless":
                m();
                if (!j) j = "containerless" === A;
                continue;
            }
            B = s.de.parse(A, C);
            $ = s.Be(B);
            O = B.target;
            V = B.rawValue;
            if (f && (!d || d && f(O))) {
                if (null != $ && 1 & $.type) {
                    m();
                    p.push(B);
                    continue;
                }
                H = O !== wn && "slot" !== O;
                if (H) {
                    M = BindablesInfo.from(a, false);
                    if (null == M.attrs[O] && !s.Re(O)?.isTemplateController) {
                        m();
                        p.push(B);
                        continue;
                    }
                }
            }
            if (null !== $ && 1 & $.type) {
                rn.node = e;
                rn.attr = B;
                rn.bindable = null;
                rn.def = null;
                (R ?? (R = [])).push($.build(rn, s.ep, s.m));
                m();
                continue;
            }
            I = s.Re(O);
            if (null !== I) {
                M = BindablesInfo.from(I, true);
                T = false === I.noMultiBindings && null === $ && tn(V);
                if (T) L = this.Se(e, V, I, s); else {
                    F = M.primary;
                    if (null === $) {
                        q = x.parse(V, 1);
                        L = [ null === q ? new SetPropertyInstruction(V, F.property) : new InterpolationInstruction(q, F.property) ];
                    } else {
                        rn.node = e;
                        rn.attr = B;
                        rn.bindable = F;
                        rn.def = I;
                        L = [ $.build(rn, s.ep, s.m) ];
                    }
                }
                m();
                if (I.isTemplateController) (D ?? (D = [])).push(new HydrateTemplateController(nn, this.resolveResources ? I : I.name, void 0, L)); else (E ?? (E = [])).push(new HydrateAttributeInstruction(this.resolveResources ? I : I.name, null != I.aliases && I.aliases.includes(O) ? O : void 0, L));
                continue;
            }
            if (null === $) {
                if (c) {
                    M = BindablesInfo.from(a, false);
                    P = M.attrs[O];
                    if (void 0 !== P) {
                        q = x.parse(V, 1);
                        (S ?? (S = [])).push(null == q ? new SetPropertyInstruction(V, P.property) : new InterpolationInstruction(q, P.property));
                        m();
                        continue;
                    }
                }
                q = x.parse(V, 1);
                if (null != q) {
                    m();
                    (R ?? (R = [])).push(new InterpolationInstruction(q, s.m.map(e, O) ?? t.camelCase(O)));
                }
                continue;
            }
            m();
            if (c) {
                M = BindablesInfo.from(a, false);
                P = M.attrs[O];
                if (void 0 !== P) {
                    rn.node = e;
                    rn.attr = B;
                    rn.bindable = P;
                    rn.def = a;
                    (S ?? (S = [])).push($.build(rn, s.ep, s.m));
                    continue;
                }
            }
            rn.node = e;
            rn.attr = B;
            rn.bindable = null;
            rn.def = null;
            (R ?? (R = [])).push($.build(rn, s.ep, s.m));
        }
        en();
        if (this.Le(e) && null != R && R.length > 1) this.De(e, R);
        if (c) {
            U = new HydrateElementInstruction(this.resolveResources ? a : a.name, void 0, S ?? t.emptyArray, null, j, p);
            if (h === wn) {
                const t = e.getAttribute("name") || vn;
                const i = s.t();
                const n = s._e();
                let r = e.firstChild;
                while (null !== r) {
                    if (1 === r.nodeType && r.hasAttribute(wn)) e.removeChild(r); else ws(i, r);
                    r = e.firstChild;
                }
                this.ke(i.content, n);
                U.auSlot = {
                    name: t,
                    fallback: CustomElementDefinition.create({
                        name: Ys(),
                        template: i,
                        instructions: n.rows,
                        needsCompile: false
                    })
                };
                e = this.qe(e, s);
            }
        }
        if (null != R || null != U || null != E) {
            v = t.emptyArray.concat(U ?? t.emptyArray, E ?? t.emptyArray, R ?? t.emptyArray);
            this.Ee(e);
        }
        let W;
        if (null != D) {
            w = D.length - 1;
            y = w;
            _ = D[y];
            let t;
            if (Zi(e)) {
                t = s.t();
                bs(t, [ s.Ue(Qi), s.Ue(Yi), this.Ee(s.h(Xi)) ]);
            } else {
                this.qe(e, s);
                if ("TEMPLATE" === e.nodeName) t = e; else {
                    t = s.t();
                    ws(t, e);
                }
            }
            const r = t;
            const o = s._e(null == v ? [] : [ v ]);
            let l;
            let f;
            let d;
            let p;
            let x;
            let m;
            let g;
            let k;
            let A = 0, C = 0;
            let B = e.firstChild;
            let R = false;
            if (false !== N) while (null !== B) {
                f = 1 === B.nodeType ? B.getAttribute(wn) : null;
                if (null !== f) B.removeAttribute(wn);
                if (c) {
                    l = B.nextSibling;
                    if (!u) {
                        R = 3 === B.nodeType && "" === B.textContent.trim();
                        if (!R) ((i = p ?? (p = {}))[n = f || vn] ?? (i[n] = [])).push(B);
                        e.removeChild(B);
                    }
                    B = l;
                } else {
                    if (null !== f) {
                        f = f || vn;
                        throw b(`AUR0706:${h}[${f}]`);
                    }
                    B = B.nextSibling;
                }
            }
            if (null != p) {
                d = {};
                for (f in p) {
                    t = s.t();
                    x = p[f];
                    for (A = 0, C = x.length; C > A; ++A) {
                        m = x[A];
                        if ("TEMPLATE" === m.nodeName) if (m.attributes.length > 0) ws(t, m); else ws(t, m.content); else ws(t, m);
                    }
                    k = s._e();
                    this.ke(t.content, k);
                    d[f] = CustomElementDefinition.create({
                        name: Ys(),
                        template: t,
                        instructions: k.rows,
                        needsCompile: false,
                        isStrictBinding: s.root.def.isStrictBinding
                    });
                }
                U.projections = d;
            }
            if (c && (j || a.containerless)) this.qe(e, s);
            W = !c || !a.containerless && !j && false !== N;
            if (W) if (e.nodeName === Ki) this.ke(e.content, o); else {
                B = e.firstChild;
                while (null !== B) B = this.ke(B, o);
            }
            _.def = CustomElementDefinition.create({
                name: Ys(),
                template: r,
                instructions: o.rows,
                needsCompile: false,
                isStrictBinding: s.root.def.isStrictBinding
            });
            while (y-- > 0) {
                _ = D[y];
                t = s.t();
                g = this.Ee(s.h(Xi));
                bs(t, [ s.Ue(Qi), s.Ue(Yi), g ]);
                _.def = CustomElementDefinition.create({
                    name: Ys(),
                    template: t,
                    needsCompile: false,
                    instructions: [ [ D[y + 1] ] ],
                    isStrictBinding: s.root.def.isStrictBinding
                });
            }
            s.rows.push([ _ ]);
        } else {
            if (null != v) s.rows.push(v);
            let t = e.firstChild;
            let i;
            let n;
            let l = null;
            let f;
            let d;
            let p;
            let x;
            let m;
            let g = false;
            let w = 0, y = 0;
            if (false !== N) while (null !== t) {
                n = 1 === t.nodeType ? t.getAttribute(wn) : null;
                if (null !== n) t.removeAttribute(wn);
                if (c) {
                    i = t.nextSibling;
                    if (!u) {
                        g = 3 === t.nodeType && "" === t.textContent.trim();
                        if (!g) ((r = f ?? (f = {}))[o = n || vn] ?? (r[o] = [])).push(t);
                        e.removeChild(t);
                    }
                    t = i;
                } else {
                    if (null !== n) {
                        n = n || vn;
                        throw b(`AUR0706:${h}[${n}]`);
                    }
                    t = t.nextSibling;
                }
            }
            if (null != f) {
                l = {};
                for (n in f) {
                    x = s.t();
                    d = f[n];
                    for (w = 0, y = d.length; y > w; ++w) {
                        p = d[w];
                        if (p.nodeName === Ki) if (p.attributes.length > 0) ws(x, p); else ws(x, p.content); else ws(x, p);
                    }
                    m = s._e();
                    this.ke(x.content, m);
                    l[n] = CustomElementDefinition.create({
                        name: Ys(),
                        template: x,
                        instructions: m.rows,
                        needsCompile: false,
                        isStrictBinding: s.root.def.isStrictBinding
                    });
                }
                U.projections = l;
            }
            if (c && (j || a.containerless)) this.qe(e, s);
            W = !c || !a.containerless && !j && false !== N;
            if (W && e.childNodes.length > 0) {
                t = e.firstChild;
                while (null !== t) t = this.ke(t, s);
            }
        }
        return l;
    }
    Pe(t, e) {
        const s = t.parentNode;
        const i = e.ep.parse(t.textContent, 1);
        const n = t.nextSibling;
        let r;
        let o;
        let l;
        let h;
        let a;
        if (null !== i) {
            ({parts: r, expressions: o} = i);
            if (a = r[0]) ms(s, e.$e(a), t);
            for (l = 0, h = o.length; h > l; ++l) {
                gs(s, t, [ e.Ue(Qi), e.Ue(Yi), this.Ee(e.h(Xi)) ]);
                if (a = r[l + 1]) ms(s, e.$e(a), t);
                e.rows.push([ new TextBindingInstruction(o[l], e.root.def.isStrictBinding) ]);
            }
            s.removeChild(t);
        }
        return n;
    }
    Se(t, e, s, i) {
        const n = BindablesInfo.from(s, true);
        const r = e.length;
        const o = [];
        let l;
        let h;
        let a = 0;
        let c = 0;
        let u;
        let f;
        let d;
        let p;
        for (let x = 0; x < r; ++x) {
            c = e.charCodeAt(x);
            if (92 === c) ++x; else if (58 === c) {
                l = e.slice(a, x);
                while (e.charCodeAt(++x) <= 32) ;
                a = x;
                for (;x < r; ++x) {
                    c = e.charCodeAt(x);
                    if (92 === c) ++x; else if (59 === c) {
                        h = e.slice(a, x);
                        break;
                    }
                }
                if (void 0 === h) h = e.slice(a);
                f = i.de.parse(l, h);
                d = i.Be(f);
                p = n.attrs[f.target];
                if (null == p) throw b(`AUR0707:${s.name}.${f.target}`);
                if (null === d) {
                    u = i.ep.parse(h, 1);
                    o.push(null === u ? new SetPropertyInstruction(h, p.property) : new InterpolationInstruction(u, p.property));
                } else {
                    rn.node = t;
                    rn.attr = f;
                    rn.bindable = p;
                    rn.def = s;
                    o.push(d.build(rn, i.ep, i.m));
                }
                while (x < r && e.charCodeAt(++x) <= 32) ;
                a = x;
                l = void 0;
                h = void 0;
            }
        }
        en();
        return o;
    }
    ye(e, s) {
        const i = e;
        const n = t.toArray(i.querySelectorAll("template[as-custom-element]"));
        const r = n.length;
        if (0 === r) return;
        if (r === i.childElementCount) throw b(`AUR0708`);
        const o = new Set;
        const l = [];
        for (const e of n) {
            if (e.parentNode !== i) throw b(`AUR0709`);
            const n = un(e, o);
            const r = class LocalTemplate {};
            const h = e.content;
            const a = t.toArray(h.querySelectorAll("bindable"));
            const c = O.for(r);
            const u = new Set;
            const f = new Set;
            for (const t of a) {
                if (t.parentNode !== h) throw b(`AUR0710`);
                const e = t.getAttribute("property");
                if (null === e) throw b(`AUR0711`);
                const s = t.getAttribute("attribute");
                if (null !== s && f.has(s) || u.has(e)) throw b(`AUR0712:${e}+${s}`); else {
                    if (null !== s) f.add(s);
                    u.add(e);
                }
                c.add({
                    property: e,
                    attribute: s ?? void 0,
                    mode: fn(t)
                });
                const i = t.getAttributeNames().filter((t => !an.includes(t)));
                if (i.length > 0) ;
                h.removeChild(t);
            }
            l.push(r);
            s.Me(Js({
                name: n,
                template: e
            }, r));
            i.removeChild(e);
        }
        let h = 0;
        const a = l.length;
        for (;a > h; ++h) ii(l[h]).dependencies.push(...s.def.dependencies ?? t.emptyArray, ...s.deps ?? t.emptyArray);
    }
    Le(t) {
        return "INPUT" === t.nodeName && 1 === ln[t.type];
    }
    De(t, e) {
        switch (t.nodeName) {
          case "INPUT":
            {
                const t = e;
                let s;
                let i;
                let n = 0;
                let r;
                for (let e = 0; e < t.length && n < 3; e++) {
                    r = t[e];
                    switch (r.to) {
                      case "model":
                      case "value":
                      case "matcher":
                        s = e;
                        n++;
                        break;

                      case "checked":
                        i = e;
                        n++;
                        break;
                    }
                }
                if (void 0 !== i && void 0 !== s && i < s) [t[s], t[i]] = [ t[i], t[s] ];
            }
        }
    }
    Fe(t) {
        return t.nodeName === Xi && Ji(Gi = vs(t)) && Gi.textContent === Yi && Ji(Gi = vs(Gi)) && Gi.textContent === Qi;
    }
    Ee(t) {
        t.classList.add("au");
        return t;
    }
    qe(t, e) {
        if (Zi(t)) return t;
        const s = t.parentNode;
        const i = this.Ee(e.h(Xi));
        gs(s, t, [ e.Ue(Qi), e.Ue(Yi), i ]);
        s.removeChild(t);
        return i;
    }
}

let Gi;

const Xi = "AU-M";

const Ki = "TEMPLATE";

const Qi = "au-start";

const Yi = "au-end";

const Zi = t => t.nodeName === Xi && Ji(Gi = vs(t)) && Gi.textContent === Yi && Ji(Gi = vs(Gi)) && Gi.textContent === Qi;

const Ji = t => 8 === t?.nodeType;

class CompilationContext {
    constructor(e, i, n, r, o, l) {
        this.hasSlot = false;
        this.Oe = w();
        const h = null !== r;
        this.c = i;
        this.root = null === o ? this : o;
        this.def = e;
        this.ci = n;
        this.parent = r;
        this.be = h ? r.be : i.get(Hi);
        this.de = h ? r.de : i.get(rt);
        this.ep = h ? r.ep : i.get(s.IExpressionParser);
        this.m = h ? r.m : i.get(Vi);
        this.Ve = h ? r.Ve : i.get(t.ILogger);
        this.p = h ? r.p : i.get(ue);
        this.localEls = h ? r.localEls : new Set;
        this.rows = l ?? [];
    }
    Me(t) {
        var e;
        ((e = this.root).deps ?? (e.deps = [])).push(t);
        this.root.c.register(t);
    }
    $e(t) {
        return xs(this.p, t);
    }
    Ue(t) {
        return ds(this.p, t);
    }
    h(t) {
        const e = fs(this.p, t);
        if ("template" === t) this.p.document.adoptNode(e.content);
        return e;
    }
    t() {
        return this.h("template");
    }
    Ce(t) {
        return this.c.find(oi, t);
    }
    Re(t) {
        return this.c.find(te, t);
    }
    _e(t) {
        return new CompilationContext(this.def, this.c, this.ci, this, this.root, t);
    }
    Be(t) {
        if (this.root !== this) return this.root.Be(t);
        const e = t.command;
        if (null === e) return null;
        let s = this.Oe[e];
        if (void 0 === s) {
            s = this.c.create(Ui, e);
            if (null === s) throw b(`AUR0713:${e}`);
            this.Oe[e] = s;
        }
        return s;
    }
}

const tn = t => {
    const e = t.length;
    let s = 0;
    let i = 0;
    while (e > i) {
        s = t.charCodeAt(i);
        if (92 === s) ++i; else if (58 === s) return true; else if (36 === s && 123 === t.charCodeAt(i + 1)) return false;
        ++i;
    }
    return false;
};

const en = () => {
    rn.node = rn.attr = rn.bindable = rn.def = null;
};

const sn = {
    projections: null
};

const nn = {
    name: "unnamed"
};

const rn = {
    node: null,
    attr: null,
    bindable: null,
    def: null
};

const on = A(w(), {
    id: true,
    name: true,
    "au-slot": true,
    "as-element": true
});

const ln = {
    checkbox: 1,
    radio: 1
};

const hn = new WeakMap;

class BindablesInfo {
    static from(t, e) {
        let s = hn.get(t);
        if (null == s) {
            const i = t.bindables;
            const n = w();
            const r = e ? void 0 === t.defaultBindingMode ? 8 : t.defaultBindingMode : 8;
            let o;
            let l;
            let h = false;
            let a;
            let c;
            for (l in i) {
                o = i[l];
                c = o.attribute;
                if (true === o.primary) {
                    if (h) throw b(`AUR0714:${t.name}`);
                    h = true;
                    a = o;
                } else if (!h && null == a) a = o;
                n[c] = BindableDefinition.create(l, t.Type, o);
            }
            if (null == o && e) a = n.value = BindableDefinition.create("value", t.Type, {
                mode: r
            });
            hn.set(t, s = new BindablesInfo(n, i, a));
        }
        return s;
    }
    constructor(t, e, s) {
        this.attrs = t;
        this.bindables = e;
        this.primary = s;
    }
}

const an = k([ "property", "attribute", "mode" ]);

const cn = "as-custom-element";

const un = (t, e) => {
    const s = t.getAttribute(cn);
    if (null === s || "" === s) throw b(`AUR0715`);
    if (e.has(s)) throw b(`AUR0716:${s}`); else {
        e.add(s);
        t.removeAttribute(cn);
    }
    return s;
};

const fn = t => {
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

const dn = G("ITemplateCompilerHooks");

const pn = new WeakMap;

const xn = d("compiler-hooks");

const mn = k({
    name: xn,
    define(t) {
        let e = pn.get(t);
        if (void 0 === e) {
            pn.set(t, e = new TemplateCompilerHooksDefinition(t));
            a(xn, e, t);
            p(t, xn);
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
        t.register(X(dn, this.Type));
    }
}

const gn = t => {
    return void 0 === t ? e : e(t);
    function e(t) {
        return mn.define(t);
    }
};

const vn = "default";

const wn = "au-slot";

const bn = new Map;

class BindingModeBehavior {
    bind(t, e) {
        bn.set(e, e.mode);
        e.mode = this.mode;
    }
    unbind(t, e) {
        e.mode = bn.get(e);
        bn.delete(e);
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

gt("oneTime")(OneTimeBindingBehavior);

gt("toView")(ToViewBindingBehavior);

gt("fromView")(FromViewBindingBehavior);

gt("twoWay")(TwoWayBindingBehavior);

const yn = new WeakMap;

const kn = 200;

class DebounceBindingBehavior {
    constructor(t) {
        this.p = t;
    }
    bind(t, e, s) {
        s = Number(s);
        const i = {
            type: "debounce",
            delay: s > 0 ? s : kn,
            now: this.p.performanceNow,
            queue: this.p.taskQueue
        };
        const n = e.limit?.(i);
        if (null == n) ; else yn.set(e, n);
    }
    unbind(t, e) {
        yn.get(e)?.dispose();
        yn.delete(e);
    }
}

DebounceBindingBehavior.inject = [ t.IPlatform ];

gt("debounce")(DebounceBindingBehavior);

class SignalBindingBehavior {
    constructor(t) {
        this.Ne = new Map;
        this.je = t;
    }
    bind(t, e, ...s) {
        if (!("handleChange" in e)) throw b(`AUR0817`);
        if (0 === s.length) throw b(`AUR0818`);
        this.Ne.set(e, s);
        let i;
        for (i of s) this.je.addSignalListener(i, e);
    }
    unbind(t, e) {
        const s = this.Ne.get(e);
        this.Ne.delete(e);
        let i;
        for (i of s) this.je.removeSignalListener(i, e);
    }
}

SignalBindingBehavior.inject = [ s.ISignaler ];

gt("signal")(SignalBindingBehavior);

const An = new WeakMap;

const Cn = 200;

class ThrottleBindingBehavior {
    constructor(t) {
        this.He = t.performanceNow;
        this.ct = t.taskQueue;
    }
    bind(t, e, s) {
        s = Number(s);
        const i = {
            type: "throttle",
            delay: s > 0 ? s : Cn,
            now: this.He,
            queue: this.ct
        };
        const n = e.limit?.(i);
        if (null == n) ; else An.set(e, n);
    }
    unbind(t, e) {
        An.get(e)?.dispose();
        An.delete(e);
    }
}

ThrottleBindingBehavior.inject = [ t.IPlatform ];

gt("throttle")(ThrottleBindingBehavior);

class DataAttributeAccessor {
    constructor() {
        this.type = 2 | 4;
    }
    getValue(t, e) {
        return t.getAttribute(e);
    }
    setValue(t, e, s) {
        if (null == t) e.removeAttribute(s); else e.setAttribute(s, t);
    }
}

xe(DataAttributeAccessor);

const Bn = new DataAttributeAccessor;

class AttrBindingBehavior {
    bind(t, e) {
        if (!(e instanceof PropertyBinding)) throw b(`AURxxxx`);
        e.useTargetObserver(Bn);
    }
}

gt("attr")(AttrBindingBehavior);

class SelfBindingBehavior {
    bind(t, e) {
        if (!(e instanceof ListenerBinding)) throw b(`AUR0801`);
        e.self = true;
    }
    unbind(t, e) {
        e.self = false;
    }
}

gt("self")(SelfBindingBehavior);

const Rn = w();

class AttributeNSAccessor {
    static forNs(t) {
        return Rn[t] ?? (Rn[t] = new AttributeNSAccessor(t));
    }
    constructor(t) {
        this.ns = t;
        this.type = 2 | 4;
    }
    getValue(t, e) {
        return t.getAttributeNS(this.ns, e);
    }
    setValue(t, e, s) {
        if (null == t) e.removeAttributeNS(this.ns, s); else e.setAttributeNS(this.ns, s, t);
    }
}

xe(AttributeNSAccessor);

function Sn(t, e) {
    return t === e;
}

class CheckedObserver {
    constructor(t, e, s, i) {
        this.type = 2 | 1 | 4;
        this.v = void 0;
        this.ov = void 0;
        this.We = void 0;
        this.ze = void 0;
        this.yt = false;
        this.bt = t;
        this.oL = i;
        this.cf = s;
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
        this.it();
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
        const s = y.call(e, "model") ? e.model : e.value;
        const i = "radio" === e.type;
        const n = void 0 !== e.matcher ? e.matcher : Sn;
        if (i) e.checked = !!n(t, s); else if (true === t) e.checked = true; else {
            let i = false;
            if (T(t)) i = -1 !== t.findIndex((t => !!n(t, s))); else if (t instanceof Set) {
                for (const e of t) if (n(e, s)) {
                    i = true;
                    break;
                }
            } else if (t instanceof Map) for (const e of t) {
                const t = e[0];
                const r = e[1];
                if (n(t, s) && true === r) {
                    i = true;
                    break;
                }
            }
            e.checked = i;
        }
    }
    handleEvent() {
        let t = this.ov = this.v;
        const e = this.bt;
        const s = y.call(e, "model") ? e.model : e.value;
        const i = e.checked;
        const n = void 0 !== e.matcher ? e.matcher : Sn;
        if ("checkbox" === e.type) {
            if (T(t)) {
                const e = t.findIndex((t => !!n(t, s)));
                if (i && -1 === e) t.push(s); else if (!i && -1 !== e) t.splice(e, 1);
                return;
            } else if (t instanceof Set) {
                const e = {};
                let r = e;
                for (const e of t) if (true === n(e, s)) {
                    r = e;
                    break;
                }
                if (i && r === e) t.add(s); else if (!i && r !== e) t.delete(r);
                return;
            } else if (t instanceof Map) {
                let e;
                for (const i of t) {
                    const t = i[0];
                    if (true === n(t, s)) {
                        e = t;
                        break;
                    }
                }
                t.set(e, i);
                return;
            }
            t = i;
        } else if (i) t = s; else return;
        this.v = t;
        this.it();
    }
    kt() {
        this.Ge();
    }
    At() {
        this.We?.unsubscribe(this);
        this.ze?.unsubscribe(this);
        this.We = this.ze = void 0;
    }
    it() {
        In = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, In);
    }
    Ge() {
        const t = this.bt;
        (this.ze ?? (this.ze = t.$observers?.model ?? t.$observers?.value))?.subscribe(this);
        this.We?.unsubscribe(this);
        this.We = void 0;
        if ("checkbox" === t.type) (this.We = On(this.v, this.oL))?.subscribe(this);
    }
}

pe(CheckedObserver);

s.subscriberCollection(CheckedObserver);

let In;

const Tn = {
    childList: true,
    subtree: true,
    characterData: true
};

function Pn(t, e) {
    return t === e;
}

class SelectValueObserver {
    constructor(t, e, s, i) {
        this.type = 2 | 1 | 4;
        this.v = void 0;
        this.ov = void 0;
        this.J = false;
        this.Ke = void 0;
        this.Qe = void 0;
        this.iO = false;
        this.yt = false;
        this.bt = t;
        this.oL = i;
        this.cf = s;
    }
    getValue() {
        return this.iO ? this.v : this.bt.multiple ? En(this.bt.options) : this.bt.value;
    }
    setValue(t) {
        this.ov = this.v;
        this.v = t;
        this.J = t !== this.ov;
        this.Ye(t instanceof Array ? t : null);
        this.st();
    }
    st() {
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
        const s = T(t);
        const i = e.matcher ?? Pn;
        const n = e.options;
        let r = n.length;
        while (r-- > 0) {
            const e = n[r];
            const o = y.call(e, "model") ? e.model : e.value;
            if (s) {
                e.selected = -1 !== t.findIndex((t => !!i(o, t)));
                continue;
            }
            e.selected = !!i(o, t);
        }
    }
    syncValue() {
        const t = this.bt;
        const e = t.options;
        const s = e.length;
        const i = this.v;
        let n = 0;
        if (t.multiple) {
            if (!(i instanceof Array)) return true;
            let r;
            const o = t.matcher || Pn;
            const l = [];
            while (n < s) {
                r = e[n];
                if (r.selected) l.push(y.call(r, "model") ? r.model : r.value);
                ++n;
            }
            let h;
            n = 0;
            while (n < i.length) {
                h = i[n];
                if (-1 === l.findIndex((t => !!o(h, t)))) i.splice(n, 1); else ++n;
            }
            n = 0;
            while (n < l.length) {
                h = l[n];
                if (-1 === i.findIndex((t => !!o(h, t)))) i.push(h);
                ++n;
            }
            return false;
        }
        let r = null;
        let o;
        while (n < s) {
            o = e[n];
            if (o.selected) {
                r = y.call(o, "model") ? o.model : o.value;
                break;
            }
            ++n;
        }
        this.ov = this.v;
        this.v = r;
        return true;
    }
    kt() {
        (this.Qe = new this.bt.ownerDocument.defaultView.MutationObserver(this.Ze.bind(this))).observe(this.bt, Tn);
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
            if (!this.bt.multiple) throw b(`AUR0654`);
            (this.Ke = this.oL.getArrayObserver(t)).subscribe(this);
        }
    }
    handleEvent() {
        const t = this.syncValue();
        if (t) this.it();
    }
    Ze(t) {
        this.syncOptions();
        const e = this.syncValue();
        if (e) this.it();
    }
    it() {
        Ln = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, Ln);
    }
}

pe(SelectValueObserver);

s.subscriberCollection(SelectValueObserver);

function En(t) {
    const e = [];
    if (0 === t.length) return e;
    const s = t.length;
    let i = 0;
    let n;
    while (s > i) {
        n = t[i];
        if (n.selected) e[e.length] = y.call(n, "model") ? n.model : n.value;
        ++i;
    }
    return e;
}

let Ln;

const Dn = "--";

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
        this.st();
    }
    Je(t) {
        const e = [];
        const s = /url\([^)]+$/;
        let i = 0;
        let n = "";
        let r;
        let o;
        let l;
        let h;
        while (i < t.length) {
            r = t.indexOf(";", i);
            if (-1 === r) r = t.length;
            n += t.substring(i, r);
            i = r + 1;
            if (s.test(n)) {
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
    ts(e) {
        let s;
        let i;
        const n = [];
        for (i in e) {
            s = e[i];
            if (null == s) continue;
            if (E(s)) {
                if (i.startsWith(Dn)) {
                    n.push([ i, s ]);
                    continue;
                }
                n.push([ t.kebabCase(i), s ]);
                continue;
            }
            n.push(...this.es(s));
        }
        return n;
    }
    ss(e) {
        const s = e.length;
        if (s > 0) {
            const t = [];
            let i = 0;
            for (;s > i; ++i) t.push(...this.es(e[i]));
            return t;
        }
        return t.emptyArray;
    }
    es(e) {
        if (E(e)) return this.Je(e);
        if (e instanceof Array) return this.ss(e);
        if (e instanceof Object) return this.ts(e);
        return t.emptyArray;
    }
    st() {
        if (this.J) {
            this.J = false;
            const t = this.v;
            const e = this.styles;
            const s = this.es(t);
            let i;
            let n = this.version;
            this.ov = t;
            let r;
            let o;
            let l;
            let h = 0;
            const a = s.length;
            for (;h < a; ++h) {
                r = s[h];
                o = r[0];
                l = r[1];
                this.setProperty(o, l);
                e[o] = n;
            }
            this.styles = e;
            this.version += 1;
            if (0 === n) return;
            n -= 1;
            for (i in e) {
                if (!y.call(e, i) || e[i] !== n) continue;
                this.obj.style.removeProperty(i);
            }
        }
    }
    setProperty(t, e) {
        let s = "";
        if (null != e && P(e.indexOf) && e.includes("!important")) {
            s = "important";
            e = e.replace("!important", "");
        }
        this.obj.style.setProperty(t, e, s);
    }
    bind() {
        this.v = this.ov = this.obj.style.cssText;
    }
}

xe(StyleAttributeAccessor);

class ValueAttributeObserver {
    constructor(t, e, s) {
        this.type = 2 | 1 | 4;
        this.v = "";
        this.ov = "";
        this.J = false;
        this.yt = false;
        this.bt = t;
        this.k = e;
        this.cf = s;
    }
    getValue() {
        return this.v;
    }
    setValue(t) {
        if (_(t, this.v)) return;
        this.ov = this.v;
        this.v = t;
        this.J = true;
        if (!this.cf.readonly) this.st();
    }
    st() {
        if (this.J) {
            this.J = false;
            this.bt[this.k] = this.v ?? this.cf.default;
            this.it();
        }
    }
    handleEvent() {
        this.ov = this.v;
        this.v = this.bt[this.k];
        if (this.ov !== this.v) {
            this.J = false;
            this.it();
        }
    }
    kt() {
        this.v = this.ov = this.bt[this.k];
    }
    it() {
        _n = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, _n);
    }
}

pe(ValueAttributeObserver);

s.subscriberCollection(ValueAttributeObserver);

let _n;

const qn = "http://www.w3.org/1999/xlink";

const Un = "http://www.w3.org/XML/1998/namespace";

const $n = "http://www.w3.org/2000/xmlns/";

const Mn = A(w(), {
    "xlink:actuate": [ "actuate", qn ],
    "xlink:arcrole": [ "arcrole", qn ],
    "xlink:href": [ "href", qn ],
    "xlink:role": [ "role", qn ],
    "xlink:show": [ "show", qn ],
    "xlink:title": [ "title", qn ],
    "xlink:type": [ "type", qn ],
    "xml:lang": [ "lang", Un ],
    "xml:space": [ "space", Un ],
    xmlns: [ "xmlns", $n ],
    "xmlns:xlink": [ "xlink", $n ]
});

const Fn = new s.PropertyAccessor;

Fn.type = 2 | 4;

class NodeObserverLocator {
    constructor(t, e, s, i) {
        this.locator = t;
        this.platform = e;
        this.dirtyChecker = s;
        this.svgAnalyzer = i;
        this.allowDirtyCheck = true;
        this.rs = w();
        this.os = w();
        this.ls = w();
        this.cs = w();
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
        K(s.INodeObserverLocator, NodeObserverLocator).register(t);
        X(s.INodeObserverLocator, NodeObserverLocator).register(t);
    }
    handles(t, e) {
        return t instanceof this.platform.Node;
    }
    useConfig(t, e, s) {
        const i = this.rs;
        let n;
        if (E(t)) {
            n = i[t] ?? (i[t] = w());
            if (null == n[e]) n[e] = s; else Vn(t, e);
        } else for (const s in t) {
            n = i[s] ?? (i[s] = w());
            const r = t[s];
            for (e in r) if (null == n[e]) n[e] = r[e]; else Vn(s, e);
        }
    }
    useConfigGlobal(t, e) {
        const s = this.os;
        if ("object" === typeof t) for (const e in t) if (null == s[e]) s[e] = t[e]; else Vn("*", e); else if (null == s[t]) s[t] = e; else Vn("*", t);
    }
    getAccessor(e, s, i) {
        if (s in this.cs || s in (this.ls[e.tagName] ?? t.emptyObject)) return this.getObserver(e, s, i);
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
            return Bn;

          default:
            {
                const t = Mn[s];
                if (void 0 !== t) return AttributeNSAccessor.forNs(t[1]);
                if (S(e, s, this.svgAnalyzer)) return Bn;
                return Fn;
            }
        }
    }
    overrideAccessor(t, e) {
        var s, i;
        let n;
        if (E(t)) {
            n = (s = this.ls)[t] ?? (s[t] = w());
            n[e] = true;
        } else for (const e in t) for (const s of t[e]) {
            n = (i = this.ls)[e] ?? (i[e] = w());
            n[s] = true;
        }
    }
    overrideAccessorGlobal(...t) {
        for (const e of t) this.cs[e] = true;
    }
    getNodeObserverConfig(t, e) {
        return this.rs[t.tagName]?.[e] ?? this.os[e];
    }
    getNodeObserver(t, e, i) {
        const n = this.rs[t.tagName]?.[e] ?? this.os[e];
        let r;
        if (null != n) {
            r = new (n.type ?? ValueAttributeObserver)(t, e, n, i, this.locator);
            if (!r.doNotCache) s.getObserverLookup(t)[e] = r;
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
        if (null != n) return n;
        const r = Mn[e];
        if (void 0 !== r) return AttributeNSAccessor.forNs(r[1]);
        if (S(t, e, this.svgAnalyzer)) return Bn;
        if (e in t.constructor.prototype) {
            if (this.allowDirtyCheck) return this.dirtyChecker.createProperty(t, e);
            throw b(`AUR0652:${String(e)}`);
        } else return new s.SetterObserver(t, e);
    }
}

NodeObserverLocator.inject = [ t.IServiceLocator, ue, s.IDirtyChecker, Fi ];

function On(t, e) {
    if (t instanceof Array) return e.getArrayObserver(t);
    if (t instanceof Map) return e.getMapObserver(t);
    if (t instanceof Set) return e.getSetObserver(t);
}

function Vn(t, e) {
    throw b(`AUR0653:${String(e)}@${t}`);
}

class UpdateTriggerBindingBehavior {
    constructor(t, e) {
        if (!(e instanceof NodeObserverLocator)) throw b("AURxxxx: updateTrigger binding behavior only works with the default implementation of Aurelia HTML observation. Implement your own node observation + updateTrigger");
        this.oL = t;
        this.us = e;
    }
    bind(t, e, ...s) {
        if (0 === s.length) throw b(`AUR0802`);
        if (!(e instanceof PropertyBinding) || !(4 & e.mode)) throw b(`AUR0803`);
        const i = this.us.getNodeObserverConfig(e.target, e.targetProperty);
        if (null == i) throw b(`AURxxxx`);
        const n = this.us.getNodeObserver(e.target, e.targetProperty, this.oL);
        n.useConfig({
            readonly: i.readonly,
            default: i.default,
            events: s
        });
        e.useTargetObserver(n);
    }
}

UpdateTriggerBindingBehavior.inject = [ s.IObserverLocator, s.INodeObserverLocator ];

gt("updateTrigger")(UpdateTriggerBindingBehavior);

class Focus {
    constructor(t, e) {
        this.ds = false;
        this.ps = t;
        this.p = e;
    }
    binding() {
        this.valueChanged();
    }
    valueChanged() {
        if (this.$controller.isActive) this.xs(); else this.ds = true;
    }
    attached() {
        if (this.ds) {
            this.ds = false;
            this.xs();
        }
        this.ps.addEventListener("focus", this);
        this.ps.addEventListener("blur", this);
    }
    afterDetachChildren() {
        const t = this.ps;
        t.removeEventListener("focus", this);
        t.removeEventListener("blur", this);
    }
    handleEvent(t) {
        if ("focus" === t.type) this.value = true; else if (!this.gs) this.value = false;
    }
    xs() {
        const t = this.ps;
        const e = this.gs;
        const s = this.value;
        if (s && !e) t.focus(); else if (!s && e) t.blur();
    }
    get gs() {
        return this.ps === this.p.document.activeElement;
    }
}

Focus.inject = [ Bs, ue ];

r([ $({
    mode: 6
}) ], Focus.prototype, "value", void 0);

Wt("focus")(Focus);

let Nn = class Show {
    constructor(t, e, s) {
        this.el = t;
        this.p = e;
        this.vs = false;
        this.lt = null;
        this.$val = "";
        this.$prio = "";
        this.update = () => {
            this.lt = null;
            if (Boolean(this.value) !== this.ws) if (this.ws === this.bs) {
                this.ws = !this.bs;
                this.$val = this.el.style.getPropertyValue("display");
                this.$prio = this.el.style.getPropertyPriority("display");
                this.el.style.setProperty("display", "none", "important");
            } else {
                this.ws = this.bs;
                this.el.style.setProperty("display", this.$val, this.$prio);
                if ("" === this.el.getAttribute("style")) this.el.removeAttribute("style");
            }
        };
        this.ws = this.bs = "hide" !== s.alias;
    }
    binding() {
        this.vs = true;
        this.update();
    }
    detaching() {
        this.vs = false;
        this.lt?.cancel();
        this.lt = null;
    }
    valueChanged() {
        if (this.vs && null === this.lt) this.lt = this.p.domWriteQueue.queueTask(this.update);
    }
};

r([ $ ], Nn.prototype, "value", void 0);

Nn = r([ o(0, Bs), o(1, ue), o(2, di) ], Nn);

J("hide")(Nn);

Wt("show")(Nn);

class Portal {
    constructor(t, e, s) {
        this.position = "beforeend";
        this.strict = false;
        this.p = s;
        this.ys = s.document.createElement("div");
        (this.view = t.create()).setLocation(this.ks = ps(s));
        Es(this.view.nodes, e);
    }
    attaching(t, e, s) {
        if (null == this.callbackContext) this.callbackContext = this.$controller.scope.bindingContext;
        const i = this.ys = this.As();
        this.Cs(i, this.position);
        return this.Bs(t, i, s);
    }
    detaching(t, e, s) {
        return this.Rs(t, this.ys, s);
    }
    targetChanged() {
        const {$controller: e} = this;
        if (!e.isActive) return;
        const s = this.As();
        if (this.ys === s) return;
        this.ys = s;
        const i = t.onResolve(this.Rs(null, s, e.flags), (() => {
            this.Cs(s, this.position);
            return this.Bs(null, s, e.flags);
        }));
        if (I(i)) i.catch(D);
    }
    positionChanged() {
        const {$controller: e, ys: s} = this;
        if (!e.isActive) return;
        const i = t.onResolve(this.Rs(null, s, e.flags), (() => {
            this.Cs(s, this.position);
            return this.Bs(null, s, e.flags);
        }));
        if (I(i)) i.catch(D);
    }
    Bs(e, s, i) {
        const {activating: n, callbackContext: r, view: o} = this;
        return t.onResolve(n?.call(r, s, o), (() => this.Ss(e, s, i)));
    }
    Ss(e, s, i) {
        const {$controller: n, view: r} = this;
        if (null === e) r.nodes.insertBefore(this.ks); else return t.onResolve(r.activate(e ?? r, n, i, n.scope), (() => this.Is(s)));
        return this.Is(s);
    }
    Is(t) {
        const {activated: e, callbackContext: s, view: i} = this;
        return e?.call(s, t, i);
    }
    Rs(e, s, i) {
        const {deactivating: n, callbackContext: r, view: o} = this;
        return t.onResolve(n?.call(r, s, o), (() => this.Ts(e, s, i)));
    }
    Ts(e, s, i) {
        const {$controller: n, view: r} = this;
        if (null === e) r.nodes.remove(); else return t.onResolve(r.deactivate(e, n, i), (() => this.Ps(s)));
        return this.Ps(s);
    }
    Ps(t) {
        const {deactivated: e, callbackContext: s, view: i} = this;
        return e?.call(s, t, i);
    }
    As() {
        const t = this.p;
        const e = t.document;
        let s = this.target;
        let i = this.renderContext;
        if ("" === s) {
            if (this.strict) throw b(`AUR0811`);
            return e.body;
        }
        if (E(s)) {
            let n = e;
            if (E(i)) i = e.querySelector(i);
            if (i instanceof t.Node) n = i;
            s = n.querySelector(s);
        }
        if (s instanceof t.Node) return s;
        if (null == s) {
            if (this.strict) throw b(`AUR0812`);
            return e.body;
        }
        return s;
    }
    Cs(t, e) {
        const s = this.ks;
        const i = s.$start;
        const n = t.parentNode;
        const r = [ i, s ];
        switch (e) {
          case "beforeend":
            gs(t, null, r);
            break;

          case "afterbegin":
            gs(t, t.firstChild, r);
            break;

          case "beforebegin":
            gs(n, t, r);
            break;

          case "afterend":
            gs(n, t.nextSibling, r);
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

Portal.inject = [ De, Ss, ue ];

r([ $({
    primary: true
}) ], Portal.prototype, "target", void 0);

r([ $() ], Portal.prototype, "position", void 0);

r([ $({
    callback: "targetChanged"
}) ], Portal.prototype, "renderContext", void 0);

r([ $() ], Portal.prototype, "strict", void 0);

r([ $() ], Portal.prototype, "deactivating", void 0);

r([ $() ], Portal.prototype, "activating", void 0);

r([ $() ], Portal.prototype, "deactivated", void 0);

r([ $() ], Portal.prototype, "activated", void 0);

r([ $() ], Portal.prototype, "callbackContext", void 0);

zt("portal")(Portal);

class If {
    constructor(t, e) {
        this.elseFactory = void 0;
        this.elseView = void 0;
        this.ifView = void 0;
        this.view = void 0;
        this.value = false;
        this.cache = true;
        this.pending = void 0;
        this.Es = false;
        this.Ls = 0;
        this.Ds = t;
        this.l = e;
    }
    attaching(e, s, i) {
        let n;
        const r = this.$controller;
        const o = this.Ls++;
        const l = () => !this.Es && this.Ls === o + 1;
        return t.onResolve(this.pending, (() => {
            if (!l()) return;
            this.pending = void 0;
            if (this.value) n = this.view = this.ifView = this.cache && null != this.ifView ? this.ifView : this.Ds.create(); else n = this.view = this.elseView = this.cache && null != this.elseView ? this.elseView : this.elseFactory?.create();
            if (null == n) return;
            n.setLocation(this.l);
            this.pending = t.onResolve(n.activate(e, r, i, r.scope), (() => {
                if (l()) this.pending = void 0;
            }));
        }));
    }
    detaching(e, s, i) {
        this.Es = true;
        return t.onResolve(this.pending, (() => {
            this.Es = false;
            this.pending = void 0;
            void this.view?.deactivate(e, this.$controller, i);
        }));
    }
    valueChanged(e, s, i) {
        if (!this.$controller.isActive) return;
        e = !!e;
        s = !!s;
        if (e === s) return;
        const n = this.view;
        const r = this.$controller;
        const o = this.Ls++;
        const l = () => !this.Es && this.Ls === o + 1;
        let h;
        return t.onResolve(this.pending, (() => this.pending = t.onResolve(n?.deactivate(n, r, i), (() => {
            if (!l()) return;
            if (e) h = this.view = this.ifView = this.cache && null != this.ifView ? this.ifView : this.Ds.create(); else h = this.view = this.elseView = this.cache && null != this.elseView ? this.elseView : this.elseFactory?.create();
            if (null == h) return;
            h.setLocation(this.l);
            return t.onResolve(h.activate(h, r, i, r.scope), (() => {
                if (l()) this.pending = void 0;
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

If.inject = [ De, Ss ];

r([ $ ], If.prototype, "value", void 0);

r([ $({
    set: t => "" === t || !!t && "false" !== t
}) ], If.prototype, "cache", void 0);

zt("if")(If);

class Else {
    constructor(t) {
        this.f = t;
    }
    link(t, e, s, i) {
        const n = t.children;
        const r = n[n.length - 1];
        if (r instanceof If) r.elseFactory = this.f; else if (r.viewModel instanceof If) r.viewModel.elseFactory = this.f; else throw b(`AUR0810`);
    }
}

Else.inject = [ De ];

zt({
    name: "else"
})(Else);

function jn(t) {
    t.dispose();
}

const Hn = [ 18, 17 ];

class Repeat {
    constructor(t, e, s, i, n) {
        this.views = [];
        this.key = null;
        this._s = new Map;
        this.qs = new Map;
        this.Us = void 0;
        this.$s = false;
        this.Ms = false;
        this.Fs = null;
        this.Os = void 0;
        this.Vs = false;
        const r = t.props[0].props[0];
        if (void 0 !== r) {
            const {to: t, value: s, command: i} = r;
            if ("key" === t) if (null === i) this.key = s; else if ("bind" === i) this.key = e.parse(s, 16); else throw b(`AUR775:${i}`); else throw b(`AUR776:${t}`);
        }
        this.l = s;
        this.Ns = i;
        this.f = n;
    }
    binding(t, e, i) {
        const n = this.Ns.bindings;
        const r = n.length;
        let o;
        let l;
        let h = 0;
        for (;r > h; ++h) {
            o = n[h];
            if (o.target === this && "items" === o.targetProperty) {
                l = this.forOf = o.ast;
                this.js = o;
                let t = l.iterable;
                while (null != t && Hn.includes(t.$kind)) {
                    t = t.expression;
                    this.$s = true;
                }
                this.Fs = t;
                break;
            }
        }
        this.Hs();
        const a = l.declaration;
        if (!(this.Vs = 24 === a.$kind || 25 === a.$kind)) this.local = s.astEvaluate(a, this.$controller.scope, o, null);
    }
    attaching(t, e, s) {
        this.Ws();
        return this.zs(t);
    }
    detaching(t, e, s) {
        this.Hs();
        return this.Gs(t);
    }
    unbinding(t, e, s) {
        this.qs.clear();
        this._s.clear();
    }
    itemsChanged() {
        if (!this.$controller.isActive) return;
        this.Hs();
        this.Ws();
        this.Xs(this.items, void 0);
    }
    handleCollectionChange(t, e) {
        const i = this.$controller;
        if (!i.isActive) return;
        if (this.$s) {
            if (this.Ms) return;
            this.Ms = true;
            this.items = s.astEvaluate(this.forOf.iterable, i.scope, this.js, null);
            this.Ms = false;
            return;
        }
        this.Ws();
        this.Xs(t, e);
    }
    Xs(e, i) {
        const n = this.views;
        const r = n.length;
        const o = this.key;
        const l = null !== o;
        if (l || void 0 === i) {
            const t = this.local;
            const e = this.Os;
            const h = e.length;
            const a = this.forOf;
            const c = a.declaration;
            const u = this.js;
            const f = this.Vs;
            i = s.createIndexMap(h);
            let d = 0;
            if (0 === r) for (;d < h; ++d) i[d] = -2; else if (0 === h) if (f) for (d = 0; d < r; ++d) {
                i.deletedIndices.push(d);
                i.deletedItems.push(s.astEvaluate(c, n[d].scope, u, null));
            } else for (d = 0; d < r; ++d) {
                i.deletedIndices.push(d);
                i.deletedItems.push(n[d].scope.bindingContext[t]);
            } else {
                const p = Array(r);
                if (f) for (d = 0; d < r; ++d) p[d] = s.astEvaluate(c, n[d].scope, u, null); else for (d = 0; d < r; ++d) p[d] = n[d].scope.bindingContext[t];
                let x;
                let m;
                let g;
                let v;
                let w = 0;
                const b = r - 1;
                const y = h - 1;
                const k = new Map;
                const A = new Map;
                const C = this._s;
                const B = this.qs;
                const R = this.$controller.scope;
                d = 0;
                t: {
                    while (true) {
                        x = p[d];
                        m = e[d];
                        g = l ? nr(C, o, x, rr(B, p[d], a, R, u, t, f), u) : x;
                        v = l ? nr(C, o, m, rr(B, e[d], a, R, u, t, f), u) : m;
                        if (g !== v) {
                            C.set(x, g);
                            C.set(m, v);
                            break;
                        }
                        ++d;
                        if (d > b || d > y) break t;
                    }
                    if (b !== y) break t;
                    w = y;
                    while (true) {
                        x = p[w];
                        m = e[w];
                        g = l ? nr(C, o, x, rr(B, x, a, R, u, t, f), u) : x;
                        v = l ? nr(C, o, m, rr(B, m, a, R, u, t, f), u) : m;
                        if (g !== v) {
                            C.set(x, g);
                            C.set(m, v);
                            break;
                        }
                        --w;
                        if (d > w) break t;
                    }
                }
                const S = d;
                const I = d;
                for (d = I; d <= y; ++d) {
                    if (C.has(m = e[d])) v = C.get(m); else {
                        v = l ? nr(C, o, m, rr(B, m, a, R, u, t, f), u) : m;
                        C.set(m, v);
                    }
                    A.set(v, d);
                }
                for (d = S; d <= b; ++d) {
                    if (C.has(x = p[d])) g = C.get(x); else g = l ? nr(C, o, x, n[d].scope, u) : x;
                    k.set(g, d);
                    if (A.has(g)) i[A.get(g)] = d; else {
                        i.deletedIndices.push(d);
                        i.deletedItems.push(x);
                    }
                }
                for (d = I; d <= y; ++d) if (!k.has(C.get(e[d]))) i[d] = -2;
                k.clear();
                A.clear();
            }
        }
        if (void 0 === i) {
            const e = t.onResolve(this.Gs(null), (() => this.zs(null)));
            if (I(e)) e.catch(D);
        } else {
            const e = s.applyMutationsToIndices(i);
            if (e.deletedIndices.length > 0) {
                const s = t.onResolve(this.Ks(e), (() => this.Qs(r, e)));
                if (I(s)) s.catch(D);
            } else this.Qs(r, e);
        }
    }
    Hs() {
        const t = this.$controller.scope;
        let e = this.Ys;
        let i = this.$s;
        let n;
        if (i) {
            e = this.Ys = s.astEvaluate(this.Fs, t, this.js, null) ?? null;
            i = this.$s = !_(this.items, e);
        }
        const r = this.Us;
        if (this.$controller.isActive) {
            n = this.Us = s.getCollectionObserver(i ? e : this.items);
            if (r !== n) {
                r?.unsubscribe(this);
                n?.subscribe(this);
            }
        } else {
            r?.unsubscribe(this);
            this.Us = void 0;
        }
    }
    Ws() {
        const {items: t} = this;
        if (T(t)) {
            this.Os = t;
            return;
        }
        const e = [];
        Jn(t, ((t, s) => {
            e[s] = t;
        }));
        this.Os = e;
    }
    zs(t) {
        let e;
        let s;
        let i;
        let n;
        const {$controller: r, f: o, local: l, l: h, items: a, qs: c, js: u, forOf: f, Vs: d} = this;
        const p = r.scope;
        const x = Zn(a);
        const m = this.views = Array(x);
        Jn(a, ((a, g) => {
            i = m[g] = o.create().setLocation(h);
            i.nodes.unlink();
            n = rr(c, a, f, p, u, l, d);
            Qn(n.overrideContext, g, x);
            s = i.activate(t ?? i, r, 0, n);
            if (I(s)) (e ?? (e = [])).push(s);
        }));
        if (void 0 !== e) return 1 === e.length ? e[0] : Promise.all(e);
    }
    Gs(t) {
        let e;
        let s;
        let i;
        let n = 0;
        const {views: r, $controller: o} = this;
        const l = r.length;
        for (;l > n; ++n) {
            i = r[n];
            i.release();
            s = i.deactivate(t ?? i, o, 0);
            if (I(s)) (e ?? (e = [])).push(s);
        }
        if (void 0 !== e) return 1 === e.length ? e[0] : Promise.all(e);
    }
    Ks(t) {
        let e;
        let s;
        let i;
        const {$controller: n, views: r} = this;
        const o = t.deletedIndices;
        const l = o.length;
        let h = 0;
        for (;l > h; ++h) {
            i = r[o[h]];
            i.release();
            s = i.deactivate(i, n, 0);
            if (I(s)) (e ?? (e = [])).push(s);
        }
        h = 0;
        let a = 0;
        for (;l > h; ++h) {
            a = o[h] - h;
            r.splice(a, 1);
        }
        if (void 0 !== e) return 1 === e.length ? e[0] : Promise.all(e);
    }
    Qs(t, e) {
        let i;
        let n;
        let r;
        let o;
        let l = 0;
        const {$controller: h, f: a, local: c, Os: u, l: f, views: d, Vs: p, js: x, qs: m, forOf: g} = this;
        const v = e.length;
        for (;v > l; ++l) if (-2 === e[l]) {
            r = a.create();
            d.splice(l, 0, r);
        }
        if (d.length !== v) throw Kn(d.length, v);
        const w = h.scope;
        const b = e.length;
        s.synchronizeIndices(d, e);
        const y = Xn(e);
        const k = y.length;
        const A = g.declaration;
        let C;
        let B = k - 1;
        l = b - 1;
        for (;l >= 0; --l) {
            r = d[l];
            C = d[l + 1];
            r.nodes.link(C?.nodes ?? f);
            if (-2 === e[l]) {
                o = rr(m, u[l], g, w, x, c, p);
                Qn(o.overrideContext, l, b);
                r.setLocation(f);
                n = r.activate(r, h, 0, o);
                if (I(n)) (i ?? (i = [])).push(n);
            } else if (B < 0 || 1 === k || l !== y[B]) {
                if (p) s.astAssign(A, r.scope, x, u[l]); else r.scope.bindingContext[c] = u[l];
                Qn(r.scope.overrideContext, l, b);
                r.nodes.insertBefore(r.location);
            } else {
                if (p) s.astAssign(A, r.scope, x, u[l]); else r.scope.bindingContext[c] = u[l];
                if (t !== b) Qn(r.scope.overrideContext, l, b);
                --B;
            }
        }
        if (void 0 !== i) return 1 === i.length ? i[0] : Promise.all(i);
    }
    dispose() {
        this.views.forEach(jn);
        this.views = void 0;
    }
    accept(t) {
        const {views: e} = this;
        if (void 0 !== e) for (let s = 0, i = e.length; s < i; ++s) if (true === e[s].accept(t)) return true;
    }
}

Repeat.inject = [ di, s.IExpressionParser, Ss, Xe, De ];

r([ $ ], Repeat.prototype, "items", void 0);

zt("repeat")(Repeat);

let Wn = 16;

let zn = new Int32Array(Wn);

let Gn = new Int32Array(Wn);

function Xn(t) {
    const e = t.length;
    if (e > Wn) {
        Wn = e;
        zn = new Int32Array(e);
        Gn = new Int32Array(e);
    }
    let s = 0;
    let i = 0;
    let n = 0;
    let r = 0;
    let o = 0;
    let l = 0;
    let h = 0;
    let a = 0;
    for (;r < e; r++) {
        i = t[r];
        if (-2 !== i) {
            o = zn[s];
            n = t[o];
            if (-2 !== n && n < i) {
                Gn[r] = o;
                zn[++s] = r;
                continue;
            }
            l = 0;
            h = s;
            while (l < h) {
                a = l + h >> 1;
                n = t[zn[a]];
                if (-2 !== n && n < i) l = a + 1; else h = a;
            }
            n = t[zn[l]];
            if (i < n || -2 === n) {
                if (l > 0) Gn[r] = zn[l - 1];
                zn[l] = r;
            }
        }
    }
    r = ++s;
    const c = new Int32Array(r);
    i = zn[s - 1];
    while (s-- > 0) {
        c[s] = i;
        i = Gn[i];
    }
    while (r-- > 0) zn[r] = 0;
    return c;
}

const Kn = (t, e) => b(`AUR0814:${t}!=${e}`);

const Qn = (t, e, s) => {
    const i = 0 === e;
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

const Yn = v.toString;

const Zn = t => {
    switch (Yn.call(t)) {
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
        throw b(`Cannot count ${Yn.call(t)}`);
    }
};

const Jn = (t, e) => {
    switch (Yn.call(t)) {
      case "[object Array]":
        return tr(t, e);

      case "[object Map]":
        return er(t, e);

      case "[object Set]":
        return sr(t, e);

      case "[object Number]":
        return ir(t, e);

      case "[object Null]":
        return;

      case "[object Undefined]":
        return;

      default:
        throw b(`Cannot iterate over ${Yn.call(t)}`);
    }
};

const tr = (t, e) => {
    const s = t.length;
    let i = 0;
    for (;i < s; ++i) e(t[i], i, t);
};

const er = (t, e) => {
    let s = -0;
    let i;
    for (i of t.entries()) e(i, s++, t);
};

const sr = (t, e) => {
    let s = 0;
    let i;
    for (i of t.keys()) e(i, s++, t);
};

const ir = (t, e) => {
    let s = 0;
    for (;s < t; ++s) e(s, s, t);
};

const nr = (t, e, i, n, r) => {
    let o = t.get(i);
    if (void 0 === o) {
        if ("string" === typeof e) o = i[e]; else o = s.astEvaluate(e, n, r, null);
        t.set(i, o);
    }
    return o;
};

const rr = (t, e, i, n, r, o, l) => {
    let h = t.get(e);
    if (void 0 === h) {
        if (l) s.astAssign(i.declaration, h = s.Scope.fromParent(n, new s.BindingContext), r, e); else h = s.Scope.fromParent(n, new s.BindingContext(o, e));
        t.set(e, h);
    }
    return h;
};

class With {
    constructor(t, e) {
        this.view = t.create().setLocation(e);
    }
    valueChanged(t, e, i) {
        const n = this.$controller;
        const r = this.view.bindings;
        let o;
        let l = 0, h = 0;
        if (n.isActive && null != r) {
            o = s.Scope.fromParent(n.scope, void 0 === t ? {} : t);
            for (h = r.length; h > l; ++l) r[l].bind(o);
        }
    }
    attaching(t, e, i) {
        const {$controller: n, value: r} = this;
        const o = s.Scope.fromParent(n.scope, void 0 === r ? {} : r);
        return this.view.activate(t, n, i, o);
    }
    detaching(t, e, s) {
        return this.view.deactivate(t, this.$controller, s);
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
    }
    accept(t) {
        if (true === this.view?.accept(t)) return true;
    }
}

With.inject = [ De, Ss ];

r([ $ ], With.prototype, "value", void 0);

zt("with")(With);

exports.Switch = class Switch {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.cases = [];
        this.activeCases = [];
        this.promise = void 0;
    }
    link(t, e, s, i) {
        this.view = this.f.create(this.$controller).setLocation(this.l);
    }
    attaching(t, e, s) {
        const i = this.view;
        const n = this.$controller;
        this.queue((() => i.activate(t, n, s, n.scope)));
        this.queue((() => this.swap(t, this.value)));
        return this.promise;
    }
    detaching(t, e, s) {
        this.queue((() => {
            const e = this.view;
            return e.deactivate(t, this.$controller, s);
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
        this.queue((() => this.Zs(t)));
    }
    Zs(e) {
        const s = e.isMatch(this.value);
        const i = this.activeCases;
        const n = i.length;
        if (!s) {
            if (n > 0 && i[0].id === e.id) return this.Js(null);
            return;
        }
        if (n > 0 && i[0].id < e.id) return;
        const r = [];
        let o = e.fallThrough;
        if (!o) r.push(e); else {
            const t = this.cases;
            const s = t.indexOf(e);
            for (let e = s, i = t.length; e < i && o; e++) {
                const s = t[e];
                r.push(s);
                o = s.fallThrough;
            }
        }
        return t.onResolve(this.Js(null, r), (() => {
            this.activeCases = r;
            return this.ti(null);
        }));
    }
    swap(e, s) {
        const i = [];
        let n = false;
        for (const t of this.cases) {
            if (n || t.isMatch(s)) {
                i.push(t);
                n = t.fallThrough;
            }
            if (i.length > 0 && !n) break;
        }
        const r = this.defaultCase;
        if (0 === i.length && void 0 !== r) i.push(r);
        return t.onResolve(this.activeCases.length > 0 ? this.Js(e, i) : void 0, (() => {
            this.activeCases = i;
            if (0 === i.length) return;
            return this.ti(e);
        }));
    }
    ti(e) {
        const s = this.$controller;
        if (!s.isActive) return;
        const i = this.activeCases;
        const n = i.length;
        if (0 === n) return;
        const r = s.scope;
        if (1 === n) return i[0].activate(e, 0, r);
        return t.resolveAll(...i.map((t => t.activate(e, 0, r))));
    }
    Js(e, s = []) {
        const i = this.activeCases;
        const n = i.length;
        if (0 === n) return;
        if (1 === n) {
            const t = i[0];
            if (!s.includes(t)) {
                i.length = 0;
                return t.deactivate(e, 0);
            }
            return;
        }
        return t.onResolve(t.resolveAll(...i.reduce(((t, i) => {
            if (!s.includes(i)) t.push(i.deactivate(e, 0));
            return t;
        }), [])), (() => {
            i.length = 0;
        }));
    }
    queue(e) {
        const s = this.promise;
        let i;
        i = this.promise = t.onResolve(t.onResolve(s, e), (() => {
            if (this.promise === i) this.promise = void 0;
        }));
    }
    accept(t) {
        if (true === this.$controller.accept(t)) return true;
        if (this.activeCases.some((e => e.accept(t)))) return true;
    }
};

r([ $ ], exports.Switch.prototype, "value", void 0);

exports.Switch = r([ zt("switch"), o(0, De), o(1, Ss) ], exports.Switch);

let or = 0;

exports.Case = class Case {
    constructor(t, e, s, i) {
        this.f = t;
        this.ei = e;
        this.l = s;
        this.id = ++or;
        this.fallThrough = false;
        this.view = void 0;
        this.si = i.config.level <= 1;
        this.Ve = i.scopeTo(`${this.constructor.name}-#${this.id}`);
    }
    link(t, e, s, i) {
        const n = t.parent;
        const r = n?.viewModel;
        if (r instanceof exports.Switch) {
            this.$switch = r;
            this.linkToSwitch(r);
        } else throw b(`AUR0815`);
    }
    detaching(t, e, s) {
        return this.deactivate(t, s);
    }
    isMatch(t) {
        this.Ve.debug("isMatch()");
        const e = this.value;
        if (T(e)) {
            if (void 0 === this.Us) this.Us = this.ii(e);
            return e.includes(t);
        }
        return e === t;
    }
    valueChanged(t, e) {
        if (T(t)) {
            this.Us?.unsubscribe(this);
            this.Us = this.ii(t);
        } else if (void 0 !== this.Us) this.Us.unsubscribe(this);
        this.$switch.caseChanged(this);
    }
    handleCollectionChange() {
        this.$switch.caseChanged(this);
    }
    activate(t, e, s) {
        let i = this.view;
        if (void 0 === i) i = this.view = this.f.create().setLocation(this.l);
        if (i.isActive) return;
        return i.activate(t ?? i, this.$controller, e, s);
    }
    deactivate(t, e) {
        const s = this.view;
        if (void 0 === s || !s.isActive) return;
        return s.deactivate(t ?? s, this.$controller, e);
    }
    dispose() {
        this.Us?.unsubscribe(this);
        this.view?.dispose();
        this.view = void 0;
    }
    linkToSwitch(t) {
        t.cases.push(this);
    }
    ii(t) {
        const e = this.ei.getArrayObserver(t);
        e.subscribe(this);
        return e;
    }
    accept(t) {
        if (true === this.$controller.accept(t)) return true;
        return this.view?.accept(t);
    }
};

exports.Case.inject = [ De, s.IObserverLocator, Ss, t.ILogger ];

r([ $ ], exports.Case.prototype, "value", void 0);

r([ $({
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
}) ], exports.Case.prototype, "fallThrough", void 0);

exports.Case = r([ zt("case") ], exports.Case);

exports.DefaultCase = class DefaultCase extends exports.Case {
    linkToSwitch(t) {
        if (void 0 !== t.defaultCase) throw b(`AUR0816`);
        t.defaultCase = this;
    }
};

exports.DefaultCase = r([ zt("default-case") ], exports.DefaultCase);

exports.PromiseTemplateController = class PromiseTemplateController {
    constructor(t, e, s, i) {
        this.f = t;
        this.l = e;
        this.p = s;
        this.preSettledTask = null;
        this.postSettledTask = null;
        this.logger = i.scopeTo("promise.resolve");
    }
    link(t, e, s, i) {
        this.view = this.f.create(this.$controller).setLocation(this.l);
    }
    attaching(e, i, n) {
        const r = this.view;
        const o = this.$controller;
        return t.onResolve(r.activate(e, o, n, this.viewScope = s.Scope.fromParent(o.scope, {})), (() => this.swap(e, n)));
    }
    valueChanged(t, e, s) {
        if (!this.$controller.isActive) return;
        this.swap(null, s);
    }
    swap(e, s) {
        const n = this.value;
        if (!I(n)) {
            this.logger.warn(`The value '${String(n)}' is not a promise. No change will be done.`);
            return;
        }
        const r = this.p.domWriteQueue;
        const o = this.fulfilled;
        const l = this.rejected;
        const h = this.pending;
        const a = this.viewScope;
        let c;
        const u = {
            reusable: false
        };
        const f = () => {
            void t.resolveAll(c = (this.preSettledTask = r.queueTask((() => t.resolveAll(o?.deactivate(e, s), l?.deactivate(e, s), h?.activate(e, s, a))), u)).result.catch((t => {
                if (!(t instanceof i.TaskAbortError)) throw t;
            })), n.then((i => {
                if (this.value !== n) return;
                const f = () => {
                    this.postSettlePromise = (this.postSettledTask = r.queueTask((() => t.resolveAll(h?.deactivate(e, s), l?.deactivate(e, s), o?.activate(e, s, a, i))), u)).result;
                };
                if (1 === this.preSettledTask.status) void c.then(f); else {
                    this.preSettledTask.cancel();
                    f();
                }
            }), (i => {
                if (this.value !== n) return;
                const f = () => {
                    this.postSettlePromise = (this.postSettledTask = r.queueTask((() => t.resolveAll(h?.deactivate(e, s), o?.deactivate(e, s), l?.activate(e, s, a, i))), u)).result;
                };
                if (1 === this.preSettledTask.status) void c.then(f); else {
                    this.preSettledTask.cancel();
                    f();
                }
            })));
        };
        if (1 === this.postSettledTask?.status) void this.postSettlePromise.then(f); else {
            this.postSettledTask?.cancel();
            f();
        }
    }
    detaching(t, e, s) {
        this.preSettledTask?.cancel();
        this.postSettledTask?.cancel();
        this.preSettledTask = this.postSettledTask = null;
        return this.view.deactivate(t, this.$controller, s);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

r([ $ ], exports.PromiseTemplateController.prototype, "value", void 0);

exports.PromiseTemplateController = r([ zt("promise"), o(0, De), o(1, Ss), o(2, ue), o(3, t.ILogger) ], exports.PromiseTemplateController);

exports.PendingTemplateController = class PendingTemplateController {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.view = void 0;
    }
    link(t, e, s, i) {
        lr(t).pending = this;
    }
    activate(t, e, s) {
        let i = this.view;
        if (void 0 === i) i = this.view = this.f.create().setLocation(this.l);
        if (i.isActive) return;
        return i.activate(i, this.$controller, e, s);
    }
    deactivate(t, e) {
        const s = this.view;
        if (void 0 === s || !s.isActive) return;
        return s.deactivate(s, this.$controller, e);
    }
    detaching(t, e, s) {
        return this.deactivate(t, s);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

r([ $({
    mode: 2
}) ], exports.PendingTemplateController.prototype, "value", void 0);

exports.PendingTemplateController = r([ zt("pending"), o(0, De), o(1, Ss) ], exports.PendingTemplateController);

exports.FulfilledTemplateController = class FulfilledTemplateController {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.view = void 0;
    }
    link(t, e, s, i) {
        lr(t).fulfilled = this;
    }
    activate(t, e, s, i) {
        this.value = i;
        let n = this.view;
        if (void 0 === n) n = this.view = this.f.create().setLocation(this.l);
        if (n.isActive) return;
        return n.activate(n, this.$controller, e, s);
    }
    deactivate(t, e) {
        const s = this.view;
        if (void 0 === s || !s.isActive) return;
        return s.deactivate(s, this.$controller, e);
    }
    detaching(t, e, s) {
        return this.deactivate(t, s);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

r([ $({
    mode: 4
}) ], exports.FulfilledTemplateController.prototype, "value", void 0);

exports.FulfilledTemplateController = r([ zt("then"), o(0, De), o(1, Ss) ], exports.FulfilledTemplateController);

exports.RejectedTemplateController = class RejectedTemplateController {
    constructor(t, e) {
        this.f = t;
        this.l = e;
        this.view = void 0;
    }
    link(t, e, s, i) {
        lr(t).rejected = this;
    }
    activate(t, e, s, i) {
        this.value = i;
        let n = this.view;
        if (void 0 === n) n = this.view = this.f.create().setLocation(this.l);
        if (n.isActive) return;
        return n.activate(n, this.$controller, e, s);
    }
    deactivate(t, e) {
        const s = this.view;
        if (void 0 === s || !s.isActive) return;
        return s.deactivate(s, this.$controller, e);
    }
    detaching(t, e, s) {
        return this.deactivate(t, s);
    }
    dispose() {
        this.view?.dispose();
        this.view = void 0;
    }
};

r([ $({
    mode: 4
}) ], exports.RejectedTemplateController.prototype, "value", void 0);

exports.RejectedTemplateController = r([ zt("catch"), o(0, De), o(1, Ss) ], exports.RejectedTemplateController);

function lr(t) {
    const e = t.parent;
    const s = e?.viewModel;
    if (s instanceof exports.PromiseTemplateController) return s;
    throw b(`AUR0813`);
}

let hr = class PromiseAttributePattern {
    "promise.resolve"(t, e, s) {
        return new AttrSyntax(t, e, "promise", "bind");
    }
};

hr = r([ ot({
    pattern: "promise.resolve",
    symbols: ""
}) ], hr);

let ar = class FulfilledAttributePattern {
    then(t, e, s) {
        return new AttrSyntax(t, e, "then", "from-view");
    }
};

ar = r([ ot({
    pattern: "then",
    symbols: ""
}) ], ar);

let cr = class RejectedAttributePattern {
    catch(t, e, s) {
        return new AttrSyntax(t, e, "catch", "from-view");
    }
};

cr = r([ ot({
    pattern: "catch",
    symbols: ""
}) ], cr);

class AuCompose {
    static get inject() {
        return [ t.IContainer, Xe, Bs, Ss, ue, di, t.transient(CompositionContextFactory) ];
    }
    get pending() {
        return this.ni;
    }
    get composition() {
        return this.ri;
    }
    constructor(t, e, s, i, n, r, o) {
        this.c = t;
        this.parent = e;
        this.host = s;
        this.l = i;
        this.p = n;
        this.scopeBehavior = "auto";
        this.ri = void 0;
        this.r = t.get(_e);
        this.oi = r;
        this.li = o;
    }
    attaching(e, s, i) {
        return this.ni = t.onResolve(this.queue(new ChangeInfo(this.template, this.component, this.model, void 0), e), (t => {
            if (this.li.isCurrent(t)) this.ni = void 0;
        }));
    }
    detaching(e) {
        const s = this.ri;
        const i = this.ni;
        this.li.invalidate();
        this.ri = this.ni = void 0;
        return t.onResolve(i, (() => s?.deactivate(e)));
    }
    propertyChanged(e) {
        if ("model" === e && null != this.ri) {
            this.ri.update(this.model);
            return;
        }
        this.ni = t.onResolve(this.ni, (() => t.onResolve(this.queue(new ChangeInfo(this.template, this.component, this.model, e), void 0), (t => {
            if (this.li.isCurrent(t)) this.ni = void 0;
        }))));
    }
    queue(e, s) {
        const i = this.li;
        const n = this.ri;
        return t.onResolve(i.create(e), (e => {
            if (i.isCurrent(e)) return t.onResolve(this.compose(e), (r => {
                if (i.isCurrent(e)) return t.onResolve(r.activate(s), (() => {
                    if (i.isCurrent(e)) {
                        this.ri = r;
                        return t.onResolve(n?.deactivate(s), (() => e));
                    } else return t.onResolve(r.controller.deactivate(r.controller, this.$controller, 2), (() => {
                        r.controller.dispose();
                        return e;
                    }));
                }));
                r.controller.dispose();
                return e;
            }));
            return e;
        }));
    }
    compose(e) {
        let i;
        let n;
        let r;
        const {we: o, hi: l, ai: h} = e.change;
        const {c: a, host: c, $controller: u, l: f} = this;
        const d = this.getDef(l);
        const p = a.createChild();
        const x = null == f ? c.parentNode : f.parentNode;
        if (null !== d) {
            if (d.containerless) throw b(`AUR0806`);
            if (null == f) {
                n = c;
                r = () => {};
            } else {
                n = x.insertBefore(this.p.document.createElement(d.name), f);
                r = () => {
                    n.remove();
                };
            }
            i = this.ui(p, l, n);
        } else {
            n = null == f ? c : f;
            i = this.ui(p, l, n);
        }
        const m = () => {
            if (null !== d) {
                const s = Controller.$el(p, i, n, {
                    projections: this.oi.projections
                }, d);
                return new CompositionController(s, (t => s.activate(t ?? s, u, 1, u.scope.parent)), (e => t.onResolve(s.deactivate(e ?? s, u, 2), r)), (t => i.activate?.(t)), e);
            } else {
                const t = CustomElementDefinition.create({
                    name: oi.generateName(),
                    template: o
                });
                const r = this.r.getViewFactory(t, p);
                const l = Controller.$view(r, u);
                const h = "auto" === this.scopeBehavior ? s.Scope.fromParent(this.parent.scope, i) : s.Scope.create(i);
                if (Ds(n)) l.setLocation(n); else l.setHost(n);
                return new CompositionController(l, (t => l.activate(t ?? l, u, 1, h)), (t => l.deactivate(t ?? l, u, 2)), (t => i.activate?.(t)), e);
            }
        };
        if ("activate" in i) return t.onResolve(i.activate(h), (() => m())); else return m();
    }
    ui(e, s, i) {
        if (null == s) return new EmptyComponent;
        if ("object" === typeof s) return s;
        const n = this.p;
        const r = Ds(i);
        Z(e, n.Element, Z(e, Bs, new t.InstanceProvider("ElementResolver", r ? null : i)));
        Z(e, Ss, new t.InstanceProvider("IRenderLocation", r ? i : null));
        const o = e.invoke(s);
        Z(e, s, new t.InstanceProvider("au-compose.component", o));
        return o;
    }
    getDef(t) {
        const e = P(t) ? t : t?.constructor;
        return oi.isType(e) ? oi.getDefinition(e) : null;
    }
}

r([ $ ], AuCompose.prototype, "template", void 0);

r([ $ ], AuCompose.prototype, "component", void 0);

r([ $ ], AuCompose.prototype, "model", void 0);

r([ $({
    set: t => {
        if ("scoped" === t || "auto" === t) return t;
        throw b(`AUR0805`);
    }
}) ], AuCompose.prototype, "scopeBehavior", void 0);

$s("au-compose")(AuCompose);

class EmptyComponent {}

class CompositionContextFactory {
    constructor() {
        this.id = 0;
    }
    isCurrent(t) {
        return t.id === this.id;
    }
    create(e) {
        return t.onResolve(e.load(), (t => new CompositionContext(++this.id, t)));
    }
    invalidate() {
        this.id++;
    }
}

class ChangeInfo {
    constructor(t, e, s, i) {
        this.we = t;
        this.hi = e;
        this.ai = s;
        this.fi = i;
    }
    load() {
        if (I(this.we) || I(this.hi)) return Promise.all([ this.we, this.hi ]).then((([t, e]) => new LoadedChangeInfo(t, e, this.ai, this.fi))); else return new LoadedChangeInfo(this.we, this.hi, this.ai, this.fi);
    }
}

class LoadedChangeInfo {
    constructor(t, e, s, i) {
        this.we = t;
        this.hi = e;
        this.ai = s;
        this.fi = i;
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
        if (0 !== this.state) throw b(`AUR0807:${this.controller.name}`);
        this.state = 1;
        return this.start(t);
    }
    deactivate(t) {
        switch (this.state) {
          case 1:
            this.state = -1;
            return this.stop(t);

          case -1:
            throw b(`AUR0808`);

          default:
            this.state = -1;
        }
    }
}

exports.AuSlot = class AuSlot {
    static get inject() {
        return [ Ss, di, Ke, _e ];
    }
    constructor(e, s, i, n) {
        this.di = null;
        this.pi = null;
        let r;
        let o;
        const l = s.auSlot;
        const h = i.instruction?.projections?.[l.name];
        if (null == h) {
            r = n.getViewFactory(l.fallback, i.controller.container);
            this.xi = false;
        } else {
            o = i.parent.controller.container.createChild();
            Z(o, i.controller.definition.Type, new t.InstanceProvider(void 0, i.controller.viewModel));
            r = n.getViewFactory(h, o);
            this.xi = true;
        }
        this.mi = i;
        this.view = r.create().setLocation(e);
    }
    binding(t, e, i) {
        this.di = this.$controller.scope.parent;
        let n;
        if (this.xi) {
            n = this.mi.controller.scope.parent;
            (this.pi = s.Scope.fromParent(n, n.bindingContext)).overrideContext.$host = this.expose ?? this.di.bindingContext;
        }
    }
    attaching(t, e, s) {
        return this.view.activate(t, this.$controller, s, this.xi ? this.pi : this.di);
    }
    detaching(t, e, s) {
        return this.view.deactivate(t, this.$controller, s);
    }
    exposeChanged(t) {
        if (this.xi && null != this.pi) this.pi.overrideContext.$host = t;
    }
    dispose() {
        this.view.dispose();
        this.view = void 0;
    }
    accept(t) {
        if (true === this.view?.accept(t)) return true;
    }
};

r([ $ ], exports.AuSlot.prototype, "expose", void 0);

exports.AuSlot = r([ $s({
    name: "au-slot",
    template: null,
    containerless: true
}) ], exports.AuSlot);

const ur = G("ISanitizer", (t => t.singleton(class {
    sanitize() {
        throw b('"sanitize" method not implemented');
    }
})));

exports.SanitizeValueConverter = class SanitizeValueConverter {
    constructor(t) {
        this.gi = t;
    }
    toView(t) {
        if (null == t) return null;
        return this.gi.sanitize(t);
    }
};

exports.SanitizeValueConverter = r([ o(0, ur) ], exports.SanitizeValueConverter);

yt("sanitize")(exports.SanitizeValueConverter);

const fr = DebounceBindingBehavior;

const dr = OneTimeBindingBehavior;

const pr = ToViewBindingBehavior;

const xr = FromViewBindingBehavior;

const mr = SignalBindingBehavior;

const gr = ThrottleBindingBehavior;

const vr = TwoWayBindingBehavior;

const wr = TemplateCompiler;

const br = NodeObserverLocator;

const yr = [ wr, br ];

const kr = SVGAnalyzer;

const Ar = exports.AtPrefixedTriggerAttributePattern;

const Cr = exports.ColonPrefixedBindAttributePattern;

const Br = exports.RefAttributePattern;

const Rr = exports.DotSeparatedAttributePattern;

const Sr = ut;

const Ir = [ Br, Rr, Sr ];

const Tr = [ Ar, Cr ];

const Pr = exports.DefaultBindingCommand;

const Er = exports.ForBindingCommand;

const Lr = exports.FromViewBindingCommand;

const Dr = exports.OneTimeBindingCommand;

const _r = exports.ToViewBindingCommand;

const qr = exports.TwoWayBindingCommand;

const Ur = $i;

const $r = exports.TriggerBindingCommand;

const Mr = exports.CaptureBindingCommand;

const Fr = exports.AttrBindingCommand;

const Or = exports.ClassBindingCommand;

const Vr = exports.StyleBindingCommand;

const Nr = Mi;

const jr = [ Pr, Dr, Lr, _r, qr, Er, Ur, $r, Mr, Or, Vr, Fr, Nr ];

const Hr = exports.SanitizeValueConverter;

const Wr = If;

const zr = Else;

const Gr = Repeat;

const Xr = With;

const Kr = exports.Switch;

const Qr = exports.Case;

const Yr = exports.DefaultCase;

const Zr = exports.PromiseTemplateController;

const Jr = exports.PendingTemplateController;

const to = exports.FulfilledTemplateController;

const eo = exports.RejectedTemplateController;

const so = hr;

const io = ar;

const no = cr;

const ro = SelfBindingBehavior;

const oo = UpdateTriggerBindingBehavior;

const lo = AuCompose;

const ho = Portal;

const ao = Focus;

const co = Nn;

const uo = [ fr, dr, pr, xr, mr, gr, vr, Hr, Wr, zr, Gr, Xr, Kr, Qr, Yr, Zr, Jr, to, eo, so, io, no, AttrBindingBehavior, ro, oo, lo, ho, ao, co, exports.AuSlot ];

const fo = [ exports.PropertyBindingRenderer, exports.IteratorBindingRenderer, exports.RefBindingRenderer, exports.InterpolationBindingRenderer, exports.SetPropertyRenderer, exports.CustomElementRenderer, exports.CustomAttributeRenderer, exports.TemplateControllerRenderer, exports.LetElementRenderer, exports.ListenerBindingRenderer, exports.AttributeBindingRenderer, exports.SetAttributeRenderer, exports.SetClassAttributeRenderer, exports.SetStyleAttributeRenderer, exports.StylePropertyBindingRenderer, exports.TextBindingRenderer, exports.SpreadRenderer ];

const po = xo(t.noop);

function xo(t) {
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
            return e.register(Q(s.ICoercionConfiguration, i.coercingOptions), ...yr, ...uo, ...Ir, ...jr, ...fo);
        },
        customize(e) {
            return xo(e ?? t);
        }
    };
}

const mo = G("IAurelia");

class Aurelia {
    get isRunning() {
        return this.ir;
    }
    get isStarting() {
        return this.vi;
    }
    get isStopping() {
        return this.wi;
    }
    get root() {
        if (null == this.bi) {
            if (null == this.next) throw b(`AUR0767`);
            return this.next;
        }
        return this.bi;
    }
    constructor(e = t.DI.createContainer()) {
        this.container = e;
        this.ir = false;
        this.vi = false;
        this.wi = false;
        this.bi = void 0;
        this.next = void 0;
        this.yi = void 0;
        this.ki = void 0;
        if (e.has(mo, true)) throw b(`AUR0768`);
        Z(e, mo, new t.InstanceProvider("IAurelia", this));
        Z(e, as, this.Ai = new t.InstanceProvider("IAppRoot"));
    }
    register(...t) {
        this.container.register(...t);
        return this;
    }
    app(t) {
        this.next = new AppRoot(t, this.Ci(t.host), this.container, this.Ai);
        return this;
    }
    enhance(e, s) {
        const i = e.container ?? this.container.createChild();
        const n = e.host;
        const r = this.Ci(n);
        const o = e.component;
        let l;
        if (P(o)) {
            Z(i, r.HTMLElement, Z(i, r.Element, Z(i, Bs, new t.InstanceProvider("ElementResolver", n))));
            l = i.invoke(o);
        } else l = o;
        Z(i, Rs, new t.InstanceProvider("IEventTarget", n));
        s = s ?? null;
        const h = Controller.$el(i, l, n, null, CustomElementDefinition.create({
            name: Ys(),
            template: n,
            enhance: true
        }));
        return t.onResolve(h.activate(h, s, 1), (() => h));
    }
    async waitForIdle() {
        const t = this.root.platform;
        await t.domWriteQueue.yield();
        await t.domReadQueue.yield();
        await t.taskQueue.yield();
    }
    Ci(t) {
        let e;
        if (!this.container.has(ue, false)) {
            if (null === t.ownerDocument.defaultView) throw b(`AUR0769`);
            e = new n.BrowserPlatform(t.ownerDocument.defaultView);
            this.container.register(Q(ue, e));
        } else e = this.container.get(ue);
        return e;
    }
    start(e = this.next) {
        if (null == e) throw b(`AUR0770`);
        if (I(this.yi)) return this.yi;
        return this.yi = t.onResolve(this.stop(), (() => {
            Reflect.set(e.host, "$aurelia", this);
            this.Ai.prepare(this.bi = e);
            this.vi = true;
            return t.onResolve(e.activate(), (() => {
                this.ir = true;
                this.vi = false;
                this.yi = void 0;
                this.Bi(e, "au-started", e.host);
            }));
        }));
    }
    stop(e = false) {
        if (I(this.ki)) return this.ki;
        if (true === this.ir) {
            const s = this.bi;
            this.ir = false;
            this.wi = true;
            return this.ki = t.onResolve(s.deactivate(), (() => {
                Reflect.deleteProperty(s.host, "$aurelia");
                if (e) s.dispose();
                this.bi = void 0;
                this.Ai.dispose();
                this.wi = false;
                this.Bi(s, "au-stopped", s.host);
            }));
        }
    }
    dispose() {
        if (this.ir || this.wi) throw b(`AUR0771`);
        this.container.dispose();
    }
    Bi(t, e, s) {
        const i = new t.platform.window.CustomEvent(e, {
            detail: this,
            bubbles: true,
            cancelable: true
        });
        s.dispatchEvent(i);
    }
}

exports.BindingMode = void 0;

(function(t) {
    t[t["oneTime"] = 1] = "oneTime";
    t[t["toView"] = 2] = "toView";
    t[t["fromView"] = 4] = "fromView";
    t[t["twoWay"] = 6] = "twoWay";
    t[t["default"] = 8] = "default";
})(exports.BindingMode || (exports.BindingMode = {}));

exports.DefinitionType = void 0;

(function(t) {
    t[t["Element"] = 1] = "Element";
    t[t["Attribute"] = 2] = "Attribute";
})(exports.DefinitionType || (exports.DefinitionType = {}));

exports.AdoptedStyleSheetsStyles = AdoptedStyleSheetsStyles;

exports.AppRoot = AppRoot;

exports.AppTask = Ft;

exports.AtPrefixedTriggerAttributePatternRegistration = Ar;

exports.AttrBindingBehavior = AttrBindingBehavior;

exports.AttrBindingCommandRegistration = Fr;

exports.AttrSyntax = AttrSyntax;

exports.AttributeBinding = AttributeBinding;

exports.AttributeBindingInstruction = AttributeBindingInstruction;

exports.AttributeNSAccessor = AttributeNSAccessor;

exports.AttributePattern = ct;

exports.AuCompose = AuCompose;

exports.AuSlotsInfo = AuSlotsInfo;

exports.Aurelia = Aurelia;

exports.Bindable = O;

exports.BindableDefinition = BindableDefinition;

exports.BindableObserver = BindableObserver;

exports.BindablesInfo = BindablesInfo;

exports.BindingBehavior = bt;

exports.BindingBehaviorDefinition = BindingBehaviorDefinition;

exports.BindingCommand = Ui;

exports.BindingCommandDefinition = BindingCommandDefinition;

exports.BindingModeBehavior = BindingModeBehavior;

exports.BindingTargetSubscriber = BindingTargetSubscriber;

exports.CSSModulesProcessorRegistry = CSSModulesProcessorRegistry;

exports.CaptureBindingCommandRegistration = Mr;

exports.CheckedObserver = CheckedObserver;

exports.Children = ne;

exports.ChildrenDefinition = ChildrenDefinition;

exports.ChildrenObserver = ChildrenObserver;

exports.ClassAttributeAccessor = ClassAttributeAccessor;

exports.ClassBindingCommandRegistration = Or;

exports.ColonPrefixedBindAttributePatternRegistration = Cr;

exports.ComputedWatcher = ComputedWatcher;

exports.ContentBinding = ContentBinding;

exports.Controller = Controller;

exports.CustomAttribute = te;

exports.CustomAttributeDefinition = CustomAttributeDefinition;

exports.CustomElement = oi;

exports.CustomElementDefinition = CustomElementDefinition;

exports.DataAttributeAccessor = DataAttributeAccessor;

exports.DebounceBindingBehavior = DebounceBindingBehavior;

exports.DebounceBindingBehaviorRegistration = fr;

exports.DefaultBindingCommandRegistration = Pr;

exports.DefaultBindingLanguage = jr;

exports.DefaultBindingSyntax = Ir;

exports.DefaultComponents = yr;

exports.DefaultRenderers = fo;

exports.DefaultResources = uo;

exports.DotSeparatedAttributePatternRegistration = Rr;

exports.Else = Else;

exports.ElseRegistration = zr;

exports.ExpressionWatcher = ExpressionWatcher;

exports.FlushQueue = FlushQueue;

exports.Focus = Focus;

exports.ForBindingCommandRegistration = Er;

exports.FragmentNodeSequence = FragmentNodeSequence;

exports.FromViewBindingBehavior = FromViewBindingBehavior;

exports.FromViewBindingBehaviorRegistration = xr;

exports.FromViewBindingCommandRegistration = Lr;

exports.HooksDefinition = HooksDefinition;

exports.HydrateAttributeInstruction = HydrateAttributeInstruction;

exports.HydrateElementInstruction = HydrateElementInstruction;

exports.HydrateLetElementInstruction = HydrateLetElementInstruction;

exports.HydrateTemplateController = HydrateTemplateController;

exports.IAppRoot = as;

exports.IAppTask = Mt;

exports.IAttrMapper = Vi;

exports.IAttributeParser = rt;

exports.IAttributePattern = nt;

exports.IAuSlotsInfo = fi;

exports.IAurelia = mo;

exports.IController = Xe;

exports.IEventTarget = Rs;

exports.IFlushQueue = It;

exports.IHistory = Us;

exports.IHydrationContext = Ke;

exports.IInstruction = di;

exports.ILifecycleHooks = Ie;

exports.ILocation = qs;

exports.INode = Bs;

exports.INodeObserverLocatorRegistration = br;

exports.IPlatform = ue;

exports.IProjections = ui;

exports.IRenderLocation = Ss;

exports.IRenderer = mi;

exports.IRendering = _e;

exports.ISVGAnalyzer = Fi;

exports.ISanitizer = ur;

exports.IShadowDOMGlobalStyles = ke;

exports.IShadowDOMStyles = ye;

exports.ISyntaxInterpreter = et;

exports.ITemplateCompiler = xi;

exports.ITemplateCompilerHooks = dn;

exports.ITemplateCompilerRegistration = wr;

exports.ITemplateElementFactory = Hi;

exports.IViewFactory = De;

exports.IWindow = _s;

exports.If = If;

exports.IfRegistration = Wr;

exports.InterpolationBinding = InterpolationBinding;

exports.InterpolationInstruction = InterpolationInstruction;

exports.InterpolationPartBinding = InterpolationPartBinding;

exports.Interpretation = Interpretation;

exports.IteratorBindingInstruction = IteratorBindingInstruction;

exports.LetBinding = LetBinding;

exports.LetBindingInstruction = LetBindingInstruction;

exports.LifecycleHooks = Ee;

exports.LifecycleHooksDefinition = LifecycleHooksDefinition;

exports.LifecycleHooksEntry = LifecycleHooksEntry;

exports.ListenerBinding = ListenerBinding;

exports.ListenerBindingInstruction = ListenerBindingInstruction;

exports.ListenerBindingOptions = ListenerBindingOptions;

exports.MultiAttrInstruction = MultiAttrInstruction;

exports.NodeObserverLocator = NodeObserverLocator;

exports.NoopSVGAnalyzer = NoopSVGAnalyzer;

exports.OneTimeBindingBehavior = OneTimeBindingBehavior;

exports.OneTimeBindingBehaviorRegistration = dr;

exports.OneTimeBindingCommandRegistration = Dr;

exports.Portal = Portal;

exports.PropertyBinding = PropertyBinding;

exports.PropertyBindingInstruction = PropertyBindingInstruction;

exports.RefAttributePatternRegistration = Br;

exports.RefBinding = RefBinding;

exports.RefBindingCommandRegistration = Ur;

exports.RefBindingInstruction = RefBindingInstruction;

exports.Rendering = Rendering;

exports.Repeat = Repeat;

exports.RepeatRegistration = Gr;

exports.SVGAnalyzer = SVGAnalyzer;

exports.SVGAnalyzerRegistration = kr;

exports.SanitizeValueConverterRegistration = Hr;

exports.SelectValueObserver = SelectValueObserver;

exports.SelfBindingBehavior = SelfBindingBehavior;

exports.SelfBindingBehaviorRegistration = ro;

exports.SetAttributeInstruction = SetAttributeInstruction;

exports.SetClassAttributeInstruction = SetClassAttributeInstruction;

exports.SetPropertyInstruction = SetPropertyInstruction;

exports.SetStyleAttributeInstruction = SetStyleAttributeInstruction;

exports.ShadowDOMRegistry = ShadowDOMRegistry;

exports.ShortHandBindingSyntax = Tr;

exports.SignalBindingBehavior = SignalBindingBehavior;

exports.SignalBindingBehaviorRegistration = mr;

exports.SpreadBindingInstruction = SpreadBindingInstruction;

exports.SpreadElementPropBindingInstruction = SpreadElementPropBindingInstruction;

exports.StandardConfiguration = po;

exports.StyleAttributeAccessor = StyleAttributeAccessor;

exports.StyleBindingCommandRegistration = Vr;

exports.StyleConfiguration = Ae;

exports.StyleElementStyles = StyleElementStyles;

exports.StylePropertyBindingInstruction = StylePropertyBindingInstruction;

exports.TemplateCompiler = TemplateCompiler;

exports.TemplateCompilerHooks = mn;

exports.TextBindingInstruction = TextBindingInstruction;

exports.ThrottleBindingBehavior = ThrottleBindingBehavior;

exports.ThrottleBindingBehaviorRegistration = gr;

exports.ToViewBindingBehavior = ToViewBindingBehavior;

exports.ToViewBindingBehaviorRegistration = pr;

exports.ToViewBindingCommandRegistration = _r;

exports.TriggerBindingCommandRegistration = $r;

exports.TwoWayBindingBehavior = TwoWayBindingBehavior;

exports.TwoWayBindingBehaviorRegistration = vr;

exports.TwoWayBindingCommandRegistration = qr;

exports.UpdateTriggerBindingBehavior = UpdateTriggerBindingBehavior;

exports.UpdateTriggerBindingBehaviorRegistration = oo;

exports.ValueAttributeObserver = ValueAttributeObserver;

exports.ValueConverter = Ct;

exports.ValueConverterDefinition = ValueConverterDefinition;

exports.ViewFactory = ViewFactory;

exports.Watch = Ht;

exports.With = With;

exports.WithRegistration = Xr;

exports.alias = J;

exports.allResources = z;

exports.attributePattern = ot;

exports.bindable = $;

exports.bindingBehavior = gt;

exports.bindingCommand = Li;

exports.capture = ci;

exports.children = ee;

exports.coercer = V;

exports.containerless = Fs;

exports.convertToRenderLocation = Ls;

exports.cssModules = ve;

exports.customAttribute = Wt;

exports.customElement = $s;

exports.getEffectiveParentNode = Ps;

exports.getRef = As;

exports.isCustomElementController = He;

exports.isCustomElementViewModel = We;

exports.isInstruction = pi;

exports.isRenderLocation = Ds;

exports.lifecycleHooks = Le;

exports.mixinAstEvaluator = Rt;

exports.mixinUseScope = Bt;

exports.mixingBindingLimited = Et;

exports.processContent = hi;

exports.registerAliases = tt;

exports.renderer = gi;

exports.setEffectiveParentNode = Es;

exports.setRef = Cs;

exports.shadowCSS = we;

exports.strict = Vs;

exports.templateCompilerHooks = gn;

exports.templateController = zt;

exports.useShadowDOM = Ms;

exports.valueConverter = yt;

exports.watch = Vt;
//# sourceMappingURL=index.cjs.map
