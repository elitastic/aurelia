import { DI as t, IEventAggregator as n, toArray as e, camelCase as i, Registration as s } from "../kernel/dist/native-modules/index.mjs";

import { bindingBehavior as r, valueConverter as o, mixinAstEvaluator as a, mixingBindingLimited as c, CustomElement as l, attributePattern as h, bindingCommand as u, renderer as f, AttrSyntax as d, AttributePattern as m, BindingCommand as p, AppTask as g } from "../runtime-html/dist/native-modules/index.mjs";

import { ValueConverterExpression as _, nowrap as b, ISignaler as T, connectable as B, CustomExpression as v, Interpolation as w, astEvaluate as I, astUnbind as C, astBind as x } from "../runtime/dist/native-modules/index.mjs";

import y from "i18next";

function __decorate(t, n, e, i) {
    var s = arguments.length, r = s < 3 ? n : i === null ? i = Object.getOwnPropertyDescriptor(n, e) : i, o;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(t, n, e, i); else for (var a = t.length - 1; a >= 0; a--) if (o = t[a]) r = (s < 3 ? o(r) : s > 3 ? o(n, e, r) : o(n, e)) || r;
    return s > 3 && r && Object.defineProperty(n, e, r), r;
}

function __param(t, n) {
    return function(e, i) {
        n(e, i, t);
    };
}

const P = {
    I18N_EA_CHANNEL: "i18n:locale:changed",
    I18N_SIGNAL: "aurelia-translation-signal",
    RT_SIGNAL: "aurelia-relativetime-signal"
};

var M;

(function(t) {
    t["translationValueConverterName"] = "t";
    t["dateFormatValueConverterName"] = "df";
    t["numberFormatValueConverterName"] = "nf";
    t["relativeTimeValueConverterName"] = "rt";
})(M || (M = {}));

function createIntlFormatValueConverterExpression(t, n) {
    const e = n.ast.expression;
    if (!(e instanceof _)) {
        const i = new _(e, t, n.ast.args);
        n.ast.expression = i;
    }
}

let A = class DateFormatBindingBehavior {
    bind(t, n) {
        createIntlFormatValueConverterExpression("df", n);
    }
};

A = __decorate([ r("df") ], A);

const L = /*@__PURE__*/ t.createInterface("I18nInitOptions");

const E = /*@__PURE__*/ t.createInterface("I18nextWrapper");

class I18nextWrapper {
    constructor() {
        this.i18next = y;
    }
}

var R;

(function(t) {
    t[t["Second"] = 1e3] = "Second";
    t[t["Minute"] = 6e4] = "Minute";
    t[t["Hour"] = 36e5] = "Hour";
    t[t["Day"] = 864e5] = "Day";
    t[t["Week"] = 6048e5] = "Week";
    t[t["Month"] = 2592e6] = "Month";
    t[t["Year"] = 31536e6] = "Year";
})(R || (R = {}));

class I18nKeyEvaluationResult {
    constructor(t) {
        this.value = void 0;
        const n = /\[([a-z\-, ]*)\]/gi;
        this.attributes = [];
        const e = n.exec(t);
        if (e) {
            t = t.replace(e[0], "");
            this.attributes = e[1].split(",");
        }
        this.key = t;
    }
}

const N = /*@__PURE__*/ t.createInterface("I18N");

let V = class I18nService {
    constructor(t, n, e, i) {
        this.ea = e;
        this.i = new Set;
        this.i18next = t.i18next;
        this.initPromise = this.h(n);
        this.u = i;
    }
    evaluate(t, n) {
        const e = t.split(";");
        const i = [];
        for (const t of e) {
            const e = new I18nKeyEvaluationResult(t);
            const s = e.key;
            const r = this.tr(s, n);
            if (this.options.skipTranslationOnMissingKey && r === s) {
                console.warn(`Couldn't find translation for key: ${s}`);
            } else {
                e.value = r;
                i.push(e);
            }
        }
        return i;
    }
    tr(t, n) {
        return this.i18next.t(t, n);
    }
    getLocale() {
        return this.i18next.language;
    }
    async setLocale(t) {
        const n = this.getLocale();
        const e = {
            oldLocale: n,
            newLocale: t
        };
        await this.i18next.changeLanguage(t);
        this.ea.publish(P.I18N_EA_CHANNEL, e);
        this.i.forEach((t => t.handleLocaleChange(e)));
        this.u.dispatchSignal(P.I18N_SIGNAL);
    }
    createNumberFormat(t, n) {
        return Intl.NumberFormat(n || this.getLocale(), t);
    }
    nf(t, n, e) {
        return this.createNumberFormat(n, e).format(t);
    }
    createDateTimeFormat(t, n) {
        return Intl.DateTimeFormat(n || this.getLocale(), t);
    }
    df(t, n, e) {
        return this.createDateTimeFormat(n, e).format(t);
    }
    uf(t, n) {
        const e = this.nf(1e4 / 3, undefined, n);
        let i = e[1];
        const s = e[5];
        if (i === ".") {
            i = "\\.";
        }
        const r = t.replace(new RegExp(i, "g"), "").replace(/[^\d.,-]/g, "").replace(s, ".");
        return Number(r);
    }
    createRelativeTimeFormat(t, n) {
        return new Intl.RelativeTimeFormat(n || this.getLocale(), t);
    }
    rt(t, n, e) {
        let i = t.getTime() - this.now();
        const s = this.options.rtEpsilon * (i > 0 ? 1 : 0);
        const r = this.createRelativeTimeFormat(n, e);
        let o = i / 31536e6;
        if (Math.abs(o + s) >= 1) {
            return r.format(Math.round(o), "year");
        }
        o = i / 2592e6;
        if (Math.abs(o + s) >= 1) {
            return r.format(Math.round(o), "month");
        }
        o = i / 6048e5;
        if (Math.abs(o + s) >= 1) {
            return r.format(Math.round(o), "week");
        }
        o = i / 864e5;
        if (Math.abs(o + s) >= 1) {
            return r.format(Math.round(o), "day");
        }
        o = i / 36e5;
        if (Math.abs(o + s) >= 1) {
            return r.format(Math.round(o), "hour");
        }
        o = i / 6e4;
        if (Math.abs(o + s) >= 1) {
            return r.format(Math.round(o), "minute");
        }
        i = Math.abs(i) < 1e3 ? 1e3 : i;
        o = i / 1e3;
        return r.format(Math.round(o), "second");
    }
    subscribeLocaleChange(t) {
        this.i.add(t);
    }
    now() {
        return (new Date).getTime();
    }
    async h(t) {
        const n = {
            lng: "en",
            fallbackLng: [ "en" ],
            debug: false,
            plugins: [],
            rtEpsilon: .01,
            skipTranslationOnMissingKey: false
        };
        this.options = {
            ...n,
            ...t
        };
        for (const t of this.options.plugins) {
            this.i18next.use(t);
        }
        await this.i18next.init(this.options);
    }
};

__decorate([ b ], V.prototype, "i18next", void 0);

V = __decorate([ __param(0, E), __param(1, L), __param(2, n), __param(3, T) ], V);

let F = class DateFormatValueConverter {
    constructor(t) {
        this.i18n = t;
        this.signals = [ P.I18N_SIGNAL ];
    }
    toView(t, n, e) {
        if (!t && t !== 0 || typeof t === "string" && t.trim() === "") {
            return t;
        }
        if (typeof t === "string") {
            const n = Number(t);
            const e = new Date(Number.isInteger(n) ? n : t);
            if (isNaN(e.getTime())) {
                return t;
            }
            t = e;
        }
        return this.i18n.df(t, n, e);
    }
};

F = __decorate([ o("df"), __param(0, N) ], F);

let k = class NumberFormatBindingBehavior {
    bind(t, n) {
        createIntlFormatValueConverterExpression("nf", n);
    }
};

k = __decorate([ r("nf") ], k);

let O = class NumberFormatValueConverter {
    constructor(t) {
        this.i18n = t;
        this.signals = [ P.I18N_SIGNAL ];
    }
    toView(t, n, e) {
        if (typeof t !== "number") {
            return t;
        }
        return this.i18n.nf(t, n, e);
    }
};

O = __decorate([ o("nf"), __param(0, N) ], O);

let D = class RelativeTimeBindingBehavior {
    bind(t, n) {
        createIntlFormatValueConverterExpression("rt", n);
    }
};

D = __decorate([ r("rt") ], D);

let j = class RelativeTimeValueConverter {
    constructor(t) {
        this.i18n = t;
        this.signals = [ P.I18N_SIGNAL, P.RT_SIGNAL ];
    }
    toView(t, n, e) {
        if (!(t instanceof Date)) {
            return t;
        }
        return this.i18n.rt(t, n, e);
    }
};

j = __decorate([ o("rt"), __param(0, N) ], j);

let S = class TranslationBindingBehavior {
    bind(t, n) {
        const e = n.ast.expression;
        if (!(e instanceof _)) {
            const t = new _(e, "t", n.ast.args);
            n.ast.expression = t;
        }
    }
};

S = __decorate([ r("t") ], S);

const K = [ "textContent", "innerHTML", "prepend", "append" ];

const $ = new Map([ [ "text", "textContent" ], [ "html", "innerHTML" ] ]);

const H = {
    optional: true
};

const W = {
    reusable: false,
    preempt: true
};

class TranslationBinding {
    constructor(t, n, e, i, s) {
        this.isBound = false;
        this._ = K;
        this.T = null;
        this.parameter = null;
        this.boundFn = false;
        this.l = n;
        this.B = t;
        this.target = s;
        this.i18n = n.get(N);
        this.p = i;
        this.I = new Set;
        this.oL = e;
        this.i18n.subscribeLocaleChange(this);
        this.C = i.domWriteQueue;
    }
    static create({parser: t, observerLocator: n, context: e, controller: i, target: s, instruction: r, platform: o, isParameterContext: a}) {
        const c = this.P({
            observerLocator: n,
            context: e,
            controller: i,
            target: s,
            platform: o
        });
        const l = typeof r.from === "string" ? t.parse(r.from, 16) : r.from;
        if (a) {
            c.useParameter(l);
        } else {
            const n = l instanceof v ? t.parse(l.value, 1) : undefined;
            c.ast = n || l;
        }
    }
    static P({observerLocator: t, context: n, controller: e, target: i, platform: s}) {
        let r = e.bindings && e.bindings.find((t => t instanceof TranslationBinding && t.target === i));
        if (!r) {
            r = new TranslationBinding(e, n, t, s, i);
            e.addBinding(r);
        }
        return r;
    }
    bind(t) {
        if (this.isBound) {
            return;
        }
        if (!this.ast) {
            throw new Error("key expression is missing");
        }
        this.s = t;
        this.M = this.ast instanceof w;
        this.A = I(this.ast, t, this, this);
        this.L();
        this.parameter?.bind(t);
        this.updateTranslations();
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        C(this.ast, this.s, this);
        this.parameter?.unbind();
        this.I.clear();
        if (this.T !== null) {
            this.T.cancel();
            this.T = null;
        }
        this.s = void 0;
        this.obs.clearAll();
    }
    handleChange(t, n) {
        this.obs.version++;
        this.A = this.M ? I(this.ast, this.s, this, this) : t;
        this.obs.clear();
        this.L();
        this.updateTranslations();
    }
    handleLocaleChange() {
        this.updateTranslations();
    }
    useParameter(t) {
        if (this.parameter != null) {
            throw new Error("This translation parameter has already been specified.");
        }
        this.parameter = new ParameterBinding(this, t, (() => this.updateTranslations()));
    }
    updateTranslations() {
        const t = this.i18n.evaluate(this.A, this.parameter?.value);
        const n = Object.create(null);
        const e = [];
        const i = this.T;
        this.I.clear();
        for (const i of t) {
            const t = i.value;
            const s = this.R(i.attributes);
            for (const i of s) {
                if (this.N(i)) {
                    n[i] = t;
                } else {
                    const n = l.for(this.target, H);
                    const s = n?.viewModel ? this.oL.getAccessor(n.viewModel, i) : this.oL.getAccessor(this.target, i);
                    const r = this.B.state !== 1 && (s.type & 4) > 0;
                    if (r) {
                        e.push(new AccessorUpdateTask(s, t, this.target, i));
                    } else {
                        s.setValue(t, this.target, i);
                    }
                    this.I.add(s);
                }
            }
        }
        let s = false;
        if (Object.keys(n).length > 0) {
            s = this.B.state !== 1;
            if (!s) {
                this.V(n);
            }
        }
        if (e.length > 0 || s) {
            this.T = this.C.queueTask((() => {
                this.T = null;
                for (const t of e) {
                    t.run();
                }
                if (s) {
                    this.V(n);
                }
            }), W);
        }
        i?.cancel();
    }
    R(t) {
        if (t.length === 0) {
            t = this.target.tagName === "IMG" ? [ "src" ] : [ "textContent" ];
        }
        for (const [n, e] of $) {
            const i = t.findIndex((t => t === n));
            if (i > -1) {
                t.splice(i, 1, e);
            }
        }
        return t;
    }
    N(t) {
        return this._.includes(t);
    }
    V(t) {
        const n = e(this.target.childNodes);
        const i = [];
        const s = "au-i18n";
        for (const t of n) {
            if (!Reflect.get(t, s)) {
                i.push(t);
            }
        }
        const r = this.F(t, s, i);
        this.target.innerHTML = "";
        for (const t of e(r.content.childNodes)) {
            this.target.appendChild(t);
        }
    }
    F(t, n, e) {
        const i = this.p.document.createElement("template");
        this.O(i, t.prepend, n);
        if (!this.O(i, t.innerHTML ?? t.textContent, n)) {
            for (const t of e) {
                i.content.append(t);
            }
        }
        this.O(i, t.append, n);
        return i;
    }
    O(t, n, i) {
        if (n !== void 0 && n !== null) {
            const s = this.p.document.createElement("div");
            s.innerHTML = n;
            for (const n of e(s.childNodes)) {
                Reflect.set(n, i, true);
                t.content.append(n);
            }
            return true;
        }
        return false;
    }
    L() {
        const t = this.A ?? (this.A = "");
        const n = typeof t;
        if (n !== "string") {
            throw new Error(`Expected the i18n key to be a string, but got ${t} of type ${n}`);
        }
    }
}

B(TranslationBinding);

a(true)(TranslationBinding);

c(TranslationBinding, (() => "updateTranslations"));

class AccessorUpdateTask {
    constructor(t, n, e, i) {
        this.accessor = t;
        this.v = n;
        this.el = e;
        this.attr = i;
    }
    run() {
        this.accessor.setValue(this.v, this.el, this.attr);
    }
}

class ParameterBinding {
    constructor(t, n, e) {
        this.owner = t;
        this.ast = n;
        this.updater = e;
        this.isBound = false;
        this.boundFn = false;
        this.oL = t.oL;
        this.l = t.l;
    }
    handleChange(t, n) {
        if (!this.isBound) {
            return;
        }
        this.obs.version++;
        this.value = I(this.ast, this.s, this, this);
        this.obs.clear();
        this.updater();
    }
    bind(t) {
        if (this.isBound) {
            return;
        }
        this.s = t;
        x(this.ast, t, this);
        this.value = I(this.ast, t, this, this);
        this.isBound = true;
    }
    unbind() {
        if (!this.isBound) {
            return;
        }
        C(this.ast, this.s, this);
        this.s = void 0;
        this.obs.clearAll();
    }
}

B(ParameterBinding);

a(true)(ParameterBinding);

const z = "tpt";

const G = "t-params.bind";

let U = class TranslationParametersAttributePattern {
    [G](t, n, e) {
        return new d(t, n, "", G);
    }
};

U = __decorate([ h({
    pattern: G,
    symbols: ""
}) ], U);

class TranslationParametersBindingInstruction {
    constructor(t, n) {
        this.from = t;
        this.to = n;
        this.type = z;
        this.mode = 2;
    }
}

let Y = class TranslationParametersBindingCommand {
    constructor() {
        this.type = 0;
    }
    get name() {
        return G;
    }
    build(t, n, e) {
        const s = t.attr;
        let r = s.target;
        if (t.bindable == null) {
            r = e.map(t.node, r) ?? i(r);
        } else {
            r = t.bindable.property;
        }
        return new TranslationParametersBindingInstruction(n.parse(s.rawValue, 16), r);
    }
};

Y = __decorate([ u(G) ], Y);

let q = class TranslationParametersBindingRenderer {
    render(t, n, e, i, s, r) {
        TranslationBinding.create({
            parser: s,
            observerLocator: r,
            context: t.container,
            controller: t,
            target: n,
            instruction: e,
            isParameterContext: true,
            platform: i
        });
    }
};

q = __decorate([ f(z) ], q);

const J = "tt";

class TranslationAttributePattern {
    static registerAlias(t) {
        this.prototype[t] = function(n, e, i) {
            return new d(n, e, "", t);
        };
    }
}

class TranslationBindingInstruction {
    constructor(t, n) {
        this.from = t;
        this.to = n;
        this.type = J;
        this.mode = 2;
    }
}

class TranslationBindingCommand {
    constructor() {
        this.type = 0;
    }
    get name() {
        return "t";
    }
    build(t, n, e) {
        let s;
        if (t.bindable == null) {
            s = e.map(t.node, t.attr.target) ?? i(t.attr.target);
        } else {
            s = t.bindable.property;
        }
        return new TranslationBindingInstruction(new v(t.attr.rawValue), s);
    }
}

let Q = class TranslationBindingRenderer {
    render(t, n, e, i, s, r) {
        TranslationBinding.create({
            parser: s,
            observerLocator: r,
            context: t.container,
            controller: t,
            target: n,
            instruction: e,
            platform: i
        });
    }
};

Q = __decorate([ f(J) ], Q);

const X = "tbt";

class TranslationBindAttributePattern {
    static registerAlias(t) {
        const n = `${t}.bind`;
        this.prototype[n] = function(t, e, i) {
            return new d(t, e, i[1], n);
        };
    }
}

class TranslationBindBindingInstruction {
    constructor(t, n) {
        this.from = t;
        this.to = n;
        this.type = X;
        this.mode = 2;
    }
}

class TranslationBindBindingCommand {
    constructor() {
        this.type = 0;
    }
    get name() {
        return "t-bind";
    }
    build(t, n, e) {
        let s;
        if (t.bindable == null) {
            s = e.map(t.node, t.attr.target) ?? i(t.attr.target);
        } else {
            s = t.bindable.property;
        }
        return new TranslationBindBindingInstruction(n.parse(t.attr.rawValue, 16), s);
    }
}

let Z = class TranslationBindBindingRenderer {
    render(t, n, e, i, s, r) {
        TranslationBinding.create({
            parser: s,
            observerLocator: r,
            context: t.container,
            controller: t,
            target: n,
            instruction: e,
            platform: i
        });
    }
};

Z = __decorate([ f(X) ], Z);

let tt = class TranslationValueConverter {
    constructor(t) {
        this.i18n = t;
        this.signals = [ P.I18N_SIGNAL ];
    }
    toView(t, n) {
        return this.i18n.tr(t, n);
    }
};

tt = __decorate([ o("t"), __param(0, N) ], tt);

const nt = [ tt, S ];

function coreComponents(t) {
    const n = t.translationAttributeAliases;
    const e = Array.isArray(n) ? n : [ "t" ];
    const i = [];
    const r = [];
    const o = [];
    const a = [];
    for (const t of e) {
        const n = `${t}.bind`;
        i.push({
            pattern: t,
            symbols: ""
        });
        TranslationAttributePattern.registerAlias(t);
        r.push({
            pattern: n,
            symbols: "."
        });
        TranslationBindAttributePattern.registerAlias(t);
        if (t !== "t") {
            o.push(t);
            a.push(n);
        }
    }
    const c = [ m.define(i, TranslationAttributePattern), p.define({
        name: "t",
        aliases: o
    }, TranslationBindingCommand), Q, m.define(r, TranslationBindAttributePattern), p.define({
        name: "t.bind",
        aliases: a
    }, TranslationBindBindingCommand), Z, U, Y, q ];
    return {
        register(n) {
            return n.register(s.callback(L, (() => t.initOptions)), g.activating(N, (t => t.initPromise)), s.singleton(E, I18nextWrapper), s.singleton(N, V), ...c, ...nt);
        }
    };
}

const et = [ F, A ];

const it = [ O, k ];

const st = [ j, D ];

function createI18nConfiguration(t) {
    return {
        optionsProvider: t,
        register(n) {
            const e = {
                initOptions: Object.create(null)
            };
            t(e);
            return n.register(coreComponents(e), ...et, ...it, ...st);
        },
        customize(n) {
            return createI18nConfiguration(n || t);
        }
    };
}

const rt = createI18nConfiguration((() => {}));

export { A as DateFormatBindingBehavior, F as DateFormatValueConverter, N as I18N, rt as I18nConfiguration, L as I18nInitOptions, I18nKeyEvaluationResult, V as I18nService, k as NumberFormatBindingBehavior, O as NumberFormatValueConverter, D as RelativeTimeBindingBehavior, j as RelativeTimeValueConverter, P as Signals, TranslationAttributePattern, TranslationBindAttributePattern, TranslationBindBindingCommand, TranslationBindBindingInstruction, Z as TranslationBindBindingRenderer, X as TranslationBindInstructionType, TranslationBinding, S as TranslationBindingBehavior, TranslationBindingCommand, TranslationBindingInstruction, Q as TranslationBindingRenderer, J as TranslationInstructionType, U as TranslationParametersAttributePattern, Y as TranslationParametersBindingCommand, TranslationParametersBindingInstruction, q as TranslationParametersBindingRenderer, z as TranslationParametersInstructionType, tt as TranslationValueConverter };

