"use strict";

Object.defineProperty(exports, "__esModule", {
    value: true
});

var t = require("@aurelia/kernel");

var e = require("@aurelia/metadata");

const s = Object.prototype.hasOwnProperty;

const r = Reflect.defineProperty;

const i = t => "function" === typeof t;

const n = t => "string" === typeof t;

function o(t, e, s) {
    r(t, e, {
        enumerable: false,
        configurable: true,
        writable: true,
        value: s
    });
    return s;
}

function u(t, e, r, i = false) {
    if (i || !s.call(t, e)) o(t, e, r);
}

const c = () => Object.create(null);

const h = e.Metadata.getOwn;

const a = e.Metadata.hasOwn;

const l = e.Metadata.define;

const f = t.Protocol.annotation.keyFor;

const d = t.Protocol.resource.keyFor;

const p = t.Protocol.resource.appendTo;

function v(...t) {
    return function(e) {
        const s = f("aliases");
        const r = h(s, e);
        if (void 0 === r) l(s, t, e); else r.push(...t);
    };
}

function g(e, s, r, i) {
    for (let n = 0, o = e.length; n < o; ++n) t.Registration.aliasTo(r, s.keyFrom(e[n])).register(i);
}

Object.freeze({});

class BindingContext {
    constructor(t, e) {
        if (void 0 !== t) if (void 0 !== e) this[t] = e; else for (const e in t) if (s.call(t, e)) this[e] = t[e];
    }
    static create(t, e) {
        return new BindingContext(t, e);
    }
    static get(t, e, s, r) {
        var i, n;
        if (null == t) throw new Error(`AUR0203:${t}`);
        let o = t.overrideContext;
        let u = t;
        if (s > 0) {
            while (s > 0) {
                s--;
                u = u.parentScope;
                if (null == (null === u || void 0 === u ? void 0 : u.overrideContext)) return;
            }
            o = u.overrideContext;
            return e in o ? o : o.bindingContext;
        }
        while (!(null === u || void 0 === u ? void 0 : u.isBoundary) && null != o && !(e in o) && !(o.bindingContext && e in o.bindingContext)) {
            u = null !== (i = u.parentScope) && void 0 !== i ? i : null;
            o = null !== (n = null === u || void 0 === u ? void 0 : u.overrideContext) && void 0 !== n ? n : null;
        }
        if (o) return e in o ? o : o.bindingContext;
        return t.bindingContext || t.overrideContext;
    }
}

class Scope {
    constructor(t, e, s, r) {
        this.parentScope = t;
        this.bindingContext = e;
        this.overrideContext = s;
        this.isBoundary = r;
    }
    static create(t, e, s) {
        return new Scope(null, t, null == e ? OverrideContext.create(t) : e, null !== s && void 0 !== s ? s : false);
    }
    static fromOverride(t) {
        if (null == t) throw new Error(`AUR0204:${t}`);
        return new Scope(null, t.bindingContext, t, false);
    }
    static fromParent(t, e) {
        if (null == t) throw new Error(`AUR0205:${t}`);
        return new Scope(t, e, OverrideContext.create(e), false);
    }
}

class OverrideContext {
    constructor(t) {
        this.bindingContext = t;
    }
    static create(t) {
        return new OverrideContext(t);
    }
}

const w = t.DI.createInterface("ISignaler", (t => t.singleton(Signaler)));

class Signaler {
    constructor() {
        this.signals = c();
    }
    dispatchSignal(t, e) {
        const s = this.signals[t];
        if (void 0 === s) return;
        let r;
        for (r of s.keys()) r.handleChange(void 0, void 0, e);
    }
    addSignalListener(t, e) {
        const s = this.signals;
        const r = s[t];
        if (void 0 === r) s[t] = new Set([ e ]); else r.add(e);
    }
    removeSignalListener(t, e) {
        const s = this.signals[t];
        if (s) s.delete(e);
    }
}

exports.BindingBehaviorStrategy = void 0;

(function(t) {
    t[t["singleton"] = 1] = "singleton";
    t[t["interceptor"] = 2] = "interceptor";
})(exports.BindingBehaviorStrategy || (exports.BindingBehaviorStrategy = {}));

function b(t) {
    return function(e) {
        return y.define(t, e);
    };
}

class BindingBehaviorDefinition {
    constructor(t, e, s, r, i) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = r;
        this.strategy = i;
    }
    static create(e, s) {
        let r;
        let i;
        if (n(e)) {
            r = e;
            i = {
                name: r
            };
        } else {
            r = e.name;
            i = e;
        }
        const o = Object.getPrototypeOf(s) === BindingInterceptor;
        return new BindingBehaviorDefinition(s, t.firstDefined(A(s, "name"), r), t.mergeArrays(A(s, "aliases"), i.aliases, s.aliases), y.keyFrom(r), t.fromAnnotationOrDefinitionOrTypeOrDefault("strategy", i, s, (() => o ? 2 : 1)));
    }
    register(e) {
        const {Type: s, key: r, aliases: i, strategy: n} = this;
        switch (n) {
          case 1:
            t.Registration.singleton(r, s).register(e);
            break;

          case 2:
            t.Registration.instance(r, new BindingBehaviorFactory(e, s)).register(e);
            break;
        }
        t.Registration.aliasTo(r, s).register(e);
        g(i, y, r, e);
    }
}

class BindingBehaviorFactory {
    constructor(e, s) {
        this.ctn = e;
        this.Type = s;
        this.deps = t.DI.getDependencies(s);
    }
    construct(t, e) {
        const s = this.ctn;
        const r = this.deps;
        switch (r.length) {
          case 0:
            return new this.Type(t, e);

          case 1:
            return new this.Type(s.get(r[0]), t, e);

          case 2:
            return new this.Type(s.get(r[0]), s.get(r[1]), t, e);

          default:
            return new this.Type(...r.map((t => s.get(t))), t, e);
        }
    }
}

class BindingInterceptor {
    constructor(t, e) {
        this.binding = t;
        this.expr = e;
        this.interceptor = this;
        let s;
        while (t.interceptor !== this) {
            s = t.interceptor;
            t.interceptor = this;
            t = s;
        }
    }
    updateTarget(t, e) {
        this.binding.updateTarget(t, e);
    }
    updateSource(t, e) {
        this.binding.updateSource(t, e);
    }
    callSource(t) {
        return this.binding.callSource(t);
    }
    handleChange(t, e, s) {
        this.binding.handleChange(t, e, s);
    }
    handleCollectionChange(t, e) {
        this.binding.handleCollectionChange(t, e);
    }
    observe(t, e) {
        this.binding.observe(t, e);
    }
    observeCollection(t) {
        this.binding.observeCollection(t);
    }
    $bind(t, e) {
        this.binding.$bind(t, e);
    }
    $unbind(t) {
        this.binding.$unbind(t);
    }
}

const x = [ "isBound", "$scope", "obs", "sourceExpression", "locator", "oL" ];

x.forEach((t => {
    r(BindingInterceptor.prototype, t, {
        enumerable: false,
        configurable: true,
        get: function() {
            return this.binding[t];
        }
    });
}));

const E = d("binding-behavior");

const A = (t, e) => h(f(e), t);

const y = Object.freeze({
    name: E,
    keyFrom(t) {
        return `${E}:${t}`;
    },
    isType(t) {
        return i(t) && a(E, t);
    },
    define(t, e) {
        const s = BindingBehaviorDefinition.create(t, e);
        l(E, s, s.Type);
        l(E, s, s);
        p(e, E);
        return s.Type;
    },
    getDefinition(t) {
        const e = h(E, t);
        if (void 0 === e) throw new Error(`AUR0151:${t.name}`);
        return e;
    },
    annotate(t, e, s) {
        l(f(e), s, t);
    },
    getAnnotation: A
});

function m(t) {
    return function(e) {
        return O.define(t, e);
    };
}

class ValueConverterDefinition {
    constructor(t, e, s, r) {
        this.Type = t;
        this.name = e;
        this.aliases = s;
        this.key = r;
    }
    static create(e, s) {
        let r;
        let i;
        if (n(e)) {
            r = e;
            i = {
                name: r
            };
        } else {
            r = e.name;
            i = e;
        }
        return new ValueConverterDefinition(s, t.firstDefined(S(s, "name"), r), t.mergeArrays(S(s, "aliases"), i.aliases, s.aliases), O.keyFrom(r));
    }
    register(e) {
        const {Type: s, key: r, aliases: i} = this;
        t.Registration.singleton(r, s).register(e);
        t.Registration.aliasTo(r, s).register(e);
        g(i, O, r, e);
    }
}

const U = d("value-converter");

const S = (t, e) => h(f(e), t);

const O = Object.freeze({
    name: U,
    keyFrom: t => `${U}:${t}`,
    isType(t) {
        return i(t) && a(U, t);
    },
    define(t, e) {
        const s = ValueConverterDefinition.create(t, e);
        l(U, s, s.Type);
        l(U, s, s);
        p(e, U);
        return s.Type;
    },
    getDefinition(t) {
        const e = h(U, t);
        if (void 0 === e) throw new Error(`AUR0152:${t.name}`);
        return e;
    },
    annotate(t, e, s) {
        l(f(e), s, t);
    },
    getAnnotation: S
});

exports.ExpressionKind = void 0;

(function(t) {
    t[t["CallsFunction"] = 128] = "CallsFunction";
    t[t["HasAncestor"] = 256] = "HasAncestor";
    t[t["IsPrimary"] = 512] = "IsPrimary";
    t[t["IsLeftHandSide"] = 1024] = "IsLeftHandSide";
    t[t["HasBind"] = 2048] = "HasBind";
    t[t["HasUnbind"] = 4096] = "HasUnbind";
    t[t["IsAssignable"] = 8192] = "IsAssignable";
    t[t["IsLiteral"] = 16384] = "IsLiteral";
    t[t["IsResource"] = 32768] = "IsResource";
    t[t["IsForDeclaration"] = 65536] = "IsForDeclaration";
    t[t["Type"] = 31] = "Type";
    t[t["AccessThis"] = 1793] = "AccessThis";
    t[t["AccessScope"] = 10082] = "AccessScope";
    t[t["ArrayLiteral"] = 17955] = "ArrayLiteral";
    t[t["ObjectLiteral"] = 17956] = "ObjectLiteral";
    t[t["PrimitiveLiteral"] = 17925] = "PrimitiveLiteral";
    t[t["Template"] = 17958] = "Template";
    t[t["Unary"] = 39] = "Unary";
    t[t["CallScope"] = 1448] = "CallScope";
    t[t["CallMember"] = 1161] = "CallMember";
    t[t["CallFunction"] = 1162] = "CallFunction";
    t[t["AccessMember"] = 9323] = "AccessMember";
    t[t["AccessKeyed"] = 9324] = "AccessKeyed";
    t[t["TaggedTemplate"] = 1197] = "TaggedTemplate";
    t[t["Binary"] = 46] = "Binary";
    t[t["Conditional"] = 63] = "Conditional";
    t[t["Assign"] = 8208] = "Assign";
    t[t["ValueConverter"] = 36913] = "ValueConverter";
    t[t["BindingBehavior"] = 38962] = "BindingBehavior";
    t[t["HtmlLiteral"] = 51] = "HtmlLiteral";
    t[t["ArrayBindingPattern"] = 65556] = "ArrayBindingPattern";
    t[t["ObjectBindingPattern"] = 65557] = "ObjectBindingPattern";
    t[t["BindingIdentifier"] = 65558] = "BindingIdentifier";
    t[t["ForOfStatement"] = 6199] = "ForOfStatement";
    t[t["Interpolation"] = 24] = "Interpolation";
    t[t["ArrayDestructuring"] = 90137] = "ArrayDestructuring";
    t[t["ObjectDestructuring"] = 106521] = "ObjectDestructuring";
    t[t["DestructuringAssignmentLeaf"] = 139289] = "DestructuringAssignmentLeaf";
})(exports.ExpressionKind || (exports.ExpressionKind = {}));

class Unparser {
    constructor() {
        this.text = "";
    }
    static unparse(t) {
        const e = new Unparser;
        t.accept(e);
        return e.text;
    }
    visitAccessMember(t) {
        t.object.accept(this);
        this.text += `${t.optional ? "?" : ""}.${t.name}`;
    }
    visitAccessKeyed(t) {
        t.object.accept(this);
        this.text += `${t.optional ? "?." : ""}[`;
        t.key.accept(this);
        this.text += "]";
    }
    visitAccessThis(t) {
        if (0 === t.ancestor) {
            this.text += "$this";
            return;
        }
        this.text += "$parent";
        let e = t.ancestor - 1;
        while (e--) this.text += ".$parent";
    }
    visitAccessScope(t) {
        let e = t.ancestor;
        while (e--) this.text += "$parent.";
        this.text += t.name;
    }
    visitArrayLiteral(t) {
        const e = t.elements;
        this.text += "[";
        for (let t = 0, s = e.length; t < s; ++t) {
            if (0 !== t) this.text += ",";
            e[t].accept(this);
        }
        this.text += "]";
    }
    visitObjectLiteral(t) {
        const e = t.keys;
        const s = t.values;
        this.text += "{";
        for (let t = 0, r = e.length; t < r; ++t) {
            if (0 !== t) this.text += ",";
            this.text += `'${e[t]}':`;
            s[t].accept(this);
        }
        this.text += "}";
    }
    visitPrimitiveLiteral(t) {
        this.text += "(";
        if (n(t.value)) {
            const e = t.value.replace(/'/g, "\\'");
            this.text += `'${e}'`;
        } else this.text += `${t.value}`;
        this.text += ")";
    }
    visitCallFunction(t) {
        this.text += "(";
        t.func.accept(this);
        this.text += t.optional ? "?." : "";
        this.writeArgs(t.args);
        this.text += ")";
    }
    visitCallMember(t) {
        this.text += "(";
        t.object.accept(this);
        this.text += `${t.optionalMember ? "?." : ""}.${t.name}${t.optionalCall ? "?." : ""}`;
        this.writeArgs(t.args);
        this.text += ")";
    }
    visitCallScope(t) {
        this.text += "(";
        let e = t.ancestor;
        while (e--) this.text += "$parent.";
        this.text += `${t.name}${t.optional ? "?." : ""}`;
        this.writeArgs(t.args);
        this.text += ")";
    }
    visitTemplate(t) {
        const {cooked: e, expressions: s} = t;
        const r = s.length;
        this.text += "`";
        this.text += e[0];
        for (let t = 0; t < r; t++) {
            s[t].accept(this);
            this.text += e[t + 1];
        }
        this.text += "`";
    }
    visitTaggedTemplate(t) {
        const {cooked: e, expressions: s} = t;
        const r = s.length;
        t.func.accept(this);
        this.text += "`";
        this.text += e[0];
        for (let t = 0; t < r; t++) {
            s[t].accept(this);
            this.text += e[t + 1];
        }
        this.text += "`";
    }
    visitUnary(t) {
        this.text += `(${t.operation}`;
        if (t.operation.charCodeAt(0) >= 97) this.text += " ";
        t.expression.accept(this);
        this.text += ")";
    }
    visitBinary(t) {
        this.text += "(";
        t.left.accept(this);
        if (105 === t.operation.charCodeAt(0)) this.text += ` ${t.operation} `; else this.text += t.operation;
        t.right.accept(this);
        this.text += ")";
    }
    visitConditional(t) {
        this.text += "(";
        t.condition.accept(this);
        this.text += "?";
        t.yes.accept(this);
        this.text += ":";
        t.no.accept(this);
        this.text += ")";
    }
    visitAssign(t) {
        this.text += "(";
        t.target.accept(this);
        this.text += "=";
        t.value.accept(this);
        this.text += ")";
    }
    visitValueConverter(t) {
        const e = t.args;
        t.expression.accept(this);
        this.text += `|${t.name}`;
        for (let t = 0, s = e.length; t < s; ++t) {
            this.text += ":";
            e[t].accept(this);
        }
    }
    visitBindingBehavior(t) {
        const e = t.args;
        t.expression.accept(this);
        this.text += `&${t.name}`;
        for (let t = 0, s = e.length; t < s; ++t) {
            this.text += ":";
            e[t].accept(this);
        }
    }
    visitArrayBindingPattern(t) {
        const e = t.elements;
        this.text += "[";
        for (let t = 0, s = e.length; t < s; ++t) {
            if (0 !== t) this.text += ",";
            e[t].accept(this);
        }
        this.text += "]";
    }
    visitObjectBindingPattern(t) {
        const e = t.keys;
        const s = t.values;
        this.text += "{";
        for (let t = 0, r = e.length; t < r; ++t) {
            if (0 !== t) this.text += ",";
            this.text += `'${e[t]}':`;
            s[t].accept(this);
        }
        this.text += "}";
    }
    visitBindingIdentifier(t) {
        this.text += t.name;
    }
    visitHtmlLiteral(t) {
        throw new Error("visitHtmlLiteral");
    }
    visitForOfStatement(t) {
        t.declaration.accept(this);
        this.text += " of ";
        t.iterable.accept(this);
    }
    visitInterpolation(t) {
        const {parts: e, expressions: s} = t;
        const r = s.length;
        this.text += "${";
        this.text += e[0];
        for (let t = 0; t < r; t++) {
            s[t].accept(this);
            this.text += e[t + 1];
        }
        this.text += "}";
    }
    visitDestructuringAssignmentExpression(t) {
        const e = t.$kind;
        const s = 106521 === e;
        this.text += s ? "{" : "[";
        const r = t.list;
        const i = r.length;
        let n;
        let o;
        for (n = 0; n < i; n++) {
            o = r[n];
            switch (o.$kind) {
              case 139289:
                o.accept(this);
                break;

              case 90137:
              case 106521:
                {
                    const t = o.source;
                    if (t) {
                        t.accept(this);
                        this.text += ":";
                    }
                    o.accept(this);
                    break;
                }
            }
        }
        this.text += s ? "}" : "]";
    }
    visitDestructuringAssignmentSingleExpression(t) {
        t.source.accept(this);
        this.text += ":";
        t.target.accept(this);
        const e = t.initializer;
        if (void 0 !== e) {
            this.text += "=";
            e.accept(this);
        }
    }
    visitDestructuringAssignmentRestExpression(t) {
        this.text += "...";
        t.accept(this);
    }
    writeArgs(t) {
        this.text += "(";
        for (let e = 0, s = t.length; e < s; ++e) {
            if (0 !== e) this.text += ",";
            t[e].accept(this);
        }
        this.text += ")";
    }
}

class CustomExpression {
    constructor(t) {
        this.value = t;
    }
    evaluate(t, e, s, r) {
        return this.value;
    }
}

class BindingBehaviorExpression {
    constructor(t, e, s) {
        this.expression = t;
        this.name = e;
        this.args = s;
        this.behaviorKey = y.keyFrom(e);
    }
    get $kind() {
        return 38962;
    }
    get hasBind() {
        return true;
    }
    get hasUnbind() {
        return true;
    }
    evaluate(t, e, s, r) {
        return this.expression.evaluate(t, e, s, r);
    }
    assign(t, e, s, r) {
        return this.expression.assign(t, e, s, r);
    }
    bind(t, e, s) {
        if (this.expression.hasBind) this.expression.bind(t, e, s);
        const r = s.locator.get(this.behaviorKey);
        if (null == r) throw new Error(`AUR0101:${this.name}`);
        if (!(r instanceof BindingBehaviorFactory)) if (void 0 === s[this.behaviorKey]) {
            s[this.behaviorKey] = r;
            r.bind(t, e, s, ...this.args.map((r => r.evaluate(t, e, s.locator, null))));
        } else throw new Error(`AUR0102:${this.name}`);
    }
    unbind(t, e, s) {
        const r = this.behaviorKey;
        const n = s;
        if (void 0 !== n[r]) {
            if (i(n[r].unbind)) n[r].unbind(t, e, s);
            n[r] = void 0;
        }
        if (this.expression.hasUnbind) this.expression.unbind(t, e, s);
    }
    accept(t) {
        return t.visitBindingBehavior(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class ValueConverterExpression {
    constructor(t, e, s) {
        this.expression = t;
        this.name = e;
        this.args = s;
        this.converterKey = O.keyFrom(e);
    }
    get $kind() {
        return 36913;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return true;
    }
    evaluate(t, e, s, r) {
        const i = s.get(this.converterKey);
        if (null == i) throw new Error(`AUR0103:${this.name}`);
        if (null !== r && "handleChange" in r) {
            const t = i.signals;
            if (null != t) {
                const e = s.get(w);
                for (let s = 0, i = t.length; s < i; ++s) e.addSignalListener(t[s], r);
            }
        }
        if ("toView" in i) return i.toView(this.expression.evaluate(t, e, s, r), ...this.args.map((i => i.evaluate(t, e, s, r))));
        return this.expression.evaluate(t, e, s, r);
    }
    assign(t, e, s, r) {
        const i = s.get(this.converterKey);
        if (null == i) throw new Error(`AUR0104:${this.name}`);
        if ("fromView" in i) r = i.fromView(r, ...this.args.map((r => r.evaluate(t, e, s, null))));
        return this.expression.assign(t, e, s, r);
    }
    unbind(t, e, s) {
        const r = s.locator.get(this.converterKey);
        if (void 0 === r.signals) return;
        const i = s.locator.get(w);
        for (let t = 0; t < r.signals.length; ++t) i.removeSignalListener(r.signals[t], s);
    }
    accept(t) {
        return t.visitValueConverter(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class AssignExpression {
    constructor(t, e) {
        this.target = t;
        this.value = e;
    }
    get $kind() {
        return 8208;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return this.target.assign(t, e, s, this.value.evaluate(t, e, s, r));
    }
    assign(t, e, s, r) {
        this.value.assign(t, e, s, r);
        return this.target.assign(t, e, s, r);
    }
    accept(t) {
        return t.visitAssign(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class ConditionalExpression {
    constructor(t, e, s) {
        this.condition = t;
        this.yes = e;
        this.no = s;
    }
    get $kind() {
        return 63;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return this.condition.evaluate(t, e, s, r) ? this.yes.evaluate(t, e, s, r) : this.no.evaluate(t, e, s, r);
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitConditional(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class AccessThisExpression {
    constructor(t = 0) {
        this.ancestor = t;
    }
    get $kind() {
        return 1793;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        var i;
        let n = e.overrideContext;
        let o = e;
        let u = this.ancestor;
        while (u-- && n) {
            o = o.parentScope;
            n = null !== (i = null === o || void 0 === o ? void 0 : o.overrideContext) && void 0 !== i ? i : null;
        }
        return u < 1 && n ? n.bindingContext : void 0;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitAccessThis(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

AccessThisExpression.$this = new AccessThisExpression(0);

AccessThisExpression.$parent = new AccessThisExpression(1);

class AccessScopeExpression {
    constructor(t, e = 0) {
        this.name = t;
        this.ancestor = e;
    }
    get $kind() {
        return 10082;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const i = BindingContext.get(e, this.name, this.ancestor, t);
        if (null !== r) r.observe(i, this.name);
        const n = i[this.name];
        if (null == n && "$host" === this.name) throw new Error(`AUR0105`);
        if (1 & t) return n;
        return null == n ? "" : n;
    }
    assign(t, e, s, r) {
        var i;
        if ("$host" === this.name) throw new Error(`AUR0106`);
        const n = BindingContext.get(e, this.name, this.ancestor, t);
        if (n instanceof Object) if (void 0 !== (null === (i = n.$observers) || void 0 === i ? void 0 : i[this.name])) {
            n.$observers[this.name].setValue(r, t);
            return r;
        } else return n[this.name] = r;
        return;
    }
    accept(t) {
        return t.visitAccessScope(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class AccessMemberExpression {
    constructor(t, e, s = false) {
        this.object = t;
        this.name = e;
        this.optional = s;
    }
    get $kind() {
        return 9323;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const i = this.object.evaluate(t, e, s, (32 & t) > 0 ? null : r);
        if (1 & t) {
            if (null == i) return i;
            if (null !== r) r.observe(i, this.name);
            return i[this.name];
        }
        if (null !== r && i instanceof Object) r.observe(i, this.name);
        return i ? i[this.name] : "";
    }
    assign(t, e, s, r) {
        const i = this.object.evaluate(t, e, s, null);
        if (i instanceof Object) if (void 0 !== i.$observers && void 0 !== i.$observers[this.name]) i.$observers[this.name].setValue(r, t); else i[this.name] = r; else this.object.assign(t, e, s, {
            [this.name]: r
        });
        return r;
    }
    accept(t) {
        return t.visitAccessMember(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class AccessKeyedExpression {
    constructor(t, e, s = false) {
        this.object = t;
        this.key = e;
        this.optional = s;
    }
    get $kind() {
        return 9324;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const i = this.object.evaluate(t, e, s, (32 & t) > 0 ? null : r);
        if (i instanceof Object) {
            const n = this.key.evaluate(t, e, s, (32 & t) > 0 ? null : r);
            if (null !== r) r.observe(i, n);
            return i[n];
        }
        return;
    }
    assign(t, e, s, r) {
        const i = this.object.evaluate(t, e, s, null);
        const n = this.key.evaluate(t, e, s, null);
        return i[n] = r;
    }
    accept(t) {
        return t.visitAccessKeyed(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class CallScopeExpression {
    constructor(t, e, s = 0, r = false) {
        this.name = t;
        this.args = e;
        this.ancestor = s;
        this.optional = r;
    }
    get $kind() {
        return 1448;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const i = this.args.map((i => i.evaluate(t, e, s, r)));
        const n = BindingContext.get(e, this.name, this.ancestor, t);
        const o = k(t, n, this.name);
        if (o) return o.apply(n, i);
        return;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitCallScope(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class CallMemberExpression {
    constructor(t, e, s, r = false, i = false) {
        this.object = t;
        this.name = e;
        this.args = s;
        this.optionalMember = r;
        this.optionalCall = i;
    }
    get $kind() {
        return 1161;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const i = this.object.evaluate(t, e, s, (32 & t) > 0 ? null : r);
        const n = this.args.map((i => i.evaluate(t, e, s, r)));
        const o = k(t, i, this.name);
        if (o) return o.apply(i, n);
        return;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitCallMember(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class CallFunctionExpression {
    constructor(t, e, s = false) {
        this.func = t;
        this.args = e;
        this.optional = s;
    }
    get $kind() {
        return 1162;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const n = this.func.evaluate(t, e, s, r);
        if (i(n)) return n(...this.args.map((i => i.evaluate(t, e, s, r))));
        if (!(8 & t) && null == n) return;
        throw new Error(`AUR0107`);
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitCallFunction(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class BinaryExpression {
    constructor(t, e, s) {
        this.operation = t;
        this.left = e;
        this.right = s;
    }
    get $kind() {
        return 46;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(e, s, r, n) {
        var o;
        switch (this.operation) {
          case "&&":
            return this.left.evaluate(e, s, r, n) && this.right.evaluate(e, s, r, n);

          case "||":
            return this.left.evaluate(e, s, r, n) || this.right.evaluate(e, s, r, n);

          case "??":
            return null !== (o = this.left.evaluate(e, s, r, n)) && void 0 !== o ? o : this.right.evaluate(e, s, r, n);

          case "==":
            return this.left.evaluate(e, s, r, n) == this.right.evaluate(e, s, r, n);

          case "===":
            return this.left.evaluate(e, s, r, n) === this.right.evaluate(e, s, r, n);

          case "!=":
            return this.left.evaluate(e, s, r, n) != this.right.evaluate(e, s, r, n);

          case "!==":
            return this.left.evaluate(e, s, r, n) !== this.right.evaluate(e, s, r, n);

          case "instanceof":
            {
                const t = this.right.evaluate(e, s, r, n);
                if (i(t)) return this.left.evaluate(e, s, r, n) instanceof t;
                return false;
            }

          case "in":
            {
                const t = this.right.evaluate(e, s, r, n);
                if (t instanceof Object) return this.left.evaluate(e, s, r, n) in t;
                return false;
            }

          case "+":
            {
                const i = this.left.evaluate(e, s, r, n);
                const o = this.right.evaluate(e, s, r, n);
                if ((1 & e) > 0) return i + o;
                if (!i || !o) {
                    if (t.isNumberOrBigInt(i) || t.isNumberOrBigInt(o)) return (i || 0) + (o || 0);
                    if (t.isStringOrDate(i) || t.isStringOrDate(o)) return (i || "") + (o || "");
                }
                return i + o;
            }

          case "-":
            return this.left.evaluate(e, s, r, n) - this.right.evaluate(e, s, r, n);

          case "*":
            return this.left.evaluate(e, s, r, n) * this.right.evaluate(e, s, r, n);

          case "/":
            return this.left.evaluate(e, s, r, n) / this.right.evaluate(e, s, r, n);

          case "%":
            return this.left.evaluate(e, s, r, n) % this.right.evaluate(e, s, r, n);

          case "<":
            return this.left.evaluate(e, s, r, n) < this.right.evaluate(e, s, r, n);

          case ">":
            return this.left.evaluate(e, s, r, n) > this.right.evaluate(e, s, r, n);

          case "<=":
            return this.left.evaluate(e, s, r, n) <= this.right.evaluate(e, s, r, n);

          case ">=":
            return this.left.evaluate(e, s, r, n) >= this.right.evaluate(e, s, r, n);

          default:
            throw new Error(`AUR0108:${this.operation}`);
        }
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitBinary(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class UnaryExpression {
    constructor(t, e) {
        this.operation = t;
        this.expression = e;
    }
    get $kind() {
        return 39;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        switch (this.operation) {
          case "void":
            return void this.expression.evaluate(t, e, s, r);

          case "typeof":
            return typeof this.expression.evaluate(1 | t, e, s, r);

          case "!":
            return !this.expression.evaluate(t, e, s, r);

          case "-":
            return -this.expression.evaluate(t, e, s, r);

          case "+":
            return +this.expression.evaluate(t, e, s, r);

          default:
            throw new Error(`AUR0109:${this.operation}`);
        }
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitUnary(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class PrimitiveLiteralExpression {
    constructor(t) {
        this.value = t;
    }
    get $kind() {
        return 17925;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return this.value;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitPrimitiveLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

PrimitiveLiteralExpression.$undefined = new PrimitiveLiteralExpression(void 0);

PrimitiveLiteralExpression.$null = new PrimitiveLiteralExpression(null);

PrimitiveLiteralExpression.$true = new PrimitiveLiteralExpression(true);

PrimitiveLiteralExpression.$false = new PrimitiveLiteralExpression(false);

PrimitiveLiteralExpression.$empty = new PrimitiveLiteralExpression("");

class HtmlLiteralExpression {
    constructor(t) {
        this.parts = t;
    }
    get $kind() {
        return 51;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        let i = "";
        for (let n = 0; n < this.parts.length; ++n) {
            const o = this.parts[n].evaluate(t, e, s, r);
            if (null == o) continue;
            i += o;
        }
        return i;
    }
    assign(t, e, s, r, i) {
        return;
    }
    accept(t) {
        return t.visitHtmlLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class ArrayLiteralExpression {
    constructor(t) {
        this.elements = t;
    }
    get $kind() {
        return 17955;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return this.elements.map((i => i.evaluate(t, e, s, r)));
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitArrayLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

ArrayLiteralExpression.$empty = new ArrayLiteralExpression(t.emptyArray);

class ObjectLiteralExpression {
    constructor(t, e) {
        this.keys = t;
        this.values = e;
    }
    get $kind() {
        return 17956;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const i = {};
        for (let n = 0; n < this.keys.length; ++n) i[this.keys[n]] = this.values[n].evaluate(t, e, s, r);
        return i;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitObjectLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

ObjectLiteralExpression.$empty = new ObjectLiteralExpression(t.emptyArray, t.emptyArray);

class TemplateExpression {
    constructor(e, s = t.emptyArray) {
        this.cooked = e;
        this.expressions = s;
    }
    get $kind() {
        return 17958;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        let i = this.cooked[0];
        for (let n = 0; n < this.expressions.length; ++n) {
            i += String(this.expressions[n].evaluate(t, e, s, r));
            i += this.cooked[n + 1];
        }
        return i;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitTemplate(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

TemplateExpression.$empty = new TemplateExpression([ "" ]);

class TaggedTemplateExpression {
    constructor(e, s, r, i = t.emptyArray) {
        this.cooked = e;
        this.func = r;
        this.expressions = i;
        e.raw = s;
    }
    get $kind() {
        return 1197;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        const n = this.expressions.map((i => i.evaluate(t, e, s, r)));
        const o = this.func.evaluate(t, e, s, r);
        if (!i(o)) throw new Error(`AUR0110`);
        return o(this.cooked, ...n);
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitTaggedTemplate(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class ArrayBindingPattern {
    constructor(t) {
        this.elements = t;
    }
    get $kind() {
        return 65556;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitArrayBindingPattern(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class ObjectBindingPattern {
    constructor(t, e) {
        this.keys = t;
        this.values = e;
    }
    get $kind() {
        return 65557;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitObjectBindingPattern(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class BindingIdentifier {
    constructor(t) {
        this.name = t;
    }
    get $kind() {
        return 65558;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return this.name;
    }
    accept(t) {
        return t.visitBindingIdentifier(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

const C = Object.prototype.toString;

class ForOfStatement {
    constructor(t, e) {
        this.declaration = t;
        this.iterable = e;
    }
    get $kind() {
        return 6199;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return this.iterable.evaluate(t, e, s, r);
    }
    assign(t, e, s, r) {
        return;
    }
    count(t, e) {
        switch (C.call(e)) {
          case "[object Array]":
            return e.length;

          case "[object Map]":
            return e.size;

          case "[object Set]":
            return e.size;

          case "[object Number]":
            return e;

          case "[object Null]":
            return 0;

          case "[object Undefined]":
            return 0;

          default:
            throw new Error(`Cannot count ${C.call(e)}`);
        }
    }
    iterate(t, e, s) {
        switch (C.call(e)) {
          case "[object Array]":
            return $(e, s);

          case "[object Map]":
            return B(e, s);

          case "[object Set]":
            return L(e, s);

          case "[object Number]":
            return R(e, s);

          case "[object Null]":
            return;

          case "[object Undefined]":
            return;

          default:
            throw new Error(`Cannot iterate over ${C.call(e)}`);
        }
    }
    bind(t, e, s) {
        if (this.iterable.hasBind) this.iterable.bind(t, e, s);
    }
    unbind(t, e, s) {
        if (this.iterable.hasUnbind) this.iterable.unbind(t, e, s);
    }
    accept(t) {
        return t.visitForOfStatement(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class Interpolation {
    constructor(e, s = t.emptyArray) {
        this.parts = e;
        this.expressions = s;
        this.isMulti = s.length > 1;
        this.firstExpression = s[0];
    }
    get $kind() {
        return 24;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        if (this.isMulti) {
            let i = this.parts[0];
            for (let n = 0; n < this.expressions.length; ++n) {
                i += String(this.expressions[n].evaluate(t, e, s, r));
                i += this.parts[n + 1];
            }
            return i;
        } else return `${this.parts[0]}${this.firstExpression.evaluate(t, e, s, r)}${this.parts[1]}`;
    }
    assign(t, e, s, r) {
        return;
    }
    accept(t) {
        return t.visitInterpolation(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class DestructuringAssignmentExpression {
    constructor(t, e, s, r) {
        this.$kind = t;
        this.list = e;
        this.source = s;
        this.initializer = r;
    }
    get hasBind() {
        return false;
    }
    get hasUnbind() {
        return false;
    }
    evaluate(t, e, s, r) {
        return;
    }
    assign(t, e, s, r) {
        var i;
        const n = this.list;
        const o = n.length;
        let u;
        let c;
        for (u = 0; u < o; u++) {
            c = n[u];
            switch (c.$kind) {
              case 139289:
                c.assign(t, e, s, r);
                break;

              case 90137:
              case 106521:
                {
                    if ("object" !== typeof r || null === r) throw new Error(`AUR0112`);
                    let n = c.source.evaluate(t, Scope.create(r), s, null);
                    if (void 0 === n) n = null === (i = c.initializer) || void 0 === i ? void 0 : i.evaluate(t, e, s, null);
                    c.assign(t, e, s, n);
                    break;
                }
            }
        }
    }
    accept(t) {
        return t.visitDestructuringAssignmentExpression(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class DestructuringAssignmentSingleExpression {
    constructor(t, e, s) {
        this.target = t;
        this.source = e;
        this.initializer = s;
    }
    get $kind() {
        return 139289;
    }
    evaluate(t, e, s, r) {
        return;
    }
    assign(t, e, s, r) {
        var i;
        if (null == r) return;
        if ("object" !== typeof r) throw new Error(`AUR0112`);
        let n = this.source.evaluate(t, Scope.create(r), s, null);
        if (void 0 === n) n = null === (i = this.initializer) || void 0 === i ? void 0 : i.evaluate(t, e, s, null);
        this.target.assign(t, e, s, n);
    }
    accept(t) {
        return t.visitDestructuringAssignmentSingleExpression(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

class DestructuringAssignmentRestExpression {
    constructor(t, e) {
        this.target = t;
        this.indexOrProperties = e;
    }
    get $kind() {
        return 139289;
    }
    evaluate(t, e, s, r) {
        return;
    }
    assign(e, s, r, i) {
        if (null == i) return;
        if ("object" !== typeof i) throw new Error(`AUR0112`);
        const n = this.indexOrProperties;
        let o;
        if (t.isArrayIndex(n)) {
            if (!Array.isArray(i)) throw new Error(`AUR0112`);
            o = i.slice(n);
        } else o = Object.entries(i).reduce(((t, [e, s]) => {
            if (!n.includes(e)) t[e] = s;
            return t;
        }), {});
        this.target.assign(e, s, r, o);
    }
    accept(t) {
        return t.visitDestructuringAssignmentRestExpression(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}

function k(t, e, s) {
    const r = null == e ? null : e[s];
    if (i(r)) return r;
    if (!(8 & t) && null == r) return null;
    throw new Error(`AUR0111:${s}`);
}

function $(t, e) {
    for (let s = 0, r = t.length; s < r; ++s) e(t, s, t[s]);
}

function B(t, e) {
    const s = Array(t.size);
    let r = -1;
    for (const e of t.entries()) s[++r] = e;
    $(s, e);
}

function L(t, e) {
    const s = Array(t.size);
    let r = -1;
    for (const e of t.keys()) s[++r] = e;
    $(s, e);
}

function R(t, e) {
    const s = Array(t);
    for (let e = 0; e < t; ++e) s[e] = e;
    $(s, e);
}

const P = t.DI.createInterface("ICoercionConfiguration");

exports.BindingMode = void 0;

(function(t) {
    t[t["oneTime"] = 1] = "oneTime";
    t[t["toView"] = 2] = "toView";
    t[t["fromView"] = 4] = "fromView";
    t[t["twoWay"] = 6] = "twoWay";
    t[t["default"] = 8] = "default";
})(exports.BindingMode || (exports.BindingMode = {}));

exports.LifecycleFlags = void 0;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["persistentBindingFlags"] = 97] = "persistentBindingFlags";
    t[t["observeLeafPropertiesOnly"] = 32] = "observeLeafPropertiesOnly";
    t[t["noFlush"] = 64] = "noFlush";
    t[t["bindingStrategy"] = 1] = "bindingStrategy";
    t[t["isStrictBindingStrategy"] = 1] = "isStrictBindingStrategy";
    t[t["fromBind"] = 2] = "fromBind";
    t[t["fromUnbind"] = 4] = "fromUnbind";
    t[t["mustEvaluate"] = 8] = "mustEvaluate";
    t[t["dispose"] = 16] = "dispose";
})(exports.LifecycleFlags || (exports.LifecycleFlags = {}));

var T;

(function(t) {
    t[t["None"] = 0] = "None";
    t[t["Subscriber0"] = 1] = "Subscriber0";
    t[t["Subscriber1"] = 2] = "Subscriber1";
    t[t["Subscriber2"] = 4] = "Subscriber2";
    t[t["SubscribersRest"] = 8] = "SubscribersRest";
    t[t["Any"] = 15] = "Any";
})(T || (T = {}));

exports.DelegationStrategy = void 0;

(function(t) {
    t[t["none"] = 0] = "none";
    t[t["capturing"] = 1] = "capturing";
    t[t["bubbling"] = 2] = "bubbling";
})(exports.DelegationStrategy || (exports.DelegationStrategy = {}));

exports.CollectionKind = void 0;

(function(t) {
    t[t["indexed"] = 8] = "indexed";
    t[t["keyed"] = 4] = "keyed";
    t[t["array"] = 9] = "array";
    t[t["map"] = 6] = "map";
    t[t["set"] = 7] = "set";
})(exports.CollectionKind || (exports.CollectionKind = {}));

exports.AccessorType = void 0;

(function(t) {
    t[t["None"] = 0] = "None";
    t[t["Observer"] = 1] = "Observer";
    t[t["Node"] = 2] = "Node";
    t[t["Layout"] = 4] = "Layout";
    t[t["Primtive"] = 8] = "Primtive";
    t[t["Array"] = 18] = "Array";
    t[t["Set"] = 34] = "Set";
    t[t["Map"] = 66] = "Map";
})(exports.AccessorType || (exports.AccessorType = {}));

function j(t, e) {
    const {length: s} = t;
    const r = Array(s);
    let i = 0;
    while (i < s) {
        r[i] = t[i];
        ++i;
    }
    if (void 0 !== e) r.deletedItems = e.slice(0); else if (void 0 !== t.deletedItems) r.deletedItems = t.deletedItems.slice(0); else r.deletedItems = [];
    r.isIndexMap = true;
    return r;
}

function I(t = 0) {
    const e = Array(t);
    let s = 0;
    while (s < t) e[s] = s++;
    e.deletedItems = [];
    e.isIndexMap = true;
    return e;
}

function M(t) {
    const e = t.slice();
    e.deletedItems = t.deletedItems.slice();
    e.isIndexMap = true;
    return e;
}

function D(t) {
    return t instanceof Array && true === t.isIndexMap;
}

function F(t) {
    return null == t ? V : V(t);
}

function V(t) {
    const e = t.prototype;
    r(e, "subs", {
        get: N
    });
    u(e, "subscribe", K);
    u(e, "unsubscribe", q);
}

class SubscriberRecord {
    constructor() {
        this.sf = 0;
        this.count = 0;
    }
    add(t) {
        if (this.has(t)) return false;
        const e = this.sf;
        if (0 === (1 & e)) {
            this.s0 = t;
            this.sf |= 1;
        } else if (0 === (2 & e)) {
            this.s1 = t;
            this.sf |= 2;
        } else if (0 === (4 & e)) {
            this.s2 = t;
            this.sf |= 4;
        } else if (0 === (8 & e)) {
            this.sr = [ t ];
            this.sf |= 8;
        } else this.sr.push(t);
        ++this.count;
        return true;
    }
    has(t) {
        const e = this.sf;
        if ((1 & e) > 0 && this.s0 === t) return true;
        if ((2 & e) > 0 && this.s1 === t) return true;
        if ((4 & e) > 0 && this.s2 === t) return true;
        if ((8 & e) > 0) {
            const e = this.sr;
            const s = e.length;
            let r = 0;
            for (;r < s; ++r) if (e[r] === t) return true;
        }
        return false;
    }
    any() {
        return 0 !== this.sf;
    }
    remove(t) {
        const e = this.sf;
        if ((1 & e) > 0 && this.s0 === t) {
            this.s0 = void 0;
            this.sf = 1 ^ (1 | this.sf);
            --this.count;
            return true;
        } else if ((2 & e) > 0 && this.s1 === t) {
            this.s1 = void 0;
            this.sf = 2 ^ (2 | this.sf);
            --this.count;
            return true;
        } else if ((4 & e) > 0 && this.s2 === t) {
            this.s2 = void 0;
            this.sf = 4 ^ (4 | this.sf);
            --this.count;
            return true;
        } else if ((8 & e) > 0) {
            const e = this.sr;
            const s = e.length;
            let r = 0;
            for (;r < s; ++r) if (e[r] === t) {
                e.splice(r, 1);
                if (1 === s) this.sf = 8 ^ (8 | this.sf);
                --this.count;
                return true;
            }
        }
        return false;
    }
    notify(t, e, s) {
        const r = this.s0;
        const i = this.s1;
        const n = this.s2;
        let o = this.sr;
        if (void 0 !== o) o = o.slice();
        if (void 0 !== r) r.handleChange(t, e, s);
        if (void 0 !== i) i.handleChange(t, e, s);
        if (void 0 !== n) n.handleChange(t, e, s);
        if (void 0 !== o) {
            const r = o.length;
            let i;
            let n = 0;
            for (;n < r; ++n) {
                i = o[n];
                if (void 0 !== i) i.handleChange(t, e, s);
            }
        }
    }
    notifyCollection(t, e) {
        const s = this.s0;
        const r = this.s1;
        const i = this.s2;
        let n = this.sr;
        if (void 0 !== n) n = n.slice();
        if (void 0 !== s) s.handleCollectionChange(t, e);
        if (void 0 !== r) r.handleCollectionChange(t, e);
        if (void 0 !== i) i.handleCollectionChange(t, e);
        if (void 0 !== n) {
            const s = n.length;
            let r;
            let i = 0;
            for (;i < s; ++i) {
                r = n[i];
                if (void 0 !== r) r.handleCollectionChange(t, e);
            }
        }
    }
}

function N() {
    return o(this, "subs", new SubscriberRecord);
}

function K(t) {
    return this.subs.add(t);
}

function q(t) {
    return this.subs.remove(t);
}

function _(t) {
    return null == t ? H : H(t);
}

function H(t) {
    const e = t.prototype;
    r(e, "queue", {
        get: Q
    });
}

class FlushQueue {
    constructor() {
        this.t = false;
        this.i = new Set;
    }
    get count() {
        return this.i.size;
    }
    add(t) {
        this.i.add(t);
        if (this.t) return;
        this.t = true;
        try {
            this.i.forEach(z);
        } finally {
            this.t = false;
        }
    }
    clear() {
        this.i.clear();
        this.t = false;
    }
}

FlushQueue.instance = new FlushQueue;

function Q() {
    return FlushQueue.instance;
}

function z(t, e, s) {
    s.delete(t);
    t.flush();
}

class CollectionLengthObserver {
    constructor(t) {
        this.owner = t;
        this.type = 18;
        this.f = 0;
        this.v = this.u = (this.o = t.collection).length;
    }
    getValue() {
        return this.o.length;
    }
    setValue(e, s) {
        const r = this.v;
        if (e !== r && t.isArrayIndex(e)) {
            if (0 === (64 & s)) this.o.length = e;
            this.v = e;
            this.u = r;
            this.f = s;
            this.queue.add(this);
        }
    }
    handleCollectionChange(t, e) {
        const s = this.v;
        const r = this.o.length;
        if ((this.v = r) !== s) {
            this.u = s;
            this.f = e;
            this.queue.add(this);
        }
    }
    flush() {
        J = this.u;
        this.u = this.v;
        this.subs.notify(this.v, J, this.f);
    }
}

class CollectionSizeObserver {
    constructor(t) {
        this.owner = t;
        this.f = 0;
        this.v = this.u = (this.o = t.collection).size;
        this.type = this.o instanceof Map ? 66 : 34;
    }
    getValue() {
        return this.o.size;
    }
    setValue() {
        throw new Error(`AUR02`);
    }
    handleCollectionChange(t, e) {
        const s = this.v;
        const r = this.o.size;
        if ((this.v = r) !== s) {
            this.u = s;
            this.f = e;
            this.queue.add(this);
        }
    }
    flush() {
        J = this.u;
        this.u = this.v;
        this.subs.notify(this.v, J, this.f);
    }
}

function W(t) {
    const e = t.prototype;
    u(e, "subscribe", G, true);
    u(e, "unsubscribe", Z, true);
    _(t);
    F(t);
}

function G(t) {
    if (this.subs.add(t) && 1 === this.subs.count) this.owner.subscribe(this);
}

function Z(t) {
    if (this.subs.remove(t) && 0 === this.subs.count) this.owner.subscribe(this);
}

W(CollectionLengthObserver);

W(CollectionSizeObserver);

let J;

const X = new WeakMap;

function Y(t, e) {
    if (t === e) return 0;
    t = null === t ? "null" : t.toString();
    e = null === e ? "null" : e.toString();
    return t < e ? -1 : 1;
}

function tt(t, e) {
    if (void 0 === t) if (void 0 === e) return 0; else return 1;
    if (void 0 === e) return -1;
    return 0;
}

function et(t, e, s, r, i) {
    let n, o, u, c, h;
    let a, l;
    for (a = s + 1; a < r; a++) {
        n = t[a];
        o = e[a];
        for (l = a - 1; l >= s; l--) {
            u = t[l];
            c = e[l];
            h = i(u, n);
            if (h > 0) {
                t[l + 1] = u;
                e[l + 1] = c;
            } else break;
        }
        t[l + 1] = n;
        e[l + 1] = o;
    }
}

function st(t, e, s, r, i) {
    let n = 0, o = 0;
    let u, c, h;
    let a, l, f;
    let d, p, v;
    let g, w;
    let b, x, E, A;
    let y, m, U, S;
    while (true) {
        if (r - s <= 10) {
            et(t, e, s, r, i);
            return;
        }
        n = s + (r - s >> 1);
        u = t[s];
        a = e[s];
        c = t[r - 1];
        l = e[r - 1];
        h = t[n];
        f = e[n];
        d = i(u, c);
        if (d > 0) {
            g = u;
            w = a;
            u = c;
            a = l;
            c = g;
            l = w;
        }
        p = i(u, h);
        if (p >= 0) {
            g = u;
            w = a;
            u = h;
            a = f;
            h = c;
            f = l;
            c = g;
            l = w;
        } else {
            v = i(c, h);
            if (v > 0) {
                g = c;
                w = l;
                c = h;
                l = f;
                h = g;
                f = w;
            }
        }
        t[s] = u;
        e[s] = a;
        t[r - 1] = h;
        e[r - 1] = f;
        b = c;
        x = l;
        E = s + 1;
        A = r - 1;
        t[n] = t[E];
        e[n] = e[E];
        t[E] = b;
        e[E] = x;
        t: for (o = E + 1; o < A; o++) {
            y = t[o];
            m = e[o];
            U = i(y, b);
            if (U < 0) {
                t[o] = t[E];
                e[o] = e[E];
                t[E] = y;
                e[E] = m;
                E++;
            } else if (U > 0) {
                do {
                    A--;
                    if (A == o) break t;
                    S = t[A];
                    U = i(S, b);
                } while (U > 0);
                t[o] = t[A];
                e[o] = e[A];
                t[A] = y;
                e[A] = m;
                if (U < 0) {
                    y = t[o];
                    m = e[o];
                    t[o] = t[E];
                    e[o] = e[E];
                    t[E] = y;
                    e[E] = m;
                    E++;
                }
            }
        }
        if (r - A < E - s) {
            st(t, e, A, r, i);
            r = E;
        } else {
            st(t, e, s, E, i);
            s = A;
        }
    }
}

const rt = Array.prototype;

const it = rt.push;

const nt = rt.unshift;

const ot = rt.pop;

const ut = rt.shift;

const ct = rt.splice;

const ht = rt.reverse;

const at = rt.sort;

const lt = {
    push: it,
    unshift: nt,
    pop: ot,
    shift: ut,
    splice: ct,
    reverse: ht,
    sort: at
};

const ft = [ "push", "unshift", "pop", "shift", "splice", "reverse", "sort" ];

const dt = {
    push: function(...t) {
        const e = X.get(this);
        if (void 0 === e) return it.apply(this, t);
        const s = this.length;
        const r = t.length;
        if (0 === r) return s;
        this.length = e.indexMap.length = s + r;
        let i = s;
        while (i < this.length) {
            this[i] = t[i - s];
            e.indexMap[i] = -2;
            i++;
        }
        e.notify();
        return this.length;
    },
    unshift: function(...t) {
        const e = X.get(this);
        if (void 0 === e) return nt.apply(this, t);
        const s = t.length;
        const r = new Array(s);
        let i = 0;
        while (i < s) r[i++] = -2;
        nt.apply(e.indexMap, r);
        const n = nt.apply(this, t);
        e.notify();
        return n;
    },
    pop: function() {
        const t = X.get(this);
        if (void 0 === t) return ot.call(this);
        const e = t.indexMap;
        const s = ot.call(this);
        const r = e.length - 1;
        if (e[r] > -1) e.deletedItems.push(e[r]);
        ot.call(e);
        t.notify();
        return s;
    },
    shift: function() {
        const t = X.get(this);
        if (void 0 === t) return ut.call(this);
        const e = t.indexMap;
        const s = ut.call(this);
        if (e[0] > -1) e.deletedItems.push(e[0]);
        ut.call(e);
        t.notify();
        return s;
    },
    splice: function(...t) {
        const e = t[0];
        const s = t[1];
        const r = X.get(this);
        if (void 0 === r) return ct.apply(this, t);
        const i = this.length;
        const n = 0 | e;
        const o = n < 0 ? Math.max(i + n, 0) : Math.min(n, i);
        const u = r.indexMap;
        const c = t.length;
        const h = 0 === c ? 0 : 1 === c ? i - o : s;
        if (h > 0) {
            let t = o;
            const e = t + h;
            while (t < e) {
                if (u[t] > -1) u.deletedItems.push(u[t]);
                t++;
            }
        }
        if (c > 2) {
            const t = c - 2;
            const r = new Array(t);
            let i = 0;
            while (i < t) r[i++] = -2;
            ct.call(u, e, s, ...r);
        } else ct.apply(u, t);
        const a = ct.apply(this, t);
        r.notify();
        return a;
    },
    reverse: function() {
        const t = X.get(this);
        if (void 0 === t) {
            ht.call(this);
            return this;
        }
        const e = this.length;
        const s = e / 2 | 0;
        let r = 0;
        while (r !== s) {
            const s = e - r - 1;
            const i = this[r];
            const n = t.indexMap[r];
            const o = this[s];
            const u = t.indexMap[s];
            this[r] = o;
            t.indexMap[r] = u;
            this[s] = i;
            t.indexMap[s] = n;
            r++;
        }
        t.notify();
        return this;
    },
    sort: function(t) {
        const e = X.get(this);
        if (void 0 === e) {
            at.call(this, t);
            return this;
        }
        const s = this.length;
        if (s < 2) return this;
        st(this, e.indexMap, 0, s, tt);
        let r = 0;
        while (r < s) {
            if (void 0 === this[r]) break;
            r++;
        }
        if (void 0 === t || !i(t)) t = Y;
        st(this, e.indexMap, 0, r, t);
        e.notify();
        return this;
    }
};

for (const t of ft) r(dt[t], "observing", {
    value: true,
    writable: false,
    configurable: false,
    enumerable: false
});

let pt = false;

function vt() {
    for (const t of ft) if (true !== rt[t].observing) o(rt, t, dt[t]);
}

function gt() {
    for (const t of ft) if (true === rt[t].observing) o(rt, t, lt[t]);
}

class ArrayObserver {
    constructor(t) {
        this.type = 18;
        if (!pt) {
            pt = true;
            vt();
        }
        this.indexObservers = {};
        this.collection = t;
        this.indexMap = I(t.length);
        this.lenObs = void 0;
        X.set(t, this);
    }
    notify() {
        const t = this.indexMap;
        const e = this.collection.length;
        this.indexMap = I(e);
        this.subs.notifyCollection(t, 0);
    }
    getLengthObserver() {
        var t;
        return null !== (t = this.lenObs) && void 0 !== t ? t : this.lenObs = new CollectionLengthObserver(this);
    }
    getIndexObserver(t) {
        var e;
        var s;
        return null !== (e = (s = this.indexObservers)[t]) && void 0 !== e ? e : s[t] = new ArrayIndexObserver(this, t);
    }
}

class ArrayIndexObserver {
    constructor(t, e) {
        this.owner = t;
        this.index = e;
        this.doNotCache = true;
        this.value = this.getValue();
    }
    getValue() {
        return this.owner.collection[this.index];
    }
    setValue(t, e) {
        if (t === this.getValue()) return;
        const s = this.owner;
        const r = this.index;
        const i = s.indexMap;
        if (i[r] > -1) i.deletedItems.push(i[r]);
        i[r] = -2;
        s.collection[r] = t;
        s.notify();
    }
    handleCollectionChange(t, e) {
        const s = this.index;
        const r = t[s] === s;
        if (r) return;
        const i = this.value;
        const n = this.value = this.getValue();
        if (i !== n) this.subs.notify(n, i, e);
    }
    subscribe(t) {
        if (this.subs.add(t) && 1 === this.subs.count) this.owner.subscribe(this);
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) this.owner.unsubscribe(this);
    }
}

F(ArrayObserver);

F(ArrayIndexObserver);

function wt(t) {
    let e = X.get(t);
    if (void 0 === e) e = new ArrayObserver(t);
    return e;
}

function bt(t) {
    let e = 0;
    let s = 0;
    let r = 0;
    const i = M(t);
    const n = i.length;
    for (;r < n; ++r) {
        while (i.deletedItems[s] <= r - e) {
            ++s;
            --e;
        }
        if (-2 === i[r]) ++e; else i[r] += e;
    }
    return i;
}

function xt(t, e) {
    const s = t.slice();
    const r = e.length;
    let i = 0;
    let n = 0;
    while (i < r) {
        n = e[i];
        if (-2 !== n) t[i] = s[n];
        ++i;
    }
}

const Et = new WeakMap;

const At = Set.prototype;

const yt = At.add;

const mt = At.clear;

const Ut = At.delete;

const St = {
    add: yt,
    clear: mt,
    delete: Ut
};

const Ot = [ "add", "clear", "delete" ];

const Ct = {
    add: function(t) {
        const e = Et.get(this);
        if (void 0 === e) {
            yt.call(this, t);
            return this;
        }
        const s = this.size;
        yt.call(this, t);
        const r = this.size;
        if (r === s) return this;
        e.indexMap[s] = -2;
        e.notify();
        return this;
    },
    clear: function() {
        const t = Et.get(this);
        if (void 0 === t) return mt.call(this);
        const e = this.size;
        if (e > 0) {
            const e = t.indexMap;
            let s = 0;
            for (const t of this.keys()) {
                if (e[s] > -1) e.deletedItems.push(e[s]);
                s++;
            }
            mt.call(this);
            e.length = 0;
            t.notify();
        }
        return;
    },
    delete: function(t) {
        const e = Et.get(this);
        if (void 0 === e) return Ut.call(this, t);
        const s = this.size;
        if (0 === s) return false;
        let r = 0;
        const i = e.indexMap;
        for (const s of this.keys()) {
            if (s === t) {
                if (i[r] > -1) i.deletedItems.push(i[r]);
                i.splice(r, 1);
                const s = Ut.call(this, t);
                if (true === s) e.notify();
                return s;
            }
            r++;
        }
        return false;
    }
};

const kt = {
    writable: true,
    enumerable: false,
    configurable: true
};

for (const t of Ot) r(Ct[t], "observing", {
    value: true,
    writable: false,
    configurable: false,
    enumerable: false
});

let $t = false;

function Bt() {
    for (const t of Ot) if (true !== At[t].observing) r(At, t, {
        ...kt,
        value: Ct[t]
    });
}

function Lt() {
    for (const t of Ot) if (true === At[t].observing) r(At, t, {
        ...kt,
        value: St[t]
    });
}

class SetObserver {
    constructor(t) {
        this.type = 34;
        if (!$t) {
            $t = true;
            Bt();
        }
        this.collection = t;
        this.indexMap = I(t.size);
        this.lenObs = void 0;
        Et.set(t, this);
    }
    notify() {
        const t = this.indexMap;
        const e = this.collection.size;
        this.indexMap = I(e);
        this.subs.notifyCollection(t, 0);
    }
    getLengthObserver() {
        var t;
        return null !== (t = this.lenObs) && void 0 !== t ? t : this.lenObs = new CollectionSizeObserver(this);
    }
}

F(SetObserver);

function Rt(t) {
    let e = Et.get(t);
    if (void 0 === e) e = new SetObserver(t);
    return e;
}

const Pt = new WeakMap;

const Tt = Map.prototype;

const jt = Tt.set;

const It = Tt.clear;

const Mt = Tt.delete;

const Dt = {
    set: jt,
    clear: It,
    delete: Mt
};

const Ft = [ "set", "clear", "delete" ];

const Vt = {
    set: function(t, e) {
        const s = Pt.get(this);
        if (void 0 === s) {
            jt.call(this, t, e);
            return this;
        }
        const r = this.get(t);
        const i = this.size;
        jt.call(this, t, e);
        const n = this.size;
        if (n === i) {
            let e = 0;
            for (const i of this.entries()) {
                if (i[0] === t) {
                    if (i[1] !== r) {
                        s.indexMap.deletedItems.push(s.indexMap[e]);
                        s.indexMap[e] = -2;
                        s.notify();
                    }
                    return this;
                }
                e++;
            }
            return this;
        }
        s.indexMap[i] = -2;
        s.notify();
        return this;
    },
    clear: function() {
        const t = Pt.get(this);
        if (void 0 === t) return It.call(this);
        const e = this.size;
        if (e > 0) {
            const e = t.indexMap;
            let s = 0;
            for (const t of this.keys()) {
                if (e[s] > -1) e.deletedItems.push(e[s]);
                s++;
            }
            It.call(this);
            e.length = 0;
            t.notify();
        }
        return;
    },
    delete: function(t) {
        const e = Pt.get(this);
        if (void 0 === e) return Mt.call(this, t);
        const s = this.size;
        if (0 === s) return false;
        let r = 0;
        const i = e.indexMap;
        for (const s of this.keys()) {
            if (s === t) {
                if (i[r] > -1) i.deletedItems.push(i[r]);
                i.splice(r, 1);
                const s = Mt.call(this, t);
                if (true === s) e.notify();
                return s;
            }
            ++r;
        }
        return false;
    }
};

const Nt = {
    writable: true,
    enumerable: false,
    configurable: true
};

for (const t of Ft) r(Vt[t], "observing", {
    value: true,
    writable: false,
    configurable: false,
    enumerable: false
});

let Kt = false;

function qt() {
    for (const t of Ft) if (true !== Tt[t].observing) r(Tt, t, {
        ...Nt,
        value: Vt[t]
    });
}

function _t() {
    for (const t of Ft) if (true === Tt[t].observing) r(Tt, t, {
        ...Nt,
        value: Dt[t]
    });
}

class MapObserver {
    constructor(t) {
        this.type = 66;
        if (!Kt) {
            Kt = true;
            qt();
        }
        this.collection = t;
        this.indexMap = I(t.size);
        this.lenObs = void 0;
        Pt.set(t, this);
    }
    notify() {
        const t = this.indexMap;
        const e = this.collection.size;
        this.indexMap = I(e);
        this.subs.notifyCollection(t, 0);
    }
    getLengthObserver() {
        var t;
        return null !== (t = this.lenObs) && void 0 !== t ? t : this.lenObs = new CollectionSizeObserver(this);
    }
}

F(MapObserver);

function Ht(t) {
    let e = Pt.get(t);
    if (void 0 === e) e = new MapObserver(t);
    return e;
}

function Qt(t, e) {
    const s = this.oL.getObserver(t, e);
    this.obs.add(s);
}

function zt() {
    return o(this, "obs", new BindingObserverRecord(this));
}

function Wt(t) {
    let e;
    if (t instanceof Array) e = wt(t); else if (t instanceof Set) e = Rt(t); else if (t instanceof Map) e = Ht(t); else throw new Error(`AUR0210`);
    this.obs.add(e);
}

function Gt(t) {
    this.obs.add(t);
}

function Zt() {
    throw new Error(`AUR2011:handleChange`);
}

function Jt() {
    throw new Error(`AUR2011:handleCollectionChange`);
}

class BindingObserverRecord {
    constructor(t) {
        this.version = 0;
        this.count = 0;
        this.o = new Map;
        this.b = t;
    }
    handleChange(t, e, s) {
        return this.b.interceptor.handleChange(t, e, s);
    }
    handleCollectionChange(t, e) {
        this.b.interceptor.handleCollectionChange(t, e);
    }
    add(t) {
        if (!this.o.has(t)) {
            t.subscribe(this);
            ++this.count;
        }
        this.o.set(t, this.version);
    }
    clear() {
        this.o.forEach(Yt, this);
        this.count = this.o.size;
    }
    clearAll() {
        this.o.forEach(Xt, this);
        this.o.clear();
        this.count = 0;
    }
}

function Xt(t, e) {
    e.unsubscribe(this);
}

function Yt(t, e) {
    if (this.version !== t) {
        e.unsubscribe(this);
        this.o.delete(e);
    }
}

function te(t) {
    const e = t.prototype;
    u(e, "observe", Qt, true);
    u(e, "observeCollection", Wt, true);
    u(e, "subscribeTo", Gt, true);
    r(e, "obs", {
        get: zt
    });
    u(e, "handleChange", Zt);
    u(e, "handleCollectionChange", Jt);
    return t;
}

function ee(t) {
    return null == t ? te : te(t);
}

class BindingMediator {
    constructor(t, e, s, r) {
        this.key = t;
        this.binding = e;
        this.oL = s;
        this.locator = r;
        this.interceptor = this;
    }
    $bind() {
        throw new Error(`AUR0213:$bind`);
    }
    $unbind() {
        throw new Error(`AUR0214:$unbind`);
    }
    handleChange(t, e, s) {
        this.binding[this.key](t, e, s);
    }
}

te(BindingMediator);

const se = t.DI.createInterface("IExpressionParser", (t => t.singleton(ExpressionParser)));

class ExpressionParser {
    constructor() {
        this.h = c();
        this.A = c();
        this.U = c();
    }
    parse(t, e) {
        let s;
        switch (e) {
          case 16:
            return new CustomExpression(t);

          case 1:
            s = this.U[t];
            if (void 0 === s) s = this.U[t] = this.$parse(t, e);
            return s;

          case 2:
            s = this.A[t];
            if (void 0 === s) s = this.A[t] = this.$parse(t, e);
            return s;

          default:
            if (0 === t.length) {
                if ((e & (4 | 8)) > 0) return PrimitiveLiteralExpression.$empty;
                throw new Error(`AUR0169`);
            }
            s = this.h[t];
            if (void 0 === s) s = this.h[t] = this.$parse(t, e);
            return s;
        }
    }
    $parse(t, e) {
        le.ip = t;
        le.length = t.length;
        le.index = 0;
        le.O = t.charCodeAt(0);
        return de(le, 61, void 0 === e ? 8 : e);
    }
}

exports.Char = void 0;

(function(t) {
    t[t["Null"] = 0] = "Null";
    t[t["Backspace"] = 8] = "Backspace";
    t[t["Tab"] = 9] = "Tab";
    t[t["LineFeed"] = 10] = "LineFeed";
    t[t["VerticalTab"] = 11] = "VerticalTab";
    t[t["FormFeed"] = 12] = "FormFeed";
    t[t["CarriageReturn"] = 13] = "CarriageReturn";
    t[t["Space"] = 32] = "Space";
    t[t["Exclamation"] = 33] = "Exclamation";
    t[t["DoubleQuote"] = 34] = "DoubleQuote";
    t[t["Dollar"] = 36] = "Dollar";
    t[t["Percent"] = 37] = "Percent";
    t[t["Ampersand"] = 38] = "Ampersand";
    t[t["SingleQuote"] = 39] = "SingleQuote";
    t[t["OpenParen"] = 40] = "OpenParen";
    t[t["CloseParen"] = 41] = "CloseParen";
    t[t["Asterisk"] = 42] = "Asterisk";
    t[t["Plus"] = 43] = "Plus";
    t[t["Comma"] = 44] = "Comma";
    t[t["Minus"] = 45] = "Minus";
    t[t["Dot"] = 46] = "Dot";
    t[t["Slash"] = 47] = "Slash";
    t[t["Semicolon"] = 59] = "Semicolon";
    t[t["Backtick"] = 96] = "Backtick";
    t[t["OpenBracket"] = 91] = "OpenBracket";
    t[t["Backslash"] = 92] = "Backslash";
    t[t["CloseBracket"] = 93] = "CloseBracket";
    t[t["Caret"] = 94] = "Caret";
    t[t["Underscore"] = 95] = "Underscore";
    t[t["OpenBrace"] = 123] = "OpenBrace";
    t[t["Bar"] = 124] = "Bar";
    t[t["CloseBrace"] = 125] = "CloseBrace";
    t[t["Colon"] = 58] = "Colon";
    t[t["LessThan"] = 60] = "LessThan";
    t[t["Equals"] = 61] = "Equals";
    t[t["GreaterThan"] = 62] = "GreaterThan";
    t[t["Question"] = 63] = "Question";
    t[t["Zero"] = 48] = "Zero";
    t[t["One"] = 49] = "One";
    t[t["Two"] = 50] = "Two";
    t[t["Three"] = 51] = "Three";
    t[t["Four"] = 52] = "Four";
    t[t["Five"] = 53] = "Five";
    t[t["Six"] = 54] = "Six";
    t[t["Seven"] = 55] = "Seven";
    t[t["Eight"] = 56] = "Eight";
    t[t["Nine"] = 57] = "Nine";
    t[t["UpperA"] = 65] = "UpperA";
    t[t["UpperB"] = 66] = "UpperB";
    t[t["UpperC"] = 67] = "UpperC";
    t[t["UpperD"] = 68] = "UpperD";
    t[t["UpperE"] = 69] = "UpperE";
    t[t["UpperF"] = 70] = "UpperF";
    t[t["UpperG"] = 71] = "UpperG";
    t[t["UpperH"] = 72] = "UpperH";
    t[t["UpperI"] = 73] = "UpperI";
    t[t["UpperJ"] = 74] = "UpperJ";
    t[t["UpperK"] = 75] = "UpperK";
    t[t["UpperL"] = 76] = "UpperL";
    t[t["UpperM"] = 77] = "UpperM";
    t[t["UpperN"] = 78] = "UpperN";
    t[t["UpperO"] = 79] = "UpperO";
    t[t["UpperP"] = 80] = "UpperP";
    t[t["UpperQ"] = 81] = "UpperQ";
    t[t["UpperR"] = 82] = "UpperR";
    t[t["UpperS"] = 83] = "UpperS";
    t[t["UpperT"] = 84] = "UpperT";
    t[t["UpperU"] = 85] = "UpperU";
    t[t["UpperV"] = 86] = "UpperV";
    t[t["UpperW"] = 87] = "UpperW";
    t[t["UpperX"] = 88] = "UpperX";
    t[t["UpperY"] = 89] = "UpperY";
    t[t["UpperZ"] = 90] = "UpperZ";
    t[t["LowerA"] = 97] = "LowerA";
    t[t["LowerB"] = 98] = "LowerB";
    t[t["LowerC"] = 99] = "LowerC";
    t[t["LowerD"] = 100] = "LowerD";
    t[t["LowerE"] = 101] = "LowerE";
    t[t["LowerF"] = 102] = "LowerF";
    t[t["LowerG"] = 103] = "LowerG";
    t[t["LowerH"] = 104] = "LowerH";
    t[t["LowerI"] = 105] = "LowerI";
    t[t["LowerJ"] = 106] = "LowerJ";
    t[t["LowerK"] = 107] = "LowerK";
    t[t["LowerL"] = 108] = "LowerL";
    t[t["LowerM"] = 109] = "LowerM";
    t[t["LowerN"] = 110] = "LowerN";
    t[t["LowerO"] = 111] = "LowerO";
    t[t["LowerP"] = 112] = "LowerP";
    t[t["LowerQ"] = 113] = "LowerQ";
    t[t["LowerR"] = 114] = "LowerR";
    t[t["LowerS"] = 115] = "LowerS";
    t[t["LowerT"] = 116] = "LowerT";
    t[t["LowerU"] = 117] = "LowerU";
    t[t["LowerV"] = 118] = "LowerV";
    t[t["LowerW"] = 119] = "LowerW";
    t[t["LowerX"] = 120] = "LowerX";
    t[t["LowerY"] = 121] = "LowerY";
    t[t["LowerZ"] = 122] = "LowerZ";
})(exports.Char || (exports.Char = {}));

function re(t) {
    switch (t) {
      case 98:
        return 8;

      case 116:
        return 9;

      case 110:
        return 10;

      case 118:
        return 11;

      case 102:
        return 12;

      case 114:
        return 13;

      case 34:
        return 34;

      case 39:
        return 39;

      case 92:
        return 92;

      default:
        return t;
    }
}

exports.Precedence = void 0;

(function(t) {
    t[t["Variadic"] = 61] = "Variadic";
    t[t["Assign"] = 62] = "Assign";
    t[t["Conditional"] = 63] = "Conditional";
    t[t["NullishCoalescing"] = 128] = "NullishCoalescing";
    t[t["LogicalOR"] = 192] = "LogicalOR";
    t[t["LogicalAND"] = 256] = "LogicalAND";
    t[t["Equality"] = 320] = "Equality";
    t[t["Relational"] = 384] = "Relational";
    t[t["Additive"] = 448] = "Additive";
    t[t["Multiplicative"] = 512] = "Multiplicative";
    t[t["Binary"] = 513] = "Binary";
    t[t["LeftHandSide"] = 514] = "LeftHandSide";
    t[t["Primary"] = 515] = "Primary";
    t[t["Unary"] = 516] = "Unary";
})(exports.Precedence || (exports.Precedence = {}));

var ie;

(function(t) {
    t[t["EOF"] = 6291456] = "EOF";
    t[t["ExpressionTerminal"] = 4194304] = "ExpressionTerminal";
    t[t["AccessScopeTerminal"] = 2097152] = "AccessScopeTerminal";
    t[t["ClosingToken"] = 1048576] = "ClosingToken";
    t[t["OpeningToken"] = 524288] = "OpeningToken";
    t[t["BinaryOp"] = 262144] = "BinaryOp";
    t[t["UnaryOp"] = 131072] = "UnaryOp";
    t[t["LeftHandSide"] = 65536] = "LeftHandSide";
    t[t["StringOrNumericLiteral"] = 49152] = "StringOrNumericLiteral";
    t[t["NumericLiteral"] = 32768] = "NumericLiteral";
    t[t["StringLiteral"] = 16384] = "StringLiteral";
    t[t["IdentifierName"] = 12288] = "IdentifierName";
    t[t["Keyword"] = 8192] = "Keyword";
    t[t["Identifier"] = 4096] = "Identifier";
    t[t["Contextual"] = 2048] = "Contextual";
    t[t["OptionalSuffix"] = 13312] = "OptionalSuffix";
    t[t["Precedence"] = 960] = "Precedence";
    t[t["Type"] = 63] = "Type";
    t[t["FalseKeyword"] = 8192] = "FalseKeyword";
    t[t["TrueKeyword"] = 8193] = "TrueKeyword";
    t[t["NullKeyword"] = 8194] = "NullKeyword";
    t[t["UndefinedKeyword"] = 8195] = "UndefinedKeyword";
    t[t["ThisScope"] = 12292] = "ThisScope";
    t[t["ParentScope"] = 12294] = "ParentScope";
    t[t["OpenParen"] = 2688007] = "OpenParen";
    t[t["OpenBrace"] = 524296] = "OpenBrace";
    t[t["Dot"] = 65545] = "Dot";
    t[t["QuestionDot"] = 2162698] = "QuestionDot";
    t[t["CloseBrace"] = 7340043] = "CloseBrace";
    t[t["CloseParen"] = 7340044] = "CloseParen";
    t[t["Comma"] = 6291469] = "Comma";
    t[t["OpenBracket"] = 2688014] = "OpenBracket";
    t[t["CloseBracket"] = 7340047] = "CloseBracket";
    t[t["Colon"] = 6291472] = "Colon";
    t[t["Question"] = 6291475] = "Question";
    t[t["Ampersand"] = 6291476] = "Ampersand";
    t[t["Bar"] = 6291477] = "Bar";
    t[t["QuestionQuestion"] = 6553750] = "QuestionQuestion";
    t[t["BarBar"] = 6553815] = "BarBar";
    t[t["AmpersandAmpersand"] = 6553880] = "AmpersandAmpersand";
    t[t["EqualsEquals"] = 6553945] = "EqualsEquals";
    t[t["ExclamationEquals"] = 6553946] = "ExclamationEquals";
    t[t["EqualsEqualsEquals"] = 6553947] = "EqualsEqualsEquals";
    t[t["ExclamationEqualsEquals"] = 6553948] = "ExclamationEqualsEquals";
    t[t["LessThan"] = 6554013] = "LessThan";
    t[t["GreaterThan"] = 6554014] = "GreaterThan";
    t[t["LessThanEquals"] = 6554015] = "LessThanEquals";
    t[t["GreaterThanEquals"] = 6554016] = "GreaterThanEquals";
    t[t["InKeyword"] = 6562209] = "InKeyword";
    t[t["InstanceOfKeyword"] = 6562210] = "InstanceOfKeyword";
    t[t["Plus"] = 2490851] = "Plus";
    t[t["Minus"] = 2490852] = "Minus";
    t[t["TypeofKeyword"] = 139301] = "TypeofKeyword";
    t[t["VoidKeyword"] = 139302] = "VoidKeyword";
    t[t["Asterisk"] = 6554151] = "Asterisk";
    t[t["Percent"] = 6554152] = "Percent";
    t[t["Slash"] = 6554153] = "Slash";
    t[t["Equals"] = 4194346] = "Equals";
    t[t["Exclamation"] = 131115] = "Exclamation";
    t[t["TemplateTail"] = 2163756] = "TemplateTail";
    t[t["TemplateContinuation"] = 2163757] = "TemplateContinuation";
    t[t["OfKeyword"] = 4204590] = "OfKeyword";
})(ie || (ie = {}));

const ne = PrimitiveLiteralExpression.$false;

const oe = PrimitiveLiteralExpression.$true;

const ue = PrimitiveLiteralExpression.$null;

const ce = PrimitiveLiteralExpression.$undefined;

const he = AccessThisExpression.$this;

const ae = AccessThisExpression.$parent;

exports.ExpressionType = void 0;

(function(t) {
    t[t["None"] = 0] = "None";
    t[t["Interpolation"] = 1] = "Interpolation";
    t[t["IsIterator"] = 2] = "IsIterator";
    t[t["IsFunction"] = 4] = "IsFunction";
    t[t["IsProperty"] = 8] = "IsProperty";
    t[t["IsCustom"] = 16] = "IsCustom";
})(exports.ExpressionType || (exports.ExpressionType = {}));

class ParserState {
    constructor(t) {
        this.ip = t;
        this.index = 0;
        this.C = 0;
        this.$ = 6291456;
        this.B = "";
        this.L = true;
        this.R = false;
        this.length = t.length;
        this.O = t.charCodeAt(0);
    }
    get P() {
        return this.ip.slice(this.C, this.index);
    }
}

const le = new ParserState("");

function fe(t, e) {
    le.ip = t;
    le.length = t.length;
    le.index = 0;
    le.O = t.charCodeAt(0);
    return de(le, 61, void 0 === e ? 8 : e);
}

function de(t, e, s) {
    if (16 === s) return new CustomExpression(t.ip);
    if (0 === t.index) {
        if (1 & s) return Se(t);
        ke(t);
        if (4194304 & t.$) throw new Error(`AUR0151:${t.ip}`);
    }
    t.L = 513 > e;
    t.R = false;
    let r = false;
    let i;
    let n = 0;
    if (131072 & t.$) {
        const e = Fe[63 & t.$];
        ke(t);
        i = new UnaryExpression(e, de(t, 514, s));
        t.L = false;
    } else {
        t: switch (t.$) {
          case 12294:
            t.L = false;
            do {
                ke(t);
                ++n;
                if (Me(t, 65545)) {
                    if (65545 === t.$) throw new Error(`AUR0152:${t.ip}`); else if (6291456 === t.$) throw new Error(`AUR0153:${t.ip}`);
                } else if (2162698 === t.$) {
                    t.R = true;
                    ke(t);
                    if (0 === (12288 & t.$)) {
                        i = 0 === n ? he : 1 === n ? ae : new AccessThisExpression(n);
                        r = true;
                        break t;
                    }
                } else if (2097152 & t.$) {
                    i = 0 === n ? he : 1 === n ? ae : new AccessThisExpression(n);
                    break t;
                } else throw new Error(`AUR0154:${t.ip}`);
            } while (12294 === t.$);

          case 4096:
            if (2 & s) i = new BindingIdentifier(t.B); else i = new AccessScopeExpression(t.B, n);
            t.L = !t.R;
            ke(t);
            break;

          case 12292:
            t.L = false;
            ke(t);
            i = he;
            break;

          case 2688007:
            {
                ke(t);
                const e = t.R;
                i = de(t, 62, s);
                t.R = e;
                De(t, 7340044);
                break;
            }

          case 2688014:
            i = t.ip.search(/\s+of\s+/) > t.index ? pe(t) : ye(t, s);
            break;

          case 524296:
            i = Ue(t, s);
            break;

          case 2163756:
            i = new TemplateExpression([ t.B ]);
            t.L = false;
            ke(t);
            break;

          case 2163757:
            i = Oe(t, s, i, false);
            break;

          case 16384:
          case 32768:
            i = new PrimitiveLiteralExpression(t.B);
            t.L = false;
            ke(t);
            break;

          case 8194:
          case 8195:
          case 8193:
          case 8192:
            i = Fe[63 & t.$];
            t.L = false;
            ke(t);
            break;

          default:
            if (t.index >= t.length) throw new Error(`AUR0155:${t.ip}`); else throw new Error(`AUR0156:${t.ip}`);
        }
        if (2 & s) return me(t, i);
        if (514 < e) return i;
        if (1793 === i.$kind) switch (t.$) {
          case 2162698:
            t.R = true;
            t.L = false;
            ke(t);
            xe(t);
            if (12288 & t.$) {
                i = new AccessScopeExpression(t.B, i.ancestor);
                ke(t);
            } else if (2688007 === t.$) i = new CallFunctionExpression(i, ve(t), true); else if (2688014 === t.$) i = ge(t, i, true); else throw Ae(t);
            break;

          case 65545:
            t.L = !t.R;
            ke(t);
            Ee(t);
            i = new AccessScopeExpression(t.B, i.ancestor);
            ke(t);
            break;

          case 2688007:
            i = new CallFunctionExpression(i, ve(t), r);
            break;

          case 2688014:
            i = ge(t, i, r);
            break;

          case 2163756:
            i = Ce(t, i);
            break;

          case 2163757:
            i = Oe(t, s, i, true);
            break;
        }
        while ((65536 & t.$) > 0) switch (t.$) {
          case 2162698:
            i = we(t, i);
            break;

          case 65545:
            ke(t);
            Ee(t);
            i = be(t, i, false);
            break;

          case 2688007:
            if (10082 === i.$kind) i = new CallScopeExpression(i.name, ve(t), i.ancestor, false); else if (9323 === i.$kind) i = new CallMemberExpression(i.object, i.name, ve(t), i.optional, false); else i = new CallFunctionExpression(i, ve(t), false);
            break;

          case 2688014:
            i = ge(t, i, false);
            break;

          case 2163756:
            if (t.R) throw Ae(t);
            i = Ce(t, i);
            break;

          case 2163757:
            if (t.R) throw Ae(t);
            i = Oe(t, s, i, true);
            break;
        }
    }
    if (513 < e) return i;
    while ((262144 & t.$) > 0) {
        const r = t.$;
        if ((960 & r) <= e) break;
        ke(t);
        i = new BinaryExpression(Fe[63 & r], i, de(t, 960 & r, s));
        t.L = false;
    }
    if (63 < e) return i;
    if (Me(t, 6291475)) {
        const e = de(t, 62, s);
        De(t, 6291472);
        i = new ConditionalExpression(i, e, de(t, 62, s));
        t.L = false;
    }
    if (62 < e) return i;
    if (Me(t, 4194346)) {
        if (!t.L) throw new Error(`AUR0158:${t.ip}`);
        i = new AssignExpression(i, de(t, 62, s));
    }
    if (61 < e) return i;
    while (Me(t, 6291477)) {
        if (6291456 === t.$) throw new Error(`AUR0159:${t.ip}`);
        const e = t.B;
        ke(t);
        const r = new Array;
        while (Me(t, 6291472)) r.push(de(t, 62, s));
        i = new ValueConverterExpression(i, e, r);
    }
    while (Me(t, 6291476)) {
        if (6291456 === t.$) throw new Error(`AUR0160:${t.ip}`);
        const e = t.B;
        ke(t);
        const r = new Array;
        while (Me(t, 6291472)) r.push(de(t, 62, s));
        i = new BindingBehaviorExpression(i, e, r);
    }
    if (6291456 !== t.$) {
        if (1 & s) return i;
        if ("of" === t.P) throw new Error(`AUR0161:${t.ip}`);
        throw new Error(`AUR0162:${t.ip}`);
    }
    return i;
}

function pe(t) {
    const e = [];
    const s = new DestructuringAssignmentExpression(90137, e, void 0, void 0);
    let r = "";
    let i = true;
    let n = 0;
    while (i) {
        ke(t);
        switch (t.$) {
          case 7340047:
            i = false;
            o();
            break;

          case 6291469:
            o();
            break;

          case 4096:
            r = t.P;
            break;

          default:
            throw new Error(`AUR0170:${t.ip}`);
        }
    }
    De(t, 7340047);
    return s;
    function o() {
        if ("" !== r) {
            e.push(new DestructuringAssignmentSingleExpression(new AccessMemberExpression(he, r), new AccessKeyedExpression(he, new PrimitiveLiteralExpression(n++)), void 0));
            r = "";
        } else n++;
    }
}

function ve(t) {
    const e = t.R;
    ke(t);
    const s = [];
    while (7340044 !== t.$) {
        s.push(de(t, 62, 0));
        if (!Me(t, 6291469)) break;
    }
    De(t, 7340044);
    t.L = false;
    t.R = e;
    return s;
}

function ge(t, e, s) {
    const r = t.R;
    ke(t);
    e = new AccessKeyedExpression(e, de(t, 62, 0), s);
    De(t, 7340047);
    t.L = !r;
    t.R = r;
    return e;
}

function we(t, e) {
    t.R = true;
    t.L = false;
    ke(t);
    xe(t);
    if (12288 & t.$) return be(t, e, true);
    if (2688007 === t.$) if (10082 === e.$kind) return new CallScopeExpression(e.name, ve(t), e.ancestor, true); else if (9323 === e.$kind) return new CallMemberExpression(e.object, e.name, ve(t), e.optional, true); else return new CallFunctionExpression(e, ve(t), true);
    if (2688014 === t.$) return ge(t, e, true);
    throw Ae(t);
}

function be(t, e, s) {
    const r = t.B;
    switch (t.$) {
      case 2162698:
        t.R = true;
        t.L = false;
        $e(t);
        ke(t);
        xe(t);
        if (2688007 === t.$) return new CallMemberExpression(e, r, ve(t), s, true);
        Be(t);
        return new AccessMemberExpression(e, r, s);

      case 2688007:
        t.L = false;
        return new CallMemberExpression(e, r, ve(t), s, false);

      default:
        t.L = !t.R;
        ke(t);
        return new AccessMemberExpression(e, r, s);
    }
}

function xe(t) {
    if (0 === (13312 & t.$)) throw new Error(`AUR0171:${t.ip}`);
}

function Ee(t) {
    if (0 === (12288 & t.$)) throw new Error(`AUR0153:${t.ip}`);
}

function Ae(t) {
    return new Error(`AUR0172:${t.ip}`);
}

function ye(t, e) {
    const s = t.R;
    ke(t);
    const r = new Array;
    while (7340047 !== t.$) if (Me(t, 6291469)) {
        r.push(ce);
        if (7340047 === t.$) break;
    } else {
        r.push(de(t, 62, ~2 & e));
        if (Me(t, 6291469)) {
            if (7340047 === t.$) break;
        } else break;
    }
    t.R = s;
    De(t, 7340047);
    if (2 & e) return new ArrayBindingPattern(r); else {
        t.L = false;
        return new ArrayLiteralExpression(r);
    }
}

function me(t, e) {
    if (0 === (65536 & e.$kind)) throw new Error(`AUR0163:${t.ip}`);
    if (4204590 !== t.$) throw new Error(`AUR0163:${t.ip}`);
    ke(t);
    const s = e;
    const r = de(t, 61, 0);
    return new ForOfStatement(s, r);
}

function Ue(t, e) {
    const s = t.R;
    const r = new Array;
    const i = new Array;
    ke(t);
    while (7340043 !== t.$) {
        r.push(t.B);
        if (49152 & t.$) {
            ke(t);
            De(t, 6291472);
            i.push(de(t, 62, ~2 & e));
        } else if (12288 & t.$) {
            const {O: s, $: r, index: n} = t;
            ke(t);
            if (Me(t, 6291472)) i.push(de(t, 62, ~2 & e)); else {
                t.O = s;
                t.$ = r;
                t.index = n;
                i.push(de(t, 515, ~2 & e));
            }
        } else throw new Error(`AUR0164:${t.ip}`);
        if (7340043 !== t.$) De(t, 6291469);
    }
    t.R = s;
    De(t, 7340043);
    if (2 & e) return new ObjectBindingPattern(r, i); else {
        t.L = false;
        return new ObjectLiteralExpression(r, i);
    }
}

function Se(t) {
    const e = [];
    const s = [];
    const r = t.length;
    let i = "";
    while (t.index < r) {
        switch (t.O) {
          case 36:
            if (123 === t.ip.charCodeAt(t.index + 1)) {
                e.push(i);
                i = "";
                t.index += 2;
                t.O = t.ip.charCodeAt(t.index);
                ke(t);
                const r = de(t, 61, 1);
                s.push(r);
                continue;
            } else i += "$";
            break;

          case 92:
            i += String.fromCharCode(re(Le(t)));
            break;

          default:
            i += String.fromCharCode(t.O);
        }
        Le(t);
    }
    if (s.length) {
        e.push(i);
        return new Interpolation(e, s);
    }
    return null;
}

function Oe(t, e, s, r) {
    const i = t.R;
    const n = [ t.B ];
    De(t, 2163757);
    const o = [ de(t, 62, e) ];
    while (2163756 !== (t.$ = Ie(t))) {
        n.push(t.B);
        De(t, 2163757);
        o.push(de(t, 62, e));
    }
    n.push(t.B);
    t.L = false;
    t.R = i;
    if (r) {
        ke(t);
        return new TaggedTemplateExpression(n, n, s, o);
    } else {
        ke(t);
        return new TemplateExpression(n, o);
    }
}

function Ce(t, e) {
    t.L = false;
    const s = [ t.B ];
    ke(t);
    return new TaggedTemplateExpression(s, s, e);
}

function ke(t) {
    while (t.index < t.length) {
        t.C = t.index;
        if (null != (t.$ = ze[t.O](t))) return;
    }
    t.$ = 6291456;
}

const {save: $e, restore: Be} = function() {
    let t = 0;
    let e = 0;
    let s = 6291456;
    let r = 0;
    let i = "";
    let n = true;
    let o = false;
    function u(u) {
        t = u.index;
        e = u.C;
        s = u.$;
        r = u.O;
        i = u.B;
        n = u.L;
        o = u.R;
    }
    function c(u) {
        u.index = t;
        u.C = e;
        u.$ = s;
        u.O = r;
        u.B = i;
        u.L = n;
        u.R = o;
    }
    return {
        save: u,
        restore: c
    };
}();

function Le(t) {
    return t.O = t.ip.charCodeAt(++t.index);
}

function Re(t) {
    while (Qe[Le(t)]) ;
    const e = Ve[t.B = t.P];
    return void 0 === e ? 4096 : e;
}

function Pe(t, e) {
    let s = t.O;
    if (false === e) {
        do {
            s = Le(t);
        } while (s <= 57 && s >= 48);
        if (46 !== s) {
            t.B = parseInt(t.P, 10);
            return 32768;
        }
        s = Le(t);
        if (t.index >= t.length) {
            t.B = parseInt(t.P.slice(0, -1), 10);
            return 32768;
        }
    }
    if (s <= 57 && s >= 48) do {
        s = Le(t);
    } while (s <= 57 && s >= 48); else t.O = t.ip.charCodeAt(--t.index);
    t.B = parseFloat(t.P);
    return 32768;
}

function Te(t) {
    const e = t.O;
    Le(t);
    let s = 0;
    const r = new Array;
    let i = t.index;
    while (t.O !== e) if (92 === t.O) {
        r.push(t.ip.slice(i, t.index));
        Le(t);
        s = re(t.O);
        Le(t);
        r.push(String.fromCharCode(s));
        i = t.index;
    } else if (t.index >= t.length) throw new Error(`AUR0165:${t.ip}`); else Le(t);
    const n = t.ip.slice(i, t.index);
    Le(t);
    r.push(n);
    const o = r.join("");
    t.B = o;
    return 16384;
}

function je(t) {
    let e = true;
    let s = "";
    while (96 !== Le(t)) if (36 === t.O) if (t.index + 1 < t.length && 123 === t.ip.charCodeAt(t.index + 1)) {
        t.index++;
        e = false;
        break;
    } else s += "$"; else if (92 === t.O) s += String.fromCharCode(re(Le(t))); else {
        if (t.index >= t.length) throw new Error(`AUR0166:${t.ip}`);
        s += String.fromCharCode(t.O);
    }
    Le(t);
    t.B = s;
    if (e) return 2163756;
    return 2163757;
}

function Ie(t) {
    if (t.index >= t.length) throw new Error(`AUR0166:${t.ip}`);
    t.index--;
    return je(t);
}

function Me(t, e) {
    if (t.$ === e) {
        ke(t);
        return true;
    }
    return false;
}

function De(t, e) {
    if (t.$ === e) ke(t); else throw new Error(`AUR0167:${t.ip}<${e}`);
}

const Fe = [ ne, oe, ue, ce, "$this", null, "$parent", "(", "{", ".", "?.", "}", ")", ",", "[", "]", ":", "?", "'", '"', "&", "|", "??", "||", "&&", "==", "!=", "===", "!==", "<", ">", "<=", ">=", "in", "instanceof", "+", "-", "typeof", "void", "*", "%", "/", "=", "!", 2163756, 2163757, "of" ];

const Ve = c();

Ve.true = 8193;

Ve.null = 8194;

Ve.false = 8192;

Ve.undefined = 8195;

Ve.$this = 12292;

Ve.$parent = 12294;

Ve.in = 6562209;

Ve.instanceof = 6562210;

Ve.typeof = 139301;

Ve.void = 139302;

Ve.of = 4204590;

const Ne = {
    AsciiIdPart: [ 36, 0, 48, 58, 65, 91, 95, 0, 97, 123 ],
    IdStart: [ 36, 0, 65, 91, 95, 0, 97, 123, 170, 0, 186, 0, 192, 215, 216, 247, 248, 697, 736, 741, 7424, 7462, 7468, 7517, 7522, 7526, 7531, 7544, 7545, 7615, 7680, 7936, 8305, 0, 8319, 0, 8336, 8349, 8490, 8492, 8498, 0, 8526, 0, 8544, 8585, 11360, 11392, 42786, 42888, 42891, 42927, 42928, 42936, 42999, 43008, 43824, 43867, 43868, 43877, 64256, 64263, 65313, 65339, 65345, 65371 ],
    Digit: [ 48, 58 ],
    Skip: [ 0, 33, 127, 161 ]
};

function Ke(t, e, s, r) {
    const i = s.length;
    for (let n = 0; n < i; n += 2) {
        const i = s[n];
        let o = s[n + 1];
        o = o > 0 ? o : i + 1;
        if (t) t.fill(r, i, o);
        if (e) for (let t = i; t < o; t++) e.add(t);
    }
}

function qe(t) {
    return e => {
        Le(e);
        return t;
    };
}

const _e = t => {
    throw new Error(`AUR0168:${t.ip}`);
};

_e.notMapped = true;

const He = new Set;

Ke(null, He, Ne.AsciiIdPart, true);

const Qe = new Uint8Array(65535);

Ke(Qe, null, Ne.IdStart, 1);

Ke(Qe, null, Ne.Digit, 1);

const ze = new Array(65535);

ze.fill(_e, 0, 65535);

Ke(ze, null, Ne.Skip, (t => {
    Le(t);
    return null;
}));

Ke(ze, null, Ne.IdStart, Re);

Ke(ze, null, Ne.Digit, (t => Pe(t, false)));

ze[34] = ze[39] = t => Te(t);

ze[96] = t => je(t);

ze[33] = t => {
    if (61 !== Le(t)) return 131115;
    if (61 !== Le(t)) return 6553946;
    Le(t);
    return 6553948;
};

ze[61] = t => {
    if (61 !== Le(t)) return 4194346;
    if (61 !== Le(t)) return 6553945;
    Le(t);
    return 6553947;
};

ze[38] = t => {
    if (38 !== Le(t)) return 6291476;
    Le(t);
    return 6553880;
};

ze[124] = t => {
    if (124 !== Le(t)) return 6291477;
    Le(t);
    return 6553815;
};

ze[63] = t => {
    if (46 === Le(t)) {
        const e = t.ip.charCodeAt(t.index + 1);
        if (e <= 48 || e >= 57) {
            Le(t);
            return 2162698;
        }
        return 6291475;
    }
    if (63 !== t.O) return 6291475;
    if (61 === Le(t)) throw new Error("Operator ??= is not supported.");
    return 6553750;
};

ze[46] = t => {
    if (Le(t) <= 57 && t.O >= 48) return Pe(t, true);
    return 65545;
};

ze[60] = t => {
    if (61 !== Le(t)) return 6554013;
    Le(t);
    return 6554015;
};

ze[62] = t => {
    if (61 !== Le(t)) return 6554014;
    Le(t);
    return 6554016;
};

ze[37] = qe(6554152);

ze[40] = qe(2688007);

ze[41] = qe(7340044);

ze[42] = qe(6554151);

ze[43] = qe(2490851);

ze[44] = qe(6291469);

ze[45] = qe(2490852);

ze[47] = qe(6554153);

ze[58] = qe(6291472);

ze[91] = qe(2688014);

ze[93] = qe(7340047);

ze[123] = qe(524296);

ze[125] = qe(7340043);

let We = null;

const Ge = [];

let Ze = false;

function Je() {
    Ze = false;
}

function Xe() {
    Ze = true;
}

function Ye() {
    return We;
}

function ts(t) {
    if (null == t) throw new Error(`AUR0206`);
    if (null == We) {
        We = t;
        Ge[0] = We;
        Ze = true;
        return;
    }
    if (We === t) throw new Error(`AUR0207`);
    Ge.push(t);
    We = t;
    Ze = true;
}

function es(t) {
    if (null == t) throw new Error(`AUR0208`);
    if (We !== t) throw new Error(`AUR0209`);
    Ge.pop();
    We = Ge.length > 0 ? Ge[Ge.length - 1] : null;
    Ze = null != We;
}

const ss = Object.freeze({
    get current() {
        return We;
    },
    get connecting() {
        return Ze;
    },
    enter: ts,
    exit: es,
    pause: Je,
    resume: Xe
});

const rs = Reflect.get;

const is = Object.prototype.toString;

const ns = new WeakMap;

function os(t) {
    switch (is.call(t)) {
      case "[object Object]":
      case "[object Array]":
      case "[object Map]":
      case "[object Set]":
        return true;

      default:
        return false;
    }
}

const us = "__raw__";

function cs(t) {
    return os(t) ? hs(t) : t;
}

function hs(t) {
    var e;
    return null !== (e = ns.get(t)) && void 0 !== e ? e : ds(t);
}

function as(t) {
    var e;
    return null !== (e = t[us]) && void 0 !== e ? e : t;
}

function ls(t) {
    return os(t) && t[us] || t;
}

function fs(t) {
    return "constructor" === t || "__proto__" === t || "$observers" === t || t === Symbol.toPrimitive || t === Symbol.toStringTag;
}

function ds(t) {
    const e = t instanceof Array ? vs : t instanceof Map || t instanceof Set ? Ds : ps;
    const s = new Proxy(t, e);
    ns.set(t, s);
    return s;
}

const ps = {
    get(t, e, s) {
        if (e === us) return t;
        const r = Ye();
        if (!Ze || fs(e) || null == r) return rs(t, e, s);
        r.observe(t, e);
        return cs(rs(t, e, s));
    }
};

const vs = {
    get(t, e, s) {
        if (e === us) return t;
        const r = Ye();
        if (!Ze || fs(e) || null == r) return rs(t, e, s);
        switch (e) {
          case "length":
            r.observe(t, "length");
            return t.length;

          case "map":
            return gs;

          case "includes":
            return xs;

          case "indexOf":
            return Es;

          case "lastIndexOf":
            return As;

          case "every":
            return ws;

          case "filter":
            return bs;

          case "find":
            return ms;

          case "findIndex":
            return ys;

          case "flat":
            return Us;

          case "flatMap":
            return Ss;

          case "join":
            return Os;

          case "push":
            return ks;

          case "pop":
            return Cs;

          case "reduce":
            return Is;

          case "reduceRight":
            return Ms;

          case "reverse":
            return Rs;

          case "shift":
            return $s;

          case "unshift":
            return Bs;

          case "slice":
            return js;

          case "splice":
            return Ls;

          case "some":
            return Ps;

          case "sort":
            return Ts;

          case "keys":
            return Qs;

          case "values":
          case Symbol.iterator:
            return zs;

          case "entries":
            return Ws;
        }
        r.observe(t, e);
        return cs(rs(t, e, s));
    },
    ownKeys(t) {
        var e;
        null === (e = Ye()) || void 0 === e ? void 0 : e.observe(t, "length");
        return Reflect.ownKeys(t);
    }
};

function gs(t, e) {
    var s;
    const r = as(this);
    const i = r.map(((s, r) => ls(t.call(e, cs(s), r, this))));
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return cs(i);
}

function ws(t, e) {
    var s;
    const r = as(this);
    const i = r.every(((s, r) => t.call(e, cs(s), r, this)));
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return i;
}

function bs(t, e) {
    var s;
    const r = as(this);
    const i = r.filter(((s, r) => ls(t.call(e, cs(s), r, this))));
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return cs(i);
}

function xs(t) {
    var e;
    const s = as(this);
    const r = s.includes(ls(t));
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return r;
}

function Es(t) {
    var e;
    const s = as(this);
    const r = s.indexOf(ls(t));
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return r;
}

function As(t) {
    var e;
    const s = as(this);
    const r = s.lastIndexOf(ls(t));
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return r;
}

function ys(t, e) {
    var s;
    const r = as(this);
    const i = r.findIndex(((s, r) => ls(t.call(e, cs(s), r, this))));
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return i;
}

function ms(t, e) {
    var s;
    const r = as(this);
    const i = r.find(((e, s) => t(cs(e), s, this)), e);
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return cs(i);
}

function Us() {
    var t;
    const e = as(this);
    null === (t = Ye()) || void 0 === t ? void 0 : t.observeCollection(e);
    return cs(e.flat());
}

function Ss(t, e) {
    var s;
    const r = as(this);
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return hs(r.flatMap(((s, r) => cs(t.call(e, cs(s), r, this)))));
}

function Os(t) {
    var e;
    const s = as(this);
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return s.join(t);
}

function Cs() {
    return cs(as(this).pop());
}

function ks(...t) {
    return as(this).push(...t);
}

function $s() {
    return cs(as(this).shift());
}

function Bs(...t) {
    return as(this).unshift(...t);
}

function Ls(...t) {
    return cs(as(this).splice(...t));
}

function Rs(...t) {
    var e;
    const s = as(this);
    const r = s.reverse();
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return cs(r);
}

function Ps(t, e) {
    var s;
    const r = as(this);
    const i = r.some(((s, r) => ls(t.call(e, cs(s), r, this))));
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return i;
}

function Ts(t) {
    var e;
    const s = as(this);
    const r = s.sort(t);
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return cs(r);
}

function js(t, e) {
    var s;
    const r = as(this);
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return hs(r.slice(t, e));
}

function Is(t, e) {
    var s;
    const r = as(this);
    const i = r.reduce(((e, s, r) => t(e, cs(s), r, this)), e);
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return cs(i);
}

function Ms(t, e) {
    var s;
    const r = as(this);
    const i = r.reduceRight(((e, s, r) => t(e, cs(s), r, this)), e);
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return cs(i);
}

const Ds = {
    get(t, e, s) {
        if (e === us) return t;
        const r = Ye();
        if (!Ze || fs(e) || null == r) return rs(t, e, s);
        switch (e) {
          case "size":
            r.observe(t, "size");
            return t.size;

          case "clear":
            return _s;

          case "delete":
            return Hs;

          case "forEach":
            return Fs;

          case "add":
            if (t instanceof Set) return qs;
            break;

          case "get":
            if (t instanceof Map) return Ns;
            break;

          case "set":
            if (t instanceof Map) return Ks;
            break;

          case "has":
            return Vs;

          case "keys":
            return Qs;

          case "values":
            return zs;

          case "entries":
            return Ws;

          case Symbol.iterator:
            return t instanceof Map ? Ws : zs;
        }
        return cs(rs(t, e, s));
    }
};

function Fs(t, e) {
    var s;
    const r = as(this);
    null === (s = Ye()) || void 0 === s ? void 0 : s.observeCollection(r);
    return r.forEach(((s, r) => {
        t.call(e, cs(s), cs(r), this);
    }));
}

function Vs(t) {
    var e;
    const s = as(this);
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return s.has(ls(t));
}

function Ns(t) {
    var e;
    const s = as(this);
    null === (e = Ye()) || void 0 === e ? void 0 : e.observeCollection(s);
    return cs(s.get(ls(t)));
}

function Ks(t, e) {
    return cs(as(this).set(ls(t), ls(e)));
}

function qs(t) {
    return cs(as(this).add(ls(t)));
}

function _s() {
    return cs(as(this).clear());
}

function Hs(t) {
    return cs(as(this).delete(ls(t)));
}

function Qs() {
    var t;
    const e = as(this);
    null === (t = Ye()) || void 0 === t ? void 0 : t.observeCollection(e);
    const s = e.keys();
    return {
        next() {
            const t = s.next();
            const e = t.value;
            const r = t.done;
            return r ? {
                value: void 0,
                done: r
            } : {
                value: cs(e),
                done: r
            };
        },
        [Symbol.iterator]() {
            return this;
        }
    };
}

function zs() {
    var t;
    const e = as(this);
    null === (t = Ye()) || void 0 === t ? void 0 : t.observeCollection(e);
    const s = e.values();
    return {
        next() {
            const t = s.next();
            const e = t.value;
            const r = t.done;
            return r ? {
                value: void 0,
                done: r
            } : {
                value: cs(e),
                done: r
            };
        },
        [Symbol.iterator]() {
            return this;
        }
    };
}

function Ws() {
    var t;
    const e = as(this);
    null === (t = Ye()) || void 0 === t ? void 0 : t.observeCollection(e);
    const s = e.entries();
    return {
        next() {
            const t = s.next();
            const e = t.value;
            const r = t.done;
            return r ? {
                value: void 0,
                done: r
            } : {
                value: [ cs(e[0]), cs(e[1]) ],
                done: r
            };
        },
        [Symbol.iterator]() {
            return this;
        }
    };
}

const Gs = Object.freeze({
    getProxy: hs,
    getRaw: as,
    wrap: cs,
    unwrap: ls,
    rawKey: us
});

class ComputedObserver {
    constructor(t, e, s, r, i) {
        this.interceptor = this;
        this.type = 1;
        this.v = void 0;
        this.ov = void 0;
        this.ir = false;
        this.D = false;
        this.o = t;
        this.get = e;
        this.set = s;
        this.up = r;
        this.oL = i;
    }
    static create(t, e, s, i, n) {
        const o = s.get;
        const u = s.set;
        const c = new ComputedObserver(t, o, u, n, i);
        const h = () => c.getValue();
        h.getObserver = () => c;
        r(t, e, {
            enumerable: s.enumerable,
            configurable: true,
            get: h,
            set: t => {
                c.setValue(t, 0);
            }
        });
        return c;
    }
    getValue() {
        if (0 === this.subs.count) return this.get.call(this.o, this);
        if (this.D) {
            this.compute();
            this.D = false;
        }
        return this.v;
    }
    setValue(t, e) {
        if (i(this.set)) {
            if (t !== this.v) {
                this.ir = true;
                this.set.call(this.o, t);
                this.ir = false;
                this.run();
            }
        } else throw new Error(`AUR0221`);
    }
    handleChange() {
        this.D = true;
        if (this.subs.count > 0) this.run();
    }
    handleCollectionChange() {
        this.D = true;
        if (this.subs.count > 0) this.run();
    }
    subscribe(t) {
        if (this.subs.add(t) && 1 === this.subs.count) {
            this.compute();
            this.D = false;
        }
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) {
            this.D = true;
            this.obs.clearAll();
        }
    }
    flush() {
        Zs = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, Zs, 0);
    }
    run() {
        if (this.ir) return;
        const t = this.v;
        const e = this.compute();
        this.D = false;
        if (!Object.is(e, t)) {
            this.ov = t;
            this.queue.add(this);
        }
    }
    compute() {
        this.ir = true;
        this.obs.version++;
        try {
            ts(this);
            return this.v = ls(this.get.call(this.up ? cs(this.o) : this.o, this));
        } finally {
            this.obs.clear();
            this.ir = false;
            es(this);
        }
    }
}

ee(ComputedObserver);

F(ComputedObserver);

_(ComputedObserver);

let Zs;

const Js = t.DI.createInterface("IDirtyChecker", (t => t.singleton(DirtyChecker)));

const Xs = {
    timeoutsPerCheck: 25,
    disabled: false,
    throw: false,
    resetToDefault() {
        this.timeoutsPerCheck = 6;
        this.disabled = false;
        this.throw = false;
    }
};

const Ys = {
    persistent: true
};

class DirtyChecker {
    constructor(t) {
        this.p = t;
        this.tracked = [];
        this.T = null;
        this.j = 0;
        this.check = () => {
            if (Xs.disabled) return;
            if (++this.j < Xs.timeoutsPerCheck) return;
            this.j = 0;
            const t = this.tracked;
            const e = t.length;
            let s;
            let r = 0;
            for (;r < e; ++r) {
                s = t[r];
                if (s.isDirty()) this.queue.add(s);
            }
        };
    }
    createProperty(t, e) {
        if (Xs.throw) throw new Error(`AUR0222:${e}`);
        return new DirtyCheckProperty(this, t, e);
    }
    addProperty(t) {
        this.tracked.push(t);
        if (1 === this.tracked.length) this.T = this.p.taskQueue.queueTask(this.check, Ys);
    }
    removeProperty(t) {
        this.tracked.splice(this.tracked.indexOf(t), 1);
        if (0 === this.tracked.length) {
            this.T.cancel();
            this.T = null;
        }
    }
}

DirtyChecker.inject = [ t.IPlatform ];

_(DirtyChecker);

class DirtyCheckProperty {
    constructor(t, e, s) {
        this.obj = e;
        this.key = s;
        this.type = 0;
        this.ov = void 0;
        this.I = t;
    }
    getValue() {
        return this.obj[this.key];
    }
    setValue(t, e) {
        throw new Error(`Trying to set value for property ${this.key} in dirty checker`);
    }
    isDirty() {
        return this.ov !== this.obj[this.key];
    }
    flush() {
        const t = this.ov;
        const e = this.getValue();
        this.ov = e;
        this.subs.notify(e, t, 0);
    }
    subscribe(t) {
        if (this.subs.add(t) && 1 === this.subs.count) {
            this.ov = this.obj[this.key];
            this.I.addProperty(this);
        }
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && 0 === this.subs.count) this.I.removeProperty(this);
    }
}

F(DirtyCheckProperty);

class PrimitiveObserver {
    constructor(t, e) {
        this.type = 0;
        this.o = t;
        this.k = e;
    }
    get doNotCache() {
        return true;
    }
    getValue() {
        return this.o[this.k];
    }
    setValue() {}
    subscribe() {}
    unsubscribe() {}
}

class PropertyAccessor {
    constructor() {
        this.type = 0;
    }
    getValue(t, e) {
        return t[e];
    }
    setValue(t, e, s, r) {
        s[r] = t;
    }
}

let tr;

class SetterObserver {
    constructor(t, e) {
        this.type = 1;
        this.v = void 0;
        this.ov = void 0;
        this.iO = false;
        this.f = 0;
        this.o = t;
        this.k = e;
    }
    getValue() {
        return this.v;
    }
    setValue(t, e) {
        if (this.iO) {
            if (Object.is(t, this.v)) return;
            this.ov = this.v;
            this.v = t;
            this.f = e;
            this.queue.add(this);
        } else this.o[this.k] = t;
    }
    subscribe(t) {
        if (false === this.iO) this.start();
        this.subs.add(t);
    }
    flush() {
        tr = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, tr, this.f);
    }
    start() {
        if (false === this.iO) {
            this.iO = true;
            this.v = this.o[this.k];
            r(this.o, this.k, {
                enumerable: true,
                configurable: true,
                get: () => this.getValue(),
                set: t => {
                    this.setValue(t, 0);
                }
            });
        }
        return this;
    }
    stop() {
        if (this.iO) {
            r(this.o, this.k, {
                enumerable: true,
                configurable: true,
                writable: true,
                value: this.v
            });
            this.iO = false;
        }
        return this;
    }
}

class SetterNotifier {
    constructor(t, e, s, r) {
        this.type = 1;
        this.v = void 0;
        this.ov = void 0;
        this.f = 0;
        this.o = t;
        this.S = s;
        this.hs = i(s);
        const n = t[e];
        this.cb = i(n) ? n : void 0;
        this.v = r;
    }
    getValue() {
        return this.v;
    }
    setValue(t, e) {
        var s;
        if (this.hs) t = this.S(t, null);
        if (!Object.is(t, this.v)) {
            this.ov = this.v;
            this.v = t;
            this.f = e;
            null === (s = this.cb) || void 0 === s ? void 0 : s.call(this.o, this.v, this.ov, e);
            this.queue.add(this);
        }
    }
    flush() {
        tr = this.ov;
        this.ov = this.v;
        this.subs.notify(this.v, tr, this.f);
    }
}

F(SetterObserver);

F(SetterNotifier);

_(SetterObserver);

_(SetterNotifier);

const er = new PropertyAccessor;

const sr = t.DI.createInterface("IObserverLocator", (t => t.singleton(ObserverLocator)));

const rr = t.DI.createInterface("INodeObserverLocator", (e => e.cachedCallback((e => {
    e.getAll(t.ILogger).forEach((t => {
        t.error("Using default INodeObserverLocator implementation. Will not be able to observe nodes (HTML etc...).");
    }));
    return new DefaultNodeObserverLocator;
}))));

class DefaultNodeObserverLocator {
    handles() {
        return false;
    }
    getObserver() {
        return er;
    }
    getAccessor() {
        return er;
    }
}

class ObserverLocator {
    constructor(t, e) {
        this.I = t;
        this.M = e;
        this.F = [];
    }
    addAdapter(t) {
        this.F.push(t);
    }
    getObserver(t, e) {
        var s, r;
        return null !== (r = null === (s = t.$observers) || void 0 === s ? void 0 : s[e]) && void 0 !== r ? r : this.V(t, e, this.createObserver(t, e));
    }
    getAccessor(t, e) {
        var s;
        const r = null === (s = t.$observers) || void 0 === s ? void 0 : s[e];
        if (void 0 !== r) return r;
        if (this.M.handles(t, e, this)) return this.M.getAccessor(t, e, this);
        return er;
    }
    getArrayObserver(t) {
        return wt(t);
    }
    getMapObserver(t) {
        return Ht(t);
    }
    getSetObserver(t) {
        return Rt(t);
    }
    createObserver(e, r) {
        var i, n, o, u;
        if (!(e instanceof Object)) return new PrimitiveObserver(e, r);
        if (this.M.handles(e, r, this)) return this.M.getObserver(e, r, this);
        switch (r) {
          case "length":
            if (e instanceof Array) return wt(e).getLengthObserver();
            break;

          case "size":
            if (e instanceof Map) return Ht(e).getLengthObserver(); else if (e instanceof Set) return Rt(e).getLengthObserver();
            break;

          default:
            if (e instanceof Array && t.isArrayIndex(r)) return wt(e).getIndexObserver(Number(r));
            break;
        }
        let c = or(e, r);
        if (void 0 === c) {
            let t = nr(e);
            while (null !== t) {
                c = or(t, r);
                if (void 0 === c) t = nr(t); else break;
            }
        }
        if (void 0 !== c && !s.call(c, "value")) {
            let t = this.N(e, r, c);
            if (null == t) t = null === (u = null !== (n = null === (i = c.get) || void 0 === i ? void 0 : i.getObserver) && void 0 !== n ? n : null === (o = c.set) || void 0 === o ? void 0 : o.getObserver) || void 0 === u ? void 0 : u(e, this);
            return null == t ? c.configurable ? ComputedObserver.create(e, r, c, this, true) : this.I.createProperty(e, r) : t;
        }
        return new SetterObserver(e, r);
    }
    N(t, e, s) {
        if (this.F.length > 0) for (const r of this.F) {
            const i = r.getObserver(t, e, s, this);
            if (null != i) return i;
        }
        return null;
    }
    V(t, e, s) {
        if (true === s.doNotCache) return s;
        if (void 0 === t.$observers) {
            r(t, "$observers", {
                value: {
                    [e]: s
                }
            });
            return s;
        }
        return t.$observers[e] = s;
    }
}

ObserverLocator.inject = [ Js, rr ];

function ir(t) {
    let e;
    if (t instanceof Array) e = wt(t); else if (t instanceof Map) e = Ht(t); else if (t instanceof Set) e = Rt(t);
    return e;
}

const nr = Object.getPrototypeOf;

const or = Object.getOwnPropertyDescriptor;

const ur = t.DI.createInterface("IObservation", (t => t.singleton(Observation)));

class Observation {
    constructor(t) {
        this.oL = t;
    }
    static get inject() {
        return [ sr ];
    }
    run(t) {
        const e = new Effect(this.oL, t);
        e.run();
        return e;
    }
}

class Effect {
    constructor(t, e) {
        this.oL = t;
        this.fn = e;
        this.interceptor = this;
        this.maxRunCount = 10;
        this.queued = false;
        this.running = false;
        this.runCount = 0;
        this.stopped = false;
    }
    handleChange() {
        this.queued = true;
        this.run();
    }
    handleCollectionChange() {
        this.queued = true;
        this.run();
    }
    run() {
        if (this.stopped) throw new Error(`AUR0225`);
        if (this.running) return;
        ++this.runCount;
        this.running = true;
        this.queued = false;
        ++this.obs.version;
        try {
            ts(this);
            this.fn(this);
        } finally {
            this.obs.clear();
            this.running = false;
            es(this);
        }
        if (this.queued) {
            if (this.runCount > this.maxRunCount) {
                this.runCount = 0;
                throw new Error(`AUR0226`);
            }
            this.run();
        } else this.runCount = 0;
    }
    stop() {
        this.stopped = true;
        this.obs.clearAll();
    }
}

ee(Effect);

function cr(t) {
    if (void 0 === t.$observers) r(t, "$observers", {
        value: {}
    });
    return t.$observers;
}

const hr = {};

function ar(t, e, s) {
    if (null == e) return (e, s, r) => i(e, s, r, t);
    return i(t, e, s);
    function i(t, e, s, i) {
        var n;
        const o = void 0 === e;
        i = "object" !== typeof i ? {
            name: i
        } : i || {};
        if (o) e = i.name;
        if (null == e || "" === e) throw new Error(`AUR0224`);
        const u = i.callback || `${String(e)}Changed`;
        let c = hr;
        if (s) {
            delete s.value;
            delete s.writable;
            c = null === (n = s.initializer) || void 0 === n ? void 0 : n.call(s);
            delete s.initializer;
        } else s = {
            configurable: true
        };
        if (!("enumerable" in s)) s.enumerable = true;
        const h = i.set;
        s.get = function t() {
            var s;
            const r = lr(this, e, u, c, h);
            null === (s = Ye()) || void 0 === s ? void 0 : s.subscribeTo(r);
            return r.getValue();
        };
        s.set = function t(s) {
            lr(this, e, u, c, h).setValue(s, 0);
        };
        s.get.getObserver = function t(s) {
            return lr(s, e, u, c, h);
        };
        if (o) r(t.prototype, e, s); else return s;
    }
}

function lr(t, e, s, r, i) {
    const n = cr(t);
    let o = n[e];
    if (null == o) {
        o = new SetterNotifier(t, s, i, r === hr ? void 0 : r);
        n[e] = o;
    }
    return o;
}

exports.AccessKeyedExpression = AccessKeyedExpression;

exports.AccessMemberExpression = AccessMemberExpression;

exports.AccessScopeExpression = AccessScopeExpression;

exports.AccessThisExpression = AccessThisExpression;

exports.ArrayBindingPattern = ArrayBindingPattern;

exports.ArrayIndexObserver = ArrayIndexObserver;

exports.ArrayLiteralExpression = ArrayLiteralExpression;

exports.ArrayObserver = ArrayObserver;

exports.AssignExpression = AssignExpression;

exports.BinaryExpression = BinaryExpression;

exports.BindingBehavior = y;

exports.BindingBehaviorDefinition = BindingBehaviorDefinition;

exports.BindingBehaviorExpression = BindingBehaviorExpression;

exports.BindingBehaviorFactory = BindingBehaviorFactory;

exports.BindingContext = BindingContext;

exports.BindingIdentifier = BindingIdentifier;

exports.BindingInterceptor = BindingInterceptor;

exports.BindingMediator = BindingMediator;

exports.BindingObserverRecord = BindingObserverRecord;

exports.CallFunctionExpression = CallFunctionExpression;

exports.CallMemberExpression = CallMemberExpression;

exports.CallScopeExpression = CallScopeExpression;

exports.CollectionLengthObserver = CollectionLengthObserver;

exports.CollectionSizeObserver = CollectionSizeObserver;

exports.ComputedObserver = ComputedObserver;

exports.ConditionalExpression = ConditionalExpression;

exports.ConnectableSwitcher = ss;

exports.CustomExpression = CustomExpression;

exports.DestructuringAssignmentExpression = DestructuringAssignmentExpression;

exports.DestructuringAssignmentRestExpression = DestructuringAssignmentRestExpression;

exports.DestructuringAssignmentSingleExpression = DestructuringAssignmentSingleExpression;

exports.DirtyCheckProperty = DirtyCheckProperty;

exports.DirtyCheckSettings = Xs;

exports.FlushQueue = FlushQueue;

exports.ForOfStatement = ForOfStatement;

exports.HtmlLiteralExpression = HtmlLiteralExpression;

exports.ICoercionConfiguration = P;

exports.IDirtyChecker = Js;

exports.IExpressionParser = se;

exports.INodeObserverLocator = rr;

exports.IObservation = ur;

exports.IObserverLocator = sr;

exports.ISignaler = w;

exports.Interpolation = Interpolation;

exports.MapObserver = MapObserver;

exports.ObjectBindingPattern = ObjectBindingPattern;

exports.ObjectLiteralExpression = ObjectLiteralExpression;

exports.Observation = Observation;

exports.ObserverLocator = ObserverLocator;

exports.OverrideContext = OverrideContext;

exports.ParserState = ParserState;

exports.PrimitiveLiteralExpression = PrimitiveLiteralExpression;

exports.PrimitiveObserver = PrimitiveObserver;

exports.PropertyAccessor = PropertyAccessor;

exports.ProxyObservable = Gs;

exports.Scope = Scope;

exports.SetObserver = SetObserver;

exports.SetterObserver = SetterObserver;

exports.SubscriberRecord = SubscriberRecord;

exports.TaggedTemplateExpression = TaggedTemplateExpression;

exports.TemplateExpression = TemplateExpression;

exports.UnaryExpression = UnaryExpression;

exports.ValueConverter = O;

exports.ValueConverterDefinition = ValueConverterDefinition;

exports.ValueConverterExpression = ValueConverterExpression;

exports.alias = v;

exports.applyMutationsToIndices = bt;

exports.bindingBehavior = b;

exports.cloneIndexMap = M;

exports.connectable = ee;

exports.copyIndexMap = j;

exports.createIndexMap = I;

exports.disableArrayObservation = gt;

exports.disableMapObservation = _t;

exports.disableSetObservation = Lt;

exports.enableArrayObservation = vt;

exports.enableMapObservation = qt;

exports.enableSetObservation = Bt;

exports.getCollectionObserver = ir;

exports.isIndexMap = D;

exports.observable = ar;

exports.parse = de;

exports.parseExpression = fe;

exports.registerAliases = g;

exports.subscriberCollection = F;

exports.synchronizeIndices = xt;

exports.valueConverter = m;

exports.withFlushQueue = _;
//# sourceMappingURL=index.cjs.map
