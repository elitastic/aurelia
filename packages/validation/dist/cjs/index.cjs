"use strict";

var e = require("@aurelia/kernel");

var t = require("@aurelia/expression-parser");

var s = require("@aurelia/runtime");

var i = require("@aurelia/runtime-html");

var r = require("@aurelia/metadata");

function _interopNamespaceDefault(e) {
    var t = Object.create(null);
    if (e) {
        for (var s in e) {
            t[s] = e[s];
        }
    }
    t.default = e;
    return Object.freeze(t);
}

var n = /*#__PURE__*/ _interopNamespaceDefault(t);

const a = /*@__PURE__*/ e.DI.createInterface("IValidationExpressionHydrator");

const o = r.Metadata.get;

const u = r.Metadata.define;

const l = r.Metadata.delete;

const {annotation: c} = e.Protocol;

const h = c.keyFor;

const d = /*@__PURE__*/ e.DI.createInterface("IValidationMessageProvider");

const $ = Object.freeze({
    aliasKey: h("validation-rule-alias-message"),
    define(e, t, s) {
        this.setDefaultMessage(e, t, s);
    },
    setDefaultMessage(t, {aliases: s}, i) {
        const r = i ? o(this.aliasKey, t) : void 0;
        if (r !== void 0) {
            const t = {
                ...Object.fromEntries(r.map((({name: e, defaultMessage: t}) => [ e, t ]))),
                ...Object.fromEntries(s.map((({name: e, defaultMessage: t}) => [ e, t ])))
            };
            s = e.toArray(Object.entries(t)).map((([e, t]) => ({
                name: e,
                defaultMessage: t
            })));
        }
        u(s, t instanceof Function ? t : t.constructor, this.aliasKey);
    },
    getDefaultMessages(e) {
        return o(this.aliasKey, e instanceof Function ? e : e.constructor);
    }
});

function validationRule(e) {
    return function(t, s) {
        s.addInitializer((function() {
            $.define(this, e, false);
        }));
        return t;
    };
}

class BaseValidationRule {
    constructor(e = void 0) {
        this.messageKey = e;
        this.tag = void 0;
    }
    canExecute(e) {
        return true;
    }
    execute(e, t) {
        throw new Error("No base implementation of execute. Did you forget to implement the execute method?");
    }
    accept(e) {
        throw new Error("No base implementation of accept. Did you forget to implement the accept method?");
    }
}

BaseValidationRule.$TYPE = "";

class RequiredRule extends BaseValidationRule {
    constructor() {
        super("required");
    }
    execute(e) {
        return e !== null && e !== void 0 && !(typeof e === "string" && !/\S/.test(e));
    }
    accept(e) {
        return e.visitRequiredRule(this);
    }
}

RequiredRule.$TYPE = "RequiredRule";

class RegexRule extends BaseValidationRule {
    constructor(e, t = "matches") {
        super(t);
        this.pattern = e;
    }
    execute(e) {
        return e === null || e === undefined || e.length === 0 || this.pattern.test(e);
    }
    accept(e) {
        return e.visitRegexRule(this);
    }
}

RegexRule.$TYPE = "RegexRule";

class LengthRule extends BaseValidationRule {
    constructor(e, t) {
        super(t ? "maxLength" : "minLength");
        this.length = e;
        this.isMax = t;
    }
    execute(e) {
        return e === null || e === undefined || e.length === 0 || (this.isMax ? e.length <= this.length : e.length >= this.length);
    }
    accept(e) {
        return e.visitLengthRule(this);
    }
}

LengthRule.$TYPE = "LengthRule";

class SizeRule extends BaseValidationRule {
    constructor(e, t) {
        super(t ? "maxItems" : "minItems");
        this.count = e;
        this.isMax = t;
    }
    execute(e) {
        return e === null || e === undefined || (this.isMax ? e.length <= this.count : e.length >= this.count);
    }
    accept(e) {
        return e.visitSizeRule(this);
    }
}

SizeRule.$TYPE = "SizeRule";

class RangeRule extends BaseValidationRule {
    constructor(e, {min: t, max: s}) {
        super(t !== void 0 && s !== void 0 ? e ? "range" : "between" : t !== void 0 ? "min" : "max");
        this.isInclusive = e;
        this.min = Number.NEGATIVE_INFINITY;
        this.max = Number.POSITIVE_INFINITY;
        this.min = t ?? this.min;
        this.max = s ?? this.max;
    }
    execute(e, t) {
        return e === null || e === undefined || (this.isInclusive ? e >= this.min && e <= this.max : e > this.min && e < this.max);
    }
    accept(e) {
        return e.visitRangeRule(this);
    }
}

RangeRule.$TYPE = "RangeRule";

class EqualsRule extends BaseValidationRule {
    constructor(e) {
        super("equals");
        this.expectedValue = e;
    }
    execute(e) {
        return e === null || e === undefined || e === "" || e === this.expectedValue;
    }
    accept(e) {
        return e.visitEqualsRule(this);
    }
}

EqualsRule.$TYPE = "EqualsRule";

$.define(EqualsRule, {
    aliases: [ {
        name: "equals",
        defaultMessage: `\${$displayName} must be \${$rule.expectedValue}.`
    } ]
}, false);

$.define(RangeRule, {
    aliases: [ {
        name: "min",
        defaultMessage: `\${$displayName} must be at least \${$rule.min}.`
    }, {
        name: "max",
        defaultMessage: `\${$displayName} must be at most \${$rule.max}.`
    }, {
        name: "range",
        defaultMessage: `\${$displayName} must be between or equal to \${$rule.min} and \${$rule.max}.`
    }, {
        name: "between",
        defaultMessage: `\${$displayName} must be between but not equal to \${$rule.min} and \${$rule.max}.`
    } ]
}, false);

$.define(SizeRule, {
    aliases: [ {
        name: "minItems",
        defaultMessage: `\${$displayName} must contain at least \${$rule.count} item\${$rule.count === 1 ? '' : 's'}.`
    }, {
        name: "maxItems",
        defaultMessage: `\${$displayName} cannot contain more than \${$rule.count} item\${$rule.count === 1 ? '' : 's'}.`
    } ]
}, false);

$.define(LengthRule, {
    aliases: [ {
        name: "minLength",
        defaultMessage: `\${$displayName} must be at least \${$rule.length} character\${$rule.length === 1 ? '' : 's'}.`
    }, {
        name: "maxLength",
        defaultMessage: `\${$displayName} cannot be longer than \${$rule.length} character\${$rule.length === 1 ? '' : 's'}.`
    } ]
}, false);

$.define(RegexRule, {
    aliases: [ {
        name: "matches",
        defaultMessage: `\${$displayName} is not correctly formatted.`
    }, {
        name: "email",
        defaultMessage: `\${$displayName} is not a valid email.`
    } ]
}, false);

$.define(RequiredRule, {
    aliases: [ {
        name: "required",
        defaultMessage: `\${$displayName} is required.`
    } ]
}, false);

$.define(BaseValidationRule, {
    aliases: [ {
        name: void 0,
        defaultMessage: `\${$displayName} is invalid.`
    } ]
}, false);

const m = /*@__PURE__*/ e.DI.createInterface("ICustomMessages");

class RuleProperty {
    constructor(e, t = void 0, s = void 0) {
        this.expression = e;
        this.name = t;
        this.displayName = s;
    }
    accept(e) {
        return e.visitRuleProperty(this);
    }
}

RuleProperty.$TYPE = "RuleProperty";

const p = Object.freeze({
    allRulesAnnotations: h("validation-rules-annotations"),
    name: "validation-rules",
    defaultRuleSetName: "__default",
    set(e, t, s) {
        const i = `${p.name}:${s ?? p.defaultRuleSetName}`;
        u(t, e, h(i));
        const r = o(this.allRulesAnnotations, e);
        if (r === void 0) {
            u([ i ], e, this.allRulesAnnotations);
        } else {
            r.push(i);
        }
    },
    get(e, t) {
        const s = h(p.name, t ?? p.defaultRuleSetName);
        return o(s, e) ?? o(s, e.constructor);
    },
    unset(e, t) {
        const s = o(this.allRulesAnnotations, e);
        if (!Array.isArray(s)) return;
        for (const i of s.slice(0)) {
            if (i.startsWith(p.name) && (t === void 0 || i.endsWith(t))) {
                l(h(i), e);
                const t = s.indexOf(i);
                if (t > -1) {
                    s.splice(t, 1);
                }
            }
        }
    },
    isValidationRulesSet(e) {
        const t = o(this.allRulesAnnotations, e);
        return t !== void 0 && t.some((e => e.startsWith(p.name)));
    }
});

class ValidationMessageEvaluationContext {
    constructor(e, t, s, i, r, n) {
        this.messageProvider = e;
        this.$displayName = t;
        this.$propertyName = s;
        this.$value = i;
        this.$rule = r;
        this.$object = n;
    }
    $getDisplayName(e, t) {
        return this.messageProvider.getDisplayName(e, t);
    }
}

class PropertyRule {
    constructor(e, t, s, i, r = [ [] ]) {
        this.validationRules = t;
        this.messageProvider = s;
        this.property = i;
        this.$rules = r;
        this.l = e;
    }
    accept(e) {
        return e.visitPropertyRule(this);
    }
    addRule(e) {
        const t = this.getLeafRules();
        t.push(this.latestRule = e);
        return this;
    }
    getLeafRules() {
        const e = this.$rules.length - 1;
        return this.$rules[e];
    }
    async validate(e, t, r) {
        if (r === void 0) {
            r = s.Scope.create({
                [v]: e
            });
        }
        const n = this.property.expression;
        let a;
        if (n === void 0) {
            a = e;
        } else {
            a = i.astEvaluate(n, r, this, null);
        }
        let o = true;
        const validateRuleset = async r => {
            const validateRule = async t => {
                let r = t.execute(a, e);
                if (r instanceof Promise) {
                    r = await r;
                }
                o = o && r;
                const {displayName: n, name: u} = this.property;
                let l;
                if (!r) {
                    const r = s.Scope.create(new ValidationMessageEvaluationContext(this.messageProvider, this.messageProvider.getDisplayName(u, n), u, a, t, e));
                    l = i.astEvaluate(this.messageProvider.getMessage(t), r, this, null);
                }
                return new ValidationResult(r, l, u, e, t, this);
            };
            const n = [];
            for (const s of r) {
                if (s.canExecute(e) && (t === void 0 || s.tag === t)) {
                    n.push(validateRule(s));
                }
            }
            return Promise.all(n);
        };
        const accumulateResult = async (e, t) => {
            const s = await validateRuleset(t);
            e.push(...s);
            return e;
        };
        return this.$rules.reduce((async (e, t) => e.then((async e => o ? accumulateResult(e, t) : Promise.resolve(e)))), Promise.resolve([]));
    }
    then() {
        this.$rules.push([]);
        return this;
    }
    withMessageKey(e) {
        this.assertLatestRule(this.latestRule);
        this.latestRule.messageKey = e;
        return this;
    }
    withMessage(e) {
        const t = this.latestRule;
        this.assertLatestRule(t);
        this.messageProvider.setMessage(t, e);
        return this;
    }
    when(e) {
        this.assertLatestRule(this.latestRule);
        this.latestRule.canExecute = e;
        return this;
    }
    tag(e) {
        this.assertLatestRule(this.latestRule);
        this.latestRule.tag = e;
        return this;
    }
    assertLatestRule(e) {
        if (e === void 0) {
            throw new Error("No rule has been added");
        }
    }
    displayName(e) {
        this.property.displayName = e;
        return this;
    }
    satisfies(e) {
        const t = new class extends BaseValidationRule {
            constructor() {
                super(...arguments);
                this.execute = e;
            }
        };
        return this.addRule(t);
    }
    satisfiesRule(e) {
        return this.addRule(e);
    }
    required() {
        return this.addRule(new RequiredRule);
    }
    matches(e) {
        return this.addRule(new RegexRule(e));
    }
    email() {
        const e = /^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
        return this.addRule(new RegexRule(e, "email"));
    }
    minLength(e) {
        return this.addRule(new LengthRule(e, false));
    }
    maxLength(e) {
        return this.addRule(new LengthRule(e, true));
    }
    minItems(e) {
        return this.addRule(new SizeRule(e, false));
    }
    maxItems(e) {
        return this.addRule(new SizeRule(e, true));
    }
    min(e) {
        return this.addRule(new RangeRule(true, {
            min: e
        }));
    }
    max(e) {
        return this.addRule(new RangeRule(true, {
            max: e
        }));
    }
    range(e, t) {
        return this.addRule(new RangeRule(true, {
            min: e,
            max: t
        }));
    }
    between(e, t) {
        return this.addRule(new RangeRule(false, {
            min: e,
            max: t
        }));
    }
    equals(e) {
        return this.addRule(new EqualsRule(e));
    }
    ensure(e) {
        this.latestRule = void 0;
        return this.validationRules.ensure(e);
    }
    ensureObject() {
        this.latestRule = void 0;
        return this.validationRules.ensureObject();
    }
    get rules() {
        return this.validationRules.rules;
    }
    on(e, t) {
        return this.validationRules.on(e, t);
    }
}

PropertyRule.$TYPE = "PropertyRule";

i.mixinAstEvaluator()(PropertyRule);

class ModelBasedRule {
    constructor(e, t = p.defaultRuleSetName) {
        this.ruleset = e;
        this.tag = t;
    }
}

const f = /*@__PURE__*/ e.DI.createInterface("IValidationRules");

class ValidationRules {
    constructor() {
        this.rules = [];
        this.targets = new Set;
        this.locator = e.resolve(e.IServiceLocator);
        this.parser = e.resolve(t.IExpressionParser);
        this.messageProvider = e.resolve(d);
        this.deserializer = e.resolve(a);
    }
    ensure(e) {
        const [t, s] = parsePropertyName(e, this.parser);
        let i = this.rules.find((e => e.property.name == t));
        if (i === void 0) {
            i = new PropertyRule(this.locator, this, this.messageProvider, new RuleProperty(s, t));
            this.rules.push(i);
        }
        return i;
    }
    ensureObject() {
        const e = new PropertyRule(this.locator, this, this.messageProvider, new RuleProperty);
        this.rules.push(e);
        return e;
    }
    on(e, t) {
        const s = p.get(e, t);
        if (Object.is(s, this.rules)) {
            return this;
        }
        this.rules = s ?? [];
        p.set(e, this.rules, t);
        this.targets.add(e);
        return this;
    }
    off(e, t) {
        const s = e !== void 0 ? [ e ] : Array.from(this.targets);
        for (const e of s) {
            p.unset(e, t);
            if (!p.isValidationRulesSet(e)) {
                this.targets.delete(e);
            }
        }
    }
    applyModelBasedRules(e, t) {
        const s = new Set;
        for (const i of t) {
            const t = i.tag;
            if (s.has(t)) {
                console.warn(`A ruleset for tag ${t} is already defined which will be overwritten`);
            }
            const r = this.deserializer.hydrateRuleset(i.ruleset, this);
            p.set(e, r, t);
            s.add(t);
        }
    }
}

const g = /^(?:function)?\s*\(?[$_\w\d]+\)?\s*(?:=>)?\s*\{(?:\s*["']{1}use strict["']{1};)?(?:[$_\s\w\d\/\*.['"\]+;\(\)]+)?\s*return\s+[$_\w\d]+((\.[$_\w\d]+|\[['"$_\w\d]+\])+)\s*;?\s*\}$/;

const R = /^\(?[$_\w\d]+\)?\s*=>\s*[$_\w\d]+((\.[$_\w\d]+|\[['"$_\w\d]+\])+)$/;

const v = "$root";

function parsePropertyName(e, t) {
    switch (typeof e) {
      case "string":
        break;

      case "function":
        {
            const t = e.toString();
            const s = R.exec(t) ?? g.exec(t);
            if (s === null) {
                throw new Error(`Unable to parse accessor function:\n${t}`);
            }
            e = s[1].substring(1);
            break;
        }

      default:
        throw new Error(`Unable to parse accessor function:\n${e}`);
    }
    return [ e, t.parse(`${v}.${e}`, "IsProperty") ];
}

class ValidationResult {
    constructor(e, t, s, i, r, n, a = false) {
        this.valid = e;
        this.message = t;
        this.propertyName = s;
        this.object = i;
        this.rule = r;
        this.propertyRule = n;
        this.isManual = a;
        this.id = ValidationResult.nextId++;
    }
    toString() {
        return this.valid ? "Valid." : this.message;
    }
}

ValidationResult.nextId = 0;

const y = new Set([ "displayName", "propertyName", "value", "object", "config", "getDisplayName" ]);

class ValidationMessageProvider {
    constructor(s = e.resolve(e.ILogger), i = e.resolve(m)) {
        this.registeredMessages = new WeakMap;
        this.parser = e.resolve(t.IExpressionParser);
        this.logger = s.scopeTo(ValidationMessageProvider.name);
        for (const {rule: e, aliases: t} of i) {
            $.setDefaultMessage(e, {
                aliases: t
            }, true);
        }
    }
    getMessage(e) {
        const t = this.registeredMessages.get(e);
        if (t !== void 0) {
            return t;
        }
        const s = $.getDefaultMessages(e);
        const i = e.messageKey;
        let r;
        const n = s.length;
        if (n === 1 && i === void 0) {
            r = s[0].defaultMessage;
        } else {
            r = s.find((e => e.name === i))?.defaultMessage;
        }
        if (!r) {
            r = $.getDefaultMessages(BaseValidationRule)[0].defaultMessage;
        }
        return this.setMessage(e, r);
    }
    setMessage(e, t) {
        const s = this.parseMessage(t);
        this.registeredMessages.set(e, s);
        return s;
    }
    parseMessage(e) {
        const s = this.parser.parse(e, "Interpolation");
        if (s?.$kind === "Interpolation") {
            for (const t of s.expressions) {
                const s = t.name;
                if (y.has(s)) {
                    this.logger.warn(`Did you mean to use "$${s}" instead of "${s}" in this validation message template: "${e}"?`);
                }
                if (t.$kind === "AccessThis" || t.ancestor > 0) {
                    throw new Error("$parent is not permitted in validation message expressions.");
                }
            }
            return s;
        }
        return new t.PrimitiveLiteralExpression(e);
    }
    getDisplayName(e, t) {
        if (t !== null && t !== undefined) {
            return t instanceof Function ? t() : t;
        }
        if (e === void 0) {
            return;
        }
        const s = e.toString().split(/(?=[A-Z])/).join(" ");
        return s.charAt(0).toUpperCase() + s.slice(1);
    }
}

const x = n.astVisit;

var w;

(function(e) {
    e["BindingBehaviorExpression"] = "BindingBehaviorExpression";
    e["ValueConverterExpression"] = "ValueConverterExpression";
    e["AssignExpression"] = "AssignExpression";
    e["ConditionalExpression"] = "ConditionalExpression";
    e["AccessThisExpression"] = "AccessThisExpression";
    e["AccessBoundaryExpression"] = "AccessBoundaryExpression";
    e["AccessScopeExpression"] = "AccessScopeExpression";
    e["AccessMemberExpression"] = "AccessMemberExpression";
    e["AccessKeyedExpression"] = "AccessKeyedExpression";
    e["CallScopeExpression"] = "CallScopeExpression";
    e["CallMemberExpression"] = "CallMemberExpression";
    e["CallFunctionExpression"] = "CallFunctionExpression";
    e["BinaryExpression"] = "BinaryExpression";
    e["UnaryExpression"] = "UnaryExpression";
    e["PrimitiveLiteralExpression"] = "PrimitiveLiteralExpression";
    e["ArrayLiteralExpression"] = "ArrayLiteralExpression";
    e["ObjectLiteralExpression"] = "ObjectLiteralExpression";
    e["TemplateExpression"] = "TemplateExpression";
    e["TaggedTemplateExpression"] = "TaggedTemplateExpression";
    e["ArrayBindingPattern"] = "ArrayBindingPattern";
    e["ObjectBindingPattern"] = "ObjectBindingPattern";
    e["BindingIdentifier"] = "BindingIdentifier";
    e["ForOfStatement"] = "ForOfStatement";
    e["Interpolation"] = "Interpolation";
    e["DestructuringAssignment"] = "DestructuringAssignment";
    e["DestructuringSingleAssignment"] = "DestructuringSingleAssignment";
    e["DestructuringRestAssignment"] = "DestructuringRestAssignment";
    e["ArrowFunction"] = "ArrowFunction";
    e["Custom"] = "Custom";
})(w || (w = {}));

class Deserializer {
    static deserialize(e) {
        const t = new Deserializer;
        const s = JSON.parse(e);
        return t.hydrate(s);
    }
    hydrate(e) {
        switch (e.$TYPE) {
          case w.AccessMemberExpression:
            {
                const t = e;
                return new n.AccessMemberExpression(this.hydrate(t.object), t.name);
            }

          case w.AccessKeyedExpression:
            {
                const t = e;
                return new n.AccessKeyedExpression(this.hydrate(t.object), this.hydrate(t.key));
            }

          case w.AccessThisExpression:
            {
                const t = e;
                return new n.AccessThisExpression(t.ancestor);
            }

          case w.AccessBoundaryExpression:
            {
                return new n.AccessBoundaryExpression;
            }

          case w.AccessScopeExpression:
            {
                const t = e;
                return new n.AccessScopeExpression(t.name, t.ancestor);
            }

          case w.ArrayLiteralExpression:
            {
                const t = e;
                return new n.ArrayLiteralExpression(this.hydrate(t.elements));
            }

          case w.ObjectLiteralExpression:
            {
                const t = e;
                return new n.ObjectLiteralExpression(this.hydrate(t.keys), this.hydrate(t.values));
            }

          case w.PrimitiveLiteralExpression:
            {
                const t = e;
                return new n.PrimitiveLiteralExpression(this.hydrate(t.value));
            }

          case w.CallFunctionExpression:
            {
                const t = e;
                return new n.CallFunctionExpression(this.hydrate(t.func), this.hydrate(t.args));
            }

          case w.CallMemberExpression:
            {
                const t = e;
                return new n.CallMemberExpression(this.hydrate(t.object), t.name, this.hydrate(t.args));
            }

          case w.CallScopeExpression:
            {
                const t = e;
                return new n.CallScopeExpression(t.name, this.hydrate(t.args), t.ancestor);
            }

          case w.TemplateExpression:
            {
                const t = e;
                return new n.TemplateExpression(this.hydrate(t.cooked), this.hydrate(t.expressions));
            }

          case w.TaggedTemplateExpression:
            {
                const t = e;
                return new n.TaggedTemplateExpression(this.hydrate(t.cooked), this.hydrate(t.raw), this.hydrate(t.func), this.hydrate(t.expressions));
            }

          case w.UnaryExpression:
            {
                const t = e;
                return new n.UnaryExpression(t.operation, this.hydrate(t.expression));
            }

          case w.BinaryExpression:
            {
                const t = e;
                return new n.BinaryExpression(t.operation, this.hydrate(t.left), this.hydrate(t.right));
            }

          case w.ConditionalExpression:
            {
                const t = e;
                return new n.ConditionalExpression(this.hydrate(t.condition), this.hydrate(t.yes), this.hydrate(t.no));
            }

          case w.AssignExpression:
            {
                const t = e;
                return new n.AssignExpression(this.hydrate(t.target), this.hydrate(t.value));
            }

          case w.ValueConverterExpression:
            {
                const t = e;
                return new n.ValueConverterExpression(this.hydrate(t.expression), t.name, this.hydrate(t.args));
            }

          case w.BindingBehaviorExpression:
            {
                const t = e;
                return new n.BindingBehaviorExpression(this.hydrate(t.expression), t.name, this.hydrate(t.args));
            }

          case w.ArrayBindingPattern:
            {
                const t = e;
                return new n.ArrayBindingPattern(this.hydrate(t.elements));
            }

          case w.ObjectBindingPattern:
            {
                const t = e;
                return new n.ObjectBindingPattern(this.hydrate(t.keys), this.hydrate(t.values));
            }

          case w.BindingIdentifier:
            {
                const t = e;
                return new n.BindingIdentifier(t.name);
            }

          case w.ForOfStatement:
            {
                const t = e;
                return new n.ForOfStatement(this.hydrate(t.declaration), this.hydrate(t.iterable), this.hydrate(t.semiIdx));
            }

          case w.Interpolation:
            {
                const t = e;
                return new n.Interpolation(this.hydrate(t.cooked), this.hydrate(t.expressions));
            }

          case w.DestructuringAssignment:
            {
                return new n.DestructuringAssignmentExpression(this.hydrate(e.$kind), this.hydrate(e.list), this.hydrate(e.source), this.hydrate(e.initializer));
            }

          case w.DestructuringSingleAssignment:
            {
                return new n.DestructuringAssignmentSingleExpression(this.hydrate(e.target), this.hydrate(e.source), this.hydrate(e.initializer));
            }

          case w.DestructuringRestAssignment:
            {
                return new n.DestructuringAssignmentRestExpression(this.hydrate(e.target), this.hydrate(e.indexOrProperties));
            }

          case w.ArrowFunction:
            {
                return new n.ArrowFunction(this.hydrate(e.parameters), this.hydrate(e.body), this.hydrate(e.rest));
            }

          default:
            if (Array.isArray(e)) {
                if (typeof e[0] === "object") {
                    return this.deserializeExpressions(e);
                } else {
                    return e.map(deserializePrimitive);
                }
            } else if (typeof e !== "object") {
                return deserializePrimitive(e);
            }
            throw new Error(`unable to deserialize the expression: ${e}`);
        }
    }
    deserializeExpressions(e) {
        const t = [];
        for (const s of e) {
            t.push(this.hydrate(s));
        }
        return t;
    }
}

class Serializer {
    static serialize(e) {
        const t = new Serializer;
        if (e == null) {
            return `${e}`;
        }
        return x(e, t);
    }
    visitAccessMember(e) {
        return `{"$TYPE":"${w.AccessMemberExpression}","name":"${e.name}","object":${x(e.object, this)}}`;
    }
    visitAccessKeyed(e) {
        return `{"$TYPE":"${w.AccessKeyedExpression}","object":${x(e.object, this)},"key":${x(e.key, this)}}`;
    }
    visitAccessThis(e) {
        return `{"$TYPE":"${w.AccessThisExpression}","ancestor":${e.ancestor}}`;
    }
    visitAccessBoundary(e) {
        return `{"$TYPE":"${w.AccessBoundaryExpression}"}`;
    }
    visitAccessScope(e) {
        return `{"$TYPE":"${w.AccessScopeExpression}","name":"${e.name}","ancestor":${e.ancestor}}`;
    }
    visitArrayLiteral(e) {
        return `{"$TYPE":"${w.ArrayLiteralExpression}","elements":${this.serializeExpressions(e.elements)}}`;
    }
    visitObjectLiteral(e) {
        return `{"$TYPE":"${w.ObjectLiteralExpression}","keys":${serializePrimitives(e.keys)},"values":${this.serializeExpressions(e.values)}}`;
    }
    visitPrimitiveLiteral(e) {
        return `{"$TYPE":"${w.PrimitiveLiteralExpression}","value":${serializePrimitive(e.value)}}`;
    }
    visitCallFunction(e) {
        return `{"$TYPE":"${w.CallFunctionExpression}","func":${x(e.func, this)},"args":${this.serializeExpressions(e.args)}}`;
    }
    visitCallMember(e) {
        return `{"$TYPE":"${w.CallMemberExpression}","name":"${e.name}","object":${x(e.object, this)},"args":${this.serializeExpressions(e.args)}}`;
    }
    visitCallScope(e) {
        return `{"$TYPE":"${w.CallScopeExpression}","name":"${e.name}","ancestor":${e.ancestor},"args":${this.serializeExpressions(e.args)}}`;
    }
    visitTemplate(e) {
        return `{"$TYPE":"${w.TemplateExpression}","cooked":${serializePrimitives(e.cooked)},"expressions":${this.serializeExpressions(e.expressions)}}`;
    }
    visitTaggedTemplate(e) {
        return `{"$TYPE":"${w.TaggedTemplateExpression}","cooked":${serializePrimitives(e.cooked)},"raw":${serializePrimitives(e.cooked.raw)},"func":${x(e.func, this)},"expressions":${this.serializeExpressions(e.expressions)}}`;
    }
    visitUnary(e) {
        return `{"$TYPE":"${w.UnaryExpression}","operation":"${e.operation}","expression":${x(e.expression, this)}}`;
    }
    visitBinary(e) {
        return `{"$TYPE":"${w.BinaryExpression}","operation":"${e.operation}","left":${x(e.left, this)},"right":${x(e.right, this)}}`;
    }
    visitConditional(e) {
        return `{"$TYPE":"${w.ConditionalExpression}","condition":${x(e.condition, this)},"yes":${x(e.yes, this)},"no":${x(e.no, this)}}`;
    }
    visitAssign(e) {
        return `{"$TYPE":"${w.AssignExpression}","target":${x(e.target, this)},"value":${x(e.value, this)}}`;
    }
    visitValueConverter(e) {
        return `{"$TYPE":"${w.ValueConverterExpression}","name":"${e.name}","expression":${x(e.expression, this)},"args":${this.serializeExpressions(e.args)}}`;
    }
    visitBindingBehavior(e) {
        return `{"$TYPE":"${w.BindingBehaviorExpression}","name":"${e.name}","expression":${x(e.expression, this)},"args":${this.serializeExpressions(e.args)}}`;
    }
    visitArrayBindingPattern(e) {
        return `{"$TYPE":"${w.ArrayBindingPattern}","elements":${this.serializeExpressions(e.elements)}}`;
    }
    visitObjectBindingPattern(e) {
        return `{"$TYPE":"${w.ObjectBindingPattern}","keys":${serializePrimitives(e.keys)},"values":${this.serializeExpressions(e.values)}}`;
    }
    visitBindingIdentifier(e) {
        return `{"$TYPE":"${w.BindingIdentifier}","name":"${e.name}"}`;
    }
    visitForOfStatement(e) {
        return `{"$TYPE":"${w.ForOfStatement}","declaration":${x(e.declaration, this)},"iterable":${x(e.iterable, this)},"semiIdx":${serializePrimitive(e.semiIdx)}}`;
    }
    visitInterpolation(e) {
        return `{"$TYPE":"${w.Interpolation}","cooked":${serializePrimitives(e.parts)},"expressions":${this.serializeExpressions(e.expressions)}}`;
    }
    visitDestructuringAssignmentExpression(e) {
        return `{"$TYPE":"${w.DestructuringAssignment}","$kind":${serializePrimitive(e.$kind)},"list":${this.serializeExpressions(e.list)},"source":${e.source === void 0 ? serializePrimitive(e.source) : x(e.source, this)},"initializer":${e.initializer === void 0 ? serializePrimitive(e.initializer) : x(e.initializer, this)}}`;
    }
    visitDestructuringAssignmentSingleExpression(e) {
        return `{"$TYPE":"${w.DestructuringSingleAssignment}","source":${x(e.source, this)},"target":${x(e.target, this)},"initializer":${e.initializer === void 0 ? serializePrimitive(e.initializer) : x(e.initializer, this)}}`;
    }
    visitDestructuringAssignmentRestExpression(e) {
        return `{"$TYPE":"${w.DestructuringRestAssignment}","target":${x(e.target, this)},"indexOrProperties":${Array.isArray(e.indexOrProperties) ? serializePrimitives(e.indexOrProperties) : serializePrimitive(e.indexOrProperties)}}`;
    }
    visitArrowFunction(e) {
        return `{"$TYPE":"${w.ArrowFunction}","parameters":${this.serializeExpressions(e.args)},"body":${x(e.body, this)},"rest":${serializePrimitive(e.rest)}}`;
    }
    visitCustom(e) {
        return `{"$TYPE":"${w.Custom}","body":${e.value}}`;
    }
    serializeExpressions(e) {
        let t = "[";
        for (let s = 0, i = e.length; s < i; ++s) {
            if (s !== 0) {
                t += ",";
            }
            t += x(e[s], this);
        }
        t += "]";
        return t;
    }
}

function serializePrimitives(e) {
    let t = "[";
    for (let s = 0, i = e.length; s < i; ++s) {
        if (s !== 0) {
            t += ",";
        }
        t += serializePrimitive(e[s]);
    }
    t += "]";
    return t;
}

function serializePrimitive(e) {
    if (typeof e === "string") {
        return `"\\"${escapeString(e)}\\""`;
    } else if (e == null) {
        return `"${e}"`;
    } else {
        return `${e}`;
    }
}

function escapeString(e) {
    let t = "";
    for (let s = 0, i = e.length; s < i; ++s) {
        t += escape(e.charAt(s));
    }
    return t;
}

function escape(e) {
    switch (e) {
      case "\b":
        return "\\b";

      case "\t":
        return "\\t";

      case "\n":
        return "\\n";

      case "\v":
        return "\\v";

      case "\f":
        return "\\f";

      case "\r":
        return "\\r";

      case '"':
        return '\\"';

      case "\\":
        return "\\\\";

      default:
        return e;
    }
}

function deserializePrimitive(e) {
    if (typeof e === "string") {
        if (e === "null") {
            return null;
        }
        if (e === "undefined") {
            return undefined;
        }
        return e.substring(1, e.length - 1);
    } else {
        return e;
    }
}

class ValidationSerializer {
    static serialize(e) {
        if (e == null || typeof e.accept !== "function") {
            return `${e}`;
        }
        const t = new ValidationSerializer;
        return e.accept(t);
    }
    visitRequiredRule(e) {
        return `{"$TYPE":"${RequiredRule.$TYPE}","messageKey":"${e.messageKey}","tag":${serializePrimitive(e.tag)}}`;
    }
    visitRegexRule(e) {
        const t = e.pattern;
        return `{"$TYPE":"${RegexRule.$TYPE}","messageKey":"${e.messageKey}","tag":${serializePrimitive(e.tag)},"pattern":{"source":${serializePrimitive(t.source)},"flags":"${t.flags}"}}`;
    }
    visitLengthRule(e) {
        return `{"$TYPE":"${LengthRule.$TYPE}","messageKey":"${e.messageKey}","tag":${serializePrimitive(e.tag)},"length":${serializePrimitive(e.length)},"isMax":${serializePrimitive(e.isMax)}}`;
    }
    visitSizeRule(e) {
        return `{"$TYPE":"${SizeRule.$TYPE}","messageKey":"${e.messageKey}","tag":${serializePrimitive(e.tag)},"count":${serializePrimitive(e.count)},"isMax":${serializePrimitive(e.isMax)}}`;
    }
    visitRangeRule(e) {
        return `{"$TYPE":"${RangeRule.$TYPE}","messageKey":"${e.messageKey}","tag":${serializePrimitive(e.tag)},"isInclusive":${e.isInclusive},"min":${this.serializeNumber(e.min)},"max":${this.serializeNumber(e.max)}}`;
    }
    visitEqualsRule(e) {
        const t = e.expectedValue;
        let s;
        if (typeof t !== "object" || t === null) {
            s = serializePrimitive(t);
        } else {
            s = JSON.stringify(t);
        }
        return `{"$TYPE":"${EqualsRule.$TYPE}","messageKey":"${e.messageKey}","tag":${serializePrimitive(e.tag)},"expectedValue":${s}}`;
    }
    visitRuleProperty(e) {
        const t = e.displayName;
        if (t !== void 0 && typeof t !== "string") {
            throw new Error("Serializing a non-string displayName for rule property is not supported.");
        }
        const s = e.expression;
        return `{"$TYPE":"${RuleProperty.$TYPE}","name":${serializePrimitive(e.name)},"expression":${s ? Serializer.serialize(s) : null},"displayName":${serializePrimitive(t)}}`;
    }
    visitPropertyRule(e) {
        return `{"$TYPE":"${PropertyRule.$TYPE}","property":${e.property.accept(this)},"$rules":${this.serializeRules(e.$rules)}}`;
    }
    serializeNumber(e) {
        return e === Number.POSITIVE_INFINITY || e === Number.NEGATIVE_INFINITY ? null : e.toString();
    }
    serializeRules(e) {
        return `[${e.map((e => `[${e.map((e => e.accept(this))).join(",")}]`)).join(",")}]`;
    }
}

class ValidationDeserializer {
    static register(e) {
        this.container = e;
    }
    static deserialize(e, s) {
        const i = this.container.get(d);
        const r = this.container.get(t.IExpressionParser);
        const n = new ValidationDeserializer(this.container, i, r);
        const a = JSON.parse(e);
        return n.hydrate(a, s);
    }
    constructor(s = e.resolve(e.IServiceLocator), i = e.resolve(d), r = e.resolve(t.IExpressionParser)) {
        this.locator = s;
        this.messageProvider = i;
        this.parser = r;
        this.astDeserializer = new Deserializer;
    }
    hydrate(e, t) {
        switch (e.$TYPE) {
          case RequiredRule.$TYPE:
            {
                const t = e;
                const s = new RequiredRule;
                s.messageKey = t.messageKey;
                s.tag = this.astDeserializer.hydrate(t.tag);
                return s;
            }

          case RegexRule.$TYPE:
            {
                const t = e;
                const s = t.pattern;
                const i = this.astDeserializer;
                const r = new RegexRule(new RegExp(i.hydrate(s.source), s.flags), t.messageKey);
                r.tag = i.hydrate(t.tag);
                return r;
            }

          case LengthRule.$TYPE:
            {
                const t = e;
                const s = new LengthRule(t.length, t.isMax);
                s.messageKey = t.messageKey;
                s.tag = this.astDeserializer.hydrate(t.tag);
                return s;
            }

          case SizeRule.$TYPE:
            {
                const t = e;
                const s = new SizeRule(t.count, t.isMax);
                s.messageKey = t.messageKey;
                s.tag = this.astDeserializer.hydrate(t.tag);
                return s;
            }

          case RangeRule.$TYPE:
            {
                const t = e;
                const s = new RangeRule(t.isInclusive, {
                    min: t.min ?? Number.NEGATIVE_INFINITY,
                    max: t.max ?? Number.POSITIVE_INFINITY
                });
                s.messageKey = t.messageKey;
                s.tag = this.astDeserializer.hydrate(t.tag);
                return s;
            }

          case EqualsRule.$TYPE:
            {
                const t = e;
                const s = this.astDeserializer;
                const i = new EqualsRule(typeof t.expectedValue !== "object" ? s.hydrate(t.expectedValue) : t.expectedValue);
                i.messageKey = t.messageKey;
                i.tag = s.hydrate(t.tag);
                return i;
            }

          case RuleProperty.$TYPE:
            {
                const t = e;
                const s = this.astDeserializer;
                let i = t.name;
                i = i === "undefined" ? void 0 : s.hydrate(i);
                let r = t.expression;
                if (r !== null && r !== void 0) {
                    r = s.hydrate(r);
                } else if (i !== void 0) {
                    [, r] = parsePropertyName(i, this.parser);
                } else {
                    r = void 0;
                }
                let n = t.displayName;
                n = n === "undefined" ? void 0 : s.hydrate(n);
                return new RuleProperty(r, i, n);
            }

          case PropertyRule.$TYPE:
            {
                const s = e;
                return new PropertyRule(this.locator, t, this.messageProvider, this.hydrate(s.property, t), s.$rules.map((e => e.map((e => this.hydrate(e, t))))));
            }
        }
    }
    hydrateRuleset(e, t) {
        if (!Array.isArray(e)) {
            throw new Error("The ruleset has to be an array of serialized property rule objects");
        }
        return e.map((e => this.hydrate(e, t)));
    }
}

class ModelValidationExpressionHydrator {
    constructor() {
        this.astDeserializer = new Deserializer;
        this.l = e.resolve(e.IServiceLocator);
        this.messageProvider = e.resolve(d);
        this.parser = e.resolve(t.IExpressionParser);
    }
    hydrate(e, t) {
        throw new Error("Method not implemented.");
    }
    hydrateRuleset(e, t) {
        const s = [];
        const iterate = (e, i = []) => {
            for (const [r, n] of e) {
                if (this.isModelPropertyRule(n)) {
                    const e = n.rules.map((e => Object.entries(e).map((([e, t]) => this.hydrateRule(e, t)))));
                    const a = i.join(".");
                    const o = this.hydrateRuleProperty({
                        name: a !== "" ? `${a}.${r}` : r,
                        displayName: n.displayName
                    });
                    s.push(new PropertyRule(this.l, t, this.messageProvider, o, e));
                } else {
                    iterate(Object.entries(n), [ ...i, r ]);
                }
            }
        };
        iterate(Object.entries(e));
        return s;
    }
    hydrateRule(e, t) {
        switch (e) {
          case "required":
            return this.hydrateRequiredRule(t);

          case "regex":
            return this.hydrateRegexRule(t);

          case "maxLength":
            return this.hydrateLengthRule({
                ...t,
                isMax: true
            });

          case "minLength":
            return this.hydrateLengthRule({
                ...t,
                isMax: false
            });

          case "maxItems":
            return this.hydrateSizeRule({
                ...t,
                isMax: true
            });

          case "minItems":
            return this.hydrateSizeRule({
                ...t,
                isMax: false
            });

          case "range":
            return this.hydrateRangeRule({
                ...t,
                isInclusive: true
            });

          case "between":
            return this.hydrateRangeRule({
                ...t,
                isInclusive: false
            });

          case "equals":
            return this.hydrateEqualsRule(t);

          default:
            throw new Error(`Unsupported rule ${e}`);
        }
    }
    setCommonRuleProperties(e, t) {
        const r = e.messageKey;
        if (r !== void 0 && r !== null) {
            t.messageKey = r;
        }
        t.tag = e.tag;
        const n = e.when;
        if (n) {
            if (typeof n === "string") {
                const e = this.parser.parse(n, "None");
                t.canExecute = t => i.astEvaluate(e, s.Scope.create({
                    $object: t
                }), this, null);
            } else if (typeof n === "function") {
                t.canExecute = n;
            }
        }
    }
    isModelPropertyRule(e) {
        return typeof e === "object" && "rules" in e;
    }
    hydrateRequiredRule(e) {
        const t = new RequiredRule;
        this.setCommonRuleProperties(e, t);
        return t;
    }
    hydrateRegexRule(e) {
        const t = e.pattern;
        const s = new RegexRule(new RegExp(t.source, t.flags), e.messageKey);
        s.tag = e.tag;
        return s;
    }
    hydrateLengthRule(e) {
        const t = new LengthRule(e.length, e.isMax);
        this.setCommonRuleProperties(e, t);
        return t;
    }
    hydrateSizeRule(e) {
        const t = new SizeRule(e.count, e.isMax);
        this.setCommonRuleProperties(e, t);
        return t;
    }
    hydrateRangeRule(e) {
        const t = new RangeRule(e.isInclusive, {
            min: e.min,
            max: e.max
        });
        this.setCommonRuleProperties(e, t);
        return t;
    }
    hydrateEqualsRule(e) {
        const t = new EqualsRule(e.expectedValue);
        this.setCommonRuleProperties(e, t);
        return t;
    }
    hydrateRuleProperty(e) {
        const t = e.name;
        if (!t || typeof t !== "string") {
            throw new Error("The property name needs to be a non-empty string");
        }
        const [s, i] = parsePropertyName(t, this.parser);
        return new RuleProperty(i, s, e.displayName);
    }
}

i.mixinAstEvaluator()(ModelValidationExpressionHydrator);

class ValidateInstruction {
    constructor(e = void 0, t = void 0, s = void 0, i = void 0, r = void 0) {
        this.object = e;
        this.propertyName = t;
        this.rules = s;
        this.objectTag = i;
        this.propertyTag = r;
    }
}

const P = /*@__PURE__*/ e.DI.createInterface("IValidator");

class StandardValidator {
    async validate(e) {
        const t = e.object;
        const i = e.propertyName;
        const r = e.propertyTag;
        const n = e.rules ?? p.get(t, e.objectTag) ?? [];
        const a = s.Scope.create({
            [v]: t
        });
        if (i !== void 0) {
            return await (n.find((e => e.property.name === i))?.validate(t, r, a)) ?? [];
        }
        return (await Promise.all(n.map((async e => e.validate(t, r, a))))).flat();
    }
}

function getDefaultValidationConfiguration() {
    return {
        ValidatorType: StandardValidator,
        MessageProviderType: ValidationMessageProvider,
        CustomMessages: [],
        HydratorType: ModelValidationExpressionHydrator
    };
}

function createConfiguration(t) {
    return {
        optionsProvider: t,
        register(s) {
            const i = getDefaultValidationConfiguration();
            t(i);
            s.register(e.Registration.instance(m, i.CustomMessages), e.Registration.singleton(P, i.ValidatorType), e.Registration.singleton(d, i.MessageProviderType), e.Registration.singleton(a, i.HydratorType), e.Registration.transient(f, ValidationRules), ValidationDeserializer);
            return s;
        },
        customize(e) {
            return createConfiguration(e ?? t);
        }
    };
}

const E = createConfiguration(e.noop);

exports.BaseValidationRule = BaseValidationRule;

exports.Deserializer = Deserializer;

exports.EqualsRule = EqualsRule;

exports.ICustomMessages = m;

exports.IValidationExpressionHydrator = a;

exports.IValidationMessageProvider = d;

exports.IValidationRules = f;

exports.IValidator = P;

exports.LengthRule = LengthRule;

exports.ModelBasedRule = ModelBasedRule;

exports.ModelValidationExpressionHydrator = ModelValidationExpressionHydrator;

exports.PropertyRule = PropertyRule;

exports.RangeRule = RangeRule;

exports.RegexRule = RegexRule;

exports.RequiredRule = RequiredRule;

exports.RuleProperty = RuleProperty;

exports.Serializer = Serializer;

exports.SizeRule = SizeRule;

exports.StandardValidator = StandardValidator;

exports.ValidateInstruction = ValidateInstruction;

exports.ValidationConfiguration = E;

exports.ValidationDeserializer = ValidationDeserializer;

exports.ValidationMessageProvider = ValidationMessageProvider;

exports.ValidationResult = ValidationResult;

exports.ValidationRuleAliasMessage = $;

exports.ValidationRules = ValidationRules;

exports.ValidationSerializer = ValidationSerializer;

exports.deserializePrimitive = deserializePrimitive;

exports.getDefaultValidationConfiguration = getDefaultValidationConfiguration;

exports.parsePropertyName = parsePropertyName;

exports.rootObjectSymbol = v;

exports.serializePrimitive = serializePrimitive;

exports.serializePrimitives = serializePrimitives;

exports.validationRule = validationRule;

exports.validationRulesRegistrar = p;
//# sourceMappingURL=index.cjs.map
