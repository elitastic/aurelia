import { Protocol, DI, emptyArray, isArrayIndex, IPlatform, ILogger } from '../../../kernel/dist/native-modules/index.mjs';
import { Metadata } from '../../../metadata/dist/native-modules/index.mjs';

class BindingContext {
    constructor(key, value) {
        if (key !== void 0) {
            this[key] = value;
        }
    }
}
class Scope {
    constructor(parentScope, bindingContext, overrideContext, isBoundary) {
        this.parentScope = parentScope;
        this.bindingContext = bindingContext;
        this.overrideContext = overrideContext;
        this.isBoundary = isBoundary;
    }
    static getContext(scope, name, ancestor) {
        if (scope == null) {
            throw nullScopeError();
        }
        let overrideContext = scope.overrideContext;
        let currentScope = scope;
        if (ancestor > 0) {
            while (ancestor > 0) {
                ancestor--;
                currentScope = currentScope.parentScope;
                if (currentScope == null) {
                    return void 0;
                }
            }
            overrideContext = currentScope.overrideContext;
            return name in overrideContext ? overrideContext : currentScope.bindingContext;
        }
        while (currentScope != null
            && !currentScope.isBoundary
            && !(name in currentScope.overrideContext)
            && !(name in currentScope.bindingContext)) {
            currentScope = currentScope.parentScope;
        }
        if (currentScope == null) {
            return scope.bindingContext;
        }
        overrideContext = currentScope.overrideContext;
        return name in overrideContext ? overrideContext : currentScope.bindingContext;
    }
    static create(bc, oc, isBoundary) {
        if (bc == null) {
            throw nullContextError();
        }
        return new Scope(null, bc, oc == null ? new OverrideContext() : oc, isBoundary ?? false);
    }
    static fromParent(ps, bc) {
        if (ps == null) {
            throw nullScopeError();
        }
        return new Scope(ps, bc, new OverrideContext(), false);
    }
}
const nullScopeError = () => {
    return new Error(`AUR0203: scope is null/undefined.`)
        ;
};
const nullContextError = () => {
    return new Error('AUR0204: binding context is null/undefined')
        ;
};
class OverrideContext {
}

const hasOwnProp = Object.prototype.hasOwnProperty;
const def = Reflect.defineProperty;
const isFunction = (v) => typeof v === 'function';
const isString = (v) => typeof v === 'string';
const isArray = (v) => v instanceof Array;
function defineHiddenProp(obj, key, value) {
    def(obj, key, {
        enumerable: false,
        configurable: true,
        writable: true,
        value
    });
    return value;
}
function ensureProto(proto, key, defaultValue) {
    if (!(key in proto)) {
        defineHiddenProp(proto, key, defaultValue);
    }
}
const safeString = String;
const createLookup = () => Object.create(null);
const getOwnMetadata = Metadata.getOwn;
Metadata.hasOwn;
const defineMetadata = Metadata.define;
Protocol.annotation.keyFor;
Protocol.resource.keyFor;
Protocol.resource.appendTo;

const ISignaler = DI.createInterface('ISignaler', x => x.singleton(Signaler));
class Signaler {
    constructor() {
        this.signals = createLookup();
    }
    dispatchSignal(name) {
        const listeners = this.signals[name];
        if (listeners === undefined) {
            return;
        }
        let listener;
        for (listener of listeners.keys()) {
            listener.handleChange(undefined, undefined);
        }
    }
    addSignalListener(name, listener) {
        const signals = this.signals;
        const listeners = signals[name];
        if (listeners === undefined) {
            signals[name] = new Set([listener]);
        }
        else {
            listeners.add(listener);
        }
    }
    removeSignalListener(name, listener) {
        this.signals[name]?.delete(listener);
    }
}

var ExpressionKind;
(function (ExpressionKind) {
    ExpressionKind[ExpressionKind["AccessThis"] = 0] = "AccessThis";
    ExpressionKind[ExpressionKind["AccessScope"] = 1] = "AccessScope";
    ExpressionKind[ExpressionKind["ArrayLiteral"] = 2] = "ArrayLiteral";
    ExpressionKind[ExpressionKind["ObjectLiteral"] = 3] = "ObjectLiteral";
    ExpressionKind[ExpressionKind["PrimitiveLiteral"] = 4] = "PrimitiveLiteral";
    ExpressionKind[ExpressionKind["Template"] = 5] = "Template";
    ExpressionKind[ExpressionKind["Unary"] = 6] = "Unary";
    ExpressionKind[ExpressionKind["CallScope"] = 7] = "CallScope";
    ExpressionKind[ExpressionKind["CallMember"] = 8] = "CallMember";
    ExpressionKind[ExpressionKind["CallFunction"] = 9] = "CallFunction";
    ExpressionKind[ExpressionKind["AccessMember"] = 10] = "AccessMember";
    ExpressionKind[ExpressionKind["AccessKeyed"] = 11] = "AccessKeyed";
    ExpressionKind[ExpressionKind["TaggedTemplate"] = 12] = "TaggedTemplate";
    ExpressionKind[ExpressionKind["Binary"] = 13] = "Binary";
    ExpressionKind[ExpressionKind["Conditional"] = 14] = "Conditional";
    ExpressionKind[ExpressionKind["Assign"] = 15] = "Assign";
    ExpressionKind[ExpressionKind["ArrowFunction"] = 16] = "ArrowFunction";
    ExpressionKind[ExpressionKind["ValueConverter"] = 17] = "ValueConverter";
    ExpressionKind[ExpressionKind["BindingBehavior"] = 18] = "BindingBehavior";
    ExpressionKind[ExpressionKind["ArrayBindingPattern"] = 19] = "ArrayBindingPattern";
    ExpressionKind[ExpressionKind["ObjectBindingPattern"] = 20] = "ObjectBindingPattern";
    ExpressionKind[ExpressionKind["BindingIdentifier"] = 21] = "BindingIdentifier";
    ExpressionKind[ExpressionKind["ForOfStatement"] = 22] = "ForOfStatement";
    ExpressionKind[ExpressionKind["Interpolation"] = 23] = "Interpolation";
    ExpressionKind[ExpressionKind["ArrayDestructuring"] = 24] = "ArrayDestructuring";
    ExpressionKind[ExpressionKind["ObjectDestructuring"] = 25] = "ObjectDestructuring";
    ExpressionKind[ExpressionKind["DestructuringAssignmentLeaf"] = 26] = "DestructuringAssignmentLeaf";
})(ExpressionKind || (ExpressionKind = {}));
class Unparser {
    constructor() {
        this.text = '';
    }
    static unparse(expr) {
        const visitor = new Unparser();
        expr.accept(visitor);
        return visitor.text;
    }
    visitAccessMember(expr) {
        expr.object.accept(this);
        this.text += `${expr.optional ? '?' : ''}.${expr.name}`;
    }
    visitAccessKeyed(expr) {
        expr.object.accept(this);
        this.text += `${expr.optional ? '?.' : ''}[`;
        expr.key.accept(this);
        this.text += ']';
    }
    visitAccessThis(expr) {
        if (expr.ancestor === 0) {
            this.text += '$this';
            return;
        }
        this.text += '$parent';
        let i = expr.ancestor - 1;
        while (i--) {
            this.text += '.$parent';
        }
    }
    visitAccessScope(expr) {
        let i = expr.ancestor;
        while (i--) {
            this.text += '$parent.';
        }
        this.text += expr.name;
    }
    visitArrayLiteral(expr) {
        const elements = expr.elements;
        this.text += '[';
        for (let i = 0, length = elements.length; i < length; ++i) {
            if (i !== 0) {
                this.text += ',';
            }
            elements[i].accept(this);
        }
        this.text += ']';
    }
    visitArrowFunction(expr) {
        const args = expr.args;
        const ii = args.length;
        let i = 0;
        let text = '(';
        let name;
        for (; i < ii; ++i) {
            name = args[i].name;
            if (i > 0) {
                text += ', ';
            }
            if (i < ii - 1) {
                text += name;
            }
            else {
                text += expr.rest ? `...${name}` : name;
            }
        }
        this.text += `${text}) => `;
        expr.body.accept(this);
    }
    visitObjectLiteral(expr) {
        const keys = expr.keys;
        const values = expr.values;
        this.text += '{';
        for (let i = 0, length = keys.length; i < length; ++i) {
            if (i !== 0) {
                this.text += ',';
            }
            this.text += `'${keys[i]}':`;
            values[i].accept(this);
        }
        this.text += '}';
    }
    visitPrimitiveLiteral(expr) {
        this.text += '(';
        if (isString(expr.value)) {
            const escaped = expr.value.replace(/'/g, '\\\'');
            this.text += `'${escaped}'`;
        }
        else {
            this.text += `${expr.value}`;
        }
        this.text += ')';
    }
    visitCallFunction(expr) {
        this.text += '(';
        expr.func.accept(this);
        this.text += expr.optional ? '?.' : '';
        this.writeArgs(expr.args);
        this.text += ')';
    }
    visitCallMember(expr) {
        this.text += '(';
        expr.object.accept(this);
        this.text += `${expr.optionalMember ? '?.' : ''}.${expr.name}${expr.optionalCall ? '?.' : ''}`;
        this.writeArgs(expr.args);
        this.text += ')';
    }
    visitCallScope(expr) {
        this.text += '(';
        let i = expr.ancestor;
        while (i--) {
            this.text += '$parent.';
        }
        this.text += `${expr.name}${expr.optional ? '?.' : ''}`;
        this.writeArgs(expr.args);
        this.text += ')';
    }
    visitTemplate(expr) {
        const { cooked, expressions } = expr;
        const length = expressions.length;
        this.text += '`';
        this.text += cooked[0];
        for (let i = 0; i < length; i++) {
            expressions[i].accept(this);
            this.text += cooked[i + 1];
        }
        this.text += '`';
    }
    visitTaggedTemplate(expr) {
        const { cooked, expressions } = expr;
        const length = expressions.length;
        expr.func.accept(this);
        this.text += '`';
        this.text += cooked[0];
        for (let i = 0; i < length; i++) {
            expressions[i].accept(this);
            this.text += cooked[i + 1];
        }
        this.text += '`';
    }
    visitUnary(expr) {
        this.text += `(${expr.operation}`;
        if (expr.operation.charCodeAt(0) >= 97) {
            this.text += ' ';
        }
        expr.expression.accept(this);
        this.text += ')';
    }
    visitBinary(expr) {
        this.text += '(';
        expr.left.accept(this);
        if (expr.operation.charCodeAt(0) === 105) {
            this.text += ` ${expr.operation} `;
        }
        else {
            this.text += expr.operation;
        }
        expr.right.accept(this);
        this.text += ')';
    }
    visitConditional(expr) {
        this.text += '(';
        expr.condition.accept(this);
        this.text += '?';
        expr.yes.accept(this);
        this.text += ':';
        expr.no.accept(this);
        this.text += ')';
    }
    visitAssign(expr) {
        this.text += '(';
        expr.target.accept(this);
        this.text += '=';
        expr.value.accept(this);
        this.text += ')';
    }
    visitValueConverter(expr) {
        const args = expr.args;
        expr.expression.accept(this);
        this.text += `|${expr.name}`;
        for (let i = 0, length = args.length; i < length; ++i) {
            this.text += ':';
            args[i].accept(this);
        }
    }
    visitBindingBehavior(expr) {
        const args = expr.args;
        expr.expression.accept(this);
        this.text += `&${expr.name}`;
        for (let i = 0, length = args.length; i < length; ++i) {
            this.text += ':';
            args[i].accept(this);
        }
    }
    visitArrayBindingPattern(expr) {
        const elements = expr.elements;
        this.text += '[';
        for (let i = 0, length = elements.length; i < length; ++i) {
            if (i !== 0) {
                this.text += ',';
            }
            elements[i].accept(this);
        }
        this.text += ']';
    }
    visitObjectBindingPattern(expr) {
        const keys = expr.keys;
        const values = expr.values;
        this.text += '{';
        for (let i = 0, length = keys.length; i < length; ++i) {
            if (i !== 0) {
                this.text += ',';
            }
            this.text += `'${keys[i]}':`;
            values[i].accept(this);
        }
        this.text += '}';
    }
    visitBindingIdentifier(expr) {
        this.text += expr.name;
    }
    visitForOfStatement(expr) {
        expr.declaration.accept(this);
        this.text += ' of ';
        expr.iterable.accept(this);
    }
    visitInterpolation(expr) {
        const { parts, expressions } = expr;
        const length = expressions.length;
        this.text += '${';
        this.text += parts[0];
        for (let i = 0; i < length; i++) {
            expressions[i].accept(this);
            this.text += parts[i + 1];
        }
        this.text += '}';
    }
    visitDestructuringAssignmentExpression(expr) {
        const $kind = expr.$kind;
        const isObjDes = $kind === 25;
        this.text += isObjDes ? '{' : '[';
        const list = expr.list;
        const len = list.length;
        let i;
        let item;
        for (i = 0; i < len; i++) {
            item = list[i];
            switch (item.$kind) {
                case 26:
                    item.accept(this);
                    break;
                case 24:
                case 25: {
                    const source = item.source;
                    if (source) {
                        source.accept(this);
                        this.text += ':';
                    }
                    item.accept(this);
                    break;
                }
            }
        }
        this.text += isObjDes ? '}' : ']';
    }
    visitDestructuringAssignmentSingleExpression(expr) {
        expr.source.accept(this);
        this.text += ':';
        expr.target.accept(this);
        const initializer = expr.initializer;
        if (initializer !== void 0) {
            this.text += '=';
            initializer.accept(this);
        }
    }
    visitDestructuringAssignmentRestExpression(expr) {
        this.text += '...';
        expr.accept(this);
    }
    writeArgs(args) {
        this.text += '(';
        for (let i = 0, length = args.length; i < length; ++i) {
            if (i !== 0) {
                this.text += ',';
            }
            args[i].accept(this);
        }
        this.text += ')';
    }
}
class CustomExpression {
    constructor(value) {
        this.value = value;
    }
    evaluate(_s, _e, _c) {
        return this.value;
    }
}
class BindingBehaviorExpression {
    constructor(expression, name, args) {
        this.expression = expression;
        this.name = name;
        this.args = args;
        this._key = `_bb_${name}`;
    }
    get $kind() { return 18; }
    get hasBind() { return true; }
    get hasUnbind() { return true; }
    evaluate(s, e, c) {
        return this.expression.evaluate(s, e, c);
    }
    assign(s, e, val) {
        return this.expression.assign(s, e, val);
    }
    bind(s, b) {
        const name = this.name;
        const key = this._key;
        const behavior = b.getBehavior?.(name);
        if (behavior == null) {
            throw behaviorNotFoundError(name);
        }
        if (b[key] === void 0) {
            b[key] = behavior;
            behavior.bind?.(s, b, ...this.args.map(a => a.evaluate(s, b, null)));
        }
        else {
            throw duplicateBehaviorAppliedError(name);
        }
        if (this.expression.hasBind) {
            this.expression.bind(s, b);
        }
    }
    unbind(s, b) {
        const internalKey = this._key;
        const $b = b;
        if ($b[internalKey] !== void 0) {
            $b[internalKey].unbind?.(s, b);
            $b[internalKey] = void 0;
        }
        if (this.expression.hasUnbind) {
            this.expression.unbind(s, b);
        }
    }
    accept(visitor) {
        return visitor.visitBindingBehavior(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
const behaviorNotFoundError = (name) => new Error(`AUR0101: BindingBehavior '${name}' could not be found. Did you forget to register it as a dependency?`)
    ;
const duplicateBehaviorAppliedError = (name) => new Error(`AUR0102: BindingBehavior '${name}' already applied.`)
    ;
class ValueConverterExpression {
    constructor(expression, name, args) {
        this.expression = expression;
        this.name = name;
        this.args = args;
    }
    get $kind() { return 17; }
    get hasBind() { return true; }
    get hasUnbind() { return true; }
    evaluate(s, e, c) {
        const name = this.name;
        const vc = e?.getConverter?.(name);
        if (vc == null) {
            throw converterNotFoundError(name);
        }
        if ('toView' in vc) {
            return vc.toView(this.expression.evaluate(s, e, c), ...this.args.map(a => a.evaluate(s, e, c)));
        }
        return this.expression.evaluate(s, e, c);
    }
    assign(s, e, val) {
        const name = this.name;
        const vc = e?.getConverter?.(name);
        if (vc == null) {
            throw converterNotFoundError(name);
        }
        if ('fromView' in vc) {
            val = vc.fromView(val, ...this.args.map(a => a.evaluate(s, e, null)));
        }
        return this.expression.assign(s, e, val);
    }
    bind(s, b) {
        const name = this.name;
        const vc = b.getConverter?.(name);
        if (vc == null) {
            throw converterNotFoundError(name);
        }
        const signals = vc.signals;
        if (signals != null) {
            const signaler = b.get?.(ISignaler);
            const ii = signals.length;
            let i = 0;
            for (; i < ii; ++i) {
                signaler?.addSignalListener(signals[i], b);
            }
        }
        if (this.expression.hasBind) {
            this.expression.bind(s, b);
        }
    }
    unbind(_s, b) {
        const vc = b.getConverter?.(this.name);
        if (vc?.signals === void 0) {
            return;
        }
        const signaler = b.get(ISignaler);
        let i = 0;
        for (; i < vc.signals.length; ++i) {
            signaler.removeSignalListener(vc.signals[i], b);
        }
    }
    accept(visitor) {
        return visitor.visitValueConverter(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
const converterNotFoundError = (name) => {
    return new Error(`AUR0103: ValueConverter '${name}' could not be found. Did you forget to register it as a dependency?`);
};
class AssignExpression {
    constructor(target, value) {
        this.target = target;
        this.value = value;
    }
    get $kind() { return 15; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        return this.target.assign(s, e, this.value.evaluate(s, e, c));
    }
    assign(s, e, val) {
        this.value.assign(s, e, val);
        return this.target.assign(s, e, val);
    }
    accept(visitor) {
        return visitor.visitAssign(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class ConditionalExpression {
    constructor(condition, yes, no) {
        this.condition = condition;
        this.yes = yes;
        this.no = no;
    }
    get $kind() { return 14; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        return this.condition.evaluate(s, e, c) ? this.yes.evaluate(s, e, c) : this.no.evaluate(s, e, c);
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitConditional(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class AccessThisExpression {
    constructor(ancestor = 0) {
        this.ancestor = ancestor;
    }
    get $kind() { return 0; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, _e, _c) {
        let currentScope = s;
        let i = this.ancestor;
        while (i-- && currentScope) {
            currentScope = currentScope.parentScope;
        }
        return i < 1 && currentScope ? currentScope.bindingContext : void 0;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitAccessThis(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
AccessThisExpression.$this = new AccessThisExpression(0);
AccessThisExpression.$parent = new AccessThisExpression(1);
class AccessScopeExpression {
    constructor(name, ancestor = 0) {
        this.name = name;
        this.ancestor = ancestor;
    }
    get $kind() { return 1; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const obj = Scope.getContext(s, this.name, this.ancestor);
        if (c !== null) {
            c.observe(obj, this.name);
        }
        const evaluatedValue = obj[this.name];
        if (evaluatedValue == null && this.name === '$host') {
            throw new Error(`AUR0105: Unable to find $host context. Did you forget [au-slot] attribute?`);
        }
        if (e?.strict) {
            return e?.boundFn && isFunction(evaluatedValue)
                ? evaluatedValue.bind(obj)
                : evaluatedValue;
        }
        return evaluatedValue == null
            ? ''
            : e?.boundFn && isFunction(evaluatedValue)
                ? evaluatedValue.bind(obj)
                : evaluatedValue;
    }
    assign(s, _e, val) {
        if (this.name === '$host') {
            throw new Error(`AUR0106: Invalid assignment. $host is a reserved keyword.`);
        }
        const obj = Scope.getContext(s, this.name, this.ancestor);
        if (obj instanceof Object) {
            if (obj.$observers?.[this.name] !== void 0) {
                obj.$observers[this.name].setValue(val);
                return val;
            }
            else {
                return obj[this.name] = val;
            }
        }
    }
    accept(visitor) {
        return visitor.visitAccessScope(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class AccessMemberExpression {
    constructor(object, name, optional = false) {
        this.object = object;
        this.name = name;
        this.optional = optional;
    }
    get $kind() { return 10; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const instance = this.object.evaluate(s, e, c);
        let ret;
        if (e?.strict) {
            if (instance == null) {
                return instance;
            }
            if (c !== null) {
                c.observe(instance, this.name);
            }
            ret = instance[this.name];
            if (e?.boundFn && isFunction(ret)) {
                return ret.bind(instance);
            }
            return ret;
        }
        if (c !== null && instance instanceof Object) {
            c.observe(instance, this.name);
        }
        if (instance) {
            ret = instance[this.name];
            if (e?.boundFn && isFunction(ret)) {
                return ret.bind(instance);
            }
            return ret;
        }
        return '';
    }
    assign(s, e, val) {
        const obj = this.object.evaluate(s, e, null);
        if (obj instanceof Object) {
            if (obj.$observers !== void 0 && obj.$observers[this.name] !== void 0) {
                obj.$observers[this.name].setValue(val);
            }
            else {
                obj[this.name] = val;
            }
        }
        else {
            this.object.assign(s, e, { [this.name]: val });
        }
        return val;
    }
    accept(visitor) {
        return visitor.visitAccessMember(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class AccessKeyedExpression {
    constructor(object, key, optional = false) {
        this.object = object;
        this.key = key;
        this.optional = optional;
    }
    get $kind() { return 11; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const instance = this.object.evaluate(s, e, c);
        if (instance instanceof Object) {
            const key = this.key.evaluate(s, e, c);
            if (c !== null) {
                c.observe(instance, key);
            }
            return instance[key];
        }
        return void 0;
    }
    assign(s, e, val) {
        const instance = this.object.evaluate(s, e, null);
        const key = this.key.evaluate(s, e, null);
        return instance[key] = val;
    }
    accept(visitor) {
        return visitor.visitAccessKeyed(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class CallScopeExpression {
    constructor(name, args, ancestor = 0, optional = false) {
        this.name = name;
        this.args = args;
        this.ancestor = ancestor;
        this.optional = optional;
    }
    get $kind() { return 7; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const args = this.args.map(a => a.evaluate(s, e, c));
        const context = Scope.getContext(s, this.name, this.ancestor);
        const func = getFunction(e?.strictFnCall, context, this.name);
        if (func) {
            return func.apply(context, args);
        }
        return void 0;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitCallScope(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
const autoObserveArrayMethods = 'at map filter includes indexOf lastIndexOf findIndex find flat flatMap join reduce reduceRight slice every some sort'.split(' ');
class CallMemberExpression {
    constructor(object, name, args, optionalMember = false, optionalCall = false) {
        this.object = object;
        this.name = name;
        this.args = args;
        this.optionalMember = optionalMember;
        this.optionalCall = optionalCall;
    }
    get $kind() { return 8; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const instance = this.object.evaluate(s, e, c);
        const args = this.args.map(a => a.evaluate(s, e, c));
        const func = getFunction(e?.strictFnCall, instance, this.name);
        if (func) {
            const ret = func.apply(instance, args);
            if (isArray(instance) && autoObserveArrayMethods.includes(this.name)) {
                c?.observeCollection(instance);
            }
            return ret;
        }
        return void 0;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitCallMember(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class CallFunctionExpression {
    constructor(func, args, optional = false) {
        this.func = func;
        this.args = args;
        this.optional = optional;
    }
    get $kind() { return 9; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const func = this.func.evaluate(s, e, c);
        if (isFunction(func)) {
            return func(...this.args.map(a => a.evaluate(s, e, c)));
        }
        if (!e?.strictFnCall && func == null) {
            return void 0;
        }
        throw new Error(`AUR0107: Expression is not a function.`);
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitCallFunction(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class BinaryExpression {
    constructor(operation, left, right) {
        this.operation = operation;
        this.left = left;
        this.right = right;
    }
    get $kind() { return 13; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        switch (this.operation) {
            case '&&':
                return this.left.evaluate(s, e, c) && this.right.evaluate(s, e, c);
            case '||':
                return this.left.evaluate(s, e, c) || this.right.evaluate(s, e, c);
            case '??':
                return this.left.evaluate(s, e, c) ?? this.right.evaluate(s, e, c);
            case '==':
                return this.left.evaluate(s, e, c) == this.right.evaluate(s, e, c);
            case '===':
                return this.left.evaluate(s, e, c) === this.right.evaluate(s, e, c);
            case '!=':
                return this.left.evaluate(s, e, c) != this.right.evaluate(s, e, c);
            case '!==':
                return this.left.evaluate(s, e, c) !== this.right.evaluate(s, e, c);
            case 'instanceof': {
                const right = this.right.evaluate(s, e, c);
                if (isFunction(right)) {
                    return this.left.evaluate(s, e, c) instanceof right;
                }
                return false;
            }
            case 'in': {
                const right = this.right.evaluate(s, e, c);
                if (right instanceof Object) {
                    return this.left.evaluate(s, e, c) in right;
                }
                return false;
            }
            case '+': {
                const left = this.left.evaluate(s, e, c);
                const right = this.right.evaluate(s, e, c);
                if (e?.strict) {
                    return left + right;
                }
                if (!left || !right) {
                    if (isNumberOrBigInt(left) || isNumberOrBigInt(right)) {
                        return (left || 0) + (right || 0);
                    }
                    if (isStringOrDate(left) || isStringOrDate(right)) {
                        return (left || '') + (right || '');
                    }
                }
                return left + right;
            }
            case '-':
                return this.left.evaluate(s, e, c) - this.right.evaluate(s, e, c);
            case '*':
                return this.left.evaluate(s, e, c) * this.right.evaluate(s, e, c);
            case '/':
                return this.left.evaluate(s, e, c) / this.right.evaluate(s, e, c);
            case '%':
                return this.left.evaluate(s, e, c) % this.right.evaluate(s, e, c);
            case '<':
                return this.left.evaluate(s, e, c) < this.right.evaluate(s, e, c);
            case '>':
                return this.left.evaluate(s, e, c) > this.right.evaluate(s, e, c);
            case '<=':
                return this.left.evaluate(s, e, c) <= this.right.evaluate(s, e, c);
            case '>=':
                return this.left.evaluate(s, e, c) >= this.right.evaluate(s, e, c);
            default:
                throw new Error(`AUR0108: Unknown binary operator: '${this.operation}'`);
        }
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitBinary(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class UnaryExpression {
    constructor(operation, expression) {
        this.operation = operation;
        this.expression = expression;
    }
    get $kind() { return 6; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        switch (this.operation) {
            case 'void':
                return void this.expression.evaluate(s, e, c);
            case 'typeof':
                return typeof this.expression.evaluate(s, e, c);
            case '!':
                return !this.expression.evaluate(s, e, c);
            case '-':
                return -this.expression.evaluate(s, e, c);
            case '+':
                return +this.expression.evaluate(s, e, c);
            default:
                throw new Error(`AUR0109: Unknown unary operator: '${this.operation}'`);
        }
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitUnary(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class PrimitiveLiteralExpression {
    constructor(value) {
        this.value = value;
    }
    get $kind() { return 4; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(_s, _e, _c) {
        return this.value;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitPrimitiveLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
PrimitiveLiteralExpression.$undefined = new PrimitiveLiteralExpression(void 0);
PrimitiveLiteralExpression.$null = new PrimitiveLiteralExpression(null);
PrimitiveLiteralExpression.$true = new PrimitiveLiteralExpression(true);
PrimitiveLiteralExpression.$false = new PrimitiveLiteralExpression(false);
PrimitiveLiteralExpression.$empty = new PrimitiveLiteralExpression('');
class ArrayLiteralExpression {
    constructor(elements) {
        this.elements = elements;
    }
    get $kind() { return 2; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        return this.elements.map(el => el.evaluate(s, e, c));
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitArrayLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
ArrayLiteralExpression.$empty = new ArrayLiteralExpression(emptyArray);
class ObjectLiteralExpression {
    constructor(keys, values) {
        this.keys = keys;
        this.values = values;
    }
    get $kind() { return 3; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const instance = {};
        for (let i = 0; i < this.keys.length; ++i) {
            instance[this.keys[i]] = this.values[i].evaluate(s, e, c);
        }
        return instance;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitObjectLiteral(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
ObjectLiteralExpression.$empty = new ObjectLiteralExpression(emptyArray, emptyArray);
class TemplateExpression {
    constructor(cooked, expressions = emptyArray) {
        this.cooked = cooked;
        this.expressions = expressions;
    }
    get $kind() { return 5; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        let result = this.cooked[0];
        for (let i = 0; i < this.expressions.length; ++i) {
            result += safeString(this.expressions[i].evaluate(s, e, c));
            result += this.cooked[i + 1];
        }
        return result;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitTemplate(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
TemplateExpression.$empty = new TemplateExpression(['']);
class TaggedTemplateExpression {
    constructor(cooked, raw, func, expressions = emptyArray) {
        this.cooked = cooked;
        this.func = func;
        this.expressions = expressions;
        cooked.raw = raw;
    }
    get $kind() { return 12; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const results = this.expressions.map(el => el.evaluate(s, e, c));
        const func = this.func.evaluate(s, e, c);
        if (!isFunction(func)) {
            throw new Error(`AUR0110: Left-hand side of tagged template expression is not a function.`);
        }
        return func(this.cooked, ...results);
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitTaggedTemplate(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class ArrayBindingPattern {
    constructor(elements) {
        this.elements = elements;
    }
    get $kind() { return 19; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(_s, _e, _c) {
        return void 0;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitArrayBindingPattern(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class ObjectBindingPattern {
    constructor(keys, values) {
        this.keys = keys;
        this.values = values;
    }
    get $kind() { return 20; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(_s, _e, _c) {
        return void 0;
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitObjectBindingPattern(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class BindingIdentifier {
    constructor(name) {
        this.name = name;
    }
    get $kind() { return 21; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(_s, _e, _c) {
        return this.name;
    }
    accept(visitor) {
        return visitor.visitBindingIdentifier(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class ForOfStatement {
    constructor(declaration, iterable) {
        this.declaration = declaration;
        this.iterable = iterable;
    }
    get $kind() { return 22; }
    get hasBind() { return true; }
    get hasUnbind() { return true; }
    evaluate(s, e, c) {
        return this.iterable.evaluate(s, e, c);
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    bind(s, b) {
        if (this.iterable.hasBind) {
            this.iterable.bind(s, b);
        }
    }
    unbind(s, b) {
        if (this.iterable.hasUnbind) {
            this.iterable.unbind(s, b);
        }
    }
    accept(visitor) {
        return visitor.visitForOfStatement(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class Interpolation {
    constructor(parts, expressions = emptyArray) {
        this.parts = parts;
        this.expressions = expressions;
        this.isMulti = expressions.length > 1;
        this.firstExpression = expressions[0];
    }
    get $kind() { return 23; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        if (this.isMulti) {
            let result = this.parts[0];
            let i = 0;
            for (; i < this.expressions.length; ++i) {
                result += safeString(this.expressions[i].evaluate(s, e, c));
                result += this.parts[i + 1];
            }
            return result;
        }
        else {
            return `${this.parts[0]}${this.firstExpression.evaluate(s, e, c)}${this.parts[1]}`;
        }
    }
    assign(_s, _e, _obj) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitInterpolation(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class DestructuringAssignmentExpression {
    constructor($kind, list, source, initializer) {
        this.$kind = $kind;
        this.list = list;
        this.source = source;
        this.initializer = initializer;
    }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(_s, _e, _c) {
        return void 0;
    }
    assign(s, l, value) {
        const list = this.list;
        const len = list.length;
        let i;
        let item;
        for (i = 0; i < len; i++) {
            item = list[i];
            switch (item.$kind) {
                case 26:
                    item.assign(s, l, value);
                    break;
                case 24:
                case 25: {
                    if (typeof value !== 'object' || value === null) {
                        {
                            throw new Error(`AUR0112: Cannot use non-object value for destructuring assignment.`);
                        }
                    }
                    let source = item.source.evaluate(Scope.create(value), l, null);
                    if (source === void 0) {
                        source = item.initializer?.evaluate(s, l, null);
                    }
                    item.assign(s, l, source);
                    break;
                }
            }
        }
    }
    accept(visitor) {
        return visitor.visitDestructuringAssignmentExpression(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class DestructuringAssignmentSingleExpression {
    constructor(target, source, initializer) {
        this.target = target;
        this.source = source;
        this.initializer = initializer;
    }
    get $kind() { return 26; }
    evaluate(_s, _e, _c) {
        return void 0;
    }
    assign(s, l, value) {
        if (value == null) {
            return;
        }
        if (typeof value !== 'object') {
            {
                throw new Error(`AUR0112: Cannot use non-object value for destructuring assignment.`);
            }
        }
        let source = this.source.evaluate(Scope.create(value), l, null);
        if (source === void 0) {
            source = this.initializer?.evaluate(s, l, null);
        }
        this.target.assign(s, l, source);
    }
    accept(visitor) {
        return visitor.visitDestructuringAssignmentSingleExpression(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class DestructuringAssignmentRestExpression {
    constructor(target, indexOrProperties) {
        this.target = target;
        this.indexOrProperties = indexOrProperties;
    }
    get $kind() { return 26; }
    evaluate(_s, _e, _c) {
        return void 0;
    }
    assign(s, l, value) {
        if (value == null) {
            return;
        }
        if (typeof value !== 'object') {
            {
                throw new Error(`AUR0112: Cannot use non-object value for destructuring assignment.`);
            }
        }
        const indexOrProperties = this.indexOrProperties;
        let restValue;
        if (isArrayIndex(indexOrProperties)) {
            if (!Array.isArray(value)) {
                {
                    throw new Error(`AUR0112: Cannot use non-array value for array-destructuring assignment.`);
                }
            }
            restValue = value.slice(indexOrProperties);
        }
        else {
            restValue = Object
                .entries(value)
                .reduce((acc, [k, v]) => {
                if (!indexOrProperties.includes(k)) {
                    acc[k] = v;
                }
                return acc;
            }, {});
        }
        this.target.assign(s, l, restValue);
    }
    accept(_visitor) {
        return _visitor.visitDestructuringAssignmentRestExpression(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
class ArrowFunction {
    constructor(args, body, rest = false) {
        this.args = args;
        this.body = body;
        this.rest = rest;
    }
    get $kind() { return 16; }
    get hasBind() { return false; }
    get hasUnbind() { return false; }
    evaluate(s, e, c) {
        const func = (...args) => {
            const params = this.args;
            const rest = this.rest;
            const lastIdx = params.length - 1;
            const context = params.reduce((map, param, i) => {
                if (rest && i === lastIdx) {
                    map[param.name] = args.slice(i);
                }
                else {
                    map[param.name] = args[i];
                }
                return map;
            }, {});
            const functionScope = Scope.fromParent(s, context);
            return this.body.evaluate(functionScope, e, c);
        };
        return func;
    }
    assign(_s, _e, _value) {
        return void 0;
    }
    accept(visitor) {
        return visitor.visitArrowFunction(this);
    }
    toString() {
        return Unparser.unparse(this);
    }
}
function getFunction(mustEvaluate, obj, name) {
    const func = obj == null ? null : obj[name];
    if (isFunction(func)) {
        return func;
    }
    if (!mustEvaluate && func == null) {
        return null;
    }
    throw new Error(`AUR0111: Expected '${name}' to be a function`);
}
function isNumberOrBigInt(value) {
    switch (typeof value) {
        case 'number':
        case 'bigint':
            return true;
        default:
            return false;
    }
}
function isStringOrDate(value) {
    switch (typeof value) {
        case 'string':
            return true;
        case 'object':
            return value instanceof Date;
        default:
            return false;
    }
}

const ICoercionConfiguration = DI.createInterface('ICoercionConfiguration');
var CollectionKind;
(function (CollectionKind) {
    CollectionKind[CollectionKind["indexed"] = 8] = "indexed";
    CollectionKind[CollectionKind["keyed"] = 4] = "keyed";
    CollectionKind[CollectionKind["array"] = 9] = "array";
    CollectionKind[CollectionKind["map"] = 6] = "map";
    CollectionKind[CollectionKind["set"] = 7] = "set";
})(CollectionKind || (CollectionKind = {}));
var AccessorType;
(function (AccessorType) {
    AccessorType[AccessorType["None"] = 0] = "None";
    AccessorType[AccessorType["Observer"] = 1] = "Observer";
    AccessorType[AccessorType["Node"] = 2] = "Node";
    AccessorType[AccessorType["Layout"] = 4] = "Layout";
    AccessorType[AccessorType["Primtive"] = 8] = "Primtive";
    AccessorType[AccessorType["Array"] = 18] = "Array";
    AccessorType[AccessorType["Set"] = 34] = "Set";
    AccessorType[AccessorType["Map"] = 66] = "Map";
})(AccessorType || (AccessorType = {}));
function copyIndexMap(existing, deletedIndices, deletedItems) {
    const { length } = existing;
    const arr = Array(length);
    let i = 0;
    while (i < length) {
        arr[i] = existing[i];
        ++i;
    }
    if (deletedIndices !== void 0) {
        arr.deletedIndices = deletedIndices.slice(0);
    }
    else if (existing.deletedIndices !== void 0) {
        arr.deletedIndices = existing.deletedIndices.slice(0);
    }
    else {
        arr.deletedIndices = [];
    }
    if (deletedItems !== void 0) {
        arr.deletedItems = deletedItems.slice(0);
    }
    else if (existing.deletedItems !== void 0) {
        arr.deletedItems = existing.deletedItems.slice(0);
    }
    else {
        arr.deletedItems = [];
    }
    arr.isIndexMap = true;
    return arr;
}
function createIndexMap(length = 0) {
    const arr = Array(length);
    let i = 0;
    while (i < length) {
        arr[i] = i++;
    }
    arr.deletedIndices = [];
    arr.deletedItems = [];
    arr.isIndexMap = true;
    return arr;
}
function cloneIndexMap(indexMap) {
    const clone = indexMap.slice();
    clone.deletedIndices = indexMap.deletedIndices.slice();
    clone.deletedItems = indexMap.deletedItems.slice();
    clone.isIndexMap = true;
    return clone;
}
function isIndexMap(value) {
    return isArray(value) && value.isIndexMap === true;
}

let currBatch = new Map();
let batching = false;
function batch(fn) {
    const prevBatch = currBatch;
    const newBatch = currBatch = new Map();
    batching = true;
    try {
        fn();
    }
    finally {
        currBatch = null;
        batching = false;
        try {
            let pair;
            let subs;
            let batchRecord;
            let col;
            let indexMap;
            let hasChanges = false;
            let i;
            let ii;
            for (pair of newBatch) {
                subs = pair[0];
                batchRecord = pair[1];
                if (prevBatch?.has(subs)) {
                    prevBatch.set(subs, batchRecord);
                }
                if (batchRecord[0] === 1) {
                    subs.notify(batchRecord[1], batchRecord[2]);
                }
                else {
                    col = batchRecord[1];
                    indexMap = batchRecord[2];
                    hasChanges = false;
                    if (indexMap.deletedIndices.length > 0) {
                        hasChanges = true;
                    }
                    else {
                        for (i = 0, ii = indexMap.length; i < ii; ++i) {
                            if (indexMap[i] !== i) {
                                hasChanges = true;
                                break;
                            }
                        }
                    }
                    if (hasChanges) {
                        subs.notifyCollection(col, indexMap);
                    }
                }
            }
        }
        finally {
            currBatch = prevBatch;
        }
    }
}
function addCollectionBatch(subs, collection, indexMap) {
    if (!currBatch.has(subs)) {
        currBatch.set(subs, [2, collection, indexMap]);
    }
}
function addValueBatch(subs, newValue, oldValue) {
    const batchRecord = currBatch.get(subs);
    if (batchRecord === void 0) {
        currBatch.set(subs, [1, newValue, oldValue]);
    }
    else {
        batchRecord[1] = newValue;
        batchRecord[2] = oldValue;
    }
}

function subscriberCollection(target) {
    return target == null ? subscriberCollectionDeco : subscriberCollectionDeco(target);
}
function subscriberCollectionDeco(target) {
    const proto = target.prototype;
    def(proto, 'subs', { get: getSubscriberRecord });
    ensureProto(proto, 'subscribe', addSubscriber);
    ensureProto(proto, 'unsubscribe', removeSubscriber);
}
class SubscriberRecord {
    constructor() {
        this.sf = 0;
        this.count = 0;
    }
    add(subscriber) {
        if (this.has(subscriber)) {
            return false;
        }
        const subscriberFlags = this.sf;
        if ((subscriberFlags & 1) === 0) {
            this.s0 = subscriber;
            this.sf |= 1;
        }
        else if ((subscriberFlags & 2) === 0) {
            this.s1 = subscriber;
            this.sf |= 2;
        }
        else if ((subscriberFlags & 4) === 0) {
            this.s2 = subscriber;
            this.sf |= 4;
        }
        else if ((subscriberFlags & 8) === 0) {
            this.sr = [subscriber];
            this.sf |= 8;
        }
        else {
            this.sr.push(subscriber);
        }
        ++this.count;
        return true;
    }
    has(subscriber) {
        const subscriberFlags = this.sf;
        if ((subscriberFlags & 1) > 0 && this.s0 === subscriber) {
            return true;
        }
        if ((subscriberFlags & 2) > 0 && this.s1 === subscriber) {
            return true;
        }
        if ((subscriberFlags & 4) > 0 && this.s2 === subscriber) {
            return true;
        }
        if ((subscriberFlags & 8) > 0) {
            const subscribers = this.sr;
            const ii = subscribers.length;
            let i = 0;
            for (; i < ii; ++i) {
                if (subscribers[i] === subscriber) {
                    return true;
                }
            }
        }
        return false;
    }
    any() {
        return this.sf !== 0;
    }
    remove(subscriber) {
        const subscriberFlags = this.sf;
        if ((subscriberFlags & 1) > 0 && this.s0 === subscriber) {
            this.s0 = void 0;
            this.sf = (this.sf | 1) ^ 1;
            --this.count;
            return true;
        }
        else if ((subscriberFlags & 2) > 0 && this.s1 === subscriber) {
            this.s1 = void 0;
            this.sf = (this.sf | 2) ^ 2;
            --this.count;
            return true;
        }
        else if ((subscriberFlags & 4) > 0 && this.s2 === subscriber) {
            this.s2 = void 0;
            this.sf = (this.sf | 4) ^ 4;
            --this.count;
            return true;
        }
        else if ((subscriberFlags & 8) > 0) {
            const subscribers = this.sr;
            const ii = subscribers.length;
            let i = 0;
            for (; i < ii; ++i) {
                if (subscribers[i] === subscriber) {
                    subscribers.splice(i, 1);
                    if (ii === 1) {
                        this.sf = (this.sf | 8) ^ 8;
                    }
                    --this.count;
                    return true;
                }
            }
        }
        return false;
    }
    notify(val, oldVal) {
        if (batching) {
            addValueBatch(this, val, oldVal);
            return;
        }
        const sub0 = this.s0;
        const sub1 = this.s1;
        const sub2 = this.s2;
        let subs = this.sr;
        if (subs !== void 0) {
            subs = subs.slice();
        }
        if (sub0 !== void 0) {
            sub0.handleChange(val, oldVal);
        }
        if (sub1 !== void 0) {
            sub1.handleChange(val, oldVal);
        }
        if (sub2 !== void 0) {
            sub2.handleChange(val, oldVal);
        }
        if (subs !== void 0) {
            const ii = subs.length;
            let sub;
            let i = 0;
            for (; i < ii; ++i) {
                sub = subs[i];
                if (sub !== void 0) {
                    sub.handleChange(val, oldVal);
                }
            }
        }
    }
    notifyCollection(collection, indexMap) {
        const sub0 = this.s0;
        const sub1 = this.s1;
        const sub2 = this.s2;
        let subs = this.sr;
        if (subs !== void 0) {
            subs = subs.slice();
        }
        if (sub0 !== void 0) {
            sub0.handleCollectionChange(collection, indexMap);
        }
        if (sub1 !== void 0) {
            sub1.handleCollectionChange(collection, indexMap);
        }
        if (sub2 !== void 0) {
            sub2.handleCollectionChange(collection, indexMap);
        }
        if (subs !== void 0) {
            const ii = subs.length;
            let sub;
            let i = 0;
            for (; i < ii; ++i) {
                sub = subs[i];
                if (sub !== void 0) {
                    sub.handleCollectionChange(collection, indexMap);
                }
            }
        }
    }
}
function getSubscriberRecord() {
    return defineHiddenProp(this, 'subs', new SubscriberRecord());
}
function addSubscriber(subscriber) {
    return this.subs.add(subscriber);
}
function removeSubscriber(subscriber) {
    return this.subs.remove(subscriber);
}
var SubFlags;
(function (SubFlags) {
    SubFlags[SubFlags["None"] = 0] = "None";
    SubFlags[SubFlags["Sub0"] = 1] = "Sub0";
    SubFlags[SubFlags["Sub1"] = 2] = "Sub1";
    SubFlags[SubFlags["Sub2"] = 4] = "Sub2";
    SubFlags[SubFlags["SubRest"] = 8] = "SubRest";
    SubFlags[SubFlags["Any"] = 15] = "Any";
})(SubFlags || (SubFlags = {}));

class CollectionLengthObserver {
    constructor(owner) {
        this.owner = owner;
        this.type = 18;
        this._value = (this._obj = owner.collection).length;
    }
    getValue() {
        return this._obj.length;
    }
    setValue(newValue) {
        const currentValue = this._value;
        if (newValue !== currentValue && isArrayIndex(newValue)) {
            this._obj.length = newValue;
            this._value = newValue;
            this.subs.notify(newValue, currentValue);
        }
    }
    handleCollectionChange(_arr, _) {
        const oldValue = this._value;
        const value = this._obj.length;
        if ((this._value = value) !== oldValue) {
            this.subs.notify(this._value, oldValue);
        }
    }
}
class CollectionSizeObserver {
    constructor(owner) {
        this.owner = owner;
        this._value = (this._obj = owner.collection).size;
        this.type = this._obj instanceof Map ? 66 : 34;
    }
    getValue() {
        return this._obj.size;
    }
    setValue() {
        throw new Error(`AUR02: Map/Set "size" is a readonly property`);
    }
    handleCollectionChange(_collection, _) {
        const oldValue = this._value;
        const value = this._obj.size;
        if ((this._value = value) !== oldValue) {
            this.subs.notify(this._value, oldValue);
        }
    }
}
function implementLengthObserver(klass) {
    const proto = klass.prototype;
    ensureProto(proto, 'subscribe', subscribe);
    ensureProto(proto, 'unsubscribe', unsubscribe);
    subscriberCollection(klass);
}
function subscribe(subscriber) {
    if (this.subs.add(subscriber) && this.subs.count === 1) {
        this.owner.subscribe(this);
    }
}
function unsubscribe(subscriber) {
    if (this.subs.remove(subscriber) && this.subs.count === 0) {
        this.owner.subscribe(this);
    }
}
implementLengthObserver(CollectionLengthObserver);
implementLengthObserver(CollectionSizeObserver);

const lookupMetadataKey$2 = '__au_array_obs__';
const observerLookup$2 = (() => {
    let lookup = getOwnMetadata(lookupMetadataKey$2, Array);
    if (lookup == null) {
        defineMetadata(lookupMetadataKey$2, lookup = new WeakMap(), Array);
    }
    return lookup;
})();
function sortCompare(x, y) {
    if (x === y) {
        return 0;
    }
    x = x === null ? 'null' : x.toString();
    y = y === null ? 'null' : y.toString();
    return x < y ? -1 : 1;
}
function preSortCompare(x, y) {
    if (x === void 0) {
        if (y === void 0) {
            return 0;
        }
        else {
            return 1;
        }
    }
    if (y === void 0) {
        return -1;
    }
    return 0;
}
function insertionSort(arr, indexMap, from, to, compareFn) {
    let velement, ielement, vtmp, itmp, order;
    let i, j;
    for (i = from + 1; i < to; i++) {
        velement = arr[i];
        ielement = indexMap[i];
        for (j = i - 1; j >= from; j--) {
            vtmp = arr[j];
            itmp = indexMap[j];
            order = compareFn(vtmp, velement);
            if (order > 0) {
                arr[j + 1] = vtmp;
                indexMap[j + 1] = itmp;
            }
            else {
                break;
            }
        }
        arr[j + 1] = velement;
        indexMap[j + 1] = ielement;
    }
}
function quickSort(arr, indexMap, from, to, compareFn) {
    let thirdIndex = 0, i = 0;
    let v0, v1, v2;
    let i0, i1, i2;
    let c01, c02, c12;
    let vtmp, itmp;
    let vpivot, ipivot, lowEnd, highStart;
    let velement, ielement, order, vtopElement;
    while (true) {
        if (to - from <= 10) {
            insertionSort(arr, indexMap, from, to, compareFn);
            return;
        }
        thirdIndex = from + ((to - from) >> 1);
        v0 = arr[from];
        i0 = indexMap[from];
        v1 = arr[to - 1];
        i1 = indexMap[to - 1];
        v2 = arr[thirdIndex];
        i2 = indexMap[thirdIndex];
        c01 = compareFn(v0, v1);
        if (c01 > 0) {
            vtmp = v0;
            itmp = i0;
            v0 = v1;
            i0 = i1;
            v1 = vtmp;
            i1 = itmp;
        }
        c02 = compareFn(v0, v2);
        if (c02 >= 0) {
            vtmp = v0;
            itmp = i0;
            v0 = v2;
            i0 = i2;
            v2 = v1;
            i2 = i1;
            v1 = vtmp;
            i1 = itmp;
        }
        else {
            c12 = compareFn(v1, v2);
            if (c12 > 0) {
                vtmp = v1;
                itmp = i1;
                v1 = v2;
                i1 = i2;
                v2 = vtmp;
                i2 = itmp;
            }
        }
        arr[from] = v0;
        indexMap[from] = i0;
        arr[to - 1] = v2;
        indexMap[to - 1] = i2;
        vpivot = v1;
        ipivot = i1;
        lowEnd = from + 1;
        highStart = to - 1;
        arr[thirdIndex] = arr[lowEnd];
        indexMap[thirdIndex] = indexMap[lowEnd];
        arr[lowEnd] = vpivot;
        indexMap[lowEnd] = ipivot;
        partition: for (i = lowEnd + 1; i < highStart; i++) {
            velement = arr[i];
            ielement = indexMap[i];
            order = compareFn(velement, vpivot);
            if (order < 0) {
                arr[i] = arr[lowEnd];
                indexMap[i] = indexMap[lowEnd];
                arr[lowEnd] = velement;
                indexMap[lowEnd] = ielement;
                lowEnd++;
            }
            else if (order > 0) {
                do {
                    highStart--;
                    if (highStart == i) {
                        break partition;
                    }
                    vtopElement = arr[highStart];
                    order = compareFn(vtopElement, vpivot);
                } while (order > 0);
                arr[i] = arr[highStart];
                indexMap[i] = indexMap[highStart];
                arr[highStart] = velement;
                indexMap[highStart] = ielement;
                if (order < 0) {
                    velement = arr[i];
                    ielement = indexMap[i];
                    arr[i] = arr[lowEnd];
                    indexMap[i] = indexMap[lowEnd];
                    arr[lowEnd] = velement;
                    indexMap[lowEnd] = ielement;
                    lowEnd++;
                }
            }
        }
        if (to - highStart < lowEnd - from) {
            quickSort(arr, indexMap, highStart, to, compareFn);
            to = lowEnd;
        }
        else {
            quickSort(arr, indexMap, from, lowEnd, compareFn);
            from = highStart;
        }
    }
}
const proto$2 = Array.prototype;
const $push = proto$2.push;
const $unshift = proto$2.unshift;
const $pop = proto$2.pop;
const $shift = proto$2.shift;
const $splice = proto$2.splice;
const $reverse = proto$2.reverse;
const $sort = proto$2.sort;
const native$2 = { push: $push, unshift: $unshift, pop: $pop, shift: $shift, splice: $splice, reverse: $reverse, sort: $sort };
const methods$2 = ['push', 'unshift', 'pop', 'shift', 'splice', 'reverse', 'sort'];
const observe$3 = {
    push: function (...args) {
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            return $push.apply(this, args);
        }
        const len = this.length;
        const argCount = args.length;
        if (argCount === 0) {
            return len;
        }
        this.length = o.indexMap.length = len + argCount;
        let i = len;
        while (i < this.length) {
            this[i] = args[i - len];
            o.indexMap[i] = -2;
            i++;
        }
        o.notify();
        return this.length;
    },
    unshift: function (...args) {
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            return $unshift.apply(this, args);
        }
        const argCount = args.length;
        const inserts = new Array(argCount);
        let i = 0;
        while (i < argCount) {
            inserts[i++] = -2;
        }
        $unshift.apply(o.indexMap, inserts);
        const len = $unshift.apply(this, args);
        o.notify();
        return len;
    },
    pop: function () {
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            return $pop.call(this);
        }
        const indexMap = o.indexMap;
        const element = $pop.call(this);
        const index = indexMap.length - 1;
        if (indexMap[index] > -1) {
            indexMap.deletedIndices.push(indexMap[index]);
            indexMap.deletedItems.push(element);
        }
        $pop.call(indexMap);
        o.notify();
        return element;
    },
    shift: function () {
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            return $shift.call(this);
        }
        const indexMap = o.indexMap;
        const element = $shift.call(this);
        if (indexMap[0] > -1) {
            indexMap.deletedIndices.push(indexMap[0]);
            indexMap.deletedItems.push(element);
        }
        $shift.call(indexMap);
        o.notify();
        return element;
    },
    splice: function (...args) {
        const start = args[0];
        const deleteCount = args[1];
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            return $splice.apply(this, args);
        }
        const len = this.length;
        const relativeStart = start | 0;
        const actualStart = relativeStart < 0 ? Math.max((len + relativeStart), 0) : Math.min(relativeStart, len);
        const indexMap = o.indexMap;
        const argCount = args.length;
        const actualDeleteCount = argCount === 0 ? 0 : argCount === 1 ? len - actualStart : deleteCount;
        if (actualDeleteCount > 0) {
            let i = actualStart;
            const to = i + actualDeleteCount;
            while (i < to) {
                if (indexMap[i] > -1) {
                    indexMap.deletedIndices.push(indexMap[i]);
                    indexMap.deletedItems.push(this[i]);
                }
                i++;
            }
        }
        if (argCount > 2) {
            const itemCount = argCount - 2;
            const inserts = new Array(itemCount);
            let i = 0;
            while (i < itemCount) {
                inserts[i++] = -2;
            }
            $splice.call(indexMap, start, deleteCount, ...inserts);
        }
        else {
            $splice.apply(indexMap, args);
        }
        const deleted = $splice.apply(this, args);
        o.notify();
        return deleted;
    },
    reverse: function () {
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            $reverse.call(this);
            return this;
        }
        const len = this.length;
        const middle = (len / 2) | 0;
        let lower = 0;
        while (lower !== middle) {
            const upper = len - lower - 1;
            const lowerValue = this[lower];
            const lowerIndex = o.indexMap[lower];
            const upperValue = this[upper];
            const upperIndex = o.indexMap[upper];
            this[lower] = upperValue;
            o.indexMap[lower] = upperIndex;
            this[upper] = lowerValue;
            o.indexMap[upper] = lowerIndex;
            lower++;
        }
        o.notify();
        return this;
    },
    sort: function (compareFn) {
        const o = observerLookup$2.get(this);
        if (o === void 0) {
            $sort.call(this, compareFn);
            return this;
        }
        let len = this.length;
        if (len < 2) {
            return this;
        }
        quickSort(this, o.indexMap, 0, len, preSortCompare);
        let i = 0;
        while (i < len) {
            if (this[i] === void 0) {
                break;
            }
            i++;
        }
        if (compareFn === void 0 || !isFunction(compareFn)) {
            compareFn = sortCompare;
        }
        quickSort(this, o.indexMap, 0, i, compareFn);
        let shouldNotify = false;
        for (i = 0, len = o.indexMap.length; len > i; ++i) {
            if (o.indexMap[i] !== i) {
                shouldNotify = true;
                break;
            }
        }
        if (shouldNotify) {
            o.notify();
        }
        return this;
    }
};
for (const method of methods$2) {
    def(observe$3[method], 'observing', { value: true, writable: false, configurable: false, enumerable: false });
}
let enableArrayObservationCalled = false;
const observationEnabledKey$2 = '__au_arr_on__';
function enableArrayObservation() {
    if (!(getOwnMetadata(observationEnabledKey$2, Array) ?? false)) {
        defineMetadata(observationEnabledKey$2, true, Array);
        for (const method of methods$2) {
            if (proto$2[method].observing !== true) {
                defineHiddenProp(proto$2, method, observe$3[method]);
            }
        }
    }
}
function disableArrayObservation() {
    for (const method of methods$2) {
        if (proto$2[method].observing === true) {
            defineHiddenProp(proto$2, method, native$2[method]);
        }
    }
}
class ArrayObserver {
    constructor(array) {
        this.type = 18;
        if (!enableArrayObservationCalled) {
            enableArrayObservationCalled = true;
            enableArrayObservation();
        }
        this.indexObservers = {};
        this.collection = array;
        this.indexMap = createIndexMap(array.length);
        this.lenObs = void 0;
        observerLookup$2.set(array, this);
    }
    notify() {
        const subs = this.subs;
        const indexMap = this.indexMap;
        if (batching) {
            addCollectionBatch(subs, this.collection, indexMap);
            return;
        }
        const arr = this.collection;
        const length = arr.length;
        this.indexMap = createIndexMap(length);
        this.subs.notifyCollection(arr, indexMap);
    }
    getLengthObserver() {
        return this.lenObs ?? (this.lenObs = new CollectionLengthObserver(this));
    }
    getIndexObserver(index) {
        var _a;
        return (_a = this.indexObservers)[index] ?? (_a[index] = new ArrayIndexObserver(this, index));
    }
}
class ArrayIndexObserver {
    constructor(owner, index) {
        this.owner = owner;
        this.index = index;
        this.doNotCache = true;
        this.value = this.getValue();
    }
    getValue() {
        return this.owner.collection[this.index];
    }
    setValue(newValue) {
        if (newValue === this.getValue()) {
            return;
        }
        const arrayObserver = this.owner;
        const index = this.index;
        const indexMap = arrayObserver.indexMap;
        if (indexMap[index] > -1) {
            indexMap.deletedIndices.push(indexMap[index]);
        }
        indexMap[index] = -2;
        arrayObserver.collection[index] = newValue;
        arrayObserver.notify();
    }
    handleCollectionChange(_arr, indexMap) {
        const index = this.index;
        const noChange = indexMap[index] === index;
        if (noChange) {
            return;
        }
        const prevValue = this.value;
        const currValue = this.value = this.getValue();
        if (prevValue !== currValue) {
            this.subs.notify(currValue, prevValue);
        }
    }
    subscribe(subscriber) {
        if (this.subs.add(subscriber) && this.subs.count === 1) {
            this.owner.subscribe(this);
        }
    }
    unsubscribe(subscriber) {
        if (this.subs.remove(subscriber) && this.subs.count === 0) {
            this.owner.unsubscribe(this);
        }
    }
}
subscriberCollection(ArrayObserver);
subscriberCollection(ArrayIndexObserver);
function getArrayObserver(array) {
    let observer = observerLookup$2.get(array);
    if (observer === void 0) {
        observer = new ArrayObserver(array);
    }
    return observer;
}
const compareNumber = (a, b) => a - b;
function applyMutationsToIndices(indexMap) {
    let offset = 0;
    let j = 0;
    let i = 0;
    const $indexMap = cloneIndexMap(indexMap);
    if ($indexMap.deletedIndices.length > 1) {
        $indexMap.deletedIndices.sort(compareNumber);
    }
    const len = $indexMap.length;
    for (; i < len; ++i) {
        while ($indexMap.deletedIndices[j] <= i - offset) {
            ++j;
            --offset;
        }
        if ($indexMap[i] === -2) {
            ++offset;
        }
        else {
            $indexMap[i] += offset;
        }
    }
    return $indexMap;
}
function synchronizeIndices(items, indexMap) {
    const copy = items.slice();
    const len = indexMap.length;
    let to = 0;
    let from = 0;
    while (to < len) {
        from = indexMap[to];
        if (from !== -2) {
            items[to] = copy[from];
        }
        ++to;
    }
}

const lookupMetadataKey$1 = '__au_set_obs__';
const observerLookup$1 = (() => {
    let lookup = getOwnMetadata(lookupMetadataKey$1, Set);
    if (lookup == null) {
        defineMetadata(lookupMetadataKey$1, lookup = new WeakMap(), Set);
    }
    return lookup;
})();
const proto$1 = Set.prototype;
const $add = proto$1.add;
const $clear$1 = proto$1.clear;
const $delete$1 = proto$1.delete;
const native$1 = { add: $add, clear: $clear$1, delete: $delete$1 };
const methods$1 = ['add', 'clear', 'delete'];
const observe$2 = {
    add: function (value) {
        const o = observerLookup$1.get(this);
        if (o === undefined) {
            $add.call(this, value);
            return this;
        }
        const oldSize = this.size;
        $add.call(this, value);
        const newSize = this.size;
        if (newSize === oldSize) {
            return this;
        }
        o.indexMap[oldSize] = -2;
        o.notify();
        return this;
    },
    clear: function () {
        const o = observerLookup$1.get(this);
        if (o === undefined) {
            return $clear$1.call(this);
        }
        const size = this.size;
        if (size > 0) {
            const indexMap = o.indexMap;
            let i = 0;
            for (const key of this.keys()) {
                if (indexMap[i] > -1) {
                    indexMap.deletedIndices.push(indexMap[i]);
                    indexMap.deletedItems.push(key);
                }
                i++;
            }
            $clear$1.call(this);
            indexMap.length = 0;
            o.notify();
        }
        return undefined;
    },
    delete: function (value) {
        const o = observerLookup$1.get(this);
        if (o === undefined) {
            return $delete$1.call(this, value);
        }
        const size = this.size;
        if (size === 0) {
            return false;
        }
        let i = 0;
        const indexMap = o.indexMap;
        for (const entry of this.keys()) {
            if (entry === value) {
                if (indexMap[i] > -1) {
                    indexMap.deletedIndices.push(indexMap[i]);
                    indexMap.deletedItems.push(entry);
                }
                indexMap.splice(i, 1);
                const deleteResult = $delete$1.call(this, value);
                if (deleteResult === true) {
                    o.notify();
                }
                return deleteResult;
            }
            i++;
        }
        return false;
    }
};
const descriptorProps$1 = {
    writable: true,
    enumerable: false,
    configurable: true
};
for (const method of methods$1) {
    def(observe$2[method], 'observing', { value: true, writable: false, configurable: false, enumerable: false });
}
let enableSetObservationCalled = false;
const observationEnabledKey$1 = '__au_set_on__';
function enableSetObservation() {
    if (!(getOwnMetadata(observationEnabledKey$1, Set) ?? false)) {
        defineMetadata(observationEnabledKey$1, true, Set);
        for (const method of methods$1) {
            if (proto$1[method].observing !== true) {
                def(proto$1, method, { ...descriptorProps$1, value: observe$2[method] });
            }
        }
    }
}
function disableSetObservation() {
    for (const method of methods$1) {
        if (proto$1[method].observing === true) {
            def(proto$1, method, { ...descriptorProps$1, value: native$1[method] });
        }
    }
}
class SetObserver {
    constructor(observedSet) {
        this.type = 34;
        if (!enableSetObservationCalled) {
            enableSetObservationCalled = true;
            enableSetObservation();
        }
        this.collection = observedSet;
        this.indexMap = createIndexMap(observedSet.size);
        this.lenObs = void 0;
        observerLookup$1.set(observedSet, this);
    }
    notify() {
        const subs = this.subs;
        const indexMap = this.indexMap;
        if (batching) {
            addCollectionBatch(subs, this.collection, indexMap);
            return;
        }
        const set = this.collection;
        const size = set.size;
        this.indexMap = createIndexMap(size);
        this.subs.notifyCollection(set, indexMap);
    }
    getLengthObserver() {
        return this.lenObs ?? (this.lenObs = new CollectionSizeObserver(this));
    }
}
subscriberCollection(SetObserver);
function getSetObserver(observedSet) {
    let observer = observerLookup$1.get(observedSet);
    if (observer === void 0) {
        observer = new SetObserver(observedSet);
    }
    return observer;
}

const lookupMetadataKey = '__au_map_obs__';
const observerLookup = (() => {
    let lookup = getOwnMetadata(lookupMetadataKey, Map);
    if (lookup == null) {
        defineMetadata(lookupMetadataKey, lookup = new WeakMap(), Map);
    }
    return lookup;
})();
const proto = Map.prototype;
const $set = proto.set;
const $clear = proto.clear;
const $delete = proto.delete;
const native = { set: $set, clear: $clear, delete: $delete };
const methods = ['set', 'clear', 'delete'];
const observe$1 = {
    set: function (key, value) {
        const o = observerLookup.get(this);
        if (o === undefined) {
            $set.call(this, key, value);
            return this;
        }
        const oldValue = this.get(key);
        const oldSize = this.size;
        $set.call(this, key, value);
        const newSize = this.size;
        if (newSize === oldSize) {
            let i = 0;
            for (const entry of this.entries()) {
                if (entry[0] === key) {
                    if (entry[1] !== oldValue) {
                        o.indexMap.deletedIndices.push(o.indexMap[i]);
                        o.indexMap.deletedItems.push(entry);
                        o.indexMap[i] = -2;
                        o.notify();
                    }
                    return this;
                }
                i++;
            }
            return this;
        }
        o.indexMap[oldSize] = -2;
        o.notify();
        return this;
    },
    clear: function () {
        const o = observerLookup.get(this);
        if (o === undefined) {
            return $clear.call(this);
        }
        const size = this.size;
        if (size > 0) {
            const indexMap = o.indexMap;
            let i = 0;
            for (const key of this.keys()) {
                if (indexMap[i] > -1) {
                    indexMap.deletedIndices.push(indexMap[i]);
                    indexMap.deletedItems.push(key);
                }
                i++;
            }
            $clear.call(this);
            indexMap.length = 0;
            o.notify();
        }
        return undefined;
    },
    delete: function (value) {
        const o = observerLookup.get(this);
        if (o === undefined) {
            return $delete.call(this, value);
        }
        const size = this.size;
        if (size === 0) {
            return false;
        }
        let i = 0;
        const indexMap = o.indexMap;
        for (const entry of this.keys()) {
            if (entry === value) {
                if (indexMap[i] > -1) {
                    indexMap.deletedIndices.push(indexMap[i]);
                    indexMap.deletedItems.push(entry);
                }
                indexMap.splice(i, 1);
                const deleteResult = $delete.call(this, value);
                if (deleteResult === true) {
                    o.notify();
                }
                return deleteResult;
            }
            ++i;
        }
        return false;
    }
};
const descriptorProps = {
    writable: true,
    enumerable: false,
    configurable: true
};
for (const method of methods) {
    def(observe$1[method], 'observing', { value: true, writable: false, configurable: false, enumerable: false });
}
let enableMapObservationCalled = false;
const observationEnabledKey = '__au_map_on__';
function enableMapObservation() {
    if (!(getOwnMetadata(observationEnabledKey, Map) ?? false)) {
        defineMetadata(observationEnabledKey, true, Map);
        for (const method of methods) {
            if (proto[method].observing !== true) {
                def(proto, method, { ...descriptorProps, value: observe$1[method] });
            }
        }
    }
}
function disableMapObservation() {
    for (const method of methods) {
        if (proto[method].observing === true) {
            def(proto, method, { ...descriptorProps, value: native[method] });
        }
    }
}
class MapObserver {
    constructor(map) {
        this.type = 66;
        if (!enableMapObservationCalled) {
            enableMapObservationCalled = true;
            enableMapObservation();
        }
        this.collection = map;
        this.indexMap = createIndexMap(map.size);
        this.lenObs = void 0;
        observerLookup.set(map, this);
    }
    notify() {
        const subs = this.subs;
        const indexMap = this.indexMap;
        if (batching) {
            addCollectionBatch(subs, this.collection, indexMap);
            return;
        }
        const map = this.collection;
        const size = map.size;
        this.indexMap = createIndexMap(size);
        subs.notifyCollection(map, indexMap);
    }
    getLengthObserver() {
        return this.lenObs ?? (this.lenObs = new CollectionSizeObserver(this));
    }
}
subscriberCollection(MapObserver);
function getMapObserver(map) {
    let observer = observerLookup.get(map);
    if (observer === void 0) {
        observer = new MapObserver(map);
    }
    return observer;
}

function observe(obj, key) {
    const observer = this.oL.getObserver(obj, key);
    this.obs.add(observer);
}
function getObserverRecord() {
    return defineHiddenProp(this, 'obs', new BindingObserverRecord(this));
}
function observeCollection(collection) {
    let obs;
    if (isArray(collection)) {
        obs = getArrayObserver(collection);
    }
    else if (collection instanceof Set) {
        obs = getSetObserver(collection);
    }
    else if (collection instanceof Map) {
        obs = getMapObserver(collection);
    }
    else {
        throw new Error(`AUR0210: Unrecognised collection type.`);
    }
    this.obs.add(obs);
}
function subscribeTo(subscribable) {
    this.obs.add(subscribable);
}
function noopHandleChange() {
    throw new Error(`AUR2011: method "handleChange" not implemented`);
}
function noopHandleCollectionChange() {
    throw new Error(`AUR2011: method "handleCollectionChange" not implemented`);
}
class BindingObserverRecord {
    constructor(b) {
        this.version = 0;
        this.count = 0;
        this.o = new Map();
        this.b = b;
    }
    handleChange(value, oldValue) {
        return this.b.interceptor.handleChange(value, oldValue);
    }
    handleCollectionChange(collection, indexMap) {
        this.b.interceptor.handleCollectionChange(collection, indexMap);
    }
    add(observer) {
        if (!this.o.has(observer)) {
            observer.subscribe(this);
            ++this.count;
        }
        this.o.set(observer, this.version);
    }
    clear() {
        this.o.forEach(unsubscribeStale, this);
        this.count = this.o.size;
    }
    clearAll() {
        this.o.forEach(unsubscribeAll, this);
        this.o.clear();
        this.count = 0;
    }
}
function unsubscribeAll(version, subscribable) {
    subscribable.unsubscribe(this);
}
function unsubscribeStale(version, subscribable) {
    if (this.version !== version) {
        subscribable.unsubscribe(this);
        this.o.delete(subscribable);
    }
}
function connectableDecorator(target) {
    const proto = target.prototype;
    ensureProto(proto, 'observe', observe);
    ensureProto(proto, 'observeCollection', observeCollection);
    ensureProto(proto, 'subscribeTo', subscribeTo);
    def(proto, 'obs', { get: getObserverRecord });
    ensureProto(proto, 'handleChange', noopHandleChange);
    ensureProto(proto, 'handleCollectionChange', noopHandleCollectionChange);
    return target;
}
function connectable(target) {
    return target == null ? connectableDecorator : connectableDecorator(target);
}

const IExpressionParser = DI.createInterface('IExpressionParser', x => x.singleton(ExpressionParser));
class ExpressionParser {
    constructor() {
        this._expressionLookup = createLookup();
        this._forOfLookup = createLookup();
        this._interpolationLookup = createLookup();
    }
    parse(expression, expressionType) {
        let found;
        switch (expressionType) {
            case 16:
                return new CustomExpression(expression);
            case 1:
                found = this._interpolationLookup[expression];
                if (found === void 0) {
                    found = this._interpolationLookup[expression] = this.$parse(expression, expressionType);
                }
                return found;
            case 2:
                found = this._forOfLookup[expression];
                if (found === void 0) {
                    found = this._forOfLookup[expression] = this.$parse(expression, expressionType);
                }
                return found;
            default: {
                if (expression.length === 0) {
                    if ((expressionType & (4 | 8)) > 0) {
                        return PrimitiveLiteralExpression.$empty;
                    }
                    throw invalidEmptyExpression();
                }
                found = this._expressionLookup[expression];
                if (found === void 0) {
                    found = this._expressionLookup[expression] = this.$parse(expression, expressionType);
                }
                return found;
            }
        }
    }
    $parse(expression, expressionType) {
        $input = expression;
        $index = 0;
        $length = expression.length;
        $scopeDepth = 0;
        $startIndex = 0;
        $currentToken = 6291456;
        $tokenValue = '';
        $currentChar = expression.charCodeAt(0);
        $assignable = true;
        $optional = false;
        return parse(61, expressionType === void 0 ? 8 : expressionType);
    }
}
var Char;
(function (Char) {
    Char[Char["Null"] = 0] = "Null";
    Char[Char["Backspace"] = 8] = "Backspace";
    Char[Char["Tab"] = 9] = "Tab";
    Char[Char["LineFeed"] = 10] = "LineFeed";
    Char[Char["VerticalTab"] = 11] = "VerticalTab";
    Char[Char["FormFeed"] = 12] = "FormFeed";
    Char[Char["CarriageReturn"] = 13] = "CarriageReturn";
    Char[Char["Space"] = 32] = "Space";
    Char[Char["Exclamation"] = 33] = "Exclamation";
    Char[Char["DoubleQuote"] = 34] = "DoubleQuote";
    Char[Char["Dollar"] = 36] = "Dollar";
    Char[Char["Percent"] = 37] = "Percent";
    Char[Char["Ampersand"] = 38] = "Ampersand";
    Char[Char["SingleQuote"] = 39] = "SingleQuote";
    Char[Char["OpenParen"] = 40] = "OpenParen";
    Char[Char["CloseParen"] = 41] = "CloseParen";
    Char[Char["Asterisk"] = 42] = "Asterisk";
    Char[Char["Plus"] = 43] = "Plus";
    Char[Char["Comma"] = 44] = "Comma";
    Char[Char["Minus"] = 45] = "Minus";
    Char[Char["Dot"] = 46] = "Dot";
    Char[Char["Slash"] = 47] = "Slash";
    Char[Char["Semicolon"] = 59] = "Semicolon";
    Char[Char["Backtick"] = 96] = "Backtick";
    Char[Char["OpenBracket"] = 91] = "OpenBracket";
    Char[Char["Backslash"] = 92] = "Backslash";
    Char[Char["CloseBracket"] = 93] = "CloseBracket";
    Char[Char["Caret"] = 94] = "Caret";
    Char[Char["Underscore"] = 95] = "Underscore";
    Char[Char["OpenBrace"] = 123] = "OpenBrace";
    Char[Char["Bar"] = 124] = "Bar";
    Char[Char["CloseBrace"] = 125] = "CloseBrace";
    Char[Char["Colon"] = 58] = "Colon";
    Char[Char["LessThan"] = 60] = "LessThan";
    Char[Char["Equals"] = 61] = "Equals";
    Char[Char["GreaterThan"] = 62] = "GreaterThan";
    Char[Char["Question"] = 63] = "Question";
    Char[Char["Zero"] = 48] = "Zero";
    Char[Char["One"] = 49] = "One";
    Char[Char["Two"] = 50] = "Two";
    Char[Char["Three"] = 51] = "Three";
    Char[Char["Four"] = 52] = "Four";
    Char[Char["Five"] = 53] = "Five";
    Char[Char["Six"] = 54] = "Six";
    Char[Char["Seven"] = 55] = "Seven";
    Char[Char["Eight"] = 56] = "Eight";
    Char[Char["Nine"] = 57] = "Nine";
    Char[Char["UpperA"] = 65] = "UpperA";
    Char[Char["UpperB"] = 66] = "UpperB";
    Char[Char["UpperC"] = 67] = "UpperC";
    Char[Char["UpperD"] = 68] = "UpperD";
    Char[Char["UpperE"] = 69] = "UpperE";
    Char[Char["UpperF"] = 70] = "UpperF";
    Char[Char["UpperG"] = 71] = "UpperG";
    Char[Char["UpperH"] = 72] = "UpperH";
    Char[Char["UpperI"] = 73] = "UpperI";
    Char[Char["UpperJ"] = 74] = "UpperJ";
    Char[Char["UpperK"] = 75] = "UpperK";
    Char[Char["UpperL"] = 76] = "UpperL";
    Char[Char["UpperM"] = 77] = "UpperM";
    Char[Char["UpperN"] = 78] = "UpperN";
    Char[Char["UpperO"] = 79] = "UpperO";
    Char[Char["UpperP"] = 80] = "UpperP";
    Char[Char["UpperQ"] = 81] = "UpperQ";
    Char[Char["UpperR"] = 82] = "UpperR";
    Char[Char["UpperS"] = 83] = "UpperS";
    Char[Char["UpperT"] = 84] = "UpperT";
    Char[Char["UpperU"] = 85] = "UpperU";
    Char[Char["UpperV"] = 86] = "UpperV";
    Char[Char["UpperW"] = 87] = "UpperW";
    Char[Char["UpperX"] = 88] = "UpperX";
    Char[Char["UpperY"] = 89] = "UpperY";
    Char[Char["UpperZ"] = 90] = "UpperZ";
    Char[Char["LowerA"] = 97] = "LowerA";
    Char[Char["LowerB"] = 98] = "LowerB";
    Char[Char["LowerC"] = 99] = "LowerC";
    Char[Char["LowerD"] = 100] = "LowerD";
    Char[Char["LowerE"] = 101] = "LowerE";
    Char[Char["LowerF"] = 102] = "LowerF";
    Char[Char["LowerG"] = 103] = "LowerG";
    Char[Char["LowerH"] = 104] = "LowerH";
    Char[Char["LowerI"] = 105] = "LowerI";
    Char[Char["LowerJ"] = 106] = "LowerJ";
    Char[Char["LowerK"] = 107] = "LowerK";
    Char[Char["LowerL"] = 108] = "LowerL";
    Char[Char["LowerM"] = 109] = "LowerM";
    Char[Char["LowerN"] = 110] = "LowerN";
    Char[Char["LowerO"] = 111] = "LowerO";
    Char[Char["LowerP"] = 112] = "LowerP";
    Char[Char["LowerQ"] = 113] = "LowerQ";
    Char[Char["LowerR"] = 114] = "LowerR";
    Char[Char["LowerS"] = 115] = "LowerS";
    Char[Char["LowerT"] = 116] = "LowerT";
    Char[Char["LowerU"] = 117] = "LowerU";
    Char[Char["LowerV"] = 118] = "LowerV";
    Char[Char["LowerW"] = 119] = "LowerW";
    Char[Char["LowerX"] = 120] = "LowerX";
    Char[Char["LowerY"] = 121] = "LowerY";
    Char[Char["LowerZ"] = 122] = "LowerZ";
})(Char || (Char = {}));
function unescapeCode(code) {
    switch (code) {
        case 98: return 8;
        case 116: return 9;
        case 110: return 10;
        case 118: return 11;
        case 102: return 12;
        case 114: return 13;
        case 34: return 34;
        case 39: return 39;
        case 92: return 92;
        default: return code;
    }
}
var Precedence;
(function (Precedence) {
    Precedence[Precedence["Variadic"] = 61] = "Variadic";
    Precedence[Precedence["Assign"] = 62] = "Assign";
    Precedence[Precedence["Conditional"] = 63] = "Conditional";
    Precedence[Precedence["NullishCoalescing"] = 128] = "NullishCoalescing";
    Precedence[Precedence["LogicalOR"] = 192] = "LogicalOR";
    Precedence[Precedence["LogicalAND"] = 256] = "LogicalAND";
    Precedence[Precedence["Equality"] = 320] = "Equality";
    Precedence[Precedence["Relational"] = 384] = "Relational";
    Precedence[Precedence["Additive"] = 448] = "Additive";
    Precedence[Precedence["Multiplicative"] = 512] = "Multiplicative";
    Precedence[Precedence["Binary"] = 513] = "Binary";
    Precedence[Precedence["LeftHandSide"] = 514] = "LeftHandSide";
    Precedence[Precedence["Primary"] = 515] = "Primary";
    Precedence[Precedence["Unary"] = 516] = "Unary";
})(Precedence || (Precedence = {}));
var Token;
(function (Token) {
    Token[Token["EOF"] = 6291456] = "EOF";
    Token[Token["ExpressionTerminal"] = 4194304] = "ExpressionTerminal";
    Token[Token["AccessScopeTerminal"] = 2097152] = "AccessScopeTerminal";
    Token[Token["ClosingToken"] = 1048576] = "ClosingToken";
    Token[Token["OpeningToken"] = 524288] = "OpeningToken";
    Token[Token["BinaryOp"] = 262144] = "BinaryOp";
    Token[Token["UnaryOp"] = 131072] = "UnaryOp";
    Token[Token["LeftHandSide"] = 65536] = "LeftHandSide";
    Token[Token["StringOrNumericLiteral"] = 49152] = "StringOrNumericLiteral";
    Token[Token["NumericLiteral"] = 32768] = "NumericLiteral";
    Token[Token["StringLiteral"] = 16384] = "StringLiteral";
    Token[Token["IdentifierName"] = 12288] = "IdentifierName";
    Token[Token["Keyword"] = 8192] = "Keyword";
    Token[Token["Identifier"] = 4096] = "Identifier";
    Token[Token["Contextual"] = 2048] = "Contextual";
    Token[Token["OptionalSuffix"] = 13312] = "OptionalSuffix";
    Token[Token["Precedence"] = 960] = "Precedence";
    Token[Token["Type"] = 63] = "Type";
    Token[Token["FalseKeyword"] = 8192] = "FalseKeyword";
    Token[Token["TrueKeyword"] = 8193] = "TrueKeyword";
    Token[Token["NullKeyword"] = 8194] = "NullKeyword";
    Token[Token["UndefinedKeyword"] = 8195] = "UndefinedKeyword";
    Token[Token["ThisScope"] = 12292] = "ThisScope";
    Token[Token["ParentScope"] = 12294] = "ParentScope";
    Token[Token["OpenParen"] = 2688007] = "OpenParen";
    Token[Token["OpenBrace"] = 524296] = "OpenBrace";
    Token[Token["Dot"] = 65545] = "Dot";
    Token[Token["DotDot"] = 10] = "DotDot";
    Token[Token["DotDotDot"] = 11] = "DotDotDot";
    Token[Token["QuestionDot"] = 2162700] = "QuestionDot";
    Token[Token["CloseBrace"] = 7340045] = "CloseBrace";
    Token[Token["CloseParen"] = 7340046] = "CloseParen";
    Token[Token["Comma"] = 6291471] = "Comma";
    Token[Token["OpenBracket"] = 2688016] = "OpenBracket";
    Token[Token["CloseBracket"] = 7340051] = "CloseBracket";
    Token[Token["Colon"] = 6291476] = "Colon";
    Token[Token["Question"] = 6291477] = "Question";
    Token[Token["Ampersand"] = 6291478] = "Ampersand";
    Token[Token["Bar"] = 6291479] = "Bar";
    Token[Token["QuestionQuestion"] = 6553752] = "QuestionQuestion";
    Token[Token["BarBar"] = 6553817] = "BarBar";
    Token[Token["AmpersandAmpersand"] = 6553882] = "AmpersandAmpersand";
    Token[Token["EqualsEquals"] = 6553947] = "EqualsEquals";
    Token[Token["ExclamationEquals"] = 6553948] = "ExclamationEquals";
    Token[Token["EqualsEqualsEquals"] = 6553949] = "EqualsEqualsEquals";
    Token[Token["ExclamationEqualsEquals"] = 6553950] = "ExclamationEqualsEquals";
    Token[Token["LessThan"] = 6554015] = "LessThan";
    Token[Token["GreaterThan"] = 6554016] = "GreaterThan";
    Token[Token["LessThanEquals"] = 6554017] = "LessThanEquals";
    Token[Token["GreaterThanEquals"] = 6554018] = "GreaterThanEquals";
    Token[Token["InKeyword"] = 6562211] = "InKeyword";
    Token[Token["InstanceOfKeyword"] = 6562212] = "InstanceOfKeyword";
    Token[Token["Plus"] = 2490853] = "Plus";
    Token[Token["Minus"] = 2490854] = "Minus";
    Token[Token["TypeofKeyword"] = 139303] = "TypeofKeyword";
    Token[Token["VoidKeyword"] = 139304] = "VoidKeyword";
    Token[Token["Asterisk"] = 6554153] = "Asterisk";
    Token[Token["Percent"] = 6554154] = "Percent";
    Token[Token["Slash"] = 6554155] = "Slash";
    Token[Token["Equals"] = 4194348] = "Equals";
    Token[Token["Exclamation"] = 131117] = "Exclamation";
    Token[Token["TemplateTail"] = 2163758] = "TemplateTail";
    Token[Token["TemplateContinuation"] = 2163759] = "TemplateContinuation";
    Token[Token["OfKeyword"] = 4204592] = "OfKeyword";
    Token[Token["Arrow"] = 49] = "Arrow";
})(Token || (Token = {}));
const $false = PrimitiveLiteralExpression.$false;
const $true = PrimitiveLiteralExpression.$true;
const $null = PrimitiveLiteralExpression.$null;
const $undefined = PrimitiveLiteralExpression.$undefined;
const $this = AccessThisExpression.$this;
const $parent = AccessThisExpression.$parent;
var ExpressionType;
(function (ExpressionType) {
    ExpressionType[ExpressionType["None"] = 0] = "None";
    ExpressionType[ExpressionType["Interpolation"] = 1] = "Interpolation";
    ExpressionType[ExpressionType["IsIterator"] = 2] = "IsIterator";
    ExpressionType[ExpressionType["IsFunction"] = 4] = "IsFunction";
    ExpressionType[ExpressionType["IsProperty"] = 8] = "IsProperty";
    ExpressionType[ExpressionType["IsCustom"] = 16] = "IsCustom";
})(ExpressionType || (ExpressionType = {}));
let $input = '';
let $index = 0;
let $length = 0;
let $scopeDepth = 0;
let $startIndex = 0;
let $currentToken = 6291456;
let $tokenValue = '';
let $currentChar;
let $assignable = true;
let $optional = false;
function $tokenRaw() {
    return $input.slice($startIndex, $index);
}
function parseExpression(input, expressionType) {
    $input = input;
    $index = 0;
    $length = input.length;
    $scopeDepth = 0;
    $startIndex = 0;
    $currentToken = 6291456;
    $tokenValue = '';
    $currentChar = input.charCodeAt(0);
    $assignable = true;
    $optional = false;
    return parse(61, expressionType === void 0 ? 8 : expressionType);
}
function parse(minPrecedence, expressionType) {
    if (expressionType === 16) {
        return new CustomExpression($input);
    }
    if ($index === 0) {
        if (expressionType & 1) {
            return parseInterpolation();
        }
        nextToken();
        if ($currentToken & 4194304) {
            throw invalidStartOfExpression();
        }
    }
    $assignable = 513 > minPrecedence;
    $optional = false;
    let optionalThisTail = false;
    let result = void 0;
    let ancestor = 0;
    if ($currentToken & 131072) {
        const op = TokenValues[$currentToken & 63];
        nextToken();
        result = new UnaryExpression(op, parse(514, expressionType));
        $assignable = false;
    }
    else {
        primary: switch ($currentToken) {
            case 12294:
                ancestor = $scopeDepth;
                $assignable = false;
                do {
                    nextToken();
                    ++ancestor;
                    switch ($currentToken) {
                        case 65545:
                            nextToken();
                            if (($currentToken & 12288) === 0) {
                                throw expectedIdentifier();
                            }
                            break;
                        case 10:
                        case 11:
                            throw expectedIdentifier();
                        case 2162700:
                            $optional = true;
                            nextToken();
                            if (($currentToken & 12288) === 0) {
                                result = ancestor === 0 ? $this : ancestor === 1 ? $parent : new AccessThisExpression(ancestor);
                                optionalThisTail = true;
                                break primary;
                            }
                            break;
                        default:
                            if ($currentToken & 2097152) {
                                result = ancestor === 0 ? $this : ancestor === 1 ? $parent : new AccessThisExpression(ancestor);
                                break primary;
                            }
                            throw invalidMemberExpression();
                    }
                } while ($currentToken === 12294);
            case 4096: {
                const id = $tokenValue;
                if (expressionType & 2) {
                    result = new BindingIdentifier(id);
                }
                else {
                    result = new AccessScopeExpression(id, ancestor);
                }
                $assignable = !$optional;
                nextToken();
                if (consumeOpt(49)) {
                    if ($currentToken === 524296) {
                        throw functionBodyInArrowFN();
                    }
                    const _optional = $optional;
                    const _scopeDepth = $scopeDepth;
                    ++$scopeDepth;
                    const body = parse(62, 0);
                    $optional = _optional;
                    $scopeDepth = _scopeDepth;
                    $assignable = false;
                    result = new ArrowFunction([new BindingIdentifier(id)], body);
                }
                break;
            }
            case 10:
                throw unexpectedDoubleDot();
            case 11:
                throw invalidSpreadOp();
            case 12292:
                $assignable = false;
                nextToken();
                switch ($scopeDepth) {
                    case 0:
                        result = $this;
                        break;
                    case 1:
                        result = $parent;
                        break;
                    default:
                        result = new AccessThisExpression($scopeDepth);
                        break;
                }
                break;
            case 2688007:
                result = parseCoverParenthesizedExpressionAndArrowParameterList(expressionType);
                break;
            case 2688016:
                result = $input.search(/\s+of\s+/) > $index ? parseArrayDestructuring() : parseArrayLiteralExpression(expressionType);
                break;
            case 524296:
                result = parseObjectLiteralExpression(expressionType);
                break;
            case 2163758:
                result = new TemplateExpression([$tokenValue]);
                $assignable = false;
                nextToken();
                break;
            case 2163759:
                result = parseTemplate(expressionType, result, false);
                break;
            case 16384:
            case 32768:
                result = new PrimitiveLiteralExpression($tokenValue);
                $assignable = false;
                nextToken();
                break;
            case 8194:
            case 8195:
            case 8193:
            case 8192:
                result = TokenValues[$currentToken & 63];
                $assignable = false;
                nextToken();
                break;
            default:
                if ($index >= $length) {
                    throw unexpectedEndOfExpression();
                }
                else {
                    throw unconsumedToken();
                }
        }
        if (expressionType & 2) {
            return parseForOfStatement(result);
        }
        if (514 < minPrecedence) {
            return result;
        }
        if ($currentToken === 10 || $currentToken === 11) {
            throw expectedIdentifier();
        }
        if (result.$kind === 0) {
            switch ($currentToken) {
                case 2162700:
                    $optional = true;
                    $assignable = false;
                    nextToken();
                    if (($currentToken & 13312) === 0) {
                        throw unexpectedTokenInOptionalChain();
                    }
                    if ($currentToken & 12288) {
                        result = new AccessScopeExpression($tokenValue, result.ancestor);
                        nextToken();
                    }
                    else if ($currentToken === 2688007) {
                        result = new CallFunctionExpression(result, parseArguments(), true);
                    }
                    else if ($currentToken === 2688016) {
                        result = parseKeyedExpression(result, true);
                    }
                    else {
                        throw invalidTaggedTemplateOnOptionalChain();
                    }
                    break;
                case 65545:
                    $assignable = !$optional;
                    nextToken();
                    if (($currentToken & 12288) === 0) {
                        throw expectedIdentifier();
                    }
                    result = new AccessScopeExpression($tokenValue, result.ancestor);
                    nextToken();
                    break;
                case 10:
                case 11:
                    throw expectedIdentifier();
                case 2688007:
                    result = new CallFunctionExpression(result, parseArguments(), optionalThisTail);
                    break;
                case 2688016:
                    result = parseKeyedExpression(result, optionalThisTail);
                    break;
                case 2163758:
                    result = createTemplateTail(result);
                    break;
                case 2163759:
                    result = parseTemplate(expressionType, result, true);
                    break;
            }
        }
        while (($currentToken & 65536) > 0) {
            switch ($currentToken) {
                case 2162700:
                    result = parseOptionalChainLHS(result);
                    break;
                case 65545:
                    nextToken();
                    if (($currentToken & 12288) === 0) {
                        throw expectedIdentifier();
                    }
                    result = parseMemberExpressionLHS(result, false);
                    break;
                case 10:
                case 11:
                    throw expectedIdentifier();
                case 2688007:
                    if (result.$kind === 1) {
                        result = new CallScopeExpression(result.name, parseArguments(), result.ancestor, false);
                    }
                    else if (result.$kind === 10) {
                        result = new CallMemberExpression(result.object, result.name, parseArguments(), result.optional, false);
                    }
                    else {
                        result = new CallFunctionExpression(result, parseArguments(), false);
                    }
                    break;
                case 2688016:
                    result = parseKeyedExpression(result, false);
                    break;
                case 2163758:
                    if ($optional) {
                        throw invalidTaggedTemplateOnOptionalChain();
                    }
                    result = createTemplateTail(result);
                    break;
                case 2163759:
                    if ($optional) {
                        throw invalidTaggedTemplateOnOptionalChain();
                    }
                    result = parseTemplate(expressionType, result, true);
                    break;
            }
        }
    }
    if ($currentToken === 10 || $currentToken === 11) {
        throw expectedIdentifier();
    }
    if (513 < minPrecedence) {
        return result;
    }
    while (($currentToken & 262144) > 0) {
        const opToken = $currentToken;
        if ((opToken & 960) <= minPrecedence) {
            break;
        }
        nextToken();
        result = new BinaryExpression(TokenValues[opToken & 63], result, parse(opToken & 960, expressionType));
        $assignable = false;
    }
    if (63 < minPrecedence) {
        return result;
    }
    if (consumeOpt(6291477)) {
        const yes = parse(62, expressionType);
        consume(6291476);
        result = new ConditionalExpression(result, yes, parse(62, expressionType));
        $assignable = false;
    }
    if (62 < minPrecedence) {
        return result;
    }
    if (consumeOpt(4194348)) {
        if (!$assignable) {
            throw lhsNotAssignable();
        }
        result = new AssignExpression(result, parse(62, expressionType));
    }
    if (61 < minPrecedence) {
        return result;
    }
    while (consumeOpt(6291479)) {
        if ($currentToken === 6291456) {
            throw expectedValueConverterIdentifier();
        }
        const name = $tokenValue;
        nextToken();
        const args = new Array();
        while (consumeOpt(6291476)) {
            args.push(parse(62, expressionType));
        }
        result = new ValueConverterExpression(result, name, args);
    }
    while (consumeOpt(6291478)) {
        if ($currentToken === 6291456) {
            throw expectedBindingBehaviorIdentifier();
        }
        const name = $tokenValue;
        nextToken();
        const args = new Array();
        while (consumeOpt(6291476)) {
            args.push(parse(62, expressionType));
        }
        result = new BindingBehaviorExpression(result, name, args);
    }
    if ($currentToken !== 6291456) {
        if ((expressionType & 1) > 0 && $currentToken === 7340045) {
            return result;
        }
        if ($tokenRaw() === 'of') {
            throw unexpectedOfKeyword();
        }
        throw unconsumedToken();
    }
    return result;
}
function parseArrayDestructuring() {
    const items = [];
    const dae = new DestructuringAssignmentExpression(24, items, void 0, void 0);
    let target = '';
    let $continue = true;
    let index = 0;
    while ($continue) {
        nextToken();
        switch ($currentToken) {
            case 7340051:
                $continue = false;
                addItem();
                break;
            case 6291471:
                addItem();
                break;
            case 4096:
                target = $tokenRaw();
                break;
            default:
                throw unexpectedTokenInDestructuring();
        }
    }
    consume(7340051);
    return dae;
    function addItem() {
        if (target !== '') {
            items.push(new DestructuringAssignmentSingleExpression(new AccessMemberExpression($this, target), new AccessKeyedExpression($this, new PrimitiveLiteralExpression(index++)), void 0));
            target = '';
        }
        else {
            index++;
        }
    }
}
function parseArguments() {
    const _optional = $optional;
    nextToken();
    const args = [];
    while ($currentToken !== 7340046) {
        args.push(parse(62, 0));
        if (!consumeOpt(6291471)) {
            break;
        }
    }
    consume(7340046);
    $assignable = false;
    $optional = _optional;
    return args;
}
function parseKeyedExpression(result, optional) {
    const _optional = $optional;
    nextToken();
    result = new AccessKeyedExpression(result, parse(62, 0), optional);
    consume(7340051);
    $assignable = !_optional;
    $optional = _optional;
    return result;
}
function parseOptionalChainLHS(lhs) {
    $optional = true;
    $assignable = false;
    nextToken();
    if (($currentToken & 13312) === 0) {
        throw unexpectedTokenInOptionalChain();
    }
    if ($currentToken & 12288) {
        return parseMemberExpressionLHS(lhs, true);
    }
    if ($currentToken === 2688007) {
        if (lhs.$kind === 1) {
            return new CallScopeExpression(lhs.name, parseArguments(), lhs.ancestor, true);
        }
        else if (lhs.$kind === 10) {
            return new CallMemberExpression(lhs.object, lhs.name, parseArguments(), lhs.optional, true);
        }
        else {
            return new CallFunctionExpression(lhs, parseArguments(), true);
        }
    }
    if ($currentToken === 2688016) {
        return parseKeyedExpression(lhs, true);
    }
    throw invalidTaggedTemplateOnOptionalChain();
}
function parseMemberExpressionLHS(lhs, optional) {
    const rhs = $tokenValue;
    switch ($currentToken) {
        case 2162700: {
            $optional = true;
            $assignable = false;
            const indexSave = $index;
            const startIndexSave = $startIndex;
            const currentTokenSave = $currentToken;
            const currentCharSave = $currentChar;
            const tokenValueSave = $tokenValue;
            const assignableSave = $assignable;
            const optionalSave = $optional;
            nextToken();
            if (($currentToken & 13312) === 0) {
                throw unexpectedTokenInOptionalChain();
            }
            if ($currentToken === 2688007) {
                return new CallMemberExpression(lhs, rhs, parseArguments(), optional, true);
            }
            $index = indexSave;
            $startIndex = startIndexSave;
            $currentToken = currentTokenSave;
            $currentChar = currentCharSave;
            $tokenValue = tokenValueSave;
            $assignable = assignableSave;
            $optional = optionalSave;
            return new AccessMemberExpression(lhs, rhs, optional);
        }
        case 2688007: {
            $assignable = false;
            return new CallMemberExpression(lhs, rhs, parseArguments(), optional, false);
        }
        default: {
            $assignable = !$optional;
            nextToken();
            return new AccessMemberExpression(lhs, rhs, optional);
        }
    }
}
var ArrowFnParams;
(function (ArrowFnParams) {
    ArrowFnParams[ArrowFnParams["Valid"] = 1] = "Valid";
    ArrowFnParams[ArrowFnParams["Invalid"] = 2] = "Invalid";
    ArrowFnParams[ArrowFnParams["Default"] = 3] = "Default";
    ArrowFnParams[ArrowFnParams["Destructuring"] = 4] = "Destructuring";
})(ArrowFnParams || (ArrowFnParams = {}));
function parseCoverParenthesizedExpressionAndArrowParameterList(expressionType) {
    nextToken();
    const indexSave = $index;
    const startIndexSave = $startIndex;
    const currentTokenSave = $currentToken;
    const currentCharSave = $currentChar;
    const tokenValueSave = $tokenValue;
    const assignableSave = $assignable;
    const optionalSave = $optional;
    const arrowParams = [];
    let paramsState = 1;
    let isParamList = false;
    loop: while (true) {
        if ($currentToken === 11) {
            nextToken();
            if ($currentToken !== 4096) {
                throw expectedIdentifier();
            }
            arrowParams.push(new BindingIdentifier($tokenValue));
            nextToken();
            if ($currentToken === 6291471) {
                throw restParamsMustBeLastParam();
            }
            if ($currentToken !== 7340046) {
                throw invalidSpreadOp();
            }
            nextToken();
            if ($currentToken !== 49) {
                throw invalidSpreadOp();
            }
            nextToken();
            const _optional = $optional;
            const _scopeDepth = $scopeDepth;
            ++$scopeDepth;
            const body = parse(62, 0);
            $optional = _optional;
            $scopeDepth = _scopeDepth;
            $assignable = false;
            return new ArrowFunction(arrowParams, body, true);
        }
        switch ($currentToken) {
            case 4096:
                arrowParams.push(new BindingIdentifier($tokenValue));
                nextToken();
                break;
            case 7340046:
                nextToken();
                break loop;
            case 524296:
            case 2688016:
                nextToken();
                paramsState = 4;
                break;
            case 6291471:
                paramsState = 2;
                isParamList = true;
                break loop;
            case 2688007:
                paramsState = 2;
                break loop;
            default:
                nextToken();
                paramsState = 2;
                break;
        }
        switch ($currentToken) {
            case 6291471:
                nextToken();
                isParamList = true;
                if (paramsState === 1) {
                    break;
                }
                break loop;
            case 7340046:
                nextToken();
                break loop;
            case 4194348:
                if (paramsState === 1) {
                    paramsState = 3;
                }
                break loop;
            case 49:
                if (isParamList) {
                    throw invalidArrowParameterList();
                }
                nextToken();
                paramsState = 2;
                break loop;
            default:
                if (paramsState === 1) {
                    paramsState = 2;
                }
                break loop;
        }
    }
    if ($currentToken === 49) {
        if (paramsState === 1) {
            nextToken();
            if ($currentToken === 524296) {
                throw functionBodyInArrowFN();
            }
            const _optional = $optional;
            const _scopeDepth = $scopeDepth;
            ++$scopeDepth;
            const body = parse(62, 0);
            $optional = _optional;
            $scopeDepth = _scopeDepth;
            $assignable = false;
            return new ArrowFunction(arrowParams, body);
        }
        throw invalidArrowParameterList();
    }
    else if (paramsState === 1 && arrowParams.length === 0) {
        throw missingExpectedToken(49);
    }
    if (isParamList) {
        switch (paramsState) {
            case 2:
                throw invalidArrowParameterList();
            case 3:
                throw defaultParamsInArrowFn();
            case 4:
                throw destructuringParamsInArrowFn();
        }
    }
    $index = indexSave;
    $startIndex = startIndexSave;
    $currentToken = currentTokenSave;
    $currentChar = currentCharSave;
    $tokenValue = tokenValueSave;
    $assignable = assignableSave;
    $optional = optionalSave;
    const _optional = $optional;
    const expr = parse(62, expressionType);
    $optional = _optional;
    consume(7340046);
    if ($currentToken === 49) {
        switch (paramsState) {
            case 2:
                throw invalidArrowParameterList();
            case 3:
                throw defaultParamsInArrowFn();
            case 4:
                throw destructuringParamsInArrowFn();
        }
    }
    return expr;
}
function parseArrayLiteralExpression(expressionType) {
    const _optional = $optional;
    nextToken();
    const elements = new Array();
    while ($currentToken !== 7340051) {
        if (consumeOpt(6291471)) {
            elements.push($undefined);
            if ($currentToken === 7340051) {
                break;
            }
        }
        else {
            elements.push(parse(62, expressionType & ~2));
            if (consumeOpt(6291471)) {
                if ($currentToken === 7340051) {
                    break;
                }
            }
            else {
                break;
            }
        }
    }
    $optional = _optional;
    consume(7340051);
    if (expressionType & 2) {
        return new ArrayBindingPattern(elements);
    }
    else {
        $assignable = false;
        return new ArrayLiteralExpression(elements);
    }
}
function parseForOfStatement(result) {
    if ((result.$kind & (19
        | 20
        | 21)) === 0) {
        throw invalidLHSBindingIdentifierInForOf();
    }
    if ($currentToken !== 4204592) {
        throw invalidLHSBindingIdentifierInForOf();
    }
    nextToken();
    const declaration = result;
    const statement = parse(61, 0);
    return new ForOfStatement(declaration, statement);
}
function parseObjectLiteralExpression(expressionType) {
    const _optional = $optional;
    const keys = new Array();
    const values = new Array();
    nextToken();
    while ($currentToken !== 7340045) {
        keys.push($tokenValue);
        if ($currentToken & 49152) {
            nextToken();
            consume(6291476);
            values.push(parse(62, expressionType & ~2));
        }
        else if ($currentToken & 12288) {
            const currentChar = $currentChar;
            const currentToken = $currentToken;
            const index = $index;
            nextToken();
            if (consumeOpt(6291476)) {
                values.push(parse(62, expressionType & ~2));
            }
            else {
                $currentChar = currentChar;
                $currentToken = currentToken;
                $index = index;
                values.push(parse(515, expressionType & ~2));
            }
        }
        else {
            throw invalidPropDefInObjLiteral();
        }
        if ($currentToken !== 7340045) {
            consume(6291471);
        }
    }
    $optional = _optional;
    consume(7340045);
    if (expressionType & 2) {
        return new ObjectBindingPattern(keys, values);
    }
    else {
        $assignable = false;
        return new ObjectLiteralExpression(keys, values);
    }
}
function parseInterpolation() {
    const parts = [];
    const expressions = [];
    const length = $length;
    let result = '';
    while ($index < length) {
        switch ($currentChar) {
            case 36:
                if ($input.charCodeAt($index + 1) === 123) {
                    parts.push(result);
                    result = '';
                    $index += 2;
                    $currentChar = $input.charCodeAt($index);
                    nextToken();
                    const expression = parse(61, 1);
                    expressions.push(expression);
                    continue;
                }
                else {
                    result += '$';
                }
                break;
            case 92:
                result += String.fromCharCode(unescapeCode(nextChar()));
                break;
            default:
                result += String.fromCharCode($currentChar);
        }
        nextChar();
    }
    if (expressions.length) {
        parts.push(result);
        return new Interpolation(parts, expressions);
    }
    return null;
}
function parseTemplate(expressionType, result, tagged) {
    const _optional = $optional;
    const cooked = [$tokenValue];
    consume(2163759);
    const expressions = [parse(62, expressionType)];
    while (($currentToken = scanTemplateTail()) !== 2163758) {
        cooked.push($tokenValue);
        consume(2163759);
        expressions.push(parse(62, expressionType));
    }
    cooked.push($tokenValue);
    $assignable = false;
    $optional = _optional;
    if (tagged) {
        nextToken();
        return new TaggedTemplateExpression(cooked, cooked, result, expressions);
    }
    else {
        nextToken();
        return new TemplateExpression(cooked, expressions);
    }
}
function createTemplateTail(result) {
    $assignable = false;
    const strings = [$tokenValue];
    nextToken();
    return new TaggedTemplateExpression(strings, strings, result);
}
function nextToken() {
    while ($index < $length) {
        $startIndex = $index;
        if (($currentToken = (CharScanners[$currentChar]())) != null) {
            return;
        }
    }
    $currentToken = 6291456;
}
function nextChar() {
    return $currentChar = $input.charCodeAt(++$index);
}
function scanIdentifier() {
    while (IdParts[nextChar()])
        ;
    const token = KeywordLookup[$tokenValue = $tokenRaw()];
    return token === undefined ? 4096 : token;
}
function scanNumber(isFloat) {
    let char = $currentChar;
    if (isFloat === false) {
        do {
            char = nextChar();
        } while (char <= 57 && char >= 48);
        if (char !== 46) {
            $tokenValue = parseInt($tokenRaw(), 10);
            return 32768;
        }
        char = nextChar();
        if ($index >= $length) {
            $tokenValue = parseInt($tokenRaw().slice(0, -1), 10);
            return 32768;
        }
    }
    if (char <= 57 && char >= 48) {
        do {
            char = nextChar();
        } while (char <= 57 && char >= 48);
    }
    else {
        $currentChar = $input.charCodeAt(--$index);
    }
    $tokenValue = parseFloat($tokenRaw());
    return 32768;
}
function scanString() {
    const quote = $currentChar;
    nextChar();
    let unescaped = 0;
    const buffer = new Array();
    let marker = $index;
    while ($currentChar !== quote) {
        if ($currentChar === 92) {
            buffer.push($input.slice(marker, $index));
            nextChar();
            unescaped = unescapeCode($currentChar);
            nextChar();
            buffer.push(String.fromCharCode(unescaped));
            marker = $index;
        }
        else if ($index >= $length) {
            throw unterminatedStringLiteral();
        }
        else {
            nextChar();
        }
    }
    const last = $input.slice(marker, $index);
    nextChar();
    buffer.push(last);
    const unescapedStr = buffer.join('');
    $tokenValue = unescapedStr;
    return 16384;
}
function scanTemplate() {
    let tail = true;
    let result = '';
    while (nextChar() !== 96) {
        if ($currentChar === 36) {
            if (($index + 1) < $length && $input.charCodeAt($index + 1) === 123) {
                $index++;
                tail = false;
                break;
            }
            else {
                result += '$';
            }
        }
        else if ($currentChar === 92) {
            result += String.fromCharCode(unescapeCode(nextChar()));
        }
        else {
            if ($index >= $length) {
                throw unterminatedTemplateLiteral();
            }
            result += String.fromCharCode($currentChar);
        }
    }
    nextChar();
    $tokenValue = result;
    if (tail) {
        return 2163758;
    }
    return 2163759;
}
function scanTemplateTail() {
    if ($index >= $length) {
        throw unterminatedTemplateLiteral();
    }
    $index--;
    return scanTemplate();
}
function consumeOpt(token) {
    if ($currentToken === token) {
        nextToken();
        return true;
    }
    return false;
}
function consume(token) {
    if ($currentToken === token) {
        nextToken();
    }
    else {
        throw missingExpectedToken(token);
    }
}
function invalidStartOfExpression() {
    {
        return new Error(`AUR0151: Invalid start of expression: '${$input}'`);
    }
}
function invalidSpreadOp() {
    {
        return new Error(`AUR0152: Spread operator is not supported: '${$input}'`);
    }
}
function expectedIdentifier() {
    {
        return new Error(`AUR0153: Expected identifier: '${$input}'`);
    }
}
function invalidMemberExpression() {
    {
        return new Error(`AUR0154: Invalid member expression: '${$input}'`);
    }
}
function unexpectedEndOfExpression() {
    {
        return new Error(`AUR0155: Unexpected end of expression: '${$input}'`);
    }
}
function unconsumedToken() {
    {
        return new Error(`AUR0156: Unconsumed token: '${$tokenRaw()}' at position ${$index} of '${$input}'`);
    }
}
function invalidEmptyExpression() {
    {
        return new Error(`AUR0157: Invalid expression. Empty expression is only valid in event bindings (trigger, delegate, capture etc...)`);
    }
}
function lhsNotAssignable() {
    {
        return new Error(`AUR0158: Left hand side of expression is not assignable: '${$input}'`);
    }
}
function expectedValueConverterIdentifier() {
    {
        return new Error(`AUR0159: Expected identifier to come after ValueConverter operator: '${$input}'`);
    }
}
function expectedBindingBehaviorIdentifier() {
    {
        return new Error(`AUR0160: Expected identifier to come after BindingBehavior operator: '${$input}'`);
    }
}
function unexpectedOfKeyword() {
    {
        return new Error(`AUR0161: Unexpected keyword "of": '${$input}'`);
    }
}
function invalidLHSBindingIdentifierInForOf() {
    {
        return new Error(`AUR0163: Invalid BindingIdentifier at left hand side of "of": '${$input}'`);
    }
}
function invalidPropDefInObjLiteral() {
    {
        return new Error(`AUR0164: Invalid or unsupported property definition in object literal: '${$input}'`);
    }
}
function unterminatedStringLiteral() {
    {
        return new Error(`AUR0165: Unterminated quote in string literal: '${$input}'`);
    }
}
function unterminatedTemplateLiteral() {
    {
        return new Error(`AUR0166: Unterminated template string: '${$input}'`);
    }
}
function missingExpectedToken(token) {
    {
        return new Error(`AUR0167: Missing expected token '${TokenValues[token & 63]}' in '${$input}' `);
    }
}
const unexpectedCharacter = () => {
    {
        throw new Error(`AUR0168: Unexpected character: '${$input}'`);
    }
};
unexpectedCharacter.notMapped = true;
function unexpectedTokenInDestructuring() {
    {
        return new Error(`AUR0170: Unexpected '${$tokenRaw()}' at position ${$index - 1} for destructuring assignment in ${$input}`);
    }
}
function unexpectedTokenInOptionalChain() {
    {
        return new Error(`AUR0171: Unexpected '${$tokenRaw()}' at position ${$index - 1} for optional chain in ${$input}`);
    }
}
function invalidTaggedTemplateOnOptionalChain() {
    {
        return new Error(`AUR0172: Invalid tagged template on optional chain in ${$input}`);
    }
}
function invalidArrowParameterList() {
    {
        return new Error(`AUR0173: Invalid arrow parameter list in ${$input}`);
    }
}
function defaultParamsInArrowFn() {
    {
        return new Error(`AUR0174: Arrow function with default parameters is not supported: ${$input}`);
    }
}
function destructuringParamsInArrowFn() {
    {
        return new Error(`AUR0175: Arrow function with destructuring parameters is not supported: ${$input}`);
    }
}
function restParamsMustBeLastParam() {
    {
        return new Error(`AUR0176: Rest parameter must be last formal parameter in arrow function: ${$input}`);
    }
}
function functionBodyInArrowFN() {
    {
        return new Error(`AUR0178: Arrow function with function body is not supported: ${$input}`);
    }
}
function unexpectedDoubleDot() {
    {
        return new Error(`AUR0179: Unexpected token '.' at position ${$index - 1} in ${$input}`);
    }
}
const TokenValues = [
    $false, $true, $null, $undefined, '$this', null, '$parent',
    '(', '{', '.', '..', '...', '?.', '}', ')', ',', '[', ']', ':', '?', '\'', '"',
    '&', '|', '??', '||', '&&', '==', '!=', '===', '!==', '<', '>',
    '<=', '>=', 'in', 'instanceof', '+', '-', 'typeof', 'void', '*', '%', '/', '=', '!',
    2163758, 2163759,
    'of', '=>'
];
const KeywordLookup = Object.assign(Object.create(null), {
    true: 8193,
    null: 8194,
    false: 8192,
    undefined: 8195,
    $this: 12292,
    $parent: 12294,
    in: 6562211,
    instanceof: 6562212,
    typeof: 139303,
    void: 139304,
    of: 4204592,
});
const codes = {
    AsciiIdPart: [0x24, 0, 0x30, 0x3A, 0x41, 0x5B, 0x5F, 0, 0x61, 0x7B],
    IdStart: [0x24, 0, 0x41, 0x5B, 0x5F, 0, 0x61, 0x7B, 0xAA, 0, 0xBA, 0, 0xC0, 0xD7, 0xD8, 0xF7, 0xF8, 0x2B9, 0x2E0, 0x2E5, 0x1D00, 0x1D26, 0x1D2C, 0x1D5D, 0x1D62, 0x1D66, 0x1D6B, 0x1D78, 0x1D79, 0x1DBF, 0x1E00, 0x1F00, 0x2071, 0, 0x207F, 0, 0x2090, 0x209D, 0x212A, 0x212C, 0x2132, 0, 0x214E, 0, 0x2160, 0x2189, 0x2C60, 0x2C80, 0xA722, 0xA788, 0xA78B, 0xA7AF, 0xA7B0, 0xA7B8, 0xA7F7, 0xA800, 0xAB30, 0xAB5B, 0xAB5C, 0xAB65, 0xFB00, 0xFB07, 0xFF21, 0xFF3B, 0xFF41, 0xFF5B],
    Digit: [0x30, 0x3A],
    Skip: [0, 0x21, 0x7F, 0xA1]
};
function decompress(lookup, $set, compressed, value) {
    const rangeCount = compressed.length;
    for (let i = 0; i < rangeCount; i += 2) {
        const start = compressed[i];
        let end = compressed[i + 1];
        end = end > 0 ? end : start + 1;
        if (lookup) {
            lookup.fill(value, start, end);
        }
        if ($set) {
            for (let ch = start; ch < end; ch++) {
                $set.add(ch);
            }
        }
    }
}
function returnToken(token) {
    return () => {
        nextChar();
        return token;
    };
}
const AsciiIdParts = new Set();
decompress(null, AsciiIdParts, codes.AsciiIdPart, true);
const IdParts = new Uint8Array(0xFFFF);
decompress(IdParts, null, codes.IdStart, 1);
decompress(IdParts, null, codes.Digit, 1);
const CharScanners = new Array(0xFFFF);
CharScanners.fill(unexpectedCharacter, 0, 0xFFFF);
decompress(CharScanners, null, codes.Skip, () => {
    nextChar();
    return null;
});
decompress(CharScanners, null, codes.IdStart, scanIdentifier);
decompress(CharScanners, null, codes.Digit, () => scanNumber(false));
CharScanners[34] =
    CharScanners[39] = () => {
        return scanString();
    };
CharScanners[96] = () => {
    return scanTemplate();
};
CharScanners[33] = () => {
    if (nextChar() !== 61) {
        return 131117;
    }
    if (nextChar() !== 61) {
        return 6553948;
    }
    nextChar();
    return 6553950;
};
CharScanners[61] = () => {
    if (nextChar() === 62) {
        nextChar();
        return 49;
    }
    if ($currentChar !== 61) {
        return 4194348;
    }
    if (nextChar() !== 61) {
        return 6553947;
    }
    nextChar();
    return 6553949;
};
CharScanners[38] = () => {
    if (nextChar() !== 38) {
        return 6291478;
    }
    nextChar();
    return 6553882;
};
CharScanners[124] = () => {
    if (nextChar() !== 124) {
        return 6291479;
    }
    nextChar();
    return 6553817;
};
CharScanners[63] = () => {
    if (nextChar() === 46) {
        const peek = $input.charCodeAt($index + 1);
        if (peek <= 48 || peek >= 57) {
            nextChar();
            return 2162700;
        }
        return 6291477;
    }
    if ($currentChar !== 63) {
        return 6291477;
    }
    nextChar();
    return 6553752;
};
CharScanners[46] = () => {
    if (nextChar() <= 57 && $currentChar >= 48) {
        return scanNumber(true);
    }
    if ($currentChar === 46) {
        if (nextChar() !== 46) {
            return 10;
        }
        nextChar();
        return 11;
    }
    return 65545;
};
CharScanners[60] = () => {
    if (nextChar() !== 61) {
        return 6554015;
    }
    nextChar();
    return 6554017;
};
CharScanners[62] = () => {
    if (nextChar() !== 61) {
        return 6554016;
    }
    nextChar();
    return 6554018;
};
CharScanners[37] = returnToken(6554154);
CharScanners[40] = returnToken(2688007);
CharScanners[41] = returnToken(7340046);
CharScanners[42] = returnToken(6554153);
CharScanners[43] = returnToken(2490853);
CharScanners[44] = returnToken(6291471);
CharScanners[45] = returnToken(2490854);
CharScanners[47] = returnToken(6554155);
CharScanners[58] = returnToken(6291476);
CharScanners[91] = returnToken(2688016);
CharScanners[93] = returnToken(7340051);
CharScanners[123] = returnToken(524296);
CharScanners[125] = returnToken(7340045);

let _connectable = null;
const connectables = [];
let connecting = false;
function pauseConnecting() {
    connecting = false;
}
function resumeConnecting() {
    connecting = true;
}
function currentConnectable() {
    return _connectable;
}
function enterConnectable(connectable) {
    if (connectable == null) {
        throw new Error(`AUR0206: Connectable cannot be null/undefined`);
    }
    if (_connectable == null) {
        _connectable = connectable;
        connectables[0] = _connectable;
        connecting = true;
        return;
    }
    if (_connectable === connectable) {
        throw new Error(`AUR0207: Trying to enter an active connectable`);
    }
    connectables.push(connectable);
    _connectable = connectable;
    connecting = true;
}
function exitConnectable(connectable) {
    if (connectable == null) {
        throw new Error(`AUR0208: Connectable cannot be null/undefined`);
    }
    if (_connectable !== connectable) {
        throw new Error(`AUR0209: Trying to exit an unactive connectable`);
    }
    connectables.pop();
    _connectable = connectables.length > 0 ? connectables[connectables.length - 1] : null;
    connecting = _connectable != null;
}
const ConnectableSwitcher = Object.freeze({
    get current() {
        return _connectable;
    },
    get connecting() {
        return connecting;
    },
    enter: enterConnectable,
    exit: exitConnectable,
    pause: pauseConnecting,
    resume: resumeConnecting,
});

const R$get = Reflect.get;
const toStringTag = Object.prototype.toString;
const proxyMap = new WeakMap();
function canWrap(obj) {
    switch (toStringTag.call(obj)) {
        case '[object Object]':
        case '[object Array]':
        case '[object Map]':
        case '[object Set]':
            return true;
        default:
            return false;
    }
}
const rawKey = '__raw__';
function wrap(v) {
    return canWrap(v) ? getProxy(v) : v;
}
function getProxy(obj) {
    return proxyMap.get(obj) ?? createProxy(obj);
}
function getRaw(obj) {
    return obj[rawKey] ?? obj;
}
function unwrap(v) {
    return canWrap(v) && v[rawKey] || v;
}
function doNotCollect(key) {
    return key === 'constructor'
        || key === '__proto__'
        || key === '$observers'
        || key === Symbol.toPrimitive
        || key === Symbol.toStringTag;
}
function createProxy(obj) {
    const handler = isArray(obj)
        ? arrayHandler
        : obj instanceof Map || obj instanceof Set
            ? collectionHandler
            : objectHandler;
    const proxiedObj = new Proxy(obj, handler);
    proxyMap.set(obj, proxiedObj);
    return proxiedObj;
}
const objectHandler = {
    get(target, key, receiver) {
        if (key === rawKey) {
            return target;
        }
        const connectable = currentConnectable();
        if (!connecting || doNotCollect(key) || connectable == null) {
            return R$get(target, key, receiver);
        }
        connectable.observe(target, key);
        return wrap(R$get(target, key, receiver));
    },
};
const arrayHandler = {
    get(target, key, receiver) {
        if (key === rawKey) {
            return target;
        }
        const connectable = currentConnectable();
        if (!connecting || doNotCollect(key) || connectable == null) {
            return R$get(target, key, receiver);
        }
        switch (key) {
            case 'length':
                connectable.observe(target, 'length');
                return target.length;
            case 'map':
                return wrappedArrayMap;
            case 'includes':
                return wrappedArrayIncludes;
            case 'indexOf':
                return wrappedArrayIndexOf;
            case 'lastIndexOf':
                return wrappedArrayLastIndexOf;
            case 'every':
                return wrappedArrayEvery;
            case 'filter':
                return wrappedArrayFilter;
            case 'find':
                return wrappedArrayFind;
            case 'findIndex':
                return wrappedArrayFindIndex;
            case 'flat':
                return wrappedArrayFlat;
            case 'flatMap':
                return wrappedArrayFlatMap;
            case 'join':
                return wrappedArrayJoin;
            case 'push':
                return wrappedArrayPush;
            case 'pop':
                return wrappedArrayPop;
            case 'reduce':
                return wrappedReduce;
            case 'reduceRight':
                return wrappedReduceRight;
            case 'reverse':
                return wrappedArrayReverse;
            case 'shift':
                return wrappedArrayShift;
            case 'unshift':
                return wrappedArrayUnshift;
            case 'slice':
                return wrappedArraySlice;
            case 'splice':
                return wrappedArraySplice;
            case 'some':
                return wrappedArraySome;
            case 'sort':
                return wrappedArraySort;
            case 'keys':
                return wrappedKeys;
            case 'values':
            case Symbol.iterator:
                return wrappedValues;
            case 'entries':
                return wrappedEntries;
        }
        connectable.observe(target, key);
        return wrap(R$get(target, key, receiver));
    },
    ownKeys(target) {
        currentConnectable()?.observe(target, 'length');
        return Reflect.ownKeys(target);
    },
};
function wrappedArrayMap(cb, thisArg) {
    const raw = getRaw(this);
    const res = raw.map((v, i) => unwrap(cb.call(thisArg, wrap(v), i, this)));
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
function wrappedArrayEvery(cb, thisArg) {
    const raw = getRaw(this);
    const res = raw.every((v, i) => cb.call(thisArg, wrap(v), i, this));
    currentConnectable()?.observeCollection(raw);
    return res;
}
function wrappedArrayFilter(cb, thisArg) {
    const raw = getRaw(this);
    const res = raw.filter((v, i) => unwrap(cb.call(thisArg, wrap(v), i, this)));
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
function wrappedArrayIncludes(v) {
    const raw = getRaw(this);
    const res = raw.includes(unwrap(v));
    currentConnectable()?.observeCollection(raw);
    return res;
}
function wrappedArrayIndexOf(v) {
    const raw = getRaw(this);
    const res = raw.indexOf(unwrap(v));
    currentConnectable()?.observeCollection(raw);
    return res;
}
function wrappedArrayLastIndexOf(v) {
    const raw = getRaw(this);
    const res = raw.lastIndexOf(unwrap(v));
    currentConnectable()?.observeCollection(raw);
    return res;
}
function wrappedArrayFindIndex(cb, thisArg) {
    const raw = getRaw(this);
    const res = raw.findIndex((v, i) => unwrap(cb.call(thisArg, wrap(v), i, this)));
    currentConnectable()?.observeCollection(raw);
    return res;
}
function wrappedArrayFind(cb, thisArg) {
    const raw = getRaw(this);
    const res = raw.find((v, i) => cb(wrap(v), i, this), thisArg);
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
function wrappedArrayFlat() {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return wrap(raw.flat());
}
function wrappedArrayFlatMap(cb, thisArg) {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return getProxy(raw.flatMap((v, i) => wrap(cb.call(thisArg, wrap(v), i, this))));
}
function wrappedArrayJoin(separator) {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return raw.join(separator);
}
function wrappedArrayPop() {
    return wrap(getRaw(this).pop());
}
function wrappedArrayPush(...args) {
    return getRaw(this).push(...args);
}
function wrappedArrayShift() {
    return wrap(getRaw(this).shift());
}
function wrappedArrayUnshift(...args) {
    return getRaw(this).unshift(...args);
}
function wrappedArraySplice(...args) {
    return wrap(getRaw(this).splice(...args));
}
function wrappedArrayReverse(..._args) {
    const raw = getRaw(this);
    const res = raw.reverse();
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
function wrappedArraySome(cb, thisArg) {
    const raw = getRaw(this);
    const res = raw.some((v, i) => unwrap(cb.call(thisArg, wrap(v), i, this)));
    currentConnectable()?.observeCollection(raw);
    return res;
}
function wrappedArraySort(cb) {
    const raw = getRaw(this);
    const res = raw.sort(cb);
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
function wrappedArraySlice(start, end) {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return getProxy(raw.slice(start, end));
}
function wrappedReduce(cb, initValue) {
    const raw = getRaw(this);
    const res = raw.reduce((curr, v, i) => cb(curr, wrap(v), i, this), initValue);
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
function wrappedReduceRight(cb, initValue) {
    const raw = getRaw(this);
    const res = raw.reduceRight((curr, v, i) => cb(curr, wrap(v), i, this), initValue);
    currentConnectable()?.observeCollection(raw);
    return wrap(res);
}
const collectionHandler = {
    get(target, key, receiver) {
        if (key === rawKey) {
            return target;
        }
        const connectable = currentConnectable();
        if (!connecting || doNotCollect(key) || connectable == null) {
            return R$get(target, key, receiver);
        }
        switch (key) {
            case 'size':
                connectable.observe(target, 'size');
                return target.size;
            case 'clear':
                return wrappedClear;
            case 'delete':
                return wrappedDelete;
            case 'forEach':
                return wrappedForEach;
            case 'add':
                if (target instanceof Set) {
                    return wrappedAdd;
                }
                break;
            case 'get':
                if (target instanceof Map) {
                    return wrappedGet;
                }
                break;
            case 'set':
                if (target instanceof Map) {
                    return wrappedSet;
                }
                break;
            case 'has':
                return wrappedHas;
            case 'keys':
                return wrappedKeys;
            case 'values':
                return wrappedValues;
            case 'entries':
                return wrappedEntries;
            case Symbol.iterator:
                return target instanceof Map ? wrappedEntries : wrappedValues;
        }
        return wrap(R$get(target, key, receiver));
    },
};
function wrappedForEach(cb, thisArg) {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return raw.forEach((v, key) => {
        cb.call(thisArg, wrap(v), wrap(key), this);
    });
}
function wrappedHas(v) {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return raw.has(unwrap(v));
}
function wrappedGet(k) {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    return wrap(raw.get(unwrap(k)));
}
function wrappedSet(k, v) {
    return wrap(getRaw(this).set(unwrap(k), unwrap(v)));
}
function wrappedAdd(v) {
    return wrap(getRaw(this).add(unwrap(v)));
}
function wrappedClear() {
    return wrap(getRaw(this).clear());
}
function wrappedDelete(k) {
    return wrap(getRaw(this).delete(unwrap(k)));
}
function wrappedKeys() {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    const iterator = raw.keys();
    return {
        next() {
            const next = iterator.next();
            const value = next.value;
            const done = next.done;
            return done
                ? { value: void 0, done }
                : { value: wrap(value), done };
        },
        [Symbol.iterator]() {
            return this;
        },
    };
}
function wrappedValues() {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    const iterator = raw.values();
    return {
        next() {
            const next = iterator.next();
            const value = next.value;
            const done = next.done;
            return done
                ? { value: void 0, done }
                : { value: wrap(value), done };
        },
        [Symbol.iterator]() {
            return this;
        },
    };
}
function wrappedEntries() {
    const raw = getRaw(this);
    currentConnectable()?.observeCollection(raw);
    const iterator = raw.entries();
    return {
        next() {
            const next = iterator.next();
            const value = next.value;
            const done = next.done;
            return done
                ? { value: void 0, done }
                : { value: [wrap(value[0]), wrap(value[1])], done };
        },
        [Symbol.iterator]() {
            return this;
        },
    };
}
const ProxyObservable = Object.freeze({
    getProxy,
    getRaw,
    wrap,
    unwrap,
    rawKey,
});

class ComputedObserver {
    constructor(obj, get, set, useProxy, observerLocator) {
        this.interceptor = this;
        this.type = 1;
        this._value = void 0;
        this._oldValue = void 0;
        this._isRunning = false;
        this._isDirty = false;
        this._obj = obj;
        this.$get = get;
        this.$set = set;
        this._useProxy = useProxy;
        this.oL = observerLocator;
    }
    static create(obj, key, descriptor, observerLocator, useProxy) {
        const getter = descriptor.get;
        const setter = descriptor.set;
        const observer = new ComputedObserver(obj, getter, setter, useProxy, observerLocator);
        const $get = (() => observer.getValue());
        $get.getObserver = () => observer;
        def(obj, key, {
            enumerable: descriptor.enumerable,
            configurable: true,
            get: $get,
            set: (v) => {
                observer.setValue(v);
            },
        });
        return observer;
    }
    getValue() {
        if (this.subs.count === 0) {
            return this.$get.call(this._obj, this);
        }
        if (this._isDirty) {
            this.compute();
            this._isDirty = false;
        }
        return this._value;
    }
    setValue(v) {
        if (isFunction(this.$set)) {
            if (v !== this._value) {
                this._isRunning = true;
                this.$set.call(this._obj, v);
                this._isRunning = false;
                this.run();
            }
        }
        else {
            throw new Error(`AUR0221: Property is readonly`);
        }
    }
    handleChange() {
        this._isDirty = true;
        if (this.subs.count > 0) {
            this.run();
        }
    }
    handleCollectionChange() {
        this._isDirty = true;
        if (this.subs.count > 0) {
            this.run();
        }
    }
    subscribe(subscriber) {
        if (this.subs.add(subscriber) && this.subs.count === 1) {
            this.compute();
            this._isDirty = false;
        }
    }
    unsubscribe(subscriber) {
        if (this.subs.remove(subscriber) && this.subs.count === 0) {
            this._isDirty = true;
            this.obs.clearAll();
        }
    }
    run() {
        if (this._isRunning) {
            return;
        }
        const oldValue = this._value;
        const newValue = this.compute();
        this._isDirty = false;
        if (!Object.is(newValue, oldValue)) {
            this._oldValue = oldValue;
            oV$1 = this._oldValue;
            this._oldValue = this._value;
            this.subs.notify(this._value, oV$1);
        }
    }
    compute() {
        this._isRunning = true;
        this.obs.version++;
        try {
            enterConnectable(this);
            return this._value = unwrap(this.$get.call(this._useProxy ? wrap(this._obj) : this._obj, this));
        }
        finally {
            this.obs.clear();
            this._isRunning = false;
            exitConnectable(this);
        }
    }
}
connectable(ComputedObserver);
subscriberCollection(ComputedObserver);
let oV$1 = void 0;

const IDirtyChecker = DI.createInterface('IDirtyChecker', x => x.singleton(DirtyChecker));
const DirtyCheckSettings = {
    timeoutsPerCheck: 25,
    disabled: false,
    throw: false,
    resetToDefault() {
        this.timeoutsPerCheck = 6;
        this.disabled = false;
        this.throw = false;
    }
};
const queueTaskOpts = {
    persistent: true,
};
class DirtyChecker {
    constructor(p) {
        this.p = p;
        this.tracked = [];
        this._task = null;
        this._elapsedFrames = 0;
        this.check = () => {
            if (DirtyCheckSettings.disabled) {
                return;
            }
            if (++this._elapsedFrames < DirtyCheckSettings.timeoutsPerCheck) {
                return;
            }
            this._elapsedFrames = 0;
            const tracked = this.tracked;
            const len = tracked.length;
            let current;
            let i = 0;
            for (; i < len; ++i) {
                current = tracked[i];
                if (current.isDirty()) {
                    current.flush();
                }
            }
        };
    }
    createProperty(obj, key) {
        if (DirtyCheckSettings.throw) {
            throw new Error(`AUR0222: Property '${safeString(key)}' is being dirty-checked.`);
        }
        return new DirtyCheckProperty(this, obj, key);
    }
    addProperty(property) {
        this.tracked.push(property);
        if (this.tracked.length === 1) {
            this._task = this.p.taskQueue.queueTask(this.check, queueTaskOpts);
        }
    }
    removeProperty(property) {
        this.tracked.splice(this.tracked.indexOf(property), 1);
        if (this.tracked.length === 0) {
            this._task.cancel();
            this._task = null;
        }
    }
}
DirtyChecker.inject = [IPlatform];
class DirtyCheckProperty {
    constructor(dirtyChecker, obj, key) {
        this.obj = obj;
        this.key = key;
        this.type = 0;
        this._oldValue = void 0;
        this._dirtyChecker = dirtyChecker;
    }
    getValue() {
        return this.obj[this.key];
    }
    setValue(_v) {
        throw new Error(`Trying to set value for property ${safeString(this.key)} in dirty checker`);
    }
    isDirty() {
        return this._oldValue !== this.obj[this.key];
    }
    flush() {
        const oldValue = this._oldValue;
        const newValue = this.getValue();
        this._oldValue = newValue;
        this.subs.notify(newValue, oldValue);
    }
    subscribe(subscriber) {
        if (this.subs.add(subscriber) && this.subs.count === 1) {
            this._oldValue = this.obj[this.key];
            this._dirtyChecker.addProperty(this);
        }
    }
    unsubscribe(subscriber) {
        if (this.subs.remove(subscriber) && this.subs.count === 0) {
            this._dirtyChecker.removeProperty(this);
        }
    }
}
subscriberCollection(DirtyCheckProperty);

class PrimitiveObserver {
    constructor(obj, key) {
        this.type = 0;
        this._obj = obj;
        this._key = key;
    }
    get doNotCache() { return true; }
    getValue() {
        return this._obj[this._key];
    }
    setValue() { }
    subscribe() { }
    unsubscribe() { }
}

class PropertyAccessor {
    constructor() {
        this.type = 0;
    }
    getValue(obj, key) {
        return obj[key];
    }
    setValue(value, obj, key) {
        obj[key] = value;
    }
}

class SetterObserver {
    constructor(obj, key) {
        this.type = 1;
        this._value = void 0;
        this._observing = false;
        this._obj = obj;
        this._key = key;
    }
    getValue() {
        return this._value;
    }
    setValue(newValue) {
        if (this._observing) {
            if (Object.is(newValue, this._value)) {
                return;
            }
            oV = this._value;
            this._value = newValue;
            this.subs.notify(newValue, oV);
        }
        else {
            this._obj[this._key] = newValue;
        }
    }
    subscribe(subscriber) {
        if (this._observing === false) {
            this.start();
        }
        this.subs.add(subscriber);
    }
    start() {
        if (this._observing === false) {
            this._observing = true;
            this._value = this._obj[this._key];
            def(this._obj, this._key, {
                enumerable: true,
                configurable: true,
                get: () => this.getValue(),
                set: (value) => {
                    this.setValue(value);
                },
            });
        }
        return this;
    }
    stop() {
        if (this._observing) {
            def(this._obj, this._key, {
                enumerable: true,
                configurable: true,
                writable: true,
                value: this._value,
            });
            this._observing = false;
        }
        return this;
    }
}
class SetterNotifier {
    constructor(obj, callbackKey, set, initialValue) {
        this.type = 1;
        this._value = void 0;
        this._oldValue = void 0;
        this._obj = obj;
        this._setter = set;
        this._hasSetter = isFunction(set);
        const callback = obj[callbackKey];
        this.cb = isFunction(callback) ? callback : void 0;
        this._value = initialValue;
    }
    getValue() {
        return this._value;
    }
    setValue(value) {
        if (this._hasSetter) {
            value = this._setter(value, null);
        }
        if (!Object.is(value, this._value)) {
            this._oldValue = this._value;
            this._value = value;
            this.cb?.call(this._obj, this._value, this._oldValue);
            oV = this._oldValue;
            this._oldValue = this._value;
            this.subs.notify(this._value, oV);
        }
    }
}
subscriberCollection(SetterObserver);
subscriberCollection(SetterNotifier);
let oV = void 0;

const propertyAccessor = new PropertyAccessor();
const IObserverLocator = DI.createInterface('IObserverLocator', x => x.singleton(ObserverLocator));
const INodeObserverLocator = DI
    .createInterface('INodeObserverLocator', x => x.cachedCallback(handler => {
    {
        handler.getAll(ILogger).forEach(logger => {
            logger.error('Using default INodeObserverLocator implementation. Will not be able to observe nodes (HTML etc...).');
        });
    }
    return new DefaultNodeObserverLocator();
}));
class DefaultNodeObserverLocator {
    handles() {
        return false;
    }
    getObserver() {
        return propertyAccessor;
    }
    getAccessor() {
        return propertyAccessor;
    }
}
class ObserverLocator {
    constructor(dirtyChecker, nodeObserverLocator) {
        this._adapters = [];
        this._dirtyChecker = dirtyChecker;
        this._nodeObserverLocator = nodeObserverLocator;
    }
    addAdapter(adapter) {
        this._adapters.push(adapter);
    }
    getObserver(obj, key) {
        if (obj == null) {
            throw nullObjectError(key);
        }
        if (!(obj instanceof Object)) {
            return new PrimitiveObserver(obj, key);
        }
        const lookup = getObserverLookup(obj);
        let observer = lookup[key];
        if (observer === void 0) {
            observer = this.createObserver(obj, key);
            if (!observer.doNotCache) {
                lookup[key] = observer;
            }
        }
        return observer;
    }
    getAccessor(obj, key) {
        const cached = obj.$observers?.[key];
        if (cached !== void 0) {
            return cached;
        }
        if (this._nodeObserverLocator.handles(obj, key, this)) {
            return this._nodeObserverLocator.getAccessor(obj, key, this);
        }
        return propertyAccessor;
    }
    getArrayObserver(observedArray) {
        return getArrayObserver(observedArray);
    }
    getMapObserver(observedMap) {
        return getMapObserver(observedMap);
    }
    getSetObserver(observedSet) {
        return getSetObserver(observedSet);
    }
    createObserver(obj, key) {
        if (this._nodeObserverLocator.handles(obj, key, this)) {
            return this._nodeObserverLocator.getObserver(obj, key, this);
        }
        switch (key) {
            case 'length':
                if (isArray(obj)) {
                    return getArrayObserver(obj).getLengthObserver();
                }
                break;
            case 'size':
                if (obj instanceof Map) {
                    return getMapObserver(obj).getLengthObserver();
                }
                else if (obj instanceof Set) {
                    return getSetObserver(obj).getLengthObserver();
                }
                break;
            default:
                if (isArray(obj) && isArrayIndex(key)) {
                    return getArrayObserver(obj).getIndexObserver(Number(key));
                }
                break;
        }
        let pd = getOwnPropDesc(obj, key);
        if (pd === void 0) {
            let proto = getProto(obj);
            while (proto !== null) {
                pd = getOwnPropDesc(proto, key);
                if (pd === void 0) {
                    proto = getProto(proto);
                }
                else {
                    break;
                }
            }
        }
        if (pd !== void 0 && !hasOwnProp.call(pd, 'value')) {
            let obs = this._getAdapterObserver(obj, key, pd);
            if (obs == null) {
                obs = (pd.get?.getObserver ?? pd.set?.getObserver)?.(obj, this);
            }
            return obs == null
                ? pd.configurable
                    ? ComputedObserver.create(obj, key, pd, this, true)
                    : this._dirtyChecker.createProperty(obj, key)
                : obs;
        }
        return new SetterObserver(obj, key);
    }
    _getAdapterObserver(obj, key, pd) {
        if (this._adapters.length > 0) {
            for (const adapter of this._adapters) {
                const observer = adapter.getObserver(obj, key, pd, this);
                if (observer != null) {
                    return observer;
                }
            }
        }
        return null;
    }
}
ObserverLocator.inject = [IDirtyChecker, INodeObserverLocator];
const getCollectionObserver = (collection) => {
    let obs;
    if (isArray(collection)) {
        obs = getArrayObserver(collection);
    }
    else if (collection instanceof Map) {
        obs = getMapObserver(collection);
    }
    else if (collection instanceof Set) {
        obs = getSetObserver(collection);
    }
    return obs;
};
const getProto = Object.getPrototypeOf;
const getOwnPropDesc = Object.getOwnPropertyDescriptor;
const getObserverLookup = (instance) => {
    let lookup = instance.$observers;
    if (lookup === void 0) {
        def(instance, '$observers', {
            enumerable: false,
            value: lookup = createLookup(),
        });
    }
    return lookup;
};
const nullObjectError = (key) => new Error(`AUR0199: trying to observe property ${safeString(key)} on null/undefined`)
    ;

const IObservation = DI.createInterface('IObservation', x => x.singleton(Observation));
class Observation {
    constructor(oL) {
        this.oL = oL;
    }
    static get inject() { return [IObserverLocator]; }
    run(fn) {
        const effect = new Effect(this.oL, fn);
        effect.run();
        return effect;
    }
}
class Effect {
    constructor(oL, fn) {
        this.oL = oL;
        this.fn = fn;
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
        if (this.stopped) {
            throw new Error(`AUR0225: Effect has already been stopped`);
        }
        if (this.running) {
            return;
        }
        ++this.runCount;
        this.running = true;
        this.queued = false;
        ++this.obs.version;
        try {
            enterConnectable(this);
            this.fn(this);
        }
        finally {
            this.obs.clear();
            this.running = false;
            exitConnectable(this);
        }
        if (this.queued) {
            if (this.runCount > this.maxRunCount) {
                this.runCount = 0;
                throw new Error(`AUR0226: Maximum number of recursive effect run reached. Consider handle effect dependencies differently.`);
            }
            this.run();
        }
        else {
            this.runCount = 0;
        }
    }
    stop() {
        this.stopped = true;
        this.obs.clearAll();
    }
}
connectable(Effect);

function getObserversLookup(obj) {
    if (obj.$observers === void 0) {
        def(obj, '$observers', { value: {} });
    }
    return obj.$observers;
}
const noValue = {};
function observable(targetOrConfig, key, descriptor) {
    if (key == null) {
        return ((t, k, d) => deco(t, k, d, targetOrConfig));
    }
    return deco(targetOrConfig, key, descriptor);
    function deco(target, key, descriptor, config) {
        const isClassDecorator = key === void 0;
        config = typeof config !== 'object'
            ? { name: config }
            : (config || {});
        if (isClassDecorator) {
            key = config.name;
        }
        if (key == null || key === '') {
            throw new Error(`AUR0224: Invalid usage, cannot determine property name for @observable`);
        }
        const callback = config.callback || `${safeString(key)}Changed`;
        let initialValue = noValue;
        if (descriptor) {
            delete descriptor.value;
            delete descriptor.writable;
            initialValue = descriptor.initializer?.();
            delete descriptor.initializer;
        }
        else {
            descriptor = { configurable: true };
        }
        if (!('enumerable' in descriptor)) {
            descriptor.enumerable = true;
        }
        const $set = config.set;
        descriptor.get = function g() {
            const notifier = getNotifier(this, key, callback, initialValue, $set);
            currentConnectable()?.subscribeTo(notifier);
            return notifier.getValue();
        };
        descriptor.set = function s(newValue) {
            getNotifier(this, key, callback, initialValue, $set).setValue(newValue);
        };
        descriptor.get.getObserver = function gO(obj) {
            return getNotifier(obj, key, callback, initialValue, $set);
        };
        if (isClassDecorator) {
            def(target.prototype, key, descriptor);
        }
        else {
            return descriptor;
        }
    }
}
function getNotifier(obj, key, callbackKey, initialValue, set) {
    const lookup = getObserversLookup(obj);
    let notifier = lookup[key];
    if (notifier == null) {
        notifier = new SetterNotifier(obj, callbackKey, set, initialValue === noValue ? void 0 : initialValue);
        lookup[key] = notifier;
    }
    return notifier;
}

export { AccessKeyedExpression, AccessMemberExpression, AccessScopeExpression, AccessThisExpression, AccessorType, ArrayBindingPattern, ArrayIndexObserver, ArrayLiteralExpression, ArrayObserver, ArrowFunction, AssignExpression, BinaryExpression, BindingBehaviorExpression, BindingContext, BindingIdentifier, BindingObserverRecord, CallFunctionExpression, CallMemberExpression, CallScopeExpression, CollectionKind, CollectionLengthObserver, CollectionSizeObserver, ComputedObserver, ConditionalExpression, ConnectableSwitcher, CustomExpression, DestructuringAssignmentExpression, DestructuringAssignmentRestExpression, DestructuringAssignmentSingleExpression, DirtyCheckProperty, DirtyCheckSettings, ExpressionKind, ExpressionType, ForOfStatement, ICoercionConfiguration, IDirtyChecker, IExpressionParser, INodeObserverLocator, IObservation, IObserverLocator, ISignaler, Interpolation, MapObserver, ObjectBindingPattern, ObjectLiteralExpression, Observation, ObserverLocator, PrimitiveLiteralExpression, PrimitiveObserver, PropertyAccessor, ProxyObservable, Scope, SetObserver, SetterObserver, SubscriberRecord, TaggedTemplateExpression, TemplateExpression, UnaryExpression, ValueConverterExpression, applyMutationsToIndices, batch, cloneIndexMap, connectable, copyIndexMap, createIndexMap, disableArrayObservation, disableMapObservation, disableSetObservation, enableArrayObservation, enableMapObservation, enableSetObservation, getCollectionObserver, getObserverLookup, isIndexMap, observable, parseExpression, subscriberCollection, synchronizeIndices };
//# sourceMappingURL=index.dev.mjs.map
