import { getResourceKeyFor as t, IEventAggregator as i, IContainer as s, Protocol as e, DI as n, resolve as r, ILogger as o, Registration as h } from "@aurelia/kernel";

import { CustomElement as u, isCustomElementViewModel as a, Controller as l, IPlatform as c, IWindow as f, IHistory as d, ILocation as p, IAppRoot as g, CustomAttribute as v, BindingMode as m, customElement as w, INode as R, IInstruction as I, bindable as y, IController as E, customAttribute as C, AppTask as S } from "@aurelia/runtime-html";

import { Metadata as N } from "@aurelia/metadata";

import { RouteRecognizer as b, ConfigurableRoute as k, RecognizedRoute as $, Endpoint as P } from "@aurelia/route-recognizer";

let V = class Endpoint {
    constructor(t, i, s, e = {}) {
        this.router = t;
        this.name = i;
        this.connectedCE = s;
        this.options = e;
        this.contents = [];
        this.transitionAction = "";
        this.path = null;
    }
    getContent() {
        return this.contents[0];
    }
    getNextContent() {
        return this.contents.length > 1 ? this.contents[this.contents.length - 1] : null;
    }
    getTimeContent(t = Infinity) {
        return this.getContent();
    }
    getNavigationContent(t) {
        if (t instanceof NavigationCoordinator) {
            t = t.navigation;
        }
        if (t instanceof Navigation) {
            return this.contents.find((i => i.navigation === t)) ?? null;
        }
        return null;
    }
    get activeContent() {
        return this.getNextContent() ?? this.getContent();
    }
    get connectedScope() {
        return this.activeContent?.connectedScope;
    }
    get scope() {
        return this.connectedScope.scope;
    }
    get owningScope() {
        return this.connectedScope.owningScope;
    }
    get connectedController() {
        return this.connectedCE?.$controller ?? null;
    }
    get isViewport() {
        return this instanceof Viewport;
    }
    get isViewportScope() {
        return this instanceof ViewportScope;
    }
    get isEmpty() {
        return false;
    }
    get pathname() {
        return this.connectedScope.pathname;
    }
    toString() {
        throw new Error(`Method 'toString' needs to be implemented in all endpoints!`);
    }
    setNextContent(t, i) {
        throw new Error(`Method 'setNextContent' needs to be implemented in all endpoints!`);
    }
    setConnectedCE(t, i) {
        throw new Error(`Method 'setConnectedCE' needs to be implemented in all endpoints!`);
    }
    transition(t) {
        throw new Error(`Method 'transition' needs to be implemented in all endpoints!`);
    }
    finalizeContentChange(t, i) {
        throw new Error(`Method 'finalizeContentChange' needs to be implemented in all endpoints!`);
    }
    cancelContentChange(t, i = null) {
        throw new Error(`Method 'cancelContentChange' needs to be implemented in all endpoints!`);
    }
    getRoutes() {
        throw new Error(`Method 'getRoutes' needs to be implemented in all endpoints!`);
    }
    getTitle(t) {
        throw new Error(`Method 'getTitle' needs to be implemented in all endpoints!`);
    }
    removeEndpoint(t, i) {
        this.contents.forEach((t => t.delete()));
        return true;
    }
    canUnload(t, i) {
        return true;
    }
    canLoad(t, i) {
        return true;
    }
    unload(t, i) {
        return;
    }
    load(t, i) {
        return;
    }
};

class EndpointContent {
    constructor(t, i, s, e, n = RoutingInstruction.create(""), r = Navigation.create({
        instruction: "",
        fullStateInstruction: ""
    })) {
        this.router = t;
        this.endpoint = i;
        this.instruction = n;
        this.navigation = r;
        this.completed = false;
        this.connectedScope = new RoutingScope(t, e, s, this);
        if (this.router.rootScope !== null) {
            (this.endpoint.connectedScope?.parent ?? this.router.rootScope.scope).addChild(this.connectedScope);
        }
    }
    get isActive() {
        return this.endpoint.activeContent === this;
    }
    delete() {
        this.connectedScope.parent?.removeChild(this.connectedScope);
    }
}

class FoundRoute {
    constructor(t = null, i = "", s = [], e = "", n = {}) {
        this.match = t;
        this.matching = i;
        this.instructions = s;
        this.remaining = e;
        this.params = n;
    }
    get foundConfiguration() {
        return this.match !== null;
    }
    get foundInstructions() {
        return this.instructions.some((t => !t.component.none));
    }
    get hasRemaining() {
        return this.instructions.some((t => t.hasNextScopeInstructions));
    }
}

function __decorate(t, i, s, e) {
    var n = arguments.length, r = n < 3 ? i : e === null ? e = Object.getOwnPropertyDescriptor(i, s) : e, o;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(t, i, s, e); else for (var h = t.length - 1; h >= 0; h--) if (o = t[h]) r = (n < 3 ? o(r) : n > 3 ? o(i, s, r) : o(i, s)) || r;
    return n > 3 && r && Object.defineProperty(i, s, r), r;
}

function __param(t, i) {
    return function(s, e) {
        i(s, e, t);
    };
}

class InstructionParser {
    static parse(t, i, s, e) {
        if (!i) {
            return {
                instructions: [],
                remaining: ""
            };
        }
        if (i.startsWith(t.sibling) && !InstructionParser.isAdd(t, i)) {
            throw new Error(`Instruction parser error: Unnecessary siblings separator ${t.sibling} in beginning of instruction part "${i}".`);
        }
        const n = [];
        let r = 1e3;
        while (i.length && r) {
            r--;
            if (i.startsWith(t.scope)) {
                if (n.length === 0) {
                    throw new Error(`Instruction parser error: Children without parent in instruction part "(${i}" is not allowed.`);
                }
                e = false;
                i = i.slice(t.scope.length);
                const r = i.startsWith(t.groupStart);
                if (r) {
                    i = i.slice(t.groupStart.length);
                    s = true;
                }
                const {instructions: o, remaining: h} = InstructionParser.parse(t, i, r, false);
                n[n.length - 1].nextScopeInstructions = o;
                i = h;
            } else if (i.startsWith(t.groupStart)) {
                i = i.slice(t.groupStart.length);
                const {instructions: s, remaining: r} = InstructionParser.parse(t, i, true, e);
                n.push(...s);
                i = r;
            } else if (i.startsWith(t.groupEnd)) {
                if (s) {
                    i = i.slice(t.groupEnd.length);
                }
                let e = 0;
                const r = i.length;
                for (;e < r; e++) {
                    if (i.slice(e, e + t.sibling.length) === t.sibling) {
                        return {
                            instructions: n,
                            remaining: i
                        };
                    }
                    if (i.slice(e, e + t.groupEnd.length) !== t.groupEnd) {
                        if (n.length > 1) {
                            throw new Error(`Instruction parser error: Children below scope ${t.groupStart}${t.groupEnd} in instruction part "(${i}" is not allowed.`);
                        } else {
                            i = i.slice(e);
                            break;
                        }
                    }
                }
                if (e >= r) {
                    return {
                        instructions: n,
                        remaining: i
                    };
                }
            } else if (i.startsWith(t.sibling) && !InstructionParser.isAdd(t, i)) {
                if (!s) {
                    return {
                        instructions: n,
                        remaining: i
                    };
                }
                i = i.slice(t.sibling.length);
            } else {
                const {instruction: s, remaining: e} = InstructionParser.parseOne(t, i);
                n.push(s);
                i = e;
            }
        }
        return {
            instructions: n,
            remaining: i
        };
    }
    static isAdd(t, i) {
        return i === t.add || i.startsWith(`${t.add}${t.viewport}`);
    }
    static parseOne(t, i) {
        const s = [ t.parameters, t.viewport, t.noScope, t.groupEnd, t.scope, t.sibling ];
        let e = void 0;
        let n = void 0;
        let r = void 0;
        let o = true;
        let h;
        let u;
        const a = i;
        const l = [ t.add, t.clear ];
        for (const n of l) {
            if (i === n) {
                e = i;
                i = "";
                s.shift();
                s.shift();
                h = t.viewport;
                break;
            }
        }
        if (e === void 0) {
            for (const n of l) {
                if (i.startsWith(`${n}${t.viewport}`)) {
                    e = n;
                    i = i.slice(`${n}${t.viewport}`.length);
                    s.shift();
                    s.shift();
                    h = t.viewport;
                    break;
                }
            }
        }
        if (e === void 0) {
            ({token: h, pos: u} = InstructionParser.findNextToken(i, s));
            e = u !== -1 ? i.slice(0, u) : i;
            i = u !== -1 ? i.slice(u + h.length) : "";
            s.shift();
            if (h === t.parameters) {
                ({token: h, pos: u} = InstructionParser.findNextToken(i, [ t.parametersEnd ]));
                n = i.slice(0, u);
                i = i.slice(u + h.length);
                ({token: h} = InstructionParser.findNextToken(i, s));
                i = i.slice(h.length);
            }
            s.shift();
        }
        if (h === t.viewport) {
            ({token: h, pos: u} = InstructionParser.findNextToken(i, s));
            r = u !== -1 ? i.slice(0, u) : i;
            i = u !== -1 ? i.slice(u + h.length) : "";
        }
        s.shift();
        if (h === t.noScope) {
            o = false;
        }
        if (h === t.groupEnd || h === t.scope || h === t.sibling) {
            i = `${h}${i}`;
        }
        if ((e ?? "") === "") {
            throw new Error(`Instruction parser error: No component specified in instruction part "${i}".`);
        }
        const c = RoutingInstruction.create(e, r, n, o);
        c.unparsed = a;
        return {
            instruction: c,
            remaining: i
        };
    }
    static findNextToken(t, i) {
        const s = {};
        for (const e of i) {
            const i = t.indexOf(e);
            if (i > -1) {
                s[e] = t.indexOf(e);
            }
        }
        const e = Math.min(...Object.values(s));
        for (const t in s) {
            if (s[t] === e) {
                return {
                    token: t,
                    pos: e
                };
            }
        }
        return {
            token: "",
            pos: -1
        };
    }
}

class TitleOptions {
    constructor(t = "${componentTitles}${appTitleSeparator}Aurelia", i = " | ", s = "top-down", e = " > ", n = true, r = "app-", o) {
        this.appTitle = t;
        this.appTitleSeparator = i;
        this.componentTitleOrder = s;
        this.componentTitleSeparator = e;
        this.useComponentNames = n;
        this.componentPrefix = r;
        this.transformTitle = o;
    }
    static create(t = {}) {
        t = typeof t === "string" ? {
            appTitle: t
        } : t;
        return new TitleOptions(t.appTitle, t.appTitleSeparator, t.componentTitleOrder, t.componentTitleSeparator, t.useComponentNames, t.componentPrefix, t.transformTitle);
    }
    static for(t) {
        return RouterOptions.for(t).title;
    }
    apply(t = {}) {
        t = typeof t === "string" ? {
            appTitle: t
        } : t;
        this.appTitle = t.appTitle ?? this.appTitle;
        this.appTitleSeparator = t.appTitleSeparator ?? this.appTitleSeparator;
        this.componentTitleOrder = t.componentTitleOrder ?? this.componentTitleOrder;
        this.componentTitleSeparator = t.componentTitleSeparator ?? this.componentTitleSeparator;
        this.useComponentNames = t.useComponentNames ?? this.useComponentNames;
        this.componentPrefix = t.componentPrefix ?? this.componentPrefix;
        this.transformTitle = "transformTitle" in t ? t.transformTitle : this.transformTitle;
    }
}

class Separators {
    constructor(t = "@", i = "+", s = "/", e = "(", n = ")", r = "!", o = "(", h = ")", u = ",", a = "=", l = "+", c = "-", f = ".") {
        this.viewport = t;
        this.sibling = i;
        this.scope = s;
        this.groupStart = e;
        this.groupEnd = n;
        this.noScope = r;
        this.parameters = o;
        this.parametersEnd = h;
        this.parameterSeparator = u;
        this.parameterKeySeparator = a;
        this.add = l;
        this.clear = c;
        this.action = f;
    }
    static create(t = {}) {
        return new Separators(t.viewport, t.sibling, t.scope, t.groupStart, t.groupEnd, t.noScope, t.parameters, t.parametersEnd, t.parameterSeparator, t.parameterKeySeparator, t.add, t.clear, t.action);
    }
    static for(t) {
        return RouterOptions.for(t).separators;
    }
    apply(t = {}) {
        this.viewport = t.viewport ?? this.viewport;
        this.sibling = t.sibling ?? this.sibling;
        this.scope = t.scope ?? this.scope;
        this.groupStart = t.groupStart ?? this.groupStart;
        this.groupEnd = t.groupEnd ?? this.groupEnd;
        this.noScope = t.noScope ?? this.noScope;
        this.parameters = t.parameters ?? this.parameters;
        this.parametersEnd = t.parametersEnd ?? this.parametersEnd;
        this.parameterSeparator = t.parameterSeparator ?? this.parameterSeparator;
        this.parameterKeySeparator = t.parameterKeySeparator ?? this.parameterKeySeparator;
        this.add = t.add ?? this.add;
        this.clear = t.clear ?? this.clear;
        this.action = t.action ?? this.action;
    }
}

class Indicators {
    constructor(t = "active", i = "navigating") {
        this.loadActive = t;
        this.viewportNavigating = i;
    }
    static create(t = {}) {
        return new Indicators(t.loadActive, t.viewportNavigating);
    }
    static for(t) {
        return RouterOptions.for(t).indicators;
    }
    apply(t = {}) {
        this.loadActive = t.loadActive ?? this.loadActive;
        this.viewportNavigating = t.viewportNavigating ?? this.viewportNavigating;
    }
}

class RouterOptions {
    constructor(t = Separators.create(), i = Indicators.create(), s = true, e = null, n = true, r = 0, o = true, h = true, u = true, a = TitleOptions.create(), l = [ "guardedUnload", "swapped", "completed" ], c = "attach-next-detach-current", f = "", d = "abort") {
        this.separators = t;
        this.indicators = i;
        this.useUrlFragmentHash = s;
        this.basePath = e;
        this.useHref = n;
        this.statefulHistoryLength = r;
        this.useDirectRouting = o;
        this.useConfiguredRoutes = h;
        this.additiveInstructionDefault = u;
        this.title = a;
        this.navigationSyncStates = l;
        this.swapOrder = c;
        this.fallback = f;
        this.fallbackAction = d;
        this.registrationHooks = [];
    }
    static create(t = {}) {
        return new RouterOptions(Separators.create(t.separators), Indicators.create(t.indicators), t.useUrlFragmentHash, t.basePath, t.useHref, t.statefulHistoryLength, t.useDirectRouting, t.useConfiguredRoutes, t.additiveInstructionDefault, TitleOptions.create(t.title), t.navigationSyncStates, t.swapOrder, t.fallback, t.fallbackAction);
    }
    static for(t) {
        if (t instanceof RouterConfiguration) {
            return t.options;
        }
        if (t instanceof Router) {
            t = t.configuration;
        } else {
            t = t.get(W);
        }
        return t.options;
    }
    apply(t) {
        t = t ?? {};
        this.separators.apply(t.separators);
        this.indicators.apply(t.indicators);
        this.useUrlFragmentHash = t.useUrlFragmentHash ?? this.useUrlFragmentHash;
        this.basePath = t.basePath ?? this.basePath;
        this.useHref = t.useHref ?? this.useHref;
        this.statefulHistoryLength = t.statefulHistoryLength ?? this.statefulHistoryLength;
        this.useDirectRouting = t.useDirectRouting ?? this.useDirectRouting;
        this.useConfiguredRoutes = t.useConfiguredRoutes ?? this.useConfiguredRoutes;
        this.additiveInstructionDefault = t.additiveInstructionDefault ?? this.additiveInstructionDefault;
        this.title.apply(t.title);
        this.navigationSyncStates = t.navigationSyncStates ?? this.navigationSyncStates;
        this.swapOrder = t.swapOrder ?? this.swapOrder;
        this.fallback = t.fallback ?? this.fallback;
        this.fallbackAction = t.fallbackAction ?? this.fallbackAction;
        if (Array.isArray(t.hooks)) {
            if (this.routerConfiguration !== void 0) {
                t.hooks.forEach((t => this.routerConfiguration.addHook(t.hook, t.options)));
            } else {
                this.registrationHooks = t.hooks;
            }
        }
    }
    setRouterConfiguration(t) {
        this.routerConfiguration = t;
        this.registrationHooks.forEach((t => this.routerConfiguration.addHook(t.hook, t.options)));
        this.registrationHooks.length = 0;
    }
}

class InstructionParameters {
    constructor() {
        this.parametersString = null;
        this.parametersRecord = null;
        this.parametersList = null;
        this.parametersType = "none";
    }
    get none() {
        return this.parametersType === "none";
    }
    static create(t) {
        const i = new InstructionParameters;
        i.set(t);
        return i;
    }
    static parse(t, i, s = false) {
        if (i == null || i.length === 0) {
            return [];
        }
        const e = Separators.for(t);
        const n = e.parameterSeparator;
        const r = e.parameterKeySeparator;
        if (typeof i === "string") {
            const t = [];
            const e = i.split(n);
            for (const i of e) {
                let e;
                let n;
                [e, n] = i.split(r);
                if (n === void 0) {
                    n = s ? decodeURIComponent(e) : e;
                    e = void 0;
                } else if (s) {
                    e = decodeURIComponent(e);
                    n = decodeURIComponent(n);
                }
                t.push({
                    key: e,
                    value: n
                });
            }
            return t;
        }
        if (Array.isArray(i)) {
            return i.map((t => ({
                key: void 0,
                value: t
            })));
        }
        const o = Object.keys(i);
        o.sort();
        return o.map((t => ({
            key: t,
            value: i[t]
        })));
    }
    get typedParameters() {
        switch (this.parametersType) {
          case "string":
            return this.parametersString;

          case "array":
            return this.parametersList;

          case "object":
            return this.parametersRecord;

          default:
            return null;
        }
    }
    static stringify(t, i, s = false) {
        if (!Array.isArray(i) || i.length === 0) {
            return "";
        }
        const e = Separators.for(t);
        return i.map((t => {
            const i = t.key !== void 0 && s ? encodeURIComponent(t.key) : t.key;
            const n = s ? encodeURIComponent(t.value) : t.value;
            return i !== void 0 && i !== n ? i + e.parameterKeySeparator + n : n;
        })).join(e.parameterSeparator);
    }
    static contains(t, i) {
        return Object.keys(i).every((s => i[s] === t[s]));
    }
    parameters(t) {
        return InstructionParameters.parse(t, this.typedParameters);
    }
    set(t) {
        this.parametersString = null;
        this.parametersList = null;
        this.parametersRecord = null;
        if (t == null || t === "") {
            this.parametersType = "none";
            t = null;
        } else if (typeof t === "string") {
            this.parametersType = "string";
            this.parametersString = t;
        } else if (Array.isArray(t)) {
            this.parametersType = "array";
            this.parametersList = t;
        } else {
            this.parametersType = "object";
            this.parametersRecord = t;
        }
    }
    get(t, i) {
        if (i === void 0) {
            return this.parameters(t);
        }
        const s = this.parameters(t).filter((t => t.key === i)).map((t => t.value));
        if (s.length === 0) {
            return;
        }
        return s.length === 1 ? s[0] : s;
    }
    addParameters(t) {
        if (this.parametersType === "none") {
            return this.set(t);
        }
        if (this.parametersType !== "object") {
            throw new Error("Can't add object parameters to existing non-object parameters!");
        }
        this.set({
            ...this.parametersRecord,
            ...t
        });
    }
    toSpecifiedParameters(t, i) {
        i = i ?? [];
        const s = this.parameters(t);
        const e = {};
        for (const t of i) {
            let i = s.findIndex((i => i.key === t));
            if (i >= 0) {
                const [n] = s.splice(i, 1);
                e[t] = n.value;
            } else {
                i = s.findIndex((t => t.key === void 0));
                if (i >= 0) {
                    const [n] = s.splice(i, 1);
                    e[t] = n.value;
                }
            }
        }
        for (const t of s.filter((t => t.key !== void 0))) {
            e[t.key] = t.value;
        }
        let n = i.length;
        for (const t of s.filter((t => t.key === void 0))) {
            e[n++] = t.value;
        }
        return e;
    }
    toSortedParameters(t, i) {
        i = i || [];
        const s = this.parameters(t);
        const e = [];
        for (const t of i) {
            let i = s.findIndex((i => i.key === t));
            if (i >= 0) {
                const t = {
                    ...s.splice(i, 1)[0]
                };
                t.key = void 0;
                e.push(t);
            } else {
                i = s.findIndex((t => t.key === void 0));
                if (i >= 0) {
                    const t = {
                        ...s.splice(i, 1)[0]
                    };
                    e.push(t);
                } else {
                    e.push({
                        value: void 0
                    });
                }
            }
        }
        const n = s.filter((t => t.key !== void 0));
        n.sort(((t, i) => (t.key || "") < (i.key || "") ? 1 : (i.key || "") < (t.key || "") ? -1 : 0));
        e.push(...n);
        e.push(...s.filter((t => t.key === void 0)));
        return e;
    }
    same(t, i, s) {
        const e = s !== null ? s.parameters : [];
        const n = this.toSpecifiedParameters(t, e);
        const r = i.toSpecifiedParameters(t, e);
        return Object.keys(n).every((t => n[t] === r[t])) && Object.keys(r).every((t => r[t] === n[t]));
    }
}

class InstructionComponent {
    constructor() {
        this.name = null;
        this.type = null;
        this.instance = null;
        this.promise = null;
        this.func = null;
    }
    static create(t) {
        const i = new InstructionComponent;
        i.set(t);
        return i;
    }
    static isName(t) {
        return typeof t === "string";
    }
    static isDefinition(t) {
        return u.isType(t.Type);
    }
    static isType(t) {
        return u.isType(t);
    }
    static isInstance(t) {
        return a(t);
    }
    static isAppelation(t) {
        return InstructionComponent.isName(t) || InstructionComponent.isType(t) || InstructionComponent.isInstance(t);
    }
    static getName(t) {
        if (InstructionComponent.isName(t)) {
            return t;
        } else if (InstructionComponent.isType(t)) {
            return u.getDefinition(t).name;
        } else {
            return InstructionComponent.getName(t.constructor);
        }
    }
    static getType(t) {
        if (InstructionComponent.isName(t)) {
            return null;
        } else if (InstructionComponent.isType(t)) {
            return t;
        } else {
            return t.constructor;
        }
    }
    static getInstance(t) {
        if (InstructionComponent.isName(t) || InstructionComponent.isType(t)) {
            return null;
        } else {
            return t;
        }
    }
    set(t) {
        let i = null;
        let s = null;
        let e = null;
        let n = null;
        let r = null;
        if (t instanceof Promise) {
            n = t;
        } else if (InstructionComponent.isName(t)) {
            i = InstructionComponent.getName(t);
        } else if (InstructionComponent.isType(t)) {
            i = this.getNewName(t);
            s = InstructionComponent.getType(t);
        } else if (InstructionComponent.isInstance(t)) {
            i = this.getNewName(InstructionComponent.getType(t));
            s = InstructionComponent.getType(t);
            e = InstructionComponent.getInstance(t);
        } else if (typeof t === "function") {
            r = t;
        }
        this.name = i;
        this.type = s;
        this.instance = e;
        this.promise = n;
        this.func = r;
    }
    resolve(t) {
        if (this.func !== null) {
            this.set(this.func(t));
        }
        if (!(this.promise instanceof Promise)) {
            return;
        }
        return this.promise.then((t => {
            if (InstructionComponent.isAppelation(t)) {
                this.set(t);
                return;
            }
            if (t.default != null) {
                this.set(t.default);
                return;
            }
            const i = Object.keys(t).filter((t => !t.startsWith("__")));
            if (i.length === 0) {
                throw new Error(`Failed to load component Type from resolved Promise since no export was specified.`);
            }
            if (i.length > 1) {
                throw new Error(`Failed to load component Type from resolved Promise since no 'default' export was specified when having multiple exports.`);
            }
            const s = i[0];
            this.set(t[s]);
        }));
    }
    get none() {
        return !this.isName() && !this.isType() && !this.isInstance() && !this.isFunction() && !this.isPromise();
    }
    isName() {
        return this.name != null && this.name !== "" && !this.isType() && !this.isInstance();
    }
    isType() {
        return this.type !== null && !this.isInstance();
    }
    isInstance() {
        return this.instance !== null;
    }
    isPromise() {
        return this.promise !== null;
    }
    isFunction() {
        return this.func !== null;
    }
    toType(t, i) {
        void this.resolve(i);
        if (this.type !== null) {
            return this.type;
        }
        if (this.name !== null && typeof this.name === "string") {
            if (t === null) {
                throw new Error(`No container available when trying to resolve component '${this.name}'!`);
            }
            if (t.has(u.keyFrom(this.name), true)) {
                const i = t.getResolver(u.keyFrom(this.name));
                if (i !== null && i.getFactory !== void 0) {
                    const s = i.getFactory(t);
                    if (s) {
                        return s.Type;
                    }
                }
            }
        }
        return null;
    }
    toInstance(t, i, s, e) {
        void this.resolve(e);
        if (this.instance !== null) {
            return this.instance;
        }
        if (t == null) {
            return null;
        }
        const n = t.createChild();
        const r = this.isType() ? n.invoke(this.type) : n.get(routerComponentResolver(this.name));
        if (r == null) {
            throw new Error(`Failed to create instance when trying to resolve component '${this.name}'!`);
        }
        const o = l.$el(n, r, s, null);
        o.parent = i;
        return r;
    }
    same(t, i = false) {
        return i ? this.type === t.type : this.name === t.name;
    }
    getNewName(t) {
        if (this.name === null) {
            return InstructionComponent.getName(t);
        }
        return this.name;
    }
}

function routerComponentResolver(t) {
    const i = u.keyFrom(t);
    return {
        $isResolver: true,
        resolve(t, s) {
            if (s.has(i, false)) {
                return s.get(i);
            }
            if (s.root.has(i, false)) {
                return s.root.get(i);
            }
            return s.get(i);
        }
    };
}

function arrayRemove(t, i) {
    const s = [];
    let e = t.findIndex(i);
    while (e >= 0) {
        s.push(t.splice(e, 1)[0]);
        e = t.findIndex(i);
    }
    return s;
}

function arrayUnique(t, i = false) {
    return t.filter(((t, s, e) => (i || t != null) && e.indexOf(t) === s));
}

class OpenPromise {
    constructor() {
        this.isPending = true;
        this.promise = new Promise(((t, i) => {
            this.t = t;
            this.i = i;
        }));
    }
    resolve(t) {
        this.t(t);
        this.isPending = false;
    }
    reject(t) {
        this.i(t);
        this.isPending = false;
    }
}

class Runner {
    constructor() {
        this.isDone = false;
        this.isCancelled = false;
        this.isResolved = false;
        this.isRejected = false;
        this.isAsync = false;
    }
    static run(t, ...i) {
        if (i.length === 0) {
            return void 0;
        }
        let s = false;
        if (t === null) {
            t = new Step;
            s = true;
        }
        const e = new Step(i.shift());
        Runner.connect(t, e, (t?.runParallel ?? false) || s);
        if (i.length > 0) {
            Runner.add(e, false, ...i);
        }
        if (s) {
            Runner.process(t);
            if (t.result instanceof Promise) {
                this.runners.set(t.result, t);
            }
            return t.result;
        }
        return e;
    }
    static runParallel(t, ...i) {
        if ((i?.length ?? 0) === 0) {
            return [];
        }
        let s = false;
        if (t === null) {
            t = new Step;
            s = true;
        } else {
            t = Runner.connect(t, new Step, true);
        }
        Runner.add(t, true, ...i);
        if (s) {
            Runner.process(t);
        }
        if (t.result instanceof Promise) {
            this.runners.set(t.result, t);
        }
        return s ? t.result ?? [] : t;
    }
    static step(t) {
        if (t instanceof Promise) {
            return Runner.runners.get(t);
        }
    }
    static cancel(t) {
        const i = Runner.step(t);
        if (i !== void 0) {
            i.cancel();
        }
    }
    static add(t, i, ...s) {
        let e = new Step(s.shift(), i);
        if (t !== null) {
            e = Runner.connect(t, e, i);
        }
        const n = e;
        while (s.length > 0) {
            e = Runner.connect(e, new Step(s.shift(), i), false);
        }
        return n;
    }
    static connect(t, i, s) {
        if (!s) {
            const s = t.next;
            t.next = i;
            i.previous = t;
            i.next = s;
            if (s !== null) {
                s.previous = i;
                s.parent = null;
            }
        } else {
            const s = t.child;
            t.child = i;
            i.parent = t;
            i.next = s;
            if (s !== null) {
                s.parent = null;
                s.previous = i;
            }
        }
        return i;
    }
    static process(t) {
        const i = t.root;
        while (t !== null && !t.isDoing && !t.isDone) {
            i.current = t;
            if (t.isParallelParent) {
                t.isDone = true;
                let i = t.child;
                while (i !== null) {
                    Runner.process(i);
                    i = i.next;
                }
            } else {
                t.isDoing = true;
                t.value = t.step;
                while (t.value instanceof Function && !t.isCancelled && !t.isExited && !t.isDone) {
                    t.value = t.value(t);
                }
                if (!t.isCancelled) {
                    if (t.value instanceof Promise) {
                        const s = t.value;
                        Runner.ensurePromise(i);
                        ((t, i) => {
                            i.then((i => {
                                t.value = i;
                                Runner.settlePromise(t);
                                t.isDone = true;
                                t.isDoing = false;
                                const s = t.nextToDo();
                                if (s !== null && !t.isExited) {
                                    Runner.process(s);
                                } else {
                                    if (t.root.doneAll || t.isExited) {
                                        Runner.settlePromise(t.root);
                                    }
                                }
                            })).catch((t => {
                                throw t;
                            }));
                        })(t, s);
                    } else {
                        t.isDone = true;
                        t.isDoing = false;
                        if (!t.isExited) {
                            t = t.nextToDo();
                        } else {
                            t = null;
                        }
                    }
                }
            }
        }
        if (i.isCancelled) {
            Runner.settlePromise(i, "reject");
        } else if (i.doneAll || i.isExited) {
            Runner.settlePromise(i);
        }
    }
    static ensurePromise(t) {
        if (t.finally === null) {
            t.finally = new OpenPromise;
            t.promise = t.finally.promise;
            return true;
        }
        return false;
    }
    static settlePromise(t, i = "resolve") {
        if (t.finally?.isPending ?? false) {
            t.promise = null;
            switch (i) {
              case "resolve":
                t.finally?.resolve(t.result);
                break;

              case "reject":
                t.finally?.reject(t.result);
                break;
            }
        }
    }
}

Runner.runners = new WeakMap;

Runner.roots = {};

class Step {
    constructor(t = void 0, i = false) {
        this.step = t;
        this.runParallel = i;
        this.promise = null;
        this.previous = null;
        this.next = null;
        this.parent = null;
        this.child = null;
        this.current = null;
        this.finally = null;
        this.isDoing = false;
        this.isDone = false;
        this.isCancelled = false;
        this.isExited = false;
        this.exited = null;
        this.id = "-1";
        this.id = `${Step.id++}`;
    }
    get isParallelParent() {
        return this.child?.runParallel ?? false;
    }
    get result() {
        if (this.promise !== null) {
            return this.promise;
        }
        if (this.child !== null) {
            if (this.isParallelParent) {
                const t = [];
                let i = this.child;
                while (i !== null) {
                    t.push(i.result);
                    i = i.next;
                }
                return t;
            } else {
                return this === this.root && this.exited !== null ? this.exited.result : this.child?.tail?.result;
            }
        }
        let t = this.value;
        while (t instanceof Step) {
            t = t.result;
        }
        return t;
    }
    get asValue() {
        return this.result;
    }
    get previousValue() {
        return this.runParallel ? this.head.parent?.parent?.previous?.result : this.previous?.result;
    }
    get name() {
        let t = `${this.id}`;
        if (this.runParallel) {
            t = `:${t}`;
        }
        if (this.value instanceof Promise || this.promise instanceof Promise) {
            t = `${t}*`;
        }
        if (this.finally !== null) {
            t = `${t}*`;
        }
        if (this.child !== null) {
            t = `${t}>`;
        }
        if (this.isDone) {
            t = `(${t})`;
        }
        return t;
    }
    get root() {
        let t = this.head;
        while (t.parent !== null) {
            t = t.parent.head;
        }
        return t;
    }
    get head() {
        let t = this;
        while (t.previous !== null) {
            t = t.previous;
        }
        return t;
    }
    get tail() {
        let t = this;
        while (t.next !== null) {
            t = t.next;
        }
        return t;
    }
    get done() {
        if (!this.isDone) {
            return false;
        }
        let t = this.child;
        while (t !== null) {
            if (!t.done) {
                return false;
            }
            t = t.next;
        }
        return true;
    }
    get doneAll() {
        if (!this.isDone || this.child !== null && !this.child.doneAll || this.next !== null && !this.next.doneAll) {
            return false;
        }
        return true;
    }
    cancel(t = true) {
        if (t) {
            return this.root.cancel(false);
        }
        if (this.isCancelled) {
            return false;
        }
        this.isCancelled = true;
        this.child?.cancel(false);
        this.next?.cancel(false);
        return true;
    }
    exit(t = true) {
        if (t) {
            this.root.exited = this;
            return this.root.exit(false);
        }
        if (this.isExited) {
            return false;
        }
        this.isExited = true;
        this.child?.exit(false);
        this.next?.exit(false);
        return true;
    }
    nextToDo() {
        if (this.child !== null && !this.child.isDoing && !this.child.isDone) {
            return this.child;
        }
        if (this.runParallel && !this.head.parent.done) {
            return null;
        }
        return this.nextOrUp();
    }
    nextOrUp() {
        let t = this.next;
        while (t !== null) {
            if (!t.isDoing && !t.isDone) {
                return t;
            }
            t = t.next;
        }
        const i = this.head.parent ?? null;
        if (i === null || !i.done) {
            return null;
        }
        return i.nextOrUp();
    }
    get path() {
        return `${this.head.parent?.path ?? ""}/${this.name}`;
    }
    get tree() {
        let t = "";
        let i = this.head;
        let s = i.parent;
        let e = "";
        while (s !== null) {
            e = `${s.path}${e}`;
            s = s.head.parent;
        }
        do {
            t += `${e}/${i.name}\n`;
            if (i === this) {
                break;
            }
            i = i.next;
        } while (i !== null);
        return t;
    }
    get report() {
        let t = `${this.path}\n`;
        t += this.child?.report ?? "";
        t += this.next?.report ?? "";
        return t;
    }
}

Step.id = 0;

class Route {
    constructor(t, i, s, e, n, r, o, h) {
        this.path = t;
        this.id = i;
        this.redirectTo = s;
        this.instructions = e;
        this.caseSensitive = n;
        this.title = r;
        this.reloadBehavior = o;
        this.data = h;
    }
    static isConfigured(t) {
        return N.hasOwn(Route.resourceKey, t) || "parameters" in t || "title" in t;
    }
    static configure(t, i) {
        const s = Route.create(t, i);
        N.define(Route.resourceKey, s, i);
        return i;
    }
    static getConfiguration(t) {
        const i = N.getOwn(Route.resourceKey, t) ?? {};
        if (Array.isArray(t.parameters)) {
            i.parameters = t.parameters;
        }
        if ("title" in t) {
            i.title = t.title;
        }
        return i instanceof Route ? i : Route.create(i, t);
    }
    static create(t, i = null) {
        if (i !== null) {
            t = Route.transferTypeToComponent(t, i);
        }
        if (u.isType(t)) {
            t = Route.getConfiguration(t);
        } else if (i === null) {
            t = {
                ...t
            };
        }
        const s = Route.transferIndividualIntoInstructions(t);
        Route.validateRouteConfiguration(s);
        let e = s.path;
        if (Array.isArray(e)) {
            e = e.join(",");
        }
        return new Route(s.path ?? "", s.id ?? e ?? null, s.redirectTo ?? null, s.instructions ?? null, s.caseSensitive ?? false, s.title ?? null, s.reloadBehavior ?? null, s.data ?? null);
    }
    static transferTypeToComponent(t, i) {
        if (u.isType(t)) {
            throw new Error(`Invalid route configuration: A component ` + `can't be specified in a component route configuration.`);
        }
        const s = {
            ...t
        };
        if ("component" in s || "instructions" in s) {
            throw new Error(`Invalid route configuration: The 'component' and 'instructions' properties ` + `can't be specified in a component route configuration.`);
        }
        if (!("redirectTo" in s)) {
            s.component = i;
        }
        if (!("path" in s) && !("redirectTo" in s)) {
            s.path = u.getDefinition(i).name;
        }
        return s;
    }
    static transferIndividualIntoInstructions(t) {
        if (t === null || t === void 0) {
            throw new Error(`Invalid route configuration: expected an object.`);
        }
        if ((t.component ?? null) !== null || (t.viewport ?? null) !== null || (t.parameters ?? null) !== null || (t.children ?? null) !== null) {
            if (t.instructions != null) {
                throw new Error(`Invalid route configuration: The 'instructions' property can't be used together with ` + `the 'component', 'viewport', 'parameters' or 'children' properties.`);
            }
            t.instructions = [ {
                component: t.component,
                viewport: t.viewport,
                parameters: t.parameters,
                children: t.children
            } ];
        }
        return t;
    }
    static validateRouteConfiguration(t) {
        if (t.redirectTo === null && t.instructions === null) {
            throw new Error(`Invalid route configuration: either 'redirectTo' or 'instructions' ` + `need to be specified.`);
        }
    }
}

Route.resourceKey = t("route");

const A = {
    name: /*@__PURE__*/ t("routes"),
    isConfigured(t) {
        return N.hasOwn(A.name, t) || "routes" in t;
    },
    configure(t, i) {
        const s = t.map((t => Route.create(t)));
        N.define(A.name, s, i);
        return i;
    },
    getConfiguration(t) {
        const i = t;
        const s = [];
        const e = N.getOwn(A.name, t);
        if (Array.isArray(e)) {
            s.push(...e);
        }
        if (Array.isArray(i.routes)) {
            s.push(...i.routes);
        }
        return s.map((t => t instanceof Route ? t : Route.create(t)));
    }
};

function routes(t) {
    return function(i) {
        return A.configure(t, i);
    };
}

class ViewportScopeContent extends EndpointContent {}

class ViewportScope extends V {
    constructor(t, i, s, e, n, r = null, o = {
        catches: [],
        source: null
    }) {
        super(t, i, s);
        this.rootComponentType = r;
        this.options = o;
        this.instruction = null;
        this.available = true;
        this.sourceItem = null;
        this.sourceItemIndex = -1;
        this.remove = false;
        this.add = false;
        this.contents.push(new ViewportScopeContent(t, this, e, n));
        if (this.catches.length > 0) {
            this.instruction = RoutingInstruction.create(this.catches[0], this.name);
        }
    }
    get isEmpty() {
        return this.instruction === null;
    }
    get passThroughScope() {
        return this.rootComponentType === null && this.catches.length === 0;
    }
    get siblings() {
        const t = this.connectedScope.parent;
        if (t === null) {
            return [ this ];
        }
        return t.enabledChildren.filter((t => t.isViewportScope && t.endpoint.name === this.name)).map((t => t.endpoint));
    }
    get source() {
        return this.options.source ?? null;
    }
    get catches() {
        let t = this.options.catches ?? [];
        if (typeof t === "string") {
            t = t.split(",");
        }
        return t;
    }
    get default() {
        if (this.catches.length > 0) {
            return this.catches[0];
        }
    }
    toString() {
        const t = this.instruction?.component.name ?? "";
        const i = this.getNextContent()?.instruction.component.name ?? "";
        return `vs:${this.name}[${t}->${i}]`;
    }
    setNextContent(t, i) {
        t.endpoint.set(this);
        this.remove = t.isClear(this.router) || t.isClearAll(this.router);
        this.add = t.isAdd(this.router) && Array.isArray(this.source);
        if (this.add) {
            t.component.name = null;
        }
        if (this.default !== void 0 && t.component.name === null) {
            t.component.name = this.default;
        }
        this.contents.push(new ViewportScopeContent(this.router, this, this.owningScope, this.scope.hasScope, t, i));
        return "swap";
    }
    transition(t) {
        Runner.run(null, (i => t.setEndpointStep(this, i.root)), (() => t.addEndpointState(this, "guardedUnload")), (() => t.addEndpointState(this, "guardedLoad")), (() => t.addEndpointState(this, "guarded")), (() => t.addEndpointState(this, "loaded")), (() => t.addEndpointState(this, "unloaded")), (() => t.addEndpointState(this, "routed")), (() => t.addEndpointState(this, "swapped")), (() => t.addEndpointState(this, "completed")));
    }
    finalizeContentChange(t, i) {
        const s = this.contents.findIndex((i => i.navigation === t.navigation));
        let e = this.contents[s];
        if (this.remove) {
            const t = new ViewportScopeContent(this.router, this, this.owningScope, this.scope.hasScope);
            this.contents.splice(s, 1, t);
            e.delete();
            e = t;
        }
        e.completed = true;
        let n = 0;
        for (let t = 0, i = s; t < i; t++) {
            if (!(this.contents[0].navigation.completed ?? false)) {
                break;
            }
            n++;
        }
        this.contents.splice(0, n);
        if (this.remove && Array.isArray(this.source)) {
            this.removeSourceItem();
        }
    }
    cancelContentChange(t, i = null) {
        [ ...new Set(this.scope.children.map((t => t.endpoint))) ].forEach((s => s.cancelContentChange(t, i)));
        const s = this.contents.findIndex((i => i.navigation === t.navigation));
        if (s < 0) {
            return;
        }
        this.contents.splice(s, 1);
        if (this.add) {
            const t = this.source.indexOf(this.sourceItem);
            this.source.splice(t, 1);
            this.sourceItem = null;
        }
    }
    acceptSegment(t) {
        if (t === null && t === void 0 || t.length === 0) {
            return true;
        }
        if (t === RoutingInstruction.clear(this.router) || t === RoutingInstruction.add(this.router) || t === this.name) {
            return true;
        }
        if (this.catches.length === 0) {
            return true;
        }
        if (this.catches.includes(t)) {
            return true;
        }
        if (this.catches.filter((t => t.includes("*"))).length) {
            return true;
        }
        return false;
    }
    binding() {
        const t = this.source || [];
        if (t.length > 0 && this.sourceItem === null) {
            this.sourceItem = this.getAvailableSourceItem();
        }
    }
    unbinding() {
        if (this.sourceItem !== null && this.source !== null) {
            arrayRemove(this.source, (t => t === this.sourceItem));
        }
        this.sourceItem = null;
    }
    getAvailableSourceItem() {
        if (this.source === null) {
            return null;
        }
        const t = this.siblings;
        for (const i of this.source) {
            if (t.every((t => t.sourceItem !== i))) {
                return i;
            }
        }
        return null;
    }
    addSourceItem() {
        const t = {};
        this.source.push(t);
        return t;
    }
    removeSourceItem() {
        this.sourceItemIndex = this.source.indexOf(this.sourceItem);
        if (this.sourceItemIndex >= 0) {
            this.source.splice(this.sourceItemIndex, 1);
        }
    }
    getRoutes() {
        const t = [];
        if (this.rootComponentType !== null) {
            const i = this.rootComponentType.constructor === this.rootComponentType.constructor.constructor ? this.rootComponentType : this.rootComponentType.constructor;
            t.push(...A.getConfiguration(i) ?? []);
        }
        return t;
    }
}

class StoredNavigation {
    constructor(t = {
        instruction: "",
        fullStateInstruction: ""
    }) {
        this.instruction = t.instruction;
        this.fullStateInstruction = t.fullStateInstruction;
        this.scope = t.scope;
        this.index = t.index;
        this.firstEntry = t.firstEntry;
        this.path = t.path;
        this.title = t.title;
        this.query = t.query;
        this.fragment = t.fragment;
        this.parameters = t.parameters;
        this.data = t.data;
    }
    toStoredNavigation() {
        return {
            instruction: this.instruction,
            fullStateInstruction: this.fullStateInstruction,
            scope: this.scope,
            index: this.index,
            firstEntry: this.firstEntry,
            path: this.path,
            title: this.title,
            query: this.query,
            fragment: this.fragment,
            parameters: this.parameters,
            data: this.data
        };
    }
}

class NavigationFlags {
    constructor() {
        this.first = false;
        this.new = false;
        this.refresh = false;
        this.forward = false;
        this.back = false;
        this.replace = false;
    }
}

class Navigation extends StoredNavigation {
    constructor(t = {
        instruction: "",
        fullStateInstruction: ""
    }) {
        super(t);
        this.navigation = new NavigationFlags;
        this.repeating = false;
        this.previous = null;
        this.fromBrowser = false;
        this.origin = null;
        this.replacing = false;
        this.refreshing = false;
        this.untracked = false;
        this.process = null;
        this.completed = true;
        this.fromBrowser = t.fromBrowser ?? this.fromBrowser;
        this.origin = t.origin ?? this.origin;
        this.replacing = t.replacing ?? this.replacing;
        this.refreshing = t.refreshing ?? this.refreshing;
        this.untracked = t.untracked ?? this.untracked;
        this.historyMovement = t.historyMovement ?? this.historyMovement;
        this.process = null;
        this.timestamp = Date.now();
    }
    get useFullStateInstruction() {
        return (this.navigation.back ?? false) || (this.navigation.forward ?? false) || (this.navigation.refresh ?? false);
    }
    static create(t = {
        instruction: "",
        fullStateInstruction: ""
    }) {
        return new Navigation(t);
    }
}

class AwaitableMap {
    constructor() {
        this.map = new Map;
    }
    set(t, i) {
        const s = this.map.get(t);
        if (s instanceof OpenPromise) {
            s.resolve(i);
        }
        this.map.set(t, i);
    }
    delete(t) {
        const i = this.map.get(t);
        if (i instanceof OpenPromise) {
            i.reject();
        }
        this.map.delete(t);
    }
    await(t) {
        if (!this.map.has(t)) {
            const i = new OpenPromise;
            this.map.set(t, i);
            return i.promise;
        }
        const i = this.map.get(t);
        if (i instanceof OpenPromise) {
            return i.promise;
        }
        return i;
    }
    has(t) {
        return this.map.has(t) && !(this.map.get(t) instanceof OpenPromise);
    }
    clone() {
        const t = new AwaitableMap;
        t.map = new Map(this.map);
        return t;
    }
}

const createMappedError = (t, ...i) => new Error(`AUR${String(t).padStart(4, "0")}:${i.map(String)}`);

class ViewportContent extends EndpointContent {
    constructor(t, i, s, e, n = RoutingInstruction.create(""), r = Navigation.create({
        instruction: "",
        fullStateInstruction: ""
    }), o = null) {
        super(t, i, s, e, n, r);
        this.router = t;
        this.instruction = n;
        this.navigation = r;
        this.contentStates = new AwaitableMap;
        this.fromCache = false;
        this.fromHistory = false;
        this.reload = false;
        this.activatedResolve = null;
        if (!this.instruction.component.isType() && o?.container != null) {
            this.instruction.component.type = this.toComponentType(o.container);
        }
    }
    get componentInstance() {
        return this.instruction.component.instance;
    }
    get reloadBehavior() {
        if (this.instruction.route instanceof FoundRoute && this.instruction.route.match?.reloadBehavior !== null) {
            return this.instruction.route.match?.reloadBehavior;
        }
        return this.instruction.component.instance !== null && "reloadBehavior" in this.instruction.component.instance && this.instruction.component.instance.reloadBehavior !== void 0 ? this.instruction.component.instance.reloadBehavior : "default";
    }
    get controller() {
        return this.instruction.component.instance?.$controller;
    }
    equalComponent(t) {
        return this.instruction.sameComponent(this.router, t.instruction);
    }
    equalParameters(t) {
        return this.instruction.sameComponent(this.router, t.instruction, true) && (this.navigation.query ?? "") === (t.navigation.query ?? "");
    }
    isCacheEqual(t) {
        return this.instruction.sameComponent(this.router, t.instruction, true);
    }
    contentController(t) {
        return l.$el(t.container.createChild(), this.instruction.component.instance, t.element, null);
    }
    createComponent(t, i, s) {
        if (this.contentStates.has("created")) {
            return;
        }
        if (!this.fromCache && !this.fromHistory) {
            try {
                this.instruction.component.set(this.toComponentInstance(t.container, t.controller, t.element));
            } catch (e) {
                if (!e.message.startsWith("AUR0009:")) {
                    throw e;
                }
                if ((i ?? "") !== "") {
                    if (s === "process-children") {
                        this.instruction.parameters.set([ this.instruction.component.name ]);
                    } else {
                        this.instruction.parameters.set([ this.instruction.unparsed ?? this.instruction.component.name ]);
                        this.instruction.nextScopeInstructions = null;
                    }
                    this.instruction.component.set(i);
                    try {
                        this.instruction.component.set(this.toComponentInstance(t.container, t.controller, t.element));
                    } catch (t) {
                        if (!t.message.startsWith("AUR0009:")) {
                            throw t;
                        }
                        const i = this.instruction.component.name;
                        throw createMappedError(2e3, i, t);
                    }
                } else {
                    const t = this.instruction.component.name;
                    throw createMappedError(2e3, t);
                }
            }
        }
        this.contentStates.set("created", void 0);
    }
    canLoad() {
        if (!this.contentStates.has("created") || this.contentStates.has("checkedLoad") && !this.reload) {
            return true;
        }
        const t = this.instruction.component.instance;
        if (t == null) {
            return true;
        }
        this.contentStates.set("checkedLoad", void 0);
        const i = this.endpoint.parentViewport?.getTimeContent(this.navigation.timestamp)?.instruction?.typeParameters(this.router);
        const s = this.instruction.typeParameters(this.router);
        const e = {
            ...this.navigation.parameters,
            ...i,
            ...s
        };
        const n = this.h(t, "canLoad").map((i => s => {
            if (s?.previousValue === false) {
                return false;
            }
            return i(t, e, this.instruction, this.navigation);
        }));
        if (t.canLoad != null) {
            n.push((i => {
                if (i?.previousValue === false) {
                    return false;
                }
                return t.canLoad(e, this.instruction, this.navigation);
            }));
        }
        if (n.length === 0) {
            return true;
        }
        if (n.length === 1) {
            return n[0](null);
        }
        return Runner.run(null, ...n);
    }
    canUnload(t) {
        if (this.contentStates.has("checkedUnload") && !this.reload) {
            return true;
        }
        this.contentStates.set("checkedUnload", void 0);
        if (!this.contentStates.has("loaded")) {
            return true;
        }
        const i = this.instruction.component.instance;
        if (t === null) {
            t = Navigation.create({
                instruction: "",
                fullStateInstruction: "",
                previous: this.navigation
            });
        }
        const s = this.h(i, "canUnload").map((s => e => {
            if (e?.previousValue === false) {
                return false;
            }
            return s(i, this.instruction, t);
        }));
        if (i.canUnload != null) {
            s.push((s => {
                if (s?.previousValue === false) {
                    return false;
                }
                const e = i.canUnload?.(this.instruction, t);
                return e instanceof Promise ? e.then(Boolean) : Boolean(e);
            }));
        }
        if (s.length === 0) {
            return true;
        }
        if (s.length === 1) {
            return s[0](null);
        }
        return Runner.run(null, ...s);
    }
    load(t) {
        return Runner.run(t, (() => this.contentStates.await("checkedLoad")), (() => {
            if (!this.contentStates.has("created") || this.contentStates.has("loaded") && !this.reload) {
                return;
            }
            this.reload = false;
            this.contentStates.set("loaded", void 0);
            const t = this.instruction.component.instance;
            const i = this.endpoint.parentViewport?.getTimeContent(this.navigation.timestamp)?.instruction?.typeParameters(this.router);
            const s = this.instruction.typeParameters(this.router);
            const e = {
                ...this.navigation.parameters,
                ...i,
                ...s
            };
            const n = this.h(t, "loading").map((i => () => i(t, e, this.instruction, this.navigation)));
            n.push(...this.h(t, "load").map((i => () => {
                console.warn(`[Deprecated] Found deprecated hook name "load" in ${this.instruction.component.name}. Please use the new name "loading" instead.`);
                return i(t, e, this.instruction, this.navigation);
            })));
            if (n.length !== 0) {
                if (typeof t.loading === "function") {
                    n.push((() => t.loading(e, this.instruction, this.navigation)));
                }
                if (hasVmHook(t, "load")) {
                    console.warn(`[Deprecated] Found deprecated hook name "load" in ${this.instruction.component.name}. Please use the new name "loading" instead.`);
                    n.push((() => t.load(e, this.instruction, this.navigation)));
                }
                return Runner.run(null, ...n);
            }
            if (hasVmHook(t, "loading")) {
                return t.loading(e, this.instruction, this.navigation);
            }
            if (hasVmHook(t, "load")) {
                console.warn(`[Deprecated] Found deprecated hook name "load" in ${this.instruction.component.name}. Please use the new name "loading" instead.`);
                return t.load(e, this.instruction, this.navigation);
            }
        }));
    }
    unload(t) {
        if (!this.contentStates.has("loaded")) {
            return;
        }
        this.contentStates.delete("loaded");
        const i = this.instruction.component.instance;
        if (t === null) {
            t = Navigation.create({
                instruction: "",
                fullStateInstruction: "",
                previous: this.navigation
            });
        }
        const s = this.h(i, "unloading").map((s => () => s(i, this.instruction, t)));
        s.push(...this.h(i, "unload").map((s => () => {
            console.warn(`[Deprecated] Found deprecated hook name "unload" in ${this.instruction.component.name}. Please use the new name "unloading" instead.`);
            return s(i, this.instruction, t);
        })));
        if (s.length !== 0) {
            if (hasVmHook(i, "unloading")) {
                s.push((() => i.unloading(this.instruction, t)));
            }
            if (hasVmHook(i, "unload")) {
                console.warn(`[Deprecated] Found deprecated hook name "unload" in ${this.instruction.component.name}. Please use the new name "unloading" instead.`);
                s.push((() => i.unload(this.instruction, t)));
            }
            return Runner.run(null, ...s);
        }
        if (hasVmHook(i, "unloading")) {
            return i.unloading(this.instruction, t);
        }
        if (hasVmHook(i, "unload")) {
            console.warn(`[Deprecated] Found deprecated hook name "unload" in ${this.instruction.component.name}. Please use the new name "unloading" instead.`);
            return i.unload(this.instruction, t);
        }
    }
    activateComponent(t, i, s, e, n, r) {
        return Runner.run(t, (() => this.contentStates.await("loaded")), (() => this.waitForParent(s)), (() => {
            if (this.contentStates.has("activating") || this.contentStates.has("activated")) {
                return;
            }
            this.contentStates.set("activating", void 0);
            return this.controller?.activate(i ?? this.controller, s, void 0);
        }), (() => {
            this.contentStates.set("activated", void 0);
        }));
    }
    deactivateComponent(t, i, s, e, n = false) {
        if (!this.contentStates.has("activated") && !this.contentStates.has("activating")) {
            return;
        }
        return Runner.run(t, (() => {
            if (n && e.element !== null) {
                const t = Array.from(e.element.getElementsByTagName("*"));
                for (const i of t) {
                    if (i.scrollTop > 0 || i.scrollLeft) {
                        i.setAttribute("au-element-scroll", `${i.scrollTop},${i.scrollLeft}`);
                    }
                }
            }
            this.contentStates.delete("activated");
            this.contentStates.delete("activating");
            return this.controller?.deactivate(i ?? this.controller, s);
        }));
    }
    disposeComponent(t, i, s = false) {
        if (!this.contentStates.has("created") || this.instruction.component.instance == null) {
            return;
        }
        if (!s) {
            this.contentStates.delete("created");
            return this.controller?.dispose();
        } else {
            i.push(this);
        }
    }
    freeContent(t, i, s, e, n = false) {
        return Runner.run(t, (() => this.unload(s)), (t => this.deactivateComponent(t, null, i.controller, i, n)), (() => this.disposeComponent(i, e, n)));
    }
    toComponentName() {
        return this.instruction.component.name;
    }
    toComponentType(t) {
        if (this.instruction.component.none) {
            return null;
        }
        return this.instruction.component.toType(t, this.instruction);
    }
    toComponentInstance(t, i, s) {
        if (this.instruction.component.none) {
            return null;
        }
        return this.instruction.component.toInstance(t, i, s, this.instruction);
    }
    waitForParent(t) {
        if (t === null) {
            return;
        }
        if (!t.isActive) {
            return new Promise((t => {
                this.endpoint.activeResolve = t;
            }));
        }
    }
    h(t, i) {
        const s = t.$controller.lifecycleHooks[i] ?? [];
        return s.map((t => t.instance[i].bind(t.instance)));
    }
}

function hasVmHook(t, i) {
    return typeof t[i] === "function";
}

class ViewportOptions {
    constructor(t = true, i = [], s = "", e = "", n = "", r = false, o = false, h = false, u = false, a = false) {
        this.scope = t;
        this.usedBy = i;
        this.fallback = e;
        this.fallbackAction = n;
        this.noLink = r;
        this.noTitle = o;
        this.stateful = h;
        this.forceDescription = u;
        this.noHistory = a;
        this.default = undefined;
        this.default = s;
    }
    static create(t) {
        const i = new ViewportOptions;
        if (t !== void 0) {
            i.apply(t);
        }
        return i;
    }
    apply(t) {
        this.scope = t.scope ?? this.scope;
        this.usedBy = (typeof t.usedBy === "string" ? t.usedBy.split(",").filter((t => t.length > 0)) : t.usedBy) ?? this.usedBy;
        this.default = t.default ?? this.default;
        this.fallback = t.fallback ?? this.fallback;
        this.fallbackAction = t.fallbackAction ?? this.fallbackAction;
        this.noLink = t.noLink ?? this.noLink;
        this.noTitle = t.noTitle ?? this.noTitle;
        this.stateful = t.stateful ?? this.stateful;
        this.forceDescription = t.forceDescription ?? this.forceDescription;
        this.noHistory = t.noHistory ?? this.noHistory;
    }
}

class Viewport extends V {
    constructor(t, i, s, e, n, r) {
        super(t, i, s);
        this.contents = [];
        this.forceRemove = false;
        this.options = new ViewportOptions;
        this.activeResolve = null;
        this.connectionResolve = null;
        this.clear = false;
        this.coordinators = [];
        this.previousViewportState = null;
        this.cache = [];
        this.historyCache = [];
        this.contents.push(new ViewportContent(t, this, e, n));
        this.contents[0].completed = true;
        if (r !== void 0) {
            this.options.apply(r);
        }
    }
    getContent() {
        if (this.contents.length === 1) {
            return this.contents[0];
        }
        let t;
        for (let i = 0, s = this.contents.length; i < s; i++) {
            if (this.contents[i].completed ?? false) {
                t = this.contents[i];
            } else {
                break;
            }
        }
        return t;
    }
    getNextContent() {
        if (this.contents.length === 1) {
            return null;
        }
        const t = this.contents.indexOf(this.getContent());
        return this.contents.length > t ? this.contents[t + 1] : null;
    }
    getTimeContent(t) {
        let i = null;
        for (let s = 0, e = this.contents.length; s < e; s++) {
            if (this.contents[s].navigation.timestamp > t) {
                break;
            }
            i = this.contents[s];
        }
        return i;
    }
    getNavigationContent(t) {
        return super.getNavigationContent(t);
    }
    get parentViewport() {
        let t = this.connectedScope;
        while (t?.parent != null) {
            t = t.parent;
            if (t.endpoint.isViewport) {
                return t.endpoint;
            }
        }
        return null;
    }
    get isEmpty() {
        return this.getContent().componentInstance === null;
    }
    get doForceRemove() {
        let t = this.connectedScope;
        while (t !== null) {
            if (t.isViewport && t.endpoint.forceRemove) {
                return true;
            }
            t = t.parent;
        }
        return false;
    }
    isActiveNavigation(t) {
        return this.coordinators[this.coordinators.length - 1] === t;
    }
    toString() {
        const t = this.getContent()?.instruction.component.name ?? "";
        const i = this.getNextContent()?.instruction.component.name ?? "";
        return `v:${this.name}[${t}->${i}]`;
    }
    setNextContent(t, i) {
        t.endpoint.set(this);
        this.clear = t.isClear(this.router);
        const s = this.getContent();
        const e = new ViewportContent(this.router, this, this.owningScope, this.scope.hasScope, !this.clear ? t : void 0, i, this.connectedCE ?? null);
        this.contents.push(e);
        e.fromHistory = e.componentInstance !== null && i.navigation != null ? !!i.navigation.back || !!i.navigation.forward : false;
        if (this.options.stateful) {
            const t = this.cache.find((t => e.isCacheEqual(t)));
            if (t !== void 0) {
                this.contents.splice(this.contents.indexOf(e), 1, t);
                e.fromCache = true;
            } else {
                this.cache.push(e);
            }
        }
        if (e.componentInstance !== null && s.componentInstance === e.componentInstance) {
            e.delete();
            this.contents.splice(this.contents.indexOf(e), 1);
            return this.transitionAction = "skip";
        }
        if (!s.equalComponent(e) || i.navigation.refresh || s.reloadBehavior === "refresh") {
            return this.transitionAction = "swap";
        }
        if (s.reloadBehavior === "disallow") {
            e.delete();
            this.contents.splice(this.contents.indexOf(e), 1);
            return this.transitionAction = "skip";
        }
        if (s.reloadBehavior === "reload") {
            s.reload = true;
            e.instruction.component.set(s.componentInstance);
            e.contentStates = s.contentStates.clone();
            e.reload = s.reload;
            return this.transitionAction = "reload";
        }
        if (this.options.stateful && s.equalParameters(e)) {
            e.delete();
            this.contents.splice(this.contents.indexOf(e), 1);
            return this.transitionAction = "skip";
        }
        if (!s.equalParameters(e)) {
            {
                return this.transitionAction = "swap";
            }
        }
        e.delete();
        this.contents.splice(this.contents.indexOf(e), 1);
        return this.transitionAction = "skip";
    }
    setConnectedCE(t, i) {
        i = i ?? {};
        if (this.connectedCE !== t) {
            this.previousViewportState = {
                ...this
            };
            this.clearState();
            this.connectedCE = t;
            this.options.apply(i);
            this.connectionResolve?.();
        }
        const s = (this.scope.parent?.endpoint.getRoutes() ?? []).filter((t => (Array.isArray(t.path) ? t.path : [ t.path ]).includes(""))).length > 0;
        if (this.getContent().componentInstance === null && this.getNextContent()?.componentInstance == null && (this.options.default || s)) {
            const t = RoutingInstruction.parse(this.router, this.options.default ?? "");
            if (t.length === 0 && s) {
                const i = this.scope.parent?.findInstructions([ RoutingInstruction.create("") ], false, this.router.configuration.options.useConfiguredRoutes);
                if (i?.foundConfiguration) {
                    t.push(...i.instructions);
                }
            }
            for (const i of t) {
                i.endpoint.set(this);
                i.scope = this.owningScope;
                i.default = true;
            }
            this.router.load(t, {
                append: true
            }).catch((t => {
                throw t;
            }));
        }
    }
    remove(t, i) {
        if (this.connectedCE === i) {
            return Runner.run(t, (t => {
                if (this.getContent().componentInstance !== null) {
                    return this.getContent().freeContent(t, this.connectedCE, this.getNextContent()?.navigation ?? null, this.historyCache, this.doForceRemove ? false : this.router.statefulHistory || this.options.stateful);
                }
            }), (t => {
                if (this.doForceRemove) {
                    const i = [];
                    for (const t of this.historyCache) {
                        i.push((i => t.freeContent(i, null, null, this.historyCache, false)));
                    }
                    i.push((() => {
                        this.historyCache = [];
                    }));
                    return Runner.run(t, ...i);
                }
                return true;
            }));
        }
        return false;
    }
    async transition(t) {
        const i = this.router.configuration.options.indicators.viewportNavigating;
        this.coordinators.push(t);
        while (this.coordinators[0] !== t) {
            await this.coordinators[0].waitForSyncState("completed");
        }
        let s = this.parentViewport;
        if (s !== null && s.transitionAction !== "reload" && s.transitionAction !== "swap") {
            s = null;
        }
        const e = [ i => {
            if (this.isActiveNavigation(t)) {
                return this.canUnload(t, i);
            }
        }, i => {
            if (this.isActiveNavigation(t)) {
                if (!i.previousValue) {
                    t.cancel();
                } else {
                    if (this.router.isRestrictedNavigation) {
                        const i = this.router.configuration.options;
                        this.getNavigationContent(t).createComponent(this.connectedCE, this.options.fallback || i.fallback, this.options.fallbackAction || i.fallbackAction);
                    }
                }
            }
            t.addEndpointState(this, "guardedUnload");
        }, () => t.waitForSyncState("guardedUnload", this), () => s !== null ? t.waitForEndpointState(s, "guardedLoad") : void 0, i => {
            if (this.isActiveNavigation(t)) {
                return this.canLoad(t, i);
            }
        }, i => {
            if (this.isActiveNavigation(t)) {
                let s = i.previousValue;
                if (typeof s === "boolean") {
                    if (!s) {
                        i.cancel();
                        t.cancel();
                        this.getNavigationContent(t).instruction.nextScopeInstructions = null;
                        return;
                    }
                } else {
                    this.getNavigationContent(t).instruction.nextScopeInstructions = null;
                    if (typeof s === "string") {
                        const t = this.scope;
                        const i = this.router.configuration.options;
                        let e = RoutingInstruction.parse(this.router, s);
                        const n = t.parent?.findInstructions(e, i.useDirectRouting, i.useConfiguredRoutes);
                        if (n?.foundConfiguration || n?.foundInstructions) {
                            e = n.instructions;
                        }
                        for (const i of e) {
                            i.endpoint.set(this);
                            i.scope = t.owningScope;
                        }
                        s = e;
                    }
                    return Runner.run(i, (i => this.cancelContentChange(t, i)), (t => {
                        void this.router.load(s, {
                            append: true
                        });
                        return t.exit();
                    }));
                }
            }
            t.addEndpointState(this, "guardedLoad");
            t.addEndpointState(this, "guarded");
        } ];
        const n = [ () => t.waitForSyncState("guarded", this), i => {
            if (this.isActiveNavigation(t)) {
                return this.unload(t, i);
            }
        }, () => t.addEndpointState(this, "unloaded"), () => t.waitForSyncState("unloaded", this), () => s !== null ? t.waitForEndpointState(s, "loaded") : void 0, i => {
            if (this.isActiveNavigation(t)) {
                return this.load(t, i);
            }
        }, () => t.addEndpointState(this, "loaded"), () => t.addEndpointState(this, "routed") ];
        const r = [ () => t.waitForSyncState("routed", this), () => t.waitForEndpointState(this, "routed") ];
        const o = this.router.configuration.options.swapOrder;
        switch (o) {
          case "detach-current-attach-next":
            r.push((i => {
                if (this.isActiveNavigation(t)) {
                    return this.removeContent(i, t);
                }
            }), (i => {
                if (this.isActiveNavigation(t)) {
                    return this.addContent(i, t);
                }
            }));
            break;

          case "attach-next-detach-current":
            r.push((i => {
                if (this.isActiveNavigation(t)) {
                    return this.addContent(i, t);
                }
            }), (i => {
                if (this.isActiveNavigation(t)) {
                    return this.removeContent(i, t);
                }
            }));
            break;

          case "detach-attach-simultaneously":
            r.push((i => Runner.runParallel(i, (i => {
                if (this.isActiveNavigation(t)) {
                    return this.removeContent(i, t);
                }
            }), (i => {
                if (this.isActiveNavigation(t)) {
                    return this.addContent(i, t);
                }
            }))));
            break;

          case "attach-detach-simultaneously":
            r.push((i => Runner.runParallel(i, (i => {
                if (this.isActiveNavigation(t)) {
                    return this.addContent(i, t);
                }
            }), (i => {
                if (this.isActiveNavigation(t)) {
                    return this.removeContent(i, t);
                }
            }))));
            break;
        }
        r.push((() => t.addEndpointState(this, "swapped")));
        this.connectedCE?.setActivity?.(i, true);
        this.connectedCE?.setActivity?.(t.navigation.navigation, true);
        const h = Runner.run(null, (i => t.setEndpointStep(this, i.root)), ...e, ...n, ...r, (() => t.addEndpointState(this, "completed")), (() => t.waitForSyncState("bound")), (() => {
            this.connectedCE?.setActivity?.(i, false);
            this.connectedCE?.setActivity?.(t.navigation.navigation, false);
        }));
        if (h instanceof Promise) {
            h.catch((t => {}));
        }
    }
    canUnload(t, i) {
        return Runner.run(i, (i => this.getContent().connectedScope.canUnload(t, i)), (i => {
            if (!i.previousValue) {
                return false;
            }
            return this.getContent().canUnload(t.navigation);
        }));
    }
    canLoad(t, i) {
        if (this.clear) {
            return true;
        }
        return Runner.run(i, (() => this.waitForConnected()), (() => {
            const i = this.router.configuration.options;
            const s = this.getNavigationContent(t);
            s.createComponent(this.connectedCE, this.options.fallback || i.fallback, this.options.fallbackAction || i.fallbackAction);
            return s.canLoad();
        }));
    }
    load(t, i) {
        if (this.clear) {
            return;
        }
        return this.getNavigationContent(t).load(i);
    }
    addContent(t, i) {
        return this.activate(t, null, this.connectedController, i);
    }
    removeContent(t, i) {
        if (this.isEmpty) {
            return;
        }
        const s = this.router.statefulHistory || (this.options.stateful ?? false);
        return Runner.run(t, (() => i.addEndpointState(this, "bound")), (() => i.waitForSyncState("bound")), (t => this.deactivate(t, null, this.connectedController)), (() => s ? this.dispose() : void 0));
    }
    activate(t, i, s, e) {
        if (this.activeContent.componentInstance !== null) {
            return Runner.run(t, (() => this.activeContent.canLoad()), (t => this.activeContent.load(t)), (t => this.activeContent.activateComponent(t, i, s, this.connectedCE, (() => e?.addEndpointState(this, "bound")), e?.waitForSyncState("bound"))));
        }
    }
    deactivate(t, i, s) {
        const e = this.getContent();
        if (e?.componentInstance != null && !e.reload && e.componentInstance !== this.getNextContent()?.componentInstance) {
            return e.deactivateComponent(t, i, s, this.connectedCE, this.router.statefulHistory || this.options.stateful);
        }
    }
    unload(t, i) {
        return Runner.run(i, (i => this.getContent().connectedScope.unload(t, i)), (() => this.getContent().componentInstance != null ? this.getContent().unload(t.navigation ?? null) : void 0));
    }
    dispose() {
        if (this.getContent().componentInstance !== null && !this.getContent().reload && this.getContent().componentInstance !== this.getNextContent()?.componentInstance) {
            this.getContent().disposeComponent(this.connectedCE, this.historyCache, this.router.statefulHistory || this.options.stateful);
        }
    }
    finalizeContentChange(t, i) {
        const s = this.contents.findIndex((i => i.navigation === t.navigation));
        let e = this.contents[s];
        const n = this.contents[s - 1];
        if (this.clear) {
            const t = new ViewportContent(this.router, this, this.owningScope, this.scope.hasScope, void 0, e.navigation);
            this.contents.splice(s, 1, t);
            e.delete();
            e = t;
        } else {
            e.reload = false;
        }
        n.delete();
        e.completed = true;
        this.transitionAction = "";
        e.contentStates.delete("checkedUnload");
        e.contentStates.delete("checkedLoad");
        this.previousViewportState = null;
        const r = this.router.configuration.options.indicators.viewportNavigating;
        this.connectedCE?.setActivity?.(r, false);
        this.connectedCE?.setActivity?.(t.navigation.navigation, false);
        let o = 0;
        for (let t = 0, i = s; t < i; t++) {
            if (!(this.contents[0].navigation.completed ?? false)) {
                break;
            }
            o++;
        }
        this.contents.splice(0, o);
        arrayRemove(this.coordinators, (i => i === t));
    }
    cancelContentChange(t, i = null) {
        [ ...new Set(this.scope.children.map((t => t.endpoint))) ].forEach((s => s.cancelContentChange(t, i)));
        const s = this.contents.findIndex((i => i.navigation === t.navigation));
        if (s < 0) {
            return;
        }
        const e = t.getEndpointStep(this)?.current ?? null;
        const n = this.contents[s];
        const r = this.contents[s - 1];
        n.instruction.cancelled = true;
        return Runner.run(e, (t => n.freeContent(t, this.connectedCE, n.navigation, this.historyCache, this.router.statefulHistory || this.options.stateful)), (() => {
            if (this.previousViewportState) {
                Object.assign(this, this.previousViewportState);
            }
            n?.delete();
            if (n !== null) {
                this.contents.splice(this.contents.indexOf(n), 1);
            }
            this.transitionAction = "";
            r?.contentStates.delete("checkedUnload");
            r?.contentStates.delete("checkedLoad");
            const i = this.router.configuration.options.indicators.viewportNavigating;
            this.connectedCE?.setActivity?.(i, false);
            this.connectedCE?.setActivity?.(t.navigation.navigation, false);
            t.removeEndpoint(this);
            arrayRemove(this.coordinators, (i => i === t));
        }), (() => {
            if (e !== i) {
                return e?.exit();
            }
        }));
    }
    wantComponent(t) {
        return this.options.usedBy.includes(t);
    }
    acceptComponent(t) {
        if (t === "-" || t === null) {
            return true;
        }
        const i = this.options.usedBy;
        if (i.length === 0) {
            return true;
        }
        if (i.includes(t)) {
            return true;
        }
        if (i.filter((t => t.includes("*"))).length) {
            return true;
        }
        return false;
    }
    freeContent(t, i) {
        const s = this.historyCache.find((t => t.componentInstance === i));
        if (s !== void 0) {
            return Runner.run(t, (t => {
                this.forceRemove = true;
                return s.freeContent(t, null, null, this.historyCache, false);
            }), (() => {
                this.forceRemove = false;
                arrayRemove(this.historyCache, (t => t === s));
            }));
        }
    }
    getRoutes() {
        const t = [];
        let i = this.getComponentType();
        if (i != null) {
            i = i.constructor === i.constructor.constructor ? i : i.constructor;
            t.push(...A.getConfiguration(i) ?? []);
        }
        return t;
    }
    getTitle(t) {
        if (this.options.noTitle) {
            return "";
        }
        const i = this.getComponentType();
        if (i === null) {
            return "";
        }
        let s = "";
        const e = i.title;
        if (e !== void 0) {
            if (typeof e === "string") {
                s = e;
            } else {
                const i = this.getComponentInstance();
                s = e.call(i, i, t);
            }
        } else if (this.router.configuration.options.title.useComponentNames) {
            let t = this.getContentInstruction().component.name ?? "";
            const i = this.router.configuration.options.title.componentPrefix ?? "";
            if (t.startsWith(i)) {
                t = t.slice(i.length);
            }
            t = t.replace("-", " ");
            s = t.slice(0, 1).toLocaleUpperCase() + t.slice(1);
        }
        return s;
    }
    getComponentType() {
        let t = this.getContentInstruction().component.type ?? null;
        if (t === null) {
            const i = u.for(this.connectedCE.element);
            t = i.container.componentType;
        }
        return t ?? null;
    }
    getComponentInstance() {
        return this.getContentInstruction().component.instance ?? null;
    }
    getContentInstruction() {
        return this.getNextContent()?.instruction ?? this.getContent().instruction ?? null;
    }
    clearState() {
        this.options = ViewportOptions.create();
        const t = this.owningScope;
        const i = this.scope.hasScope;
        this.getContent().delete();
        this.contents.shift();
        if (this.contents.length < 1) {
            throw new Error("no content!");
        }
        this.contents.push(new ViewportContent(this.router, this, t, i));
        this.cache = [];
    }
    waitForConnected() {
        if (this.connectedCE === null) {
            return new Promise((t => {
                this.connectionResolve = t;
            }));
        }
    }
}

class InstructionEndpoint {
    constructor() {
        this.name = null;
        this.instance = null;
        this.scope = null;
    }
    get none() {
        return this.name === null && this.instance === null;
    }
    get endpointType() {
        if (this.instance instanceof Viewport) {
            return "Viewport";
        }
        if (this.instance instanceof ViewportScope) {
            return "ViewportScope";
        }
        return null;
    }
    static create(t) {
        const i = new InstructionEndpoint;
        i.set(t);
        return i;
    }
    static isName(t) {
        return typeof t === "string";
    }
    static isInstance(t) {
        return t instanceof V;
    }
    static getName(t) {
        if (InstructionEndpoint.isName(t)) {
            return t;
        } else {
            return t ? t.name : null;
        }
    }
    static getInstance(t) {
        if (InstructionEndpoint.isName(t)) {
            return null;
        } else {
            return t;
        }
    }
    set(t) {
        if (t === undefined || t === "") {
            t = null;
        }
        if (typeof t === "string") {
            this.name = t;
            this.instance = null;
        } else {
            this.instance = t;
            if (t !== null) {
                this.name = t.name;
                this.scope = t.owningScope;
            }
        }
    }
    toInstance(t) {
        if (this.instance !== null) {
            return this.instance;
        }
        return t.getEndpoint(this.endpointType, this.name);
    }
    same(t, i) {
        if (this.instance !== null && t.instance !== null) {
            return this.instance === t.instance;
        }
        return (this.endpointType === null || t.endpointType === null || this.endpointType === t.endpointType) && (!i || this.scope === t.scope) && (this.instance !== null ? this.instance.name : this.name) === (t.instance !== null ? t.instance.name : t.name);
    }
}

class RoutingInstruction {
    constructor(t, i, s) {
        this.ownsScope = true;
        this.nextScopeInstructions = null;
        this.scope = null;
        this.scopeModifier = "";
        this.needsEndpointDescribed = false;
        this.route = null;
        this.routeStart = false;
        this.default = false;
        this.topInstruction = false;
        this.unparsed = null;
        this.cancelled = false;
        this.component = InstructionComponent.create(t);
        this.endpoint = InstructionEndpoint.create(i);
        this.parameters = InstructionParameters.create(s);
    }
    static create(t, i, s, e = true, n = null) {
        const r = new RoutingInstruction(t, i, s);
        r.ownsScope = e;
        r.nextScopeInstructions = n;
        return r;
    }
    static createClear(t, i) {
        return RoutingInstruction.create(RoutingInstruction.clear(t), i);
    }
    static from(t, i) {
        if (!Array.isArray(i)) {
            i = [ i ];
        }
        const s = [];
        for (const e of i) {
            if (typeof e === "string") {
                s.push(...RoutingInstruction.parse(t, e));
            } else if (e instanceof RoutingInstruction) {
                s.push(e);
            } else if (e instanceof Promise) {
                s.push(RoutingInstruction.create(e));
            } else if (InstructionComponent.isAppelation(e)) {
                s.push(RoutingInstruction.create(e));
            } else if (InstructionComponent.isDefinition(e)) {
                s.push(RoutingInstruction.create(e.Type));
            } else if ("component" in e || "id" in e) {
                const i = e;
                const n = RoutingInstruction.create(i.component, i.viewport, i.parameters);
                n.route = e.id ?? null;
                if (i.children !== void 0 && i.children !== null) {
                    n.nextScopeInstructions = RoutingInstruction.from(t, i.children);
                }
                s.push(n);
            } else if (typeof e === "object" && e !== null) {
                const t = u.define(e);
                s.push(RoutingInstruction.create(t));
            } else {
                s.push(RoutingInstruction.create(e));
            }
        }
        return s;
    }
    static clear(t) {
        return Separators.for(t).clear;
    }
    static add(t) {
        return Separators.for(t).add;
    }
    static parse(t, i) {
        const s = Separators.for(t);
        let e = "";
        const n = /^[./]+/.exec(i);
        if (Array.isArray(n) && n.length > 0) {
            e = n[0];
            i = i.slice(e.length);
        }
        const r = InstructionParser.parse(s, i, true, true).instructions;
        for (const t of r) {
            t.scopeModifier = e;
        }
        return r;
    }
    static stringify(t, i, s = false, e = false) {
        return typeof i === "string" ? i : i.map((i => i.stringify(t, s, e))).filter((t => t.length > 0)).join(Separators.for(t).sibling);
    }
    static resolve(t) {
        const i = t.filter((t => t.isUnresolved)).map((t => t.resolve())).filter((t => t instanceof Promise));
        if (i.length > 0) {
            return Promise.all(i);
        }
    }
    static containsSiblings(t, i) {
        if (i === null) {
            return false;
        }
        if (i.filter((i => !i.isClear(t) && !i.isClearAll(t))).length > 1) {
            return true;
        }
        return i.some((i => RoutingInstruction.containsSiblings(t, i.nextScopeInstructions)));
    }
    static flat(t) {
        const i = [];
        for (const s of t) {
            i.push(s);
            if (s.hasNextScopeInstructions) {
                i.push(...RoutingInstruction.flat(s.nextScopeInstructions));
            }
        }
        return i;
    }
    static clone(t, i = false, s = false) {
        return t.map((t => t.clone(i, s)));
    }
    static contains(t, i, s, e) {
        return s.every((s => s.isIn(t, i, e)));
    }
    get viewport() {
        return this.endpoint.instance instanceof Viewport || this.endpoint.endpointType === null ? this.endpoint : null;
    }
    get viewportScope() {
        return this.endpoint.instance instanceof ViewportScope || this.endpoint.endpointType === null ? this.endpoint : null;
    }
    get previous() {
        return this.endpoint.instance?.getContent()?.instruction;
    }
    isAdd(t) {
        return this.component.name === Separators.for(t).add;
    }
    isClear(t) {
        return this.component.name === Separators.for(t).clear;
    }
    isAddAll(t) {
        return this.isAdd(t) && (this.endpoint.name?.length ?? 0) === 0;
    }
    isClearAll(t) {
        return this.isClear(t) && (this.endpoint.name?.length ?? 0) === 0;
    }
    get hasNextScopeInstructions() {
        return (this.nextScopeInstructions?.length ?? 0) > 0;
    }
    get isUnresolved() {
        return this.component.isFunction() || this.component.isPromise();
    }
    resolve() {
        return this.component.resolve(this);
    }
    typeParameters(t) {
        return this.parameters.toSpecifiedParameters(t, this.component.type?.parameters ?? []);
    }
    sameRoute(t) {
        const i = this.route?.match;
        const s = t.route?.match;
        if (i == null || s == null) {
            return false;
        }
        if (typeof i === "string" || typeof s === "string") {
            return i === s;
        }
        return i.id === s.id;
    }
    sameComponent(t, i, s = false, e = false) {
        if (s && !this.sameParameters(t, i, e)) {
            return false;
        }
        return this.component.same(i.component, e);
    }
    sameEndpoint(t, i) {
        return this.endpoint.same(t.endpoint, i);
    }
    sameParameters(t, i, s = false) {
        if (!this.component.same(i.component, s)) {
            return false;
        }
        return this.parameters.same(t, i.parameters, this.component.type);
    }
    stringify(t, i = false, s = false, e = false) {
        const n = Separators.for(t);
        let r = i;
        let o = false;
        if (s) {
            const t = this.viewport?.instance ?? null;
            if (t?.options.noLink ?? false) {
                return "";
            }
            if (!this.needsEndpointDescribed && (!(t?.options.forceDescription ?? false) || this.viewportScope?.instance != null)) {
                r = true;
            }
            if (t?.options.fallback === this.component.name) {
                o = true;
            }
            if (t?.options.default === this.component.name) {
                o = true;
            }
        }
        const h = this.nextScopeInstructions;
        let u = this.scopeModifier;
        if (this.route instanceof FoundRoute && !this.routeStart) {
            return !e && Array.isArray(h) ? RoutingInstruction.stringify(t, h, i, s) : "";
        }
        const a = this.stringifyShallow(t, r, o);
        u += a.endsWith(n.scope) ? a.slice(0, -n.scope.length) : a;
        if (!e && Array.isArray(h) && h.length > 0) {
            const e = RoutingInstruction.stringify(t, h, i, s);
            if (e.length > 0) {
                u += n.scope;
                u += h.length === 1 ? e : `${n.groupStart}${e}${n.groupEnd}`;
            }
        }
        return u;
    }
    clone(t = false, i = false, s = false) {
        const e = RoutingInstruction.create(this.component.func ?? this.component.promise ?? this.component.type ?? this.component.name, this.endpoint.name, this.parameters.typedParameters ?? void 0);
        if (t) {
            e.component.set(this.component.instance ?? this.component.type ?? this.component.name);
            e.endpoint.set(this.endpoint.instance ?? this.endpoint.name);
        }
        e.component.name = this.component.name;
        e.needsEndpointDescribed = this.needsEndpointDescribed;
        e.route = this.route;
        e.routeStart = this.routeStart;
        e.default = this.default;
        if (i) {
            e.scopeModifier = this.scopeModifier;
        }
        e.scope = t ? this.scope : null;
        if (this.hasNextScopeInstructions && !s) {
            e.nextScopeInstructions = RoutingInstruction.clone(this.nextScopeInstructions, t, i);
        }
        return e;
    }
    isIn(t, i, s) {
        const e = i.filter((i => {
            if (this.route != null || i.route != null) {
                if (!i.sameRoute(this)) {
                    return false;
                }
            } else {
                if (!i.sameComponent(t, this)) {
                    return false;
                }
            }
            const s = i.component.type ?? this.component.type;
            const e = this.component.type ?? i.component.type;
            const n = i.parameters.toSpecifiedParameters(t, s?.parameters);
            const r = this.parameters.toSpecifiedParameters(t, e?.parameters);
            if (!InstructionParameters.contains(n, r)) {
                return false;
            }
            return this.endpoint.none || i.sameEndpoint(this, false);
        }));
        if (e.length === 0) {
            return false;
        }
        if (!s || !this.hasNextScopeInstructions) {
            return true;
        }
        if (e.some((i => RoutingInstruction.contains(t, i.nextScopeInstructions ?? [], this.nextScopeInstructions, s)))) {
            return true;
        }
        return false;
    }
    getTitle(t) {
        if (this.route instanceof FoundRoute) {
            const i = this.route.match?.title;
            if (i != null) {
                if (this.routeStart) {
                    return typeof i === "string" ? i : i(this, t);
                } else {
                    return "";
                }
            }
        }
        return this.endpoint.instance.getTitle(t);
    }
    toJSON() {
        return {
            component: this.component.name ?? undefined,
            viewport: this.endpoint.name ?? undefined,
            parameters: this.parameters.parametersRecord ?? undefined,
            children: this.hasNextScopeInstructions ? this.nextScopeInstructions : undefined
        };
    }
    stringifyShallow(t, i = false, s = false) {
        if (this.route != null) {
            const i = this.route instanceof FoundRoute ? this.route.matching : this.route;
            return i.split("/").map((i => i.startsWith(":") ? this.parameters.get(t, i.slice(1)) : i)).join("/");
        }
        const e = Separators.for(t);
        let n = !s ? this.component.name ?? "" : "";
        const r = this.component.type ? this.component.type.parameters : null;
        const o = InstructionParameters.stringify(t, this.parameters.toSortedParameters(t, r));
        if (o.length > 0) {
            n += !s ? `${e.parameters}${o}${e.parametersEnd}` : o;
        }
        if (this.endpoint.name != null && !i) {
            n += `${e.viewport}${this.endpoint.name}`;
        }
        if (!this.ownsScope) {
            n += e.noScope;
        }
        return n || "";
    }
}

class NavigatorNavigateEvent {
    constructor(t, i) {
        this.eventName = t;
        this.navigation = i;
    }
    static create(t) {
        return new NavigatorNavigateEvent(NavigatorNavigateEvent.eventName, t);
    }
}

NavigatorNavigateEvent.eventName = "au:router:navigation-navigate";

let _ = class Navigator {
    constructor(t, i) {
        this.ea = t;
        this.container = i;
        this.lastNavigationIndex = -1;
        this.navigations = [];
        this.options = {
            statefulHistoryLength: 0
        };
        this.isActive = false;
        this.uninitializedNavigation = Navigation.create({
            instruction: "NAVIGATOR UNINITIALIZED",
            fullStateInstruction: "",
            index: 0,
            completed: true
        });
        this.lastNavigationIndex = -1;
    }
    start(t) {
        if (this.isActive) {
            throw new Error("Navigator has already been started");
        }
        this.isActive = true;
        this.options = {
            ...t
        };
    }
    stop() {
        if (!this.isActive) {
            throw new Error("Navigator has not been started");
        }
        this.isActive = false;
    }
    navigate(t) {
        if (!(t instanceof Navigation)) {
            t = Navigation.create(t);
        }
        const i = new NavigationFlags;
        if (this.lastNavigationIndex === -1) {
            this.loadState();
            if (this.lastNavigationIndex !== -1) {
                i.refresh = true;
            } else {
                i.first = true;
                i.new = true;
                this.lastNavigationIndex = 0;
                this.navigations = [ Navigation.create({
                    index: 0,
                    instruction: "",
                    fullStateInstruction: ""
                }) ];
            }
        }
        if (t.index !== void 0 && !(t.replacing ?? false) && !(t.refreshing ?? false)) {
            t.historyMovement = t.index - Math.max(this.lastNavigationIndex, 0);
            t.instruction = this.navigations[t.index] != null ? this.navigations[t.index].fullStateInstruction : t.fullStateInstruction;
            t.replacing = true;
            if (t.historyMovement > 0) {
                i.forward = true;
            } else if (t.historyMovement < 0) {
                i.back = true;
            }
        } else if ((t.refreshing ?? false) || i.refresh) {
            t = this.navigations[this.lastNavigationIndex];
            t.replacing = true;
            t.refreshing = true;
        } else if (t.replacing ?? false) {
            i.replace = true;
            i.new = true;
            t.index = this.lastNavigationIndex;
        } else {
            i.new = true;
            t.index = this.lastNavigationIndex + 1;
            this.navigations[t.index] = t;
        }
        t.navigation = i;
        t.previous = this.navigations[Math.max(this.lastNavigationIndex, 0)];
        t.process = new OpenPromise;
        this.lastNavigationIndex = t.index;
        this.notifySubscribers(t);
        return t.process.promise;
    }
    async finalize(t, i) {
        if (t.untracked ?? false) {
            if ((t.fromBrowser ?? false) && this.options.store != null) {
                await this.options.store.popNavigatorState();
            }
        } else if (t.replacing ?? false) {
            if ((t.historyMovement ?? 0) === 0) {
                this.navigations[t.previous.index] = t;
            }
            await this.saveState(t.index, false);
        } else {
            const s = t.index;
            if (i) {
                this.navigations = this.navigations.slice(0, s);
            }
            this.navigations[s] = t;
            if ((this.options.statefulHistoryLength ?? 0) > 0) {
                const t = this.navigations.length - (this.options.statefulHistoryLength ?? 0);
                for (const i of this.navigations.slice(s)) {
                    if (typeof i.instruction !== "string" || typeof i.fullStateInstruction !== "string") {
                        await this.serializeNavigation(i, this.navigations.slice(t, s));
                    }
                }
            }
            await this.saveState(t.index, !(t.fromBrowser ?? false));
        }
    }
    async cancel(t) {
        if (this.options.store != null) {
            if (t.navigation?.new) {
                if (t.fromBrowser ?? false) {
                    await this.options.store.popNavigatorState();
                }
            } else if ((t.historyMovement ?? 0) !== 0) {
                await this.options.store.go(-t.historyMovement, true);
            }
        }
    }
    async go(t) {
        let i = this.lastNavigationIndex + t;
        i = Math.min(i, this.navigations.length - 1);
        await this.options.store.go(t, true);
        const s = this.navigations[i];
        return this.navigate(s);
    }
    getState() {
        const t = this.options.store != null ? {
            ...this.options.store.state
        } : {};
        const i = t?.navigations ?? [];
        const s = t?.navigationIndex ?? -1;
        return {
            navigations: i,
            navigationIndex: s
        };
    }
    loadState() {
        const {navigations: t, navigationIndex: i} = this.getState();
        this.navigations = t.map((t => Navigation.create(t)));
        this.lastNavigationIndex = i;
    }
    async saveState(t, i) {
        for (let t = 0; t < this.navigations.length; t++) {
            this.navigations[t] = Navigation.create(this.navigations[t].toStoredNavigation());
        }
        if ((this.options.statefulHistoryLength ?? 0) > 0) {
            const t = this.navigations.length - (this.options.statefulHistoryLength ?? 0);
            for (let i = 0; i < t; i++) {
                const s = this.navigations[i];
                if (typeof s.instruction !== "string" || typeof s.fullStateInstruction !== "string") {
                    await this.serializeNavigation(s, this.navigations.slice(t));
                }
            }
        }
        if (this.options.store == null) {
            return Promise.resolve();
        }
        const s = {
            navigations: (this.navigations ?? []).map((t => this.toStoreableNavigation(t))),
            navigationIndex: t
        };
        if (i) {
            return this.options?.store?.pushNavigatorState(s);
        } else {
            return this.options.store.replaceNavigatorState(s);
        }
    }
    async refresh() {
        if (this.lastNavigationIndex === -1) {
            return Promise.reject();
        }
        const t = this.navigations[this.lastNavigationIndex];
        t.replacing = true;
        t.refreshing = true;
        return this.navigate(t);
    }
    notifySubscribers(t) {
        this.ea.publish(NavigatorNavigateEvent.eventName, NavigatorNavigateEvent.create(t));
    }
    toStoreableNavigation(t) {
        const i = t instanceof Navigation ? t.toStoredNavigation() : t;
        i.instruction = RoutingInstruction.stringify(this.container, i.instruction);
        i.fullStateInstruction = RoutingInstruction.stringify(this.container, i.fullStateInstruction, false, true);
        if (typeof i.scope !== "string") {
            i.scope = null;
        }
        return i;
    }
    async serializeNavigation(t, i) {
        let s = [];
        for (const t of i) {
            if (typeof t.instruction !== "string") {
                s.push(...RoutingInstruction.flat(t.instruction).filter((t => t.endpoint.instance !== null)).map((t => t.component.instance)));
            }
            if (typeof t.fullStateInstruction !== "string") {
                s.push(...RoutingInstruction.flat(t.fullStateInstruction).filter((t => t.endpoint.instance !== null)).map((t => t.component.instance)));
            }
        }
        s = arrayUnique(s);
        let e = [];
        if (typeof t.fullStateInstruction !== "string") {
            e.push(...t.fullStateInstruction);
            t.fullStateInstruction = RoutingInstruction.stringify(this.container, t.fullStateInstruction, false, true);
        }
        if (typeof t.instruction !== "string") {
            e.push(...t.instruction);
            t.instruction = RoutingInstruction.stringify(this.container, t.instruction);
        }
        e = e.filter(((t, i, s) => t.component.instance != null && s.indexOf(t) === i));
        const n = [];
        for (const t of e) {
            await this.freeInstructionComponents(t, s, n);
        }
    }
    freeInstructionComponents(t, i, s) {
        const e = t.component.instance;
        const n = t.viewport?.instance ?? null;
        if (e === null || n === null || s.some((t => t === e))) {
            return;
        }
        if (!i.some((t => t === e))) {
            return Runner.run(null, (t => n.freeContent(t, e)), (() => {
                s.push(e);
            }));
        }
        if (t.hasNextScopeInstructions) {
            for (const e of t.nextScopeInstructions) {
                return this.freeInstructionComponents(e, i, s);
            }
        }
    }
};

_ = __decorate([ __param(0, i), __param(1, s) ], _);

const T = b;

const O = k;

const x = $;

const U = P;

class Collection extends Array {
    constructor() {
        super(...arguments);
        this.currentIndex = -1;
    }
    next() {
        if (this.length > this.currentIndex + 1) {
            return this[++this.currentIndex];
        } else {
            this.currentIndex = -1;
            return null;
        }
    }
    removeCurrent() {
        this.splice(this.currentIndex--, 1);
    }
    remove(t) {
        arrayRemove(this, (i => i === t));
    }
}

class EndpointMatcher {
    static matchEndpoints(t, i, s, e = false) {
        const n = [];
        const r = t.getOwnedRoutingScopes(Infinity);
        const o = r.map((t => t.endpoint));
        const h = o.filter((t => t !== null && !s.some((i => t === i.endpoint.instance && !i.cancelled))));
        const u = new Collection(...i.slice());
        let a = null;
        EndpointMatcher.matchKnownEndpoints(t.router, "ViewportScope", u, h, n, false);
        if (!e) {
            EndpointMatcher.matchKnownEndpoints(t.router, "Viewport", u, h, n, false);
        }
        EndpointMatcher.matchViewportScopeSegment(t.router, t, u, h, n);
        while ((a = u.next()) !== null) {
            a.needsEndpointDescribed = true;
        }
        EndpointMatcher.matchViewportConfiguration(u, h, n);
        if (!e) {
            EndpointMatcher.matchSpecifiedViewport(u, h, n, false);
        }
        EndpointMatcher.matchLastViewport(u, h, n);
        if (e) {
            EndpointMatcher.matchSpecifiedViewport(u, h, n, false);
        }
        return {
            matchedInstructions: n,
            remainingInstructions: [ ...u ]
        };
    }
    static matchKnownEndpoints(t, i, s, e, n, r = false) {
        let o;
        while ((o = s.next()) !== null) {
            if (o.endpoint.instance !== null && !o.isAdd(t) && o.endpoint.endpointType === i) {
                EndpointMatcher.matchEndpoint(o, o.endpoint.instance, r);
                n.push(o);
                arrayRemove(e, (t => t === o.endpoint.instance));
                s.removeCurrent();
            }
        }
    }
    static matchViewportScopeSegment(t, i, s, e, n) {
        let r;
        while ((r = s.next()) !== null) {
            for (let o of e) {
                if (!(o instanceof ViewportScope)) {
                    continue;
                }
                if (o.acceptSegment(r.component.name)) {
                    if (Array.isArray(o.source)) {
                        let s = e.find((t => t instanceof ViewportScope && t.name === o.name));
                        if (s === void 0 || r.isAdd(t)) {
                            const t = o.addSourceItem();
                            s = i.getOwnedScopes().filter((t => t.isViewportScope)).map((t => t.endpoint)).find((i => i.sourceItem === t));
                        }
                        o = s;
                    }
                    EndpointMatcher.matchEndpoint(r, o, false);
                    n.push(r);
                    arrayRemove(e, (t => t === r.endpoint.instance));
                    s.removeCurrent();
                    break;
                }
            }
        }
    }
    static matchViewportConfiguration(t, i, s) {
        let e;
        while ((e = t.next()) !== null) {
            for (const n of i) {
                if (!(n instanceof Viewport)) {
                    continue;
                }
                if (n?.wantComponent(e.component.name)) {
                    EndpointMatcher.matchEndpoint(e, n, true);
                    s.push(e);
                    arrayRemove(i, (t => t === e.endpoint.instance));
                    t.removeCurrent();
                    break;
                }
            }
        }
    }
    static matchSpecifiedViewport(t, i, s, e) {
        let n;
        while ((n = t.next()) !== null) {
            let r = n.endpoint.instance;
            if (r == null) {
                const t = n.endpoint.name;
                if ((t?.length ?? 0) === 0) {
                    continue;
                }
                for (const s of i) {
                    if (!(s instanceof Viewport)) {
                        continue;
                    }
                    if (t === s.name) {
                        r = s;
                        break;
                    }
                }
            }
            if (r?.acceptComponent(n.component.name)) {
                EndpointMatcher.matchEndpoint(n, r, e);
                s.push(n);
                arrayRemove(i, (t => t === n.endpoint.instance));
                t.removeCurrent();
            }
        }
    }
    static matchLastViewport(t, i, s) {
        let e;
        while ((e = t.next()) !== null) {
            const n = [];
            for (const t of i) {
                if (!(t instanceof Viewport)) {
                    continue;
                }
                if (t.acceptComponent(e.component.name)) {
                    n.push(t);
                }
            }
            if (n.length === 1) {
                const r = n[0];
                EndpointMatcher.matchEndpoint(e, r, true);
                s.push(e);
                arrayRemove(i, (t => t === e.endpoint.instance));
                t.removeCurrent();
            }
        }
    }
    static matchEndpoint(t, i, s) {
        t.endpoint.set(i);
        if (s) {
            t.needsEndpointDescribed = false;
        }
        if (t.hasNextScopeInstructions) {
            t.nextScopeInstructions.forEach((t => {
                if (t.scope === null) {
                    t.scope = i instanceof Viewport ? i.scope : i.scope.scope;
                }
            }));
        }
    }
}

class RoutingScope {
    constructor(t, i, s, e) {
        this.router = t;
        this.hasScope = i;
        this.owningScope = s;
        this.endpointContent = e;
        this.id = -1;
        this.parent = null;
        this.children = [];
        this.id = ++RoutingScope.lastId;
        this.owningScope = s ?? this;
    }
    static for(t, i) {
        if (t == null) {
            return {
                scope: null,
                instruction: i
            };
        }
        if (t instanceof RoutingScope || t instanceof Viewport || t instanceof ViewportScope) {
            return {
                scope: t.scope,
                instruction: i
            };
        }
        let s;
        if ("res" in t) {
            s = t;
        } else {
            if ("container" in t) {
                s = t.container;
            } else if ("$controller" in t) {
                s = t.$controller.container;
            } else {
                const i = u.for(t, {
                    searchParents: true
                });
                s = i?.container;
            }
        }
        if (s == null) {
            return {
                scope: null,
                instruction: i
            };
        }
        const e = s.has(Router.closestEndpointKey, true) ? s.get(Router.closestEndpointKey) : null;
        let n = e?.scope ?? null;
        if (n === null || i === undefined) {
            const t = i ?? "";
            return {
                scope: n,
                instruction: t.startsWith("/") ? t.slice(1) : i
            };
        }
        if (i.startsWith("/")) {
            return {
                scope: null,
                instruction: i.slice(1)
            };
        }
        while (i.startsWith(".")) {
            if (i.startsWith("./")) {
                i = i.slice(2);
            } else if (i.startsWith("../")) {
                n = n.parent ?? n;
                i = i.slice(3);
            } else {
                break;
            }
        }
        return {
            scope: n,
            instruction: i
        };
    }
    get scope() {
        return this.hasScope ? this : this.owningScope.scope;
    }
    get endpoint() {
        return this.endpointContent.endpoint;
    }
    get isViewport() {
        return this.endpoint instanceof Viewport;
    }
    get isViewportScope() {
        return this.endpoint instanceof ViewportScope;
    }
    get type() {
        return this.isViewport ? "Viewport" : "ViewportScope";
    }
    get enabled() {
        return this.endpointContent.isActive;
    }
    get passThroughScope() {
        return this.isViewportScope && this.endpoint.passThroughScope;
    }
    get pathname() {
        return `${this.owningScope !== this ? this.owningScope.pathname : ""}/${this.endpoint.name}`;
    }
    get path() {
        const t = this.parent?.path ?? "";
        const i = this.routingInstruction?.stringify(this.router, false, true, true) ?? "";
        const s = this.routingInstruction ? Separators.for(this.router).scope : "";
        return `${t}${i}${s}`;
    }
    toString(t = false) {
        return `${this.owningScope !== this ? this.owningScope.toString() : ""}/${!this.enabled ? "(" : ""}${this.endpoint.toString()}#${this.id}${!this.enabled ? ")" : ""}` + `${t ? `\n` + this.children.map((t => t.toString(true))).join("") : ""}`;
    }
    toStringOwning(t = false) {
        return `${this.owningScope !== this ? this.owningScope.toString() : ""}/${!this.enabled ? "(" : ""}${this.endpoint.toString()}#${this.id}${!this.enabled ? ")" : ""}` + `${t ? `\n` + this.ownedScopes.map((t => t.toStringOwning(true))).join("") : ""}`;
    }
    get enabledChildren() {
        return this.children.filter((t => t.enabled));
    }
    get hoistedChildren() {
        const t = this.enabledChildren;
        while (t.some((t => t.passThroughScope))) {
            for (const i of t.slice()) {
                if (i.passThroughScope) {
                    const s = t.indexOf(i);
                    t.splice(s, 1, ...i.enabledChildren);
                }
            }
        }
        return t;
    }
    get ownedScopes() {
        return this.getOwnedScopes();
    }
    get routingInstruction() {
        if (this.endpoint.isViewportScope) {
            return this.endpoint.instruction;
        }
        if (this.isViewport) {
            return this.endpoint.activeContent.instruction;
        }
        return null;
    }
    getOwnedScopes(t = false) {
        const i = this.allScopes(t).filter((t => t.owningScope === this));
        for (const t of i.slice()) {
            if (t.passThroughScope) {
                const s = i.indexOf(t);
                i.splice(s, 1, ...t.getOwnedScopes());
            }
        }
        return i;
    }
    async processInstructions(t, i, s, e, n = "") {
        const r = this.router;
        const o = r.configuration.options;
        const h = t.filter((t => !(t.route instanceof Route)));
        if (h.length > 0) {
            const i = this.findInstructions(h, o.useDirectRouting, o.useConfiguredRoutes);
            if (h.some((t => !t.component.none || t.route != null)) && !i.foundConfiguration && !i.foundInstructions) {
                this.unknownRoute(h);
            }
            t = [ ...t.filter((t => t.route instanceof Route)), ...i.instructions ];
            if (t.some((t => t.scope !== this))) {
                console.warn("Not the current scope for instruction(s)!", this, t);
            }
            if (i.foundConfiguration) {
                n = (n ?? "") + i.matching;
            }
        }
        const u = RoutingInstruction.resolve(t);
        if (u instanceof Promise) {
            await u;
        }
        if (!o.additiveInstructionDefault) {
            t = this.ensureClearStateInstruction(t);
        }
        let a = [];
        ({clearEndpoints: a, instructions: t} = this.getClearAllEndpoints(t));
        for (const i of t.filter((t => t.isAddAll(r)))) {
            i.endpoint.set(i.scope.endpoint.name);
            i.scope = i.scope.owningScope;
        }
        let l = [];
        let {matchedInstructions: c, remainingInstructions: f} = this.matchEndpoints(t, i);
        let d = 100;
        do {
            if (!d--) {
                r.unresolvedInstructionsError(s, f);
            }
            const o = [];
            const h = c.map((t => t.endpoint.instance));
            c.push(...a.filter((t => !h.includes(t))).map((t => RoutingInstruction.createClear(r, t))));
            const u = await RoutingHook.invokeBeforeNavigation(c, s);
            if (u === false) {
                r.cancelNavigation(s, e);
                return [];
            } else if (u !== true && u !== c) {
                const t = RoutingInstruction.flat(c);
                f = f.filter((i => !t.includes(i)));
                c = u;
            }
            for (const t of c) {
                const h = t.endpoint.instance;
                if (h !== null) {
                    const u = h.setNextContent(t, s);
                    if (u !== "skip") {
                        o.push(h);
                        e.addEndpoint(h);
                    }
                    const f = [ h ];
                    if (u === "swap") {
                        f.push(...h.getContent().connectedScope.allScopes(true).map((t => t.endpoint)));
                    }
                    arrayRemove(a, (t => f.includes(t)));
                    arrayRemove(c, (i => i !== t && i.isClear(r) && f.includes(i.endpoint.instance)));
                    if (!t.isClear(r) && t.scope?.parent?.isViewportScope) {
                        arrayRemove(a, (i => i === t.scope.parent.endpoint));
                        arrayRemove(c, (i => i !== t && i.isClear(r) && i.endpoint.instance === t.scope.parent.endpoint));
                    }
                    if (u !== "skip" && t.hasNextScopeInstructions) {
                        for (const i of t.nextScopeInstructions) {
                            i.scope = h.scope;
                            i.endpoint.instance = null;
                        }
                    }
                    if (u === "skip" && !t.hasNextScopeInstructions) {
                        l.push(...await h.scope.processInstructions([], i, s, e, n));
                    }
                }
            }
            const p = c.filter((t => t.endpoint.instance?.transitionAction === "skip"));
            const g = p.filter((t => t.hasNextScopeInstructions));
            if (p.length === 0 || g.length === 0) {
                if (!r.isRestrictedNavigation) {
                    e.finalEndpoint();
                }
                e.run();
                if (e.hasAllEndpoints) {
                    const t = e.waitForSyncState("guardedUnload");
                    if (t instanceof Promise) {
                        await t;
                    }
                }
            }
            if (e.cancelled) {
                r.cancelNavigation(s, e);
                return [];
            }
            for (const t of o) {
                if (l.every((i => i !== t))) {
                    l.push(t);
                }
            }
            i.push(...c.splice(0));
            if (f.length > 0) {
                ({matchedInstructions: c, remainingInstructions: f} = this.matchEndpoints(f, i));
            }
            if (!r.isRestrictedNavigation && (c.length > 0 || f.length > 0) && e.running) {
                const t = e.waitForSyncState("swapped");
                if (t instanceof Promise) {
                    await t;
                }
            }
            if (c.length === 0 && f.length === 0) {
                const r = [];
                for (const o of t) {
                    if (!o.hasNextScopeInstructions) {
                        continue;
                    }
                    const t = o.endpoint.instance?.scope ?? o.endpoint.scope;
                    r.push(t.processInstructions(o.nextScopeInstructions, i, s, e, n));
                }
                l.push(...(await Promise.all(r)).flat());
            }
            ({matchedInstructions: c, remainingInstructions: f} = e.dequeueAppendedInstructions(c, i, f));
            if (c.length === 0 && f.length === 0) {
                const t = i.map((t => (t.endpoint.instance?.connectedCE).pendingPromise?.promise)).filter((t => t != null));
                if (t.length > 0) {
                    await Promise.any(t);
                    ({matchedInstructions: c, remainingInstructions: f} = e.dequeueAppendedInstructions(c, i, f));
                } else {
                    c = a.map((t => RoutingInstruction.createClear(r, t)));
                }
            }
            const v = RoutingInstruction.resolve(c);
            if (v instanceof Promise) {
                await v;
            }
            l = l.filter((t => !([ ...i ].reverse().find((i => i.endpoint.instance === t))?.cancelled ?? false)));
        } while (c.length > 0 || f.length > 0);
        return l;
    }
    unknownRoute(t) {
        const i = this.router.configuration.options;
        const s = RoutingInstruction.stringify(this.router, t);
        if (t[0].route != null) {
            if (!i.useConfiguredRoutes) {
                throw new Error("Can not match '" + s + "' since the router is configured to not use configured routes.");
            } else {
                throw new Error("No matching configured route found for '" + s + "'.");
            }
        } else if (i.useConfiguredRoutes && i.useDirectRouting) {
            throw new Error("No matching configured route or component found for '" + s + "'.");
        } else if (i.useConfiguredRoutes) {
            throw new Error("No matching configured route found for '" + s + "'.");
        } else {
            throw new Error("No matching route/component found for '" + s + "'.");
        }
    }
    ensureClearStateInstruction(t) {
        const i = this.router;
        if (!t.some((t => t.isClearAll(i)))) {
            const s = RoutingInstruction.create(RoutingInstruction.clear(i));
            s.scope = this;
            return [ s, ...t ];
        }
        return t;
    }
    getClearAllEndpoints(t) {
        const i = this.router;
        let s = [];
        if (t.some((t => (t.scope ?? this) === this && t.isClearAll(i)))) {
            s = this.enabledChildren.filter((t => !t.endpoint.isEmpty)).map((t => t.endpoint));
            t = t.filter((t => !((t.scope ?? this) === this && t.isClearAll(i))));
        }
        return {
            clearEndpoints: s,
            instructions: t
        };
    }
    findInstructions(t, i, s) {
        const e = this.router;
        let n = new FoundRoute;
        if (s && !RoutingInstruction.containsSiblings(e, t)) {
            let s = t.filter((t => t.isClear(e) || t.isClearAll(e)));
            const r = t.filter((t => !t.isClear(e) && !t.isClearAll(e)));
            if (r.length > 0) {
                for (const o of r) {
                    const r = typeof o.route === "string" ? o.route : o.unparsed ?? RoutingInstruction.stringify(e, [ o ]);
                    const h = this.findMatchingRoute(r, o.parameters.parametersRecord ?? {});
                    if (h.foundConfiguration) {
                        n = h;
                        n.instructions = [ ...s, ...n.instructions ];
                        s = [];
                    } else if (i) {
                        n.instructions = [ ...s, ...n.instructions, o ];
                        s = [];
                        n.remaining = RoutingInstruction.stringify(e, o.nextScopeInstructions ?? []);
                    } else {
                        throw new Error(`No route found for: ${RoutingInstruction.stringify(e, t)}!`);
                    }
                }
            } else {
                n.instructions = [ ...s ];
            }
        } else if (i) {
            n.instructions.push(...t);
        } else {
            throw new Error(`No way to process sibling viewport routes with direct routing disabled: ${RoutingInstruction.stringify(e, t)}!`);
        }
        n.instructions = n.instructions.filter((t => t.component.name !== ""));
        for (const t of n.instructions) {
            if (t.scope === null) {
                t.scope = this;
            }
        }
        return n;
    }
    matchEndpoints(t, i, s = false) {
        const e = [];
        const n = t.filter((t => (t.scope ?? this) === this));
        const r = t.filter((t => (t.scope ?? this) !== this));
        const {matchedInstructions: o, remainingInstructions: h} = EndpointMatcher.matchEndpoints(this, n, i, s);
        e.push(...o);
        r.push(...h);
        return {
            matchedInstructions: e,
            remainingInstructions: r
        };
    }
    addEndpoint(t, i, s, e = {}) {
        let n = this.getOwnedScopes().find((s => s.type === t && s.endpoint.name === i))?.endpoint ?? null;
        if (s != null && n?.connectedCE != null && n.connectedCE !== s) {
            n = this.getOwnedScopes(true).find((e => e.type === t && e.endpoint.name === i && e.endpoint.connectedCE === s))?.endpoint ?? null;
        }
        if (n == null) {
            n = t === "Viewport" ? new Viewport(this.router, i, s, this.scope, !!e.scope, e) : new ViewportScope(this.router, i, s, this.scope, true, null, e);
            this.addChild(n.connectedScope);
        }
        if (s != null) {
            n.setConnectedCE(s, e);
        }
        return n;
    }
    removeEndpoint(t, i, s) {
        if ((s ?? null) !== null || i.removeEndpoint(t, s)) {
            this.removeChild(i.connectedScope);
            return true;
        }
        return false;
    }
    addChild(t) {
        if (!this.children.some((i => i === t))) {
            if (t.parent !== null) {
                t.parent.removeChild(t);
            }
            this.children.push(t);
            t.parent = this;
        }
    }
    removeChild(t) {
        const i = this.children.indexOf(t);
        if (i >= 0) {
            this.children.splice(i, 1);
            t.parent = null;
        }
    }
    allScopes(t = false) {
        const i = t ? this.children.slice() : this.enabledChildren;
        for (const s of i.slice()) {
            i.push(...s.allScopes(t));
        }
        return i;
    }
    reparentRoutingInstructions() {
        const t = this.hoistedChildren.filter((t => t.routingInstruction !== null && t.routingInstruction.component.name));
        if (!t.length) {
            return null;
        }
        for (const i of t) {
            const t = i.reparentRoutingInstructions();
            i.routingInstruction.nextScopeInstructions = t !== null && t.length > 0 ? t : null;
        }
        return t.map((t => t.routingInstruction));
    }
    getChildren(t) {
        const i = this.children.map((i => i.endpoint.getTimeContent(t))).filter((t => t !== null));
        return i.map((t => t.connectedScope));
    }
    getAllRoutingScopes(t) {
        const i = this.getChildren(t);
        for (const s of i.slice()) {
            i.push(...s.getAllRoutingScopes(t));
        }
        return i;
    }
    getOwnedRoutingScopes(t) {
        const i = this.getAllRoutingScopes(t).filter((t => t.owningScope === this));
        for (const s of i.slice()) {
            if (s.passThroughScope) {
                const e = i.indexOf(s);
                i.splice(e, 1, ...s.getOwnedRoutingScopes(t));
            }
        }
        return arrayUnique(i);
    }
    getRoutingInstructions(t) {
        const i = arrayUnique(this.getOwnedRoutingScopes(t).map((t => t.endpoint))).map((i => i.getTimeContent(t))).filter((t => t !== null));
        const s = [];
        for (const e of i) {
            const i = e.instruction.clone(true, false, false);
            if ((i.component.name ?? "") !== "") {
                i.nextScopeInstructions = e.connectedScope.getRoutingInstructions(t);
                s.push(i);
            }
        }
        return s;
    }
    canUnload(t, i) {
        return Runner.run(i, (i => Runner.runParallel(i, ...this.children.map((i => i.endpoint !== null ? s => i.endpoint.canUnload(t, s) : s => i.canUnload(t, s))))), (t => t.previousValue.every((t => t))));
    }
    unload(t, i) {
        return Runner.runParallel(i, ...this.children.map((i => i.endpoint !== null ? s => i.endpoint.unload(t, s) : s => i.unload(t, s))));
    }
    matchScope(t, i = false) {
        const s = [];
        for (const e of t) {
            if (e.scope === this) {
                s.push(e);
            } else if (i && e.hasNextScopeInstructions) {
                s.push(...this.matchScope(e.nextScopeInstructions, i));
            }
        }
        return s;
    }
    findMatchingRoute(t, i) {
        let s = new FoundRoute;
        if (this.isViewportScope && !this.passThroughScope) {
            s = this.findMatchingRouteInRoutes(t, this.endpoint.getRoutes(), i);
        } else if (this.isViewport) {
            s = this.findMatchingRouteInRoutes(t, this.endpoint.getRoutes(), i);
        } else {
            for (const e of this.enabledChildren) {
                s = e.findMatchingRoute(t, i);
                if (s.foundConfiguration) {
                    break;
                }
            }
        }
        if (s.foundConfiguration) {
            return s;
        }
        if (this.parent != null) {
            return this.parent.findMatchingRoute(t, i);
        }
        return s;
    }
    findMatchingRouteInRoutes(t, i, s) {
        const e = new FoundRoute;
        if (i.length === 0) {
            return e;
        }
        i = i.map((t => this.ensureProperRoute(t)));
        const n = [];
        for (const t of i) {
            const i = Array.isArray(t.path) ? t.path : [ t.path ];
            for (const s of i) {
                n.push({
                    ...t,
                    path: s,
                    handler: t
                });
                if (s !== "") {
                    n.push({
                        ...t,
                        path: `${s}/*remainingPath`,
                        handler: t
                    });
                }
            }
        }
        if (t.startsWith("/") || t.startsWith("+")) {
            t = t.slice(1);
        }
        const r = i.find((i => i.id === t));
        let o = {
            params: {},
            endpoint: {}
        };
        if (r != null) {
            o.endpoint = {
                route: {
                    handler: r
                }
            };
            t = Array.isArray(r.path) ? r.path[0] : r.path;
            const i = t.split("/").map((t => {
                if (t.startsWith(":")) {
                    const i = t.slice(1).replace(/\?$/, "");
                    const e = s[i];
                    o.params[i] = e;
                    return e;
                } else {
                    return t;
                }
            }));
            t = i.join("/");
        } else {
            const i = new T;
            i.add(n);
            o = i.recognize(t);
        }
        if (o != null) {
            e.match = o.endpoint.route.handler;
            e.matching = t;
            const n = {
                ...o.params
            };
            if (n.remainingPath != null) {
                e.remaining = n.remainingPath;
                Reflect.deleteProperty(n, "remainingPath");
                e.matching = e.matching.slice(0, e.matching.indexOf(e.remaining));
            }
            e.params = n;
            if (e.match?.redirectTo != null) {
                let t = e.match?.redirectTo;
                if ((e.remaining ?? "").length > 0) {
                    t += `/${e.remaining}`;
                }
                return this.findMatchingRouteInRoutes(t, i, s);
            }
        }
        if (e.foundConfiguration) {
            e.instructions = RoutingInstruction.clone(e.match.instructions, false, true);
            const t = e.instructions.slice();
            while (t.length > 0) {
                const i = t.shift();
                i.parameters.addParameters(e.params);
                i.route = e;
                if (i.hasNextScopeInstructions) {
                    t.unshift(...i.nextScopeInstructions);
                }
            }
            if (e.instructions.length > 0) {
                e.instructions[0].routeStart = true;
            }
            const i = RoutingInstruction.parse(this.router, e.remaining);
            if (i.length > 0) {
                let t = e.instructions[0];
                while (t.hasNextScopeInstructions) {
                    t = t.nextScopeInstructions[0];
                }
                t.nextScopeInstructions = i;
            }
        }
        return e;
    }
    ensureProperRoute(t) {
        if (t.id === void 0) {
            t.id = Array.isArray(t.path) ? t.path.join(",") : t.path;
        }
        if (t.instructions === void 0) {
            t.instructions = [ {
                component: t.component,
                viewport: t.viewport,
                parameters: t.parameters,
                children: t.children
            } ];
        }
        if (t.redirectTo === null) {
            t.instructions = RoutingInstruction.from(this.router, t.instructions);
        }
        return t;
    }
}

RoutingScope.lastId = 0;

class QueueTask {
    constructor(t, i, s = 0) {
        this.taskQueue = t;
        this.item = i;
        this.cost = s;
        this.done = false;
        this.promise = new Promise(((t, i) => {
            this.resolve = () => {
                this.taskQueue.resolve(this, t);
            };
            this.reject = t => {
                this.taskQueue.reject(this, i, t);
            };
        }));
    }
    async execute() {
        if ("execute" in this.item) {
            await this.item.execute(this);
        } else {
            await this.item(this);
        }
    }
    wait() {
        return this.promise;
    }
}

class TaskQueue {
    get isActive() {
        return this.task !== null;
    }
    constructor(t) {
        this.callback = t;
        this.pending = [];
        this.processing = null;
        this.allowedExecutionCostWithinTick = null;
        this.currentExecutionCostInCurrentTick = 0;
        this.platform = null;
        this.task = null;
        this.dequeue = t => {
            if (this.processing !== null) {
                return;
            }
            if (t !== undefined) {
                this.currentExecutionCostInCurrentTick = 0;
            }
            if (this.pending.length === 0) {
                return;
            }
            if (this.allowedExecutionCostWithinTick !== null && t === undefined && this.currentExecutionCostInCurrentTick + (this.pending[0].cost || 0) > this.allowedExecutionCostWithinTick) {
                return;
            }
            this.processing = this.pending.shift() || null;
            if (this.processing) {
                this.currentExecutionCostInCurrentTick += this.processing.cost ?? 0;
                if (this.callback !== void 0) {
                    this.callback(this.processing);
                } else {
                    this.processing.execute().catch((t => {
                        throw t;
                    }));
                }
            }
        };
    }
    get length() {
        return this.pending.length;
    }
    start(t) {
        this.platform = t.platform;
        this.allowedExecutionCostWithinTick = t.allowedExecutionCostWithinTick;
        this.task = this.platform.domWriteQueue.queueTask(this.dequeue, {
            persistent: true
        });
    }
    stop() {
        this.task.cancel();
        this.task = null;
        this.allowedExecutionCostWithinTick = null;
        this.clear();
    }
    enqueue(t, i) {
        const s = Array.isArray(t);
        const e = s ? t : [ t ];
        const n = e.map(((t, s) => !Array.isArray(i) ? i : i[s])).map((t => t !== undefined ? t : 1));
        const r = [];
        for (const t of e) {
            r.push(t instanceof QueueTask ? t : this.createQueueTask(t, n.shift()));
        }
        this.pending.push(...r);
        this.dequeue();
        return s ? r : r[0];
    }
    createQueueTask(t, i) {
        return new QueueTask(this, t, i);
    }
    clear() {
        this.pending.length = 0;
    }
    resolve(t, i) {
        i();
        this.processing = null;
        this.dequeue();
    }
    reject(t, i, s) {
        i(s);
        this.processing = null;
        this.dequeue();
    }
}

let F = class BrowserViewerStore {
    constructor(t, i, s, e, n) {
        this.platform = t;
        this.window = i;
        this.history = s;
        this.location = e;
        this.ea = n;
        this.allowedExecutionCostWithinTick = 2;
        this.isActive = false;
        this.options = {
            useUrlFragmentHash: true
        };
        this.forwardedState = {
            eventTask: null,
            suppressPopstate: false
        };
        this.pendingCalls = new TaskQueue;
    }
    start(t) {
        if (this.isActive) {
            throw new Error("Browser navigation has already been started");
        }
        this.isActive = true;
        if (t.useUrlFragmentHash != void 0) {
            this.options.useUrlFragmentHash = t.useUrlFragmentHash;
        }
        this.pendingCalls.start({
            platform: this.platform,
            allowedExecutionCostWithinTick: this.allowedExecutionCostWithinTick
        });
        this.window.addEventListener("popstate", this);
    }
    stop() {
        if (!this.isActive) {
            throw new Error("Browser navigation has not been started");
        }
        this.window.removeEventListener("popstate", this);
        this.pendingCalls.stop();
        this.options = {
            useUrlFragmentHash: true
        };
        this.isActive = false;
    }
    get length() {
        return this.history.length;
    }
    get state() {
        return this.history.state;
    }
    get viewerState() {
        const {pathname: t, search: i, hash: s} = this.location;
        const e = this.options.useUrlFragmentHash ?? false ? s.slice(1) : `${t}${i}`;
        const n = this.options.useUrlFragmentHash ?? false ? s.slice(1).includes("#") ? s.slice(s.slice(1).indexOf("#", 1)) : "" : s.slice(1);
        return new NavigatorViewerState(t, i.slice(1), n, e);
    }
    async go(t, i = false) {
        const s = this.pendingCalls.createQueueTask((t => t.resolve()), 1);
        this.pendingCalls.enqueue([ t => {
            const e = s;
            const n = i;
            this.forwardState({
                eventTask: e,
                suppressPopstate: n
            });
            t.resolve();
        }, i => {
            const s = this.history;
            const e = t;
            s.go(e);
            i.resolve();
        } ], [ 0, 1 ]);
        return s.wait();
    }
    async pushNavigatorState(t) {
        const {title: i, path: s} = t.navigations[t.navigationIndex];
        const e = this.options.useUrlFragmentHash ? "#/" : "";
        return this.pendingCalls.enqueue((n => {
            const r = this.history;
            const o = t;
            const h = i || "";
            const u = `${e}${s}`;
            try {
                r.pushState(o, h, u);
                this.setTitle(h);
            } catch (t) {
                const i = this.tryCleanState(o, "push", t);
                r.pushState(i, h, u);
                this.setTitle(h);
            }
            n.resolve();
        }), 1).wait();
    }
    async replaceNavigatorState(t, i, s) {
        const e = t.navigations[t.navigationIndex];
        i ??= e.title;
        s ??= e.path;
        const n = this.options.useUrlFragmentHash ? "#/" : "";
        return this.pendingCalls.enqueue((e => {
            const r = this.history;
            const o = t;
            const h = i || "";
            const u = `${n}${s}`;
            try {
                r.replaceState(o, h, u);
                this.setTitle(h);
            } catch (t) {
                const i = this.tryCleanState(o, "replace", t);
                r.replaceState(i, h, u);
                this.setTitle(h);
            }
            e.resolve();
        }), 1).wait();
    }
    async popNavigatorState() {
        const t = this.pendingCalls.createQueueTask((t => t.resolve()), 1);
        this.pendingCalls.enqueue((async i => {
            const s = t;
            await this.popState(s);
            i.resolve();
        }), 1);
        return t.wait();
    }
    setTitle(t) {
        this.window.document.title = t;
    }
    handleEvent(t) {
        this.handlePopStateEvent(t);
    }
    handlePopStateEvent(t) {
        const {eventTask: i, suppressPopstate: s} = this.forwardedState;
        this.forwardedState = {
            eventTask: null,
            suppressPopstate: false
        };
        this.pendingCalls.enqueue((async e => {
            if (!s) {
                this.notifySubscribers(t);
            }
            if (i !== null) {
                await i.execute();
            }
            e.resolve();
        }), 1);
    }
    notifySubscribers(t) {
        this.ea.publish(NavigatorStateChangeEvent.eventName, NavigatorStateChangeEvent.create(this.viewerState, t, this.history.state));
    }
    async popState(t) {
        await this.go(-1, true);
        const i = this.history.state;
        const s = i?.navigations?.[i?.navigationIndex ?? 0];
        if (s != null && !s.firstEntry) {
            await this.go(-1, true);
            await this.pushNavigatorState(i);
        }
        await t.execute();
    }
    forwardState(t) {
        this.forwardedState = t;
    }
    tryCleanState(t, i, s) {
        try {
            return JSON.parse(JSON.stringify(t));
        } catch (t) {
            throw new Error(`Failed to ${i} state, probably due to unserializable data and/or parameters: ${t}${s}`);
        }
    }
};

F = __decorate([ __param(0, c), __param(1, f), __param(2, d), __param(3, p), __param(4, i) ], F);

class NavigatorViewerState {
    constructor(t, i, s, e) {
        this.path = t;
        this.query = i;
        this.hash = s;
        this.instruction = e;
    }
}

class NavigatorStateChangeEvent {
    constructor(t, i, s, e) {
        this.eventName = t;
        this.viewerState = i;
        this.event = s;
        this.state = e;
    }
    static create(t, i, s) {
        return new NavigatorStateChangeEvent(NavigatorStateChangeEvent.eventName, t, i, s);
    }
}

NavigatorStateChangeEvent.eventName = "au:router:navigation-state-change";

class Entity {
    constructor(t) {
        this.endpoint = t;
        this.running = false;
        this.states = new Map;
        this.checkedStates = [];
        this.syncingState = null;
        this.syncPromise = null;
        this.step = null;
    }
    hasReachedState(t) {
        return this.states.has(t) && this.states.get(t) === null;
    }
}

class NavigationCoordinator {
    constructor(t, i) {
        this.router = t;
        this.navigation = i;
        this.running = false;
        this.completed = false;
        this.cancelled = false;
        this.hasAllEndpoints = false;
        this.appendedInstructions = [];
        this.entities = [];
        this.syncStates = new Map;
        this.checkedSyncStates = new Set;
    }
    static create(t, i, s) {
        const e = new NavigationCoordinator(t, i);
        s.syncStates.forEach((t => e.addSyncState(t)));
        return e;
    }
    run() {
        if (!this.running) {
            this.running = true;
            for (const t of this.entities) {
                if (!t.running) {
                    t.running = true;
                    t.endpoint.transition(this);
                }
            }
        }
    }
    addSyncState(t) {
        const i = new OpenPromise;
        this.syncStates.set(t, i);
    }
    addEndpoint(t) {
        const i = new Entity(t);
        this.entities.push(i);
        this.recheckSyncStates();
        if (this.running) {
            i.endpoint.transition(this);
        }
        return i;
    }
    removeEndpoint(t) {
        const i = this.entities.find((i => i.endpoint === t));
        if (i !== void 0) {
            arrayRemove(this.entities, (t => t === i));
        }
    }
    setEndpointStep(t, i) {
        let s = this.entities.find((i => i.endpoint === t));
        if (s === void 0) {
            s = this.addEndpoint(t);
        }
        s.step = i;
    }
    getEndpointStep(t) {
        const i = this.entities.find((i => i.endpoint === t));
        return i?.step ?? null;
    }
    addEndpointState(t, i) {
        let s = this.entities.find((i => i.endpoint === t));
        if (s === void 0) {
            s = this.addEndpoint(t);
        }
        const e = s.states.get(i);
        if (e instanceof OpenPromise) {
            e.resolve();
        }
        s.states.set(i, null);
        this.checkSyncState(i);
    }
    waitForSyncState(t, i = null) {
        if (this.entities.length === 0) {
            return;
        }
        const s = this.syncStates.get(t);
        if (s === void 0) {
            return;
        }
        if (i !== null) {
            const e = this.entities.find((t => t.endpoint === i));
            if (e?.syncPromise === null && s.isPending) {
                e.syncingState = t;
                e.syncPromise = new OpenPromise;
                e.checkedStates.push(t);
                this.checkedSyncStates.add(t);
                Promise.resolve().then((() => {
                    this.checkSyncState(t);
                })).catch((t => {
                    throw t;
                }));
                return e.syncPromise.promise;
            }
        }
        return s.isPending ? s.promise : void 0;
    }
    waitForEndpointState(t, i) {
        if (!this.syncStates.has(i)) {
            return;
        }
        let s = this.entities.find((i => i.endpoint === t));
        if (s == null) {
            s = this.addEndpoint(t);
        }
        if (s.hasReachedState(i)) {
            return;
        }
        let e = s.states.get(i);
        if (e == null) {
            e = new OpenPromise;
            s.states.set(i, e);
        }
        return e.promise;
    }
    finalEndpoint() {
        this.hasAllEndpoints = true;
        this.syncStates.forEach(((t, i) => this.checkSyncState(i)));
    }
    finalize() {
        this.entities.forEach((t => t.endpoint.finalizeContentChange(this, null)));
        this.completed = true;
        this.navigation.completed = true;
        this.syncStates.clear();
    }
    cancel() {
        this.cancelled = true;
        this.entities.forEach((t => {
            const i = t.endpoint.cancelContentChange(this);
            if (i instanceof Promise) {
                i.catch((t => {
                    throw t;
                }));
            }
        }));
        this.router.navigator.cancel(this.navigation).then((() => {
            this.navigation.process?.resolve(false);
        })).catch((t => {
            throw t;
        }));
        this.completed = true;
        this.navigation.completed = true;
        [ ...this.syncStates.values() ].forEach((t => {
            if (t.isPending) {
                t.resolve();
            }
        }));
        this.syncStates.clear();
    }
    enqueueAppendedInstructions(t) {
        this.appendedInstructions.push(...t);
    }
    dequeueAppendedInstructions(t, i, s) {
        let e = [ ...this.appendedInstructions ];
        t = [ ...t ];
        s = [ ...s ];
        const n = e.filter((t => !t.default));
        const r = e.filter((t => t.default));
        e = n.length > 0 ? [ ...n ] : [ ...r ];
        while (e.length > 0) {
            const n = e.shift();
            arrayRemove(this.appendedInstructions, (t => t === n));
            const r = i.some((t => !t.cancelled && t.sameEndpoint(n, true)));
            const o = t.find((t => t.sameEndpoint(n, true)));
            const h = s.find((t => t.sameEndpoint(n, true)));
            if (n.default && (r || o !== void 0 && !o.default || h !== void 0 && !h.default)) {
                continue;
            }
            if (o !== void 0) {
                arrayRemove(t, (t => t === o));
            }
            if (h !== void 0) {
                arrayRemove(s, (t => t === h));
            }
            if (n.endpoint.instance !== null) {
                t.push(n);
            } else {
                s.push(n);
            }
        }
        return {
            matchedInstructions: t,
            remainingInstructions: s
        };
    }
    checkSyncState(t) {
        const i = this.syncStates.get(t);
        if (i === void 0) {
            return;
        }
        if (this.hasAllEndpoints && i.isPending && this.entities.every((i => i.hasReachedState(t))) && (!this.checkedSyncStates.has(t) || this.entities.every((i => i.checkedStates.includes(t))))) {
            for (const i of this.entities) {
                if (i.syncingState === t) {
                    i.syncPromise?.resolve();
                    i.syncPromise = null;
                    i.syncingState = null;
                }
            }
            i.resolve();
        }
    }
    recheckSyncStates() {
        this.syncStates.forEach(((t, i) => {
            if (!t.isPending && !this.entities.every((t => t.hasReachedState(i)))) {
                this.addSyncState(i);
            }
        }));
    }
}

class RoutingHook {
    constructor(t, i, s) {
        this.hook = t;
        this.id = s;
        this.type = "beforeNavigation";
        this.includeTargets = [];
        this.excludeTargets = [];
        if (i.type !== void 0) {
            this.type = i.type;
        }
        for (const t of i.include ?? []) {
            this.includeTargets.push(new Target(t));
        }
        for (const t of i.exclude ?? []) {
            this.excludeTargets.push(new Target(t));
        }
    }
    static add(t, i) {
        const s = new RoutingHook(t, i ?? {}, ++this.lastIdentity);
        this.hooks[s.type].push(s);
        return this.lastIdentity;
    }
    static remove(t) {
        for (const i in this.hooks) {
            if (Object.prototype.hasOwnProperty.call(this.hooks, i)) {
                const s = this.hooks[i].findIndex((i => i.id === t));
                if (s >= 0) {
                    this.hooks[i].splice(s, 1);
                }
            }
        }
    }
    static removeAll() {
        for (const t in this.hooks) {
            this.hooks[t] = [];
        }
    }
    static async invokeBeforeNavigation(t, i) {
        return this.invoke("beforeNavigation", i, t);
    }
    static async invokeTransformFromUrl(t, i) {
        return this.invoke("transformFromUrl", i, t);
    }
    static async invokeTransformToUrl(t, i) {
        return this.invoke("transformToUrl", i, t);
    }
    static async invokeTransformTitle(t, i) {
        return this.invoke("transformTitle", i, t);
    }
    static async invoke(t, i, s) {
        let e = s;
        for (const n of this.hooks[t]) {
            if (!n.wantsMatch || n.matches(s)) {
                e = await n.invoke(i, s);
                if (typeof e === "boolean") {
                    if (!e) {
                        return false;
                    }
                } else {
                    s = e;
                }
            }
        }
        return e;
    }
    get wantsMatch() {
        return this.includeTargets.length > 0 || this.excludeTargets.length > 0;
    }
    matches(t) {
        if (this.includeTargets.length && !this.includeTargets.some((i => i.matches(t)))) {
            return false;
        }
        if (this.excludeTargets.length && this.excludeTargets.some((i => i.matches(t)))) {
            return false;
        }
        return true;
    }
    invoke(t, i) {
        return this.hook(i, t);
    }
}

RoutingHook.hooks = {
    beforeNavigation: [],
    transformFromUrl: [],
    transformToUrl: [],
    transformTitle: []
};

RoutingHook.lastIdentity = 0;

class Target {
    constructor(t) {
        this.componentType = null;
        this.componentName = null;
        this.viewport = null;
        this.viewportName = null;
        if (typeof t === "string") {
            this.componentName = t;
        } else if (InstructionComponent.isType(t)) {
            this.componentType = t;
            this.componentName = InstructionComponent.getName(t);
        } else {
            const i = t;
            if (i.component != null) {
                this.componentType = InstructionComponent.isType(i.component) ? InstructionComponent.getType(i.component) : null;
                this.componentName = InstructionComponent.getName(i.component);
            }
            if (i.viewport != null) {
                this.viewport = InstructionEndpoint.isInstance(i.viewport) ? i.viewport : null;
                this.viewportName = InstructionEndpoint.getName(i.viewport);
            }
        }
    }
    matches(t) {
        const i = t.slice();
        if (!i.length) {
            i.push(RoutingInstruction.create(""));
        }
        for (const t of i) {
            if (this.componentName !== null && this.componentName === t.component.name || this.componentType !== null && this.componentType === t.component.type || this.viewportName !== null && this.viewportName === t.endpoint.name || this.viewport !== null && this.viewport === t.endpoint.instance) {
                return true;
            }
        }
        return false;
    }
}

class Title {
    static async getTitle(t, i, s) {
        let e = await RoutingHook.invokeTransformTitle(t, i);
        if (typeof e !== "string") {
            const t = Title.stringifyTitles(e, i, s);
            e = s.appTitle;
            e = e.replace(/\${componentTitles}/g, t);
            e = e.replace(/\${appTitleSeparator}/g, t !== "" ? s.appTitleSeparator : "");
        }
        e = await RoutingHook.invokeTransformTitle(e, i);
        return e;
    }
    static stringifyTitles(t, i, s) {
        const e = t.map((t => Title.stringifyTitle(t, i, s))).filter((t => (t?.length ?? 0) > 0));
        return e.join(" + ");
    }
    static stringifyTitle(t, i, s) {
        const e = t.nextScopeInstructions;
        let n = Title.resolveTitle(t, i, s);
        if (Array.isArray(e) && e.length > 0) {
            let t = Title.stringifyTitles(e, i, s);
            if (t.length > 0) {
                if (e.length !== 1) {
                    t = `[ ${t} ]`;
                }
                if (n.length > 0) {
                    n = s.componentTitleOrder === "top-down" ? n + s.componentTitleSeparator + t : t + s.componentTitleSeparator + n;
                } else {
                    n = t;
                }
            }
        }
        return n;
    }
    static resolveTitle(t, i, s) {
        let e = t.getTitle(i);
        if (s.transformTitle != null) {
            e = s.transformTitle(e, t, i);
        }
        return e;
    }
}

const H = /*@__PURE__*/ n.createInterface("IRouter", (t => t.singleton(Router)));

class Router {
    static get inject() {
        return [ s, i, _, F, F, W ];
    }
    constructor(t, i, s, e, n, h) {
        this.container = t;
        this.ea = i;
        this.navigator = s;
        this.viewer = e;
        this.store = n;
        this.configuration = h;
        this.rootScope = null;
        this.activeComponents = [];
        this.appendedInstructions = [];
        this.isActive = false;
        this.coordinators = [];
        this.loadedFirst = false;
        this.u = false;
        this.R = r(o);
        this.handleNavigatorNavigateEvent = t => {
            void this.I(t);
        };
        this.handleNavigatorStateChangeEvent = t => {
            if (t.state?.navigationIndex != null) {
                const i = Navigation.create(t.state.navigations[t.state.navigationIndex]);
                i.instruction = t.viewerState.instruction;
                i.fromBrowser = true;
                this.navigator.navigate(i).catch((t => {
                    throw t;
                }));
            } else {
                this.load(t.viewerState.instruction, {
                    fromBrowser: true
                }).catch((t => {
                    throw t;
                }));
            }
        };
        this.processNavigation = async t => {
            this.loadedFirst = true;
            const i = this.configuration.options;
            const s = NavigationCoordinator.create(this, t, {
                syncStates: this.configuration.options.navigationSyncStates
            });
            this.coordinators.push(s);
            s.appendedInstructions.push(...this.appendedInstructions.splice(0));
            this.ea.publish(RouterNavigationStartEvent.eventName, RouterNavigationStartEvent.create(t));
            let e = typeof t.instruction === "string" && !t.useFullStateInstruction ? await RoutingHook.invokeTransformFromUrl(t.instruction, s.navigation) : t.useFullStateInstruction ? t.fullStateInstruction : t.instruction;
            const n = i.basePath;
            if (n !== null && typeof e === "string" && e.startsWith(n) && !i.useUrlFragmentHash) {
                e = e.slice(n.length);
            }
            if (e === "/") {
                e = "";
            }
            if (typeof e === "string") {
                e = e === "" ? [ new RoutingInstruction("") ] : RoutingInstruction.parse(this, e);
            }
            t.scope ??= this.rootScope.scope;
            const r = await t.scope.processInstructions(e, [], t, s);
            return Runner.run(null, (() => {
                s.finalEndpoint();
                return s.waitForSyncState("completed");
            }), (() => {
                s.finalize();
                return this.updateNavigation(t);
            }), (() => {
                if (t.navigation.new && !t.navigation.first && !t.repeating && r.every((t => t.options.noHistory))) {
                    t.untracked = true;
                }
            }), (async () => {
                while (this.coordinators.length > 0 && this.coordinators[0].completed) {
                    const t = this.coordinators.shift();
                    await this.navigator.finalize(t.navigation, false);
                    this.ea.publish(RouterNavigationCompleteEvent.eventName, RouterNavigationCompleteEvent.create(t.navigation));
                    this.ea.publish(RouterNavigationEndEvent.eventName, RouterNavigationEndEvent.create(t.navigation));
                    t.navigation.process?.resolve(true);
                }
            }));
        };
    }
    get isNavigating() {
        return this.coordinators.length > 0;
    }
    get isRestrictedNavigation() {
        const t = this.configuration.options.navigationSyncStates;
        return t.includes("guardedLoad") || t.includes("unloaded") || t.includes("loaded") || t.includes("guarded") || t.includes("routed");
    }
    get statefulHistory() {
        return this.configuration.options.statefulHistoryLength !== void 0 && this.configuration.options.statefulHistoryLength > 0;
    }
    start() {
        if (this.isActive) {
            throw new Error("Router has already been started");
        }
        this.isActive = true;
        const t = this.container.get(g);
        this.rootScope = new ViewportScope(this, "rootScope", t.controller.viewModel, null, true, t.config.component);
        const i = this.configuration.options;
        if (i.basePath === null) {
            const s = new URL(t.host.baseURI);
            i.basePath = s.pathname;
        }
        if (i.basePath.endsWith("/")) {
            i.basePath = i.basePath.slice(0, -1);
        }
        this.navigator.start({
            store: this.store,
            viewer: this.viewer,
            statefulHistoryLength: this.configuration.options.statefulHistoryLength
        });
        this.navigatorStateChangeEventSubscription = this.ea.subscribe(NavigatorStateChangeEvent.eventName, this.handleNavigatorStateChangeEvent);
        this.navigatorNavigateEventSubscription = this.ea.subscribe(NavigatorNavigateEvent.eventName, this.handleNavigatorNavigateEvent);
        this.viewer.start({
            useUrlFragmentHash: this.configuration.options.useUrlFragmentHash
        });
        this.ea.publish(RouterStartEvent.eventName, RouterStartEvent.create());
    }
    stop() {
        if (!this.isActive) {
            throw new Error("Router has not been started");
        }
        this.ea.publish(RouterStopEvent.eventName, RouterStopEvent.create());
        this.navigator.stop();
        this.viewer.stop();
        this.navigatorStateChangeEventSubscription.dispose();
        this.navigatorNavigateEventSubscription.dispose();
    }
    async initialLoad() {
        const {instruction: t, hash: i} = this.viewer.viewerState;
        const s = this.load(t, {
            fragment: i,
            replacing: true,
            fromBrowser: false
        });
        this.loadedFirst = true;
        return s;
    }
    async I(t) {
        if (this.u) {
            if (this.C) {
                this.C.navigation.process?.resolve(false);
            }
            this.C = t;
            return;
        }
        this.u = true;
        try {
            await this.processNavigation(t.navigation);
        } catch (i) {
            t.navigation.process?.reject(i);
        } finally {
            this.u = false;
        }
        if (this.C) {
            const t = this.C;
            this.C = undefined;
            await this.I(t);
        }
    }
    get isProcessingNav() {
        return this.u || this.C != null;
    }
    getEndpoint(t, i) {
        return this.allEndpoints(t).find((t => t.name === i)) ?? null;
    }
    allEndpoints(t, i = false) {
        return this.rootScope.scope.allScopes(i).filter((i => t === null || i.type === t)).map((t => t.endpoint));
    }
    addEndpoint(t, ...i) {
        throw new Error("Not implemented");
    }
    connectEndpoint(t, i, s, e, n) {
        const r = s.container;
        const o = r.has(Router.closestEndpointKey, true) ? r.get(Router.closestEndpointKey) : this.rootScope;
        const u = o.connectedScope;
        if (t === null) {
            t = u.addEndpoint(i, e, s, n);
            h.instance(Router.closestEndpointKey, t).register(r);
        }
        return t;
    }
    disconnectEndpoint(t, i, s) {
        if (!i.connectedScope.parent.removeEndpoint(t, i, s)) {
            throw new Error("Router failed to remove endpoint: " + i.name);
        }
    }
    async load(t, i) {
        i = i ?? {};
        t = this.extractFragment(t, i);
        t = this.extractQuery(t, i);
        let s = null;
        ({instructions: t, scope: s} = this.applyLoadOptions(t, i));
        if ((i.append ?? false) && (!this.loadedFirst || this.isNavigating)) {
            t = RoutingInstruction.from(this, t);
            this.appendInstructions(t, s);
            return Promise.resolve();
        }
        const e = Navigation.create({
            instruction: t,
            fullStateInstruction: "",
            scope: s,
            title: i.title,
            data: i.data,
            query: i.query,
            fragment: i.fragment,
            parameters: i.parameters,
            replacing: (i.replacing ?? false) || i.replace,
            repeating: i.append,
            fromBrowser: i.fromBrowser ?? false,
            origin: i.origin,
            completed: false
        });
        return this.navigator.navigate(e);
    }
    applyLoadOptions(t, i, s = true) {
        i = i ?? {};
        if ("origin" in i && !("context" in i)) {
            i.context = i.origin;
        }
        const {scope: e, instruction: n} = RoutingScope.for(i.context ?? null, typeof t === "string" ? t : undefined);
        if (typeof t === "string") {
            if (!s) {
                t = RoutingInstruction.from(this, n);
                for (const i of t) {
                    if (i.scope === null) {
                        i.scope = e;
                    }
                }
            } else {
                t = n;
            }
        } else {
            t = RoutingInstruction.from(this, t);
            for (const i of t) {
                if (i.scope === null) {
                    i.scope = e;
                }
            }
        }
        return {
            instructions: t,
            scope: e
        };
    }
    refresh() {
        return this.navigator.refresh();
    }
    back() {
        return this.navigator.go(-1);
    }
    forward() {
        return this.navigator.go(1);
    }
    go(t) {
        return this.navigator.go(t);
    }
    checkActive(t, i) {
        if (typeof t === "string") {
            throw new Error(`Parameter instructions to checkActivate can not be a string ('${t}')!`);
        }
        i = i ?? {};
        ({instructions: t} = this.applyLoadOptions(t, i));
        t.forEach((t => t.scope ??= this.rootScope.scope));
        const s = arrayUnique(t.map((t => t.scope)));
        for (const i of s) {
            const s = i.matchScope(t, false);
            const e = i.matchScope(this.activeComponents, true);
            if (!RoutingInstruction.contains(this, e, s, true)) {
                return false;
            }
        }
        return true;
    }
    unresolvedInstructionsError(t, i) {
        this.ea.publish(RouterNavigationErrorEvent.eventName, RouterNavigationErrorEvent.create(t));
        this.ea.publish(RouterNavigationEndEvent.eventName, RouterNavigationEndEvent.create(t));
        throw createUnresolvedinstructionsError(i, this.R);
    }
    cancelNavigation(t, i) {
        i.cancel();
        this.ea.publish(RouterNavigationCancelEvent.eventName, RouterNavigationCancelEvent.create(t));
        this.ea.publish(RouterNavigationEndEvent.eventName, RouterNavigationEndEvent.create(t));
    }
    appendInstructions(t, i = null) {
        if (i === null) {
            i = this.rootScope.scope;
        }
        for (const s of t) {
            if (s.scope === null) {
                s.scope = i;
            }
        }
        let s = null;
        for (let t = this.coordinators.length - 1; t >= 0; t--) {
            if (!this.coordinators[t].completed) {
                s = this.coordinators[t];
                break;
            }
        }
        if (s === null) {
            if (!this.loadedFirst) {
                this.appendedInstructions.push(...t);
            } else {
                throw Error("Router failed to append routing instructions to coordinator");
            }
        }
        s?.enqueueAppendedInstructions(t);
    }
    async updateNavigation(t) {
        this.rootScope.scope.reparentRoutingInstructions();
        const i = this.rootScope.scope.getRoutingInstructions(t.timestamp);
        let {matchedInstructions: s} = this.rootScope.scope.matchEndpoints(i, [], true);
        let e = 100;
        while (s.length > 0) {
            if (e-- === 0) {
                throw new Error("Router failed to find viewport when updating viewer paths.");
            }
            s = s.map((t => {
                const {matchedInstructions: i} = t.endpoint.instance.scope.matchEndpoints(t.nextScopeInstructions ?? [], [], true);
                return i;
            })).flat();
        }
        if (t.timestamp >= (this.activeNavigation?.timestamp ?? 0)) {
            this.activeNavigation = t;
            this.activeComponents = i;
        }
        let n = await RoutingHook.invokeTransformToUrl(i, t);
        if (typeof n !== "string") {
            n = RoutingInstruction.stringify(this, n, false, true);
        }
        n = await RoutingHook.invokeTransformToUrl(n, t);
        if (t.query == null && t.parameters != null) {
            const i = new URLSearchParams;
            for (let [s, e] of Object.entries(t.parameters)) {
                s = encodeURIComponent(s);
                if (!Array.isArray(e)) {
                    e = [ e ];
                }
                for (const t of e) {
                    i.append(s, encodeURIComponent(t));
                }
            }
            t.query = i.toString();
        }
        let r = `${this.configuration.options.basePath}/`;
        if (r === null || n !== "" && n[0] === "/" || this.configuration.options.useUrlFragmentHash) {
            r = "";
        }
        const o = (t.query?.length ?? 0) > 0 ? "?" + t.query : "";
        const h = (t.fragment?.length ?? 0) > 0 ? "#" + t.fragment : "";
        t.path = r + n + o + h;
        const u = [];
        const a = i.length === 1 ? i[0].route : null;
        if (!(a != null && (typeof a === "string" && a === "" || a.matching === ""))) {
            u.push(RoutingInstruction.create(RoutingInstruction.clear(this)));
        }
        u.push(...RoutingInstruction.clone(i, this.statefulHistory));
        t.fullStateInstruction = u;
        if ((t.title ?? null) === null) {
            const s = await Title.getTitle(i, t, this.configuration.options.title);
            if (s !== null) {
                t.title = s;
            }
        }
        return Promise.resolve();
    }
    extractFragment(t, i) {
        if (typeof t === "string" && i.fragment == null) {
            const [s, e] = t.split("#");
            t = s;
            i.fragment = e;
        }
        return t;
    }
    extractQuery(t, i) {
        if (typeof t === "string" && i.query == null) {
            const [s, e] = t.split("?");
            t = s;
            i.query = e;
        }
        if (typeof i.parameters === "string" && i.query == null) {
            i.query = i.parameters;
            i.parameters = void 0;
        }
        if (typeof i.query === "string" && i.query.length > 0) {
            i.parameters ??= {};
            const t = new URLSearchParams(i.query);
            t.forEach(((t, s) => {
                s = decodeURIComponent(s);
                t = decodeURIComponent(t);
                if (s in i.parameters) {
                    if (!Array.isArray(i.parameters[s])) {
                        i.parameters[s] = [ i.parameters[s] ];
                    }
                    i.parameters[s].push(t);
                } else {
                    i.parameters[s] = t;
                }
            }));
        }
        return t;
    }
}

Router.closestEndpointKey = e.annotation.keyFor("closest-endpoint");

function createUnresolvedinstructionsError(t, i) {
    const s = new Error(`${t.length} remaining instructions after 100 iterations; there is likely an infinite loop.`);
    s.remainingInstructions = t;
    i.warn(s, s.remainingInstructions);
    return s;
}

class RouterEvent {
    constructor(t) {
        this.eventName = t;
    }
}

class RouterStartEvent extends RouterEvent {
    static create() {
        return new RouterStartEvent(this.eventName);
    }
}

RouterStartEvent.eventName = "au:router:router-start";

class RouterStopEvent extends RouterEvent {
    static create() {
        return new RouterStopEvent(this.eventName);
    }
}

RouterStopEvent.eventName = "au:router:router-stop";

class RouterNavigationEvent {
    constructor(t, i) {
        this.eventName = t;
        this.navigation = i;
    }
}

class RouterNavigationStartEvent extends RouterNavigationEvent {
    static create(t) {
        return new RouterNavigationStartEvent(this.eventName, t);
    }
}

RouterNavigationStartEvent.eventName = "au:router:navigation-start";

class RouterNavigationEndEvent extends RouterNavigationEvent {
    static create(t) {
        return new RouterNavigationEndEvent(this.eventName, t);
    }
}

RouterNavigationEndEvent.eventName = "au:router:navigation-end";

class RouterNavigationCancelEvent extends RouterNavigationEvent {
    static create(t) {
        return new RouterNavigationCancelEvent(this.eventName, t);
    }
}

RouterNavigationCancelEvent.eventName = "au:router:navigation-cancel";

class RouterNavigationCompleteEvent extends RouterNavigationEvent {
    static create(t) {
        return new RouterNavigationCompleteEvent(this.eventName, t);
    }
}

RouterNavigationCompleteEvent.eventName = "au:router:navigation-complete";

class RouterNavigationErrorEvent extends RouterNavigationEvent {
    static create(t) {
        return new RouterNavigationErrorEvent(this.eventName, t);
    }
}

RouterNavigationErrorEvent.eventName = "au:router:navigation-error";

const M = /*@__PURE__*/ n.createInterface("ILinkHandler", (t => t.singleton(LinkHandler)));

class LinkHandler {
    constructor() {
        this.window = r(f);
        this.router = r(H);
    }
    handleEvent(t) {
        this.handleClick(t);
    }
    handleClick(t) {
        if (t.button !== 0 || t.altKey || t.ctrlKey || t.metaKey || t.shiftKey) {
            return;
        }
        const i = t.currentTarget;
        if (i.hasAttribute("external")) {
            return;
        }
        const s = i.getAttribute("target") ?? "";
        if (s.length > 0 && s !== this.window.name && s !== "_self") {
            return;
        }
        const e = v.for(i, "load");
        const n = e !== void 0 ? e.viewModel.value : null;
        const r = this.router.configuration.options.useHref && i.hasAttribute("href") ? i.getAttribute("href") : null;
        if ((n === null || n.length === 0) && (r === null || r.length === 0)) {
            return;
        }
        t.preventDefault();
        let o = n ?? r ?? "";
        if (typeof o === "string" && o.startsWith("#")) {
            o = o.slice(1);
            if (!o.startsWith("/")) {
                o = `/${o}`;
            }
        }
        this.router.load(o, {
            origin: i
        }).catch((t => {
            throw t;
        }));
    }
}

function route(t) {
    return function(i) {
        return Route.configure(t, i);
    };
}

function getValueOrAttribute(t, i, s, e, n = false) {
    if (n) {
        return i === "";
    }
    if (s) {
        return i;
    }
    const r = e.getAttribute(t) ?? "";
    return r.length > 0 ? r : i;
}

function waitForRouterStart(t, i) {
    if (t.isActive) {
        return;
    }
    return new Promise((t => {
        const s = i.subscribe(RouterStartEvent.eventName, (() => {
            t();
            s.dispose();
        }));
    }));
}

function getConsideredActiveInstructions(t, i, s, e) {
    let n = v.for(s, "considered-active")?.viewModel?.value;
    if (n === void 0) {
        n = e;
    }
    const r = t.applyLoadOptions(n, {
        context: i
    });
    const o = RoutingInstruction.from(t, r.instructions);
    for (const t of o) {
        if (t.scope === null) {
            t.scope = r.scope;
        }
    }
    return o;
}

function getLoadIndicator(t) {
    let i = t.parentElement;
    while (i != null) {
        if (i.tagName === "AU-VIEWPORT") {
            i = null;
            break;
        }
        if (i.hasAttribute("load-active")) {
            break;
        }
        i = i.parentElement;
    }
    i ??= t;
    return i;
}

const L = m.toView;

const j = u.createInjectable();

let B = class ViewportCustomElement {
    constructor() {
        this.name = "default";
        this.usedBy = "";
        this.default = "";
        this.fallback = "";
        this.fallbackAction = "";
        this.noScope = false;
        this.noLink = false;
        this.noTitle = false;
        this.noHistory = false;
        this.stateful = false;
        this.endpoint = null;
        this.pendingChildren = [];
        this.pendingPromise = null;
        this.isBound = false;
        this.router = r(H);
        this.element = r(R);
        this.container = r(s);
        this.ea = r(i);
        this.parentViewport = r(j);
        this.instruction = r(I);
    }
    hydrated(t) {
        this.controller = t;
        const i = this.instruction.props.filter((t => t.to === "default")).length > 0;
        if (i && this.parentViewport != null) {
            this.parentViewport.pendingChildren.push(this);
            if (this.parentViewport.pendingPromise === null) {
                this.parentViewport.pendingPromise = new OpenPromise;
            }
        }
        return Runner.run(null, (() => waitForRouterStart(this.router, this.ea)), (() => {
            if (this.router.isRestrictedNavigation) {
                this.connect();
            }
        }));
    }
    binding(t, i) {
        this.isBound = true;
        return Runner.run(null, (() => waitForRouterStart(this.router, this.ea)), (() => {
            if (!this.router.isRestrictedNavigation) {
                this.connect();
            }
        }), (() => {
            if (this.endpoint?.activeResolve != null) {
                this.endpoint.activeResolve();
                this.endpoint.activeResolve = null;
            }
        }), (() => {
            if (this.endpoint !== null && this.endpoint.getNextContent() === null) {
                return this.endpoint.activate(null, t, this.controller, void 0)?.asValue;
            }
        }));
    }
    detaching(t, i) {
        if (this.endpoint !== null) {
            this.isBound = false;
            return this.endpoint.deactivate(null, t, i);
        }
    }
    unbinding(t, i) {
        if (this.endpoint !== null) {
            return this.disconnect(null);
        }
    }
    dispose() {
        this.endpoint?.dispose();
        this.endpoint = null;
    }
    connect() {
        const {isBound: t, element: i} = this;
        const s = getValueOrAttribute("name", this.name, t, i);
        const e = {};
        e.scope = !getValueOrAttribute("no-scope", this.noScope, false, i, true);
        e.usedBy = getValueOrAttribute("used-by", this.usedBy, t, i);
        e.default = getValueOrAttribute("default", this.default, t, i);
        e.fallback = getValueOrAttribute("fallback", this.fallback, t, i);
        e.fallbackAction = getValueOrAttribute("fallback-action", this.fallbackAction, t, i);
        e.noLink = getValueOrAttribute("no-link", this.noLink, t, i, true);
        e.noTitle = getValueOrAttribute("no-title", this.noTitle, t, i, true);
        e.noHistory = getValueOrAttribute("no-history", this.noHistory, t, i, true);
        e.stateful = getValueOrAttribute("stateful", this.stateful, t, i, true);
        Object.keys(e).forEach((t => {
            if (e[t] === undefined) {
                delete e[t];
            }
        }));
        this.endpoint = this.router.connectEndpoint(this.endpoint, "Viewport", this, s, e);
        const n = this.parentViewport;
        if (n != null) {
            arrayRemove(n.pendingChildren, (t => t === this));
            if (n.pendingChildren.length === 0 && n.pendingPromise !== null) {
                n.pendingPromise.resolve();
                n.pendingPromise = null;
            }
        }
    }
    disconnect(t) {
        if (this.endpoint !== null) {
            this.router.disconnectEndpoint(t, this.endpoint, this);
        }
    }
    setActivity(t, i) {
        const s = this.router.configuration.options.indicators.viewportNavigating;
        if (typeof t === "string") {
            this.element.classList.toggle(t, i);
        } else {
            for (const e in t) {
                this.element.classList.toggle(`${s}-${e}`, i && t[e]);
            }
        }
    }
};

__decorate([ y ], B.prototype, "name", void 0);

__decorate([ y ], B.prototype, "usedBy", void 0);

__decorate([ y ], B.prototype, "default", void 0);

__decorate([ y ], B.prototype, "fallback", void 0);

__decorate([ y ], B.prototype, "fallbackAction", void 0);

__decorate([ y ], B.prototype, "noScope", void 0);

__decorate([ y ], B.prototype, "noLink", void 0);

__decorate([ y ], B.prototype, "noTitle", void 0);

__decorate([ y ], B.prototype, "noHistory", void 0);

__decorate([ y ], B.prototype, "stateful", void 0);

B = __decorate([ w({
    name: "au-viewport",
    injectable: j
}) ], B);

const q = u.createInjectable();

let z = class ViewportScopeCustomElement {
    constructor() {
        this.name = "default";
        this.catches = "";
        this.collection = false;
        this.source = null;
        this.viewportScope = null;
        this.isBound = false;
        this.router = r(H);
        this.element = r(R);
        this.container = r(s);
        this.parent = r(q);
        this.parentController = r(E);
    }
    hydrated(t) {
        this.controller = t;
    }
    bound(t, i) {
        this.isBound = true;
        this.$controller.scope = this.parentController.scope;
        this.connect();
        if (this.viewportScope !== null) {
            this.viewportScope.binding();
        }
    }
    unbinding(t, i) {
        if (this.viewportScope !== null) {
            this.viewportScope.unbinding();
        }
        return Promise.resolve();
    }
    connect() {
        if (this.router.rootScope === null) {
            return;
        }
        const t = this.getAttribute("name", this.name);
        const i = {};
        let s = this.getAttribute("catches", this.catches);
        if (s !== void 0) {
            i.catches = s;
        }
        s = this.getAttribute("collection", this.collection, true);
        if (s !== void 0) {
            i.collection = s;
        }
        i.source = this.source ?? null;
        this.viewportScope = this.router.connectEndpoint(this.viewportScope, "ViewportScope", this, t, i);
    }
    disconnect() {
        if (this.viewportScope) {
            this.router.disconnectEndpoint(null, this.viewportScope, this);
        }
        this.viewportScope = null;
    }
    getAttribute(t, i, s = false) {
        if (this.isBound) {
            return i;
        } else {
            if (this.element.hasAttribute(t)) {
                if (s) {
                    return true;
                } else {
                    i = this.element.getAttribute(t);
                    if (i.length > 0) {
                        return i;
                    }
                }
            }
        }
        return void 0;
    }
};

__decorate([ y ], z.prototype, "name", void 0);

__decorate([ y ], z.prototype, "catches", void 0);

__decorate([ y ], z.prototype, "collection", void 0);

__decorate([ y ], z.prototype, "source", void 0);

z = __decorate([ w({
    name: "au-viewport-scope",
    template: "<template></template>",
    containerless: false,
    injectable: q
}) ], z);

let D = class LoadCustomAttribute {
    constructor() {
        this.N = false;
        this.hasHref = null;
        this.element = r(R);
        this.router = r(H);
        this.linkHandler = r(M);
        this.ea = r(i);
        this.activeClass = this.router.configuration.options.indicators.loadActive;
        this.navigationEndHandler = t => {
            void this.updateActive();
        };
    }
    binding() {
        if (this.value == null) {
            this.N = true;
        }
        this.element.addEventListener("click", this.linkHandler);
        this.updateValue();
        void this.updateActive();
        this.routerNavigationSubscription = this.ea.subscribe(RouterNavigationEndEvent.eventName, this.navigationEndHandler);
    }
    unbinding() {
        this.element.removeEventListener("click", this.linkHandler);
        this.routerNavigationSubscription.dispose();
    }
    valueChanged(t) {
        this.updateValue();
        void this.updateActive();
    }
    updateValue() {
        if (this.N) {
            this.value = {
                component: this.component,
                parameters: this.parameters,
                viewport: this.viewport,
                id: this.id
            };
        }
        if (this.hasHref === null) {
            this.hasHref = this.element.hasAttribute("href");
        }
        if (!this.hasHref) {
            let t = this.value;
            if (typeof t !== "string") {
                const i = RoutingInstruction.from(this.router, t).shift();
                const s = this.$(t);
                if (s.foundConfiguration) {
                    i.route = s.matching;
                }
                t = RoutingInstruction.stringify(this.router, [ i ]);
            }
            const {scope: i, instruction: s} = RoutingScope.for(this.element, t);
            const e = i?.path ?? "";
            t = `${e}${s ?? ""}`;
            if (this.router.configuration.options.useUrlFragmentHash && !t.startsWith("#")) {
                t = `#/${t}`;
            }
            this.element.setAttribute("href", t);
        }
    }
    async updateActive() {
        const t = v.for(this.element, "load").parent;
        const i = typeof this.value === "string" ? {
            id: this.value,
            path: this.value
        } : this.value;
        const s = this.$(i);
        const e = s.foundConfiguration ? s.instructions : getConsideredActiveInstructions(this.router, t, this.element, this.value);
        const n = getLoadIndicator(this.element);
        n.classList.toggle(this.activeClass, this.router.checkActive(e, {
            context: t
        }));
    }
    $(t) {
        if (typeof t === "string") {
            return new FoundRoute;
        }
        const i = RoutingScope.for(this.element).scope ?? this.router.rootScope.scope;
        if (t.id != null) {
            return i.findMatchingRoute(t.id, t.parameters ?? {});
        }
        const s = t.path;
        if (s != null) {
            return i.findMatchingRoute(s, t.parameters ?? {});
        }
        return new FoundRoute;
    }
};

__decorate([ y({
    mode: L
}) ], D.prototype, "value", void 0);

__decorate([ y ], D.prototype, "component", void 0);

__decorate([ y ], D.prototype, "parameters", void 0);

__decorate([ y ], D.prototype, "viewport", void 0);

__decorate([ y ], D.prototype, "id", void 0);

D = __decorate([ C("load") ], D);

let Q = class HrefCustomAttribute {
    constructor() {
        this.element = r(R);
        this.router = r(H);
        this.linkHandler = r(M);
        this.ea = r(i);
        this.activeClass = this.router.configuration.options.indicators.loadActive;
        this.navigationEndHandler = t => {
            this.updateActive();
        };
    }
    binding() {
        if (this.router.configuration.options.useHref && !this.hasLoad() && !this.element.hasAttribute("external")) {
            this.element.addEventListener("click", this.linkHandler);
            this.routerNavigationSubscription = this.ea.subscribe(RouterNavigationEndEvent.eventName, this.navigationEndHandler);
        }
        this.updateValue();
        this.updateActive();
    }
    unbinding() {
        this.element.removeEventListener("click", this.linkHandler);
        this.routerNavigationSubscription?.dispose();
    }
    valueChanged() {
        this.updateValue();
        this.updateActive();
    }
    updateValue() {
        this.element.setAttribute("href", this.value);
    }
    updateActive() {
        if (this.router.configuration.options.useHref && !this.hasLoad() && !this.element.hasAttribute("external")) {
            const t = v.for(this.element, "href").parent;
            const i = getConsideredActiveInstructions(this.router, t, this.element, this.value);
            const s = getLoadIndicator(this.element);
            s.classList.toggle(this.activeClass, this.router.checkActive(i, {
                context: t
            }));
        }
    }
    hasLoad() {
        const t = this.$controller.parent;
        const i = t.children;
        return i?.some((t => t.vmKind === "customAttribute" && t.viewModel instanceof D)) ?? false;
    }
};

__decorate([ y({
    mode: L
}) ], Q.prototype, "value", void 0);

Q = __decorate([ C({
    name: "href",
    noMultiBindings: true
}) ], Q);

let J = class ConsideredActiveCustomAttribute {};

__decorate([ y({
    mode: L
}) ], J.prototype, "value", void 0);

J = __decorate([ C("considered-active") ], J);

const W = /*@__PURE__*/ n.createInterface("IRouterConfiguration", (t => t.singleton(RouterConfiguration)));

const G = H;

const K = [ G ];

const Z = B;

const X = z;

const Y = D;

const tt = Q;

const it = [ B, z, D, Q, J ];

class RouterConfiguration {
    static register(t) {
        const i = t.get(W);
        i.options = RouterConfiguration.options;
        i.options.setRouterConfiguration(i);
        RouterConfiguration.options = RouterOptions.create();
        return t.register(...K, ...it, S.activating(H, RouterConfiguration.configurationCall), S.activated(H, (t => t.initialLoad())), S.deactivated(H, (t => t.stop())));
    }
    static customize(t) {
        if (t === undefined) {
            RouterConfiguration.options = RouterOptions.create();
            RouterConfiguration.configurationCall = t => {
                t.start();
            };
        } else if (t instanceof Function) {
            RouterConfiguration.configurationCall = t;
        } else {
            RouterConfiguration.options = RouterOptions.create();
            RouterConfiguration.options.apply(t);
        }
        return RouterConfiguration;
    }
    static createContainer() {
        return this.register(n.createContainer());
    }
    static for(t) {
        if (t instanceof Router) {
            return t.configuration;
        }
        return t.get(W);
    }
    apply(t, i = false) {
        if (i) {
            this.options = RouterOptions.create();
        }
        this.options.apply(t);
    }
    addHook(t, i) {
        return RoutingHook.add(t, i);
    }
    removeHook(t) {
        return RoutingHook.remove(t);
    }
    removeAllHooks() {
        return RoutingHook.removeAll();
    }
}

RouterConfiguration.options = RouterOptions.create();

RouterConfiguration.configurationCall = t => {
    t.start();
};

export { O as ConfigurableRoute, J as ConsideredActiveCustomAttribute, K as DefaultComponents, it as DefaultResources, V as Endpoint, EndpointContent, FoundRoute, Q as HrefCustomAttribute, tt as HrefCustomAttributeRegistration, M as ILinkHandler, H as IRouter, W as IRouterConfiguration, InstructionParameters, LinkHandler, D as LoadCustomAttribute, Y as LoadCustomAttributeRegistration, Navigation, NavigationCoordinator, NavigationFlags, _ as Navigator, x as RecognizedRoute, U as RecognizerEndpoint, Route, T as RouteRecognizer, Router, RouterConfiguration, RouterNavigationCancelEvent, RouterNavigationCompleteEvent, RouterNavigationEndEvent, RouterNavigationErrorEvent, RouterNavigationStartEvent, RouterOptions, G as RouterRegistration, RouterStartEvent, RouterStopEvent, A as Routes, RoutingHook, RoutingInstruction, RoutingScope, Runner, Step, Viewport, ViewportContent, B as ViewportCustomElement, Z as ViewportCustomElementRegistration, ViewportOptions, ViewportScope, ViewportScopeContent, z as ViewportScopeCustomElement, X as ViewportScopeCustomElementRegistration, route, routes };
//# sourceMappingURL=index.mjs.map
