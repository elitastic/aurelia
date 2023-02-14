import { IPlatform as t, ILogger as e, Registration as r } from "@aurelia/kernel";

import { IWindow as i, Controller as s } from "@aurelia/runtime-html";

import { BehaviorSubject as n, Subscription as o, Observable as c } from "rxjs";

import { skip as a, take as h, delay as u } from "rxjs/operators";

function f(t, e) {
    if (!y(t)) throw Error("Provided state is not of type StateHistory");
    if (e > 0) return d(t, e - 1);
    if (e < 0) return p(t, t.past.length + e);
    return t;
}

function d(t, e) {
    if (e < 0 || e >= t.future.length) return t;
    const {past: r, future: i, present: s} = t;
    const n = [ ...r, s, ...i.slice(0, e) ];
    const o = i[e];
    const c = i.slice(e + 1);
    return {
        past: n,
        present: o,
        future: c
    };
}

function p(t, e) {
    if (e < 0 || e >= t.past.length) return t;
    const {past: r, future: i, present: s} = t;
    const n = r.slice(0, e);
    const o = [ ...r.slice(e + 1), s, ...i ];
    const c = r[e];
    return {
        past: n,
        present: c,
        future: o
    };
}

function l(t, e) {
    return {
        ...t,
        past: [ ...t.past, t.present ],
        present: e,
        future: []
    };
}

function w(t, e) {
    if (y(t)) {
        if (t.past.length > e) t.past = t.past.slice(t.past.length - e);
        if (t.future.length > e) t.future = t.future.slice(0, e);
    }
    return t;
}

function y(t) {
    return "undefined" !== typeof t.present && "undefined" !== typeof t.future && "undefined" !== typeof t.past && Array.isArray(t.future) && Array.isArray(t.past);
}

function g(t, e, r, i) {
    var s = arguments.length, n = s < 3 ? e : null === i ? i = Object.getOwnPropertyDescriptor(e, r) : i, o;
    if ("object" === typeof Reflect && "function" === typeof Reflect.decorate) n = Reflect.decorate(t, e, r, i); else for (var c = t.length - 1; c >= 0; c--) if (o = t[c]) n = (s < 3 ? o(n) : s > 3 ? o(e, r, n) : o(e, r)) || n;
    return s > 3 && n && Object.defineProperty(e, r, n), n;
}

function m(t, e) {
    return function(r, i) {
        e(r, i, t);
    };
}

const v = "aurelia-store-state";

var E;

(function(t) {
    t["Before"] = "before";
    t["After"] = "after";
})(E || (E = {}));

function T(e, r, i) {
    const s = j.container.get(t).console;
    const n = void 0 !== i?.logType && void 0 !== s[i.logType] ? i.logType : "log";
    s[n]("New state: ", e);
}

function A(t, e, r) {
    const s = j.container.get(i).localStorage;
    if (void 0 !== s) {
        const e = void 0 !== r?.key ? r.key : v;
        s.setItem(e, JSON.stringify(t));
    }
}

function D(t, e) {
    const r = j.container.get(i).localStorage;
    if (void 0 === r) return t;
    const s = r.getItem(void 0 === e ? v : e);
    if (null === s || "" === s) return t;
    try {
        return JSON.parse(s);
    } catch {}
    return t;
}

var R;

(function(t) {
    t["trace"] = "trace";
    t["debug"] = "debug";
    t["info"] = "info";
    t["warn"] = "warn";
    t["error"] = "error";
})(R || (R = {}));

function S(t, e, r) {
    if (t.logDefinitions?.hasOwnProperty(e) && t.logDefinitions[e] && Object.values(R).includes(t.logDefinitions[e])) return t.logDefinitions[e];
    return r;
}

var b;

(function(t) {
    t["StartEnd"] = "startEnd";
    t["All"] = "all";
})(b || (b = {}));

const j = {
    container: null
};

class UnregisteredActionError extends Error {
    constructor(t) {
        super(`Tried to dispatch an unregistered action ${void 0 !== t && ("string" === typeof t ? t : t.name)}`);
    }
}

class DevToolsRemoteDispatchError extends Error {}

class ActionRegistrationError extends Error {}

class ReducerNoStateError extends Error {}

let O = class Store {
    constructor(t, e, r, i) {
        this.initialState = t;
        this.logger = e;
        this.t = r;
        this.devToolsAvailable = false;
        this.actions = new Map;
        this.middlewares = new Map;
        this.dispatchQueue = [];
        this.options = i ?? {};
        const s = true === this.options?.history?.undoable;
        this._state = new n(t);
        this.state = this._state.asObservable();
        if (true !== this.options?.devToolsOptions?.disable) this.setupDevTools();
        if (s) this.registerHistoryMethods();
    }
    registerMiddleware(t, e, r) {
        this.middlewares.set(t, {
            placement: e,
            settings: r
        });
    }
    unregisterMiddleware(t) {
        if (this.middlewares.has(t)) this.middlewares.delete(t);
    }
    isMiddlewareRegistered(t) {
        return this.middlewares.has(t);
    }
    registerAction(t, e) {
        if (0 === e.length) throw new ActionRegistrationError("The reducer is expected to have one or more parameters, where the first will be the present state");
        this.actions.set(e, {
            type: t
        });
    }
    unregisterAction(t) {
        if (this.actions.has(t)) this.actions.delete(t);
    }
    isActionRegistered(t) {
        if ("string" === typeof t) return void 0 !== Array.from(this.actions).find((e => e[1].type === t));
        return this.actions.has(t);
    }
    resetToState(t) {
        this._state.next(t);
    }
    async dispatch(t, ...e) {
        const r = this.lookupAction(t);
        if (!r) return Promise.reject(new UnregisteredActionError(t));
        return this.queueDispatch([ {
            reducer: r,
            params: e
        } ]);
    }
    pipe(t, ...e) {
        const r = [];
        const i = {
            dispatch: async () => this.queueDispatch(r),
            pipe: (e, ...s) => {
                const n = this.lookupAction(e);
                if (!n) throw new UnregisteredActionError(t);
                r.push({
                    reducer: n,
                    params: s
                });
                return i;
            }
        };
        return i.pipe(t, ...e);
    }
    lookupAction(t) {
        if ("string" === typeof t) {
            const e = Array.from(this.actions).find((([e, r]) => r.type === t));
            if (e) return e[0];
        } else if (this.actions.has(t)) return t;
        return;
    }
    async queueDispatch(t) {
        return new Promise(((e, r) => {
            this.dispatchQueue.push({
                actions: t,
                resolve: e,
                reject: r
            });
            if (1 === this.dispatchQueue.length) this.handleQueue();
        }));
    }
    async handleQueue() {
        if (this.dispatchQueue.length > 0) {
            const t = this.dispatchQueue[0];
            try {
                await this.internalDispatch(t.actions);
                t.resolve();
            } catch (e) {
                t.reject(e);
            }
            this.dispatchQueue.shift();
            this.handleQueue();
        }
    }
    async internalDispatch(t) {
        const e = t.find((t => !this.actions.has(t.reducer)));
        if (e) throw new UnregisteredActionError(e.reducer);
        j.container.get(i).performance.mark("dispatch-start");
        const r = t.map((t => ({
            type: this.actions.get(t.reducer).type,
            params: t.params,
            reducer: t.reducer
        })));
        const s = {
            name: r.map((t => t.type)).join("->"),
            params: r.reduce(((t, e) => t.concat(e.params)), []),
            pipedActions: r.map((t => ({
                name: t.type,
                params: t.params
            })))
        };
        if (this.options.logDispatchedActions) this.logger[S(this.options, "dispatchedActions", R.info)](`Dispatching: ${s.name}`);
        const n = await this.executeMiddlewares(this._state.getValue(), E.Before, s);
        if (false === n) {
            j.container.get(i).performance.clearMarks();
            j.container.get(i).performance.clearMeasures();
            return;
        }
        let o = n;
        for (const t of r) {
            o = await t.reducer(o, ...t.params);
            if (false === o) {
                j.container.get(i).performance.clearMarks();
                j.container.get(i).performance.clearMeasures();
                return;
            }
            j.container.get(i).performance.mark(`dispatch-after-reducer-${t.type}`);
            if (!o && "object" !== typeof o) throw new ReducerNoStateError("The reducer has to return a new state");
        }
        let c = await this.executeMiddlewares(o, E.After, s);
        if (false === c) {
            j.container.get(i).performance.clearMarks();
            j.container.get(i).performance.clearMeasures();
            return;
        }
        if (y(c) && this.options.history?.limit) c = w(c, this.options.history.limit);
        this._state.next(c);
        j.container.get(i).performance.mark("dispatch-end");
        if (this.options.measurePerformance === b.StartEnd) {
            j.container.get(i).performance.measure("startEndDispatchDuration", "dispatch-start", "dispatch-end");
            const t = j.container.get(i).performance.getEntriesByName("startEndDispatchDuration");
            this.logger[S(this.options, "performanceLog", R.info)](`Total duration ${t[0].duration} of dispatched action ${s.name}:`, t);
        } else if (this.options.measurePerformance === b.All) {
            const t = j.container.get(i).performance.getEntriesByType("mark");
            const e = t[t.length - 1].startTime - t[0].startTime;
            this.logger[S(this.options, "performanceLog", R.info)](`Total duration ${e} of dispatched action ${s.name}:`, t);
        }
        j.container.get(i).performance.clearMarks();
        j.container.get(i).performance.clearMeasures();
        this.updateDevToolsState({
            type: s.name,
            params: s.params
        }, c);
    }
    executeMiddlewares(t, e, r) {
        return Array.from(this.middlewares).filter((t => t[1].placement === e)).reduce((async (t, s, n) => {
            try {
                const n = await s[0](await t, this._state.getValue(), s[1].settings, r);
                if (false === n) return false;
                return n || await t;
            } catch (e) {
                if (this.options.propagateError) throw e;
                return await t;
            } finally {
                j.container.get(i).performance.mark(`dispatch-${e}-${s[0].name}`);
            }
        }), t);
    }
    setupDevTools() {
        if (this.t.__REDUX_DEVTOOLS_EXTENSION__) {
            this.logger[S(this.options, "devToolsStatus", R.debug)]("DevTools are available");
            this.devToolsAvailable = true;
            this.devTools = this.t.__REDUX_DEVTOOLS_EXTENSION__.connect(this.options.devToolsOptions);
            this.devTools.init(this.initialState);
            this.devTools.subscribe((t => {
                this.logger[S(this.options, "devToolsStatus", R.debug)](`DevTools sent change ${t.type}`);
                if ("ACTION" === t.type && void 0 !== t.payload) {
                    const e = Array.from(this.actions).find((function([e]) {
                        return e.name === t.payload?.name;
                    }));
                    const r = this.lookupAction(t.payload?.name) ?? e?.[0];
                    if (!r) throw new DevToolsRemoteDispatchError("Tried to remotely dispatch an unregistered action");
                    if (!t.payload.args || t.payload.args.length < 1) throw new DevToolsRemoteDispatchError("No action arguments provided");
                    this.dispatch(r, ...t.payload.args.slice(1).map((t => JSON.parse(t)))).catch((() => {
                        throw new DevToolsRemoteDispatchError("Issue when trying to dispatch an action through devtools");
                    }));
                    return;
                }
                if ("DISPATCH" === t.type && t.payload) switch (t.payload.type) {
                  case "JUMP_TO_STATE":
                  case "JUMP_TO_ACTION":
                    this._state.next(JSON.parse(t.state));
                    return;

                  case "COMMIT":
                    this.devTools.init(this._state.getValue());
                    return;

                  case "RESET":
                    this.devTools.init(this.initialState);
                    this.resetToState(this.initialState);
                    return;

                  case "ROLLBACK":
                    {
                        const e = JSON.parse(t.state);
                        this.resetToState(e);
                        this.devTools.init(e);
                        return;
                    }
                }
            }));
        }
    }
    updateDevToolsState(t, e) {
        if (this.devToolsAvailable && this.devTools) this.devTools.send(t, e);
    }
    registerHistoryMethods() {
        this.registerAction("jump", f);
    }
};

O = g([ m(1, e), m(2, i) ], O);

function M(t) {
    const e = j.container.get(O);
    return async function(...r) {
        return e.dispatch(t, ...r);
    };
}

async function N(t, e, ...r) {
    const i = (t, r) => async i => {
        if (e) {
            console.group(`Step ${r}`);
            console.log(i);
            console.groupEnd();
        }
        await t(i);
    };
    const s = (t, e) => async r => {
        try {
            await t(r);
        } catch (t) {
            e(t);
        }
    };
    const n = (t, e) => async r => {
        await t(r);
        e();
    };
    return new Promise(((e, o) => {
        let c = 0;
        r.slice(0, -1).forEach((e => {
            t.state.pipe(a(c), h(1), u(0)).subscribe(s(i(e, c), o));
            c++;
        }));
        t.state.pipe(a(c), h(1)).subscribe(n(s(i(r[r.length - 1], c), o), e));
    }));
}

const $ = t => t.state;

function x(t) {
    const e = {
        selector: "function" === typeof t ? t : $,
        ...t
    };
    function r(t, e) {
        const r = e(t);
        if (r instanceof c) return r;
        return t.state;
    }
    function i() {
        const t = "object" === typeof e.selector;
        const r = {
            [e.target || "state"]: e.selector || $
        };
        return Object.entries({
            ...t ? e.selector : r
        }).map((([r, i]) => ({
            targets: e.target && t ? [ e.target, r ] : [ r ],
            selector: i,
            changeHandlers: {
                [e.onChanged ?? ""]: 1,
                [`${e.target ?? r}Changed`]: e.target ? 0 : 1,
                propertyChanged: 0
            }
        })));
    }
    return function(e) {
        const n = "object" === typeof t && t.setup ? e.prototype[t.setup] : e.prototype.binding;
        const c = "object" === typeof t && t.teardown ? e.prototype[t.teardown] : e.prototype.unbinding;
        e.prototype["object" === typeof t && void 0 !== t.setup ? t.setup : "binding"] = function() {
            if ("object" === typeof t && "string" === typeof t.onChanged && !(t.onChanged in this)) throw new Error("Provided onChanged handler does not exist on target VM");
            const e = s.getCached(this) ? s.getCached(this).container.get(O) : j.container.get(O);
            this._stateSubscriptions = i().map((t => r(e, t.selector).subscribe((e => {
                const r = t.targets.length - 1;
                const i = t.targets.reduce(((t = {}, e) => t[e]), this);
                Object.entries(t.changeHandlers).forEach((([s, n]) => {
                    if (s in this) this[s](...[ t.targets[r], e, i ].slice(n, 3));
                }));
                t.targets.reduce(((t, i, s) => {
                    t[i] = s === r ? e : t[i] || {};
                    return t[i];
                }), this);
            }))));
            if (n) return n.apply(this, arguments);
        };
        e.prototype["object" === typeof t && t.teardown ? t.teardown : "unbinding"] = function() {
            if (this._stateSubscriptions && Array.isArray(this._stateSubscriptions)) this._stateSubscriptions.forEach((t => {
                if (t instanceof o && false === t.closed) t.unsubscribe();
            }));
            if (c) return c.apply(this, arguments);
        };
    };
}

const C = {
    withInitialState(t) {
        Reflect.set(this, "state", t);
        return this;
    },
    withOptions(t) {
        Reflect.set(this, "options", t);
        return this;
    },
    register(t) {
        j.container = t;
        const s = Reflect.get(this, "state");
        const n = Reflect.get(this, "options");
        const o = t.get(e);
        const c = t.get(i);
        if (!s) throw new Error("initialState must be provided via withInitialState builder method");
        let a = s;
        if (n?.history?.undoable && !y(s)) a = {
            past: [],
            present: s,
            future: []
        };
        r.instance(O, new O(a, o, c, n)).register(t);
        return t;
    }
};

export { ActionRegistrationError, v as DEFAULT_LOCAL_STORAGE_KEY, DevToolsRemoteDispatchError, R as LogLevel, E as MiddlewarePlacement, b as PerformanceMeasurement, ReducerNoStateError, j as STORE, O as Store, C as StoreConfiguration, UnregisteredActionError, w as applyLimits, x as connectTo, M as dispatchify, N as executeSteps, S as getLogType, y as isStateHistory, f as jump, A as localStorageMiddleware, T as logMiddleware, l as nextStateHistory, D as rehydrateFromLocalStorage };
//# sourceMappingURL=index.mjs.map
