"use strict";

var t = require("@aurelia/kernel");

var e = require("@aurelia/metadata");

const r = Object.prototype.hasOwnProperty;

const s = Reflect.defineProperty;

function rtDefineHiddenProp(t, e, r) {
    s(t, e, {
        enumerable: false,
        configurable: true,
        writable: true,
        value: r
    });
    return r;
}

function ensureProto(t, e, r) {
    if (!(e in t)) {
        rtDefineHiddenProp(t, e, r);
    }
}

const i = Object.assign;

const n = Object.freeze;

const o = String;

const c = t.DI.createInterface;

const u = e.Metadata.get;

const a = e.Metadata.define;

const h = /*@__PURE__*/ t.DI.createInterface("ICoercionConfiguration");

const l = 0;

const f = 1;

const p = 2;

const d = 4;

const b = /*@__PURE__*/ n({
    None: l,
    Observer: f,
    Node: p,
    Layout: d
});

function copyIndexMap(t, e, r) {
    const {length: s} = t;
    const i = Array(s);
    let n = 0;
    while (n < s) {
        i[n] = t[n];
        ++n;
    }
    if (e !== void 0) {
        i.deletedIndices = e.slice(0);
    } else if (t.deletedIndices !== void 0) {
        i.deletedIndices = t.deletedIndices.slice(0);
    } else {
        i.deletedIndices = [];
    }
    if (r !== void 0) {
        i.deletedItems = r.slice(0);
    } else if (t.deletedItems !== void 0) {
        i.deletedItems = t.deletedItems.slice(0);
    } else {
        i.deletedItems = [];
    }
    i.isIndexMap = true;
    return i;
}

function createIndexMap(t = 0) {
    const e = Array(t);
    let r = 0;
    while (r < t) {
        e[r] = r++;
    }
    e.deletedIndices = [];
    e.deletedItems = [];
    e.isIndexMap = true;
    return e;
}

function cloneIndexMap(t) {
    const e = t.slice();
    e.deletedIndices = t.deletedIndices.slice();
    e.deletedItems = t.deletedItems.slice();
    e.isIndexMap = true;
    return e;
}

function isIndexMap(e) {
    return t.isArray(e) && e.isIndexMap === true;
}

let w = new Map;

let v = false;

function batch(t) {
    const e = w;
    const r = w = new Map;
    v = true;
    try {
        t();
    } finally {
        w = null;
        v = false;
        try {
            let t;
            let s;
            let i;
            let n;
            let o;
            let c = false;
            let u;
            let a;
            for (t of r) {
                s = t[0];
                i = t[1];
                if (e?.has(s)) {
                    e.set(s, i);
                }
                if (i[0] === 1) {
                    s.notify(i[1], i[2]);
                } else {
                    n = i[1];
                    o = i[2];
                    c = false;
                    if (o.deletedIndices.length > 0) {
                        c = true;
                    } else {
                        for (u = 0, a = o.length; u < a; ++u) {
                            if (o[u] !== u) {
                                c = true;
                                break;
                            }
                        }
                    }
                    if (c) {
                        s.notifyCollection(n, o);
                    }
                }
            }
        } finally {
            w = e;
        }
    }
}

function addCollectionBatch(t, e, r) {
    if (!w.has(t)) {
        w.set(t, [ 2, e, r ]);
    } else {
        w.get(t)[2] = r;
    }
}

function addValueBatch(t, e, r) {
    const s = w.get(t);
    if (s === void 0) {
        w.set(t, [ 1, e, r ]);
    } else {
        s[1] = e;
        s[2] = r;
    }
}

const g = /*@__PURE__*/ (() => {
    function subscriberCollection(t, e) {
        return t == null ? subscriberCollectionDeco : subscriberCollectionDeco(t);
    }
    function getSubscriberRecord() {
        return rtDefineHiddenProp(this, "subs", new SubscriberRecord);
    }
    function addSubscriber(t) {
        return this.subs.add(t);
    }
    function removeSubscriber(t) {
        return this.subs.remove(t);
    }
    const t = new WeakSet;
    function subscriberCollectionDeco(e, r) {
        if (!t.has(e)) {
            t.add(e);
            const r = e.prototype;
            s(r, "subs", {
                get: getSubscriberRecord
            });
            ensureProto(r, "subscribe", addSubscriber);
            ensureProto(r, "unsubscribe", removeSubscriber);
        }
        return e;
    }
    class SubscriberRecord {
        constructor() {
            this.count = 0;
            this.t = [];
            this.i = [];
            this.u = false;
        }
        add(t) {
            if (this.t.includes(t)) {
                return false;
            }
            this.t[this.t.length] = t;
            if ("handleDirty" in t) {
                this.i[this.i.length] = t;
                this.u = true;
            }
            ++this.count;
            return true;
        }
        remove(t) {
            let e = this.t.indexOf(t);
            if (e !== -1) {
                this.t.splice(e, 1);
                e = this.i.indexOf(t);
                if (e !== -1) {
                    this.i.splice(e, 1);
                    this.u = this.i.length > 0;
                }
                --this.count;
                return true;
            }
            return false;
        }
        notify(t, e) {
            if (v) {
                addValueBatch(this, t, e);
                return;
            }
            for (const r of this.t.slice(0)) {
                r.handleChange(t, e);
            }
        }
        notifyCollection(t, e) {
            const r = this.t.slice(0);
            const s = r.length;
            let i = 0;
            for (;i < s; ++i) {
                r[i].handleCollectionChange(t, e);
            }
            return;
        }
        notifyDirty() {
            if (this.u) {
                for (const t of this.i.slice(0)) {
                    t.handleDirty();
                }
            }
        }
    }
    return subscriberCollection;
})();

const createMappedError = (t, ...e) => new Error(`AUR${o(t).padStart(4, "0")}:${e.map(o)}`);

class CollectionLengthObserver {
    constructor(t) {
        this.owner = t;
        this.type = f;
        this.v = (this.o = t.collection).length;
    }
    getValue() {
        return this.o.length;
    }
    setValue(t) {
        if (t !== this.v) {
            if (!Number.isNaN(t)) {
                this.o.splice(t);
                this.v = this.o.length;
            }
        }
    }
    handleDirty() {
        if (this.v !== this.o.length) {
            this.subs.notifyDirty();
        }
    }
    handleCollectionChange(t, e) {
        const r = this.v;
        const s = this.o.length;
        if ((this.v = s) !== r) {
            this.subs.notifyDirty();
            this.subs.notify(this.v, r);
        }
    }
}

(() => {
    implementLengthObserver(CollectionLengthObserver);
})();

class CollectionSizeObserver {
    constructor(t) {
        this.owner = t;
        this.type = f;
        this.v = (this.o = t.collection).size;
    }
    getValue() {
        return this.o.size;
    }
    setValue() {
        throw createMappedError(220);
    }
    handleDirty() {
        if (this.v !== this.o.size) {
            this.subs.notifyDirty();
        }
    }
    handleCollectionChange(t, e) {
        const r = this.v;
        const s = this.o.size;
        if ((this.v = s) !== r) {
            this.subs.notify(this.v, r);
        }
    }
}

(() => {
    implementLengthObserver(CollectionSizeObserver);
})();

function implementLengthObserver(t) {
    const e = t.prototype;
    ensureProto(e, "subscribe", subscribe);
    ensureProto(e, "unsubscribe", unsubscribe);
    return g(t, null);
}

function subscribe(t) {
    if (this.subs.add(t) && this.subs.count === 1) {
        this.owner.subscribe(this);
    }
}

function unsubscribe(t) {
    if (this.subs.remove(t) && this.subs.count === 0) {
        this.owner.subscribe(this);
    }
}

const y = /*@__PURE__*/ (() => {
    const e = Symbol.for("__au_arr_obs__");
    const r = Array[e] ?? rtDefineHiddenProp(Array, e, new WeakMap);
    function sortCompare(t, e) {
        if (t === e) {
            return 0;
        }
        t = t === null ? "null" : t.toString();
        e = e === null ? "null" : e.toString();
        return t < e ? -1 : 1;
    }
    function preSortCompare(t, e) {
        if (t === void 0) {
            if (e === void 0) {
                return 0;
            } else {
                return 1;
            }
        }
        if (e === void 0) {
            return -1;
        }
        return 0;
    }
    function insertionSort(t, e, r, s, i) {
        let n, o, c, u, a;
        let h, l;
        for (h = r + 1; h < s; h++) {
            n = t[h];
            o = e[h];
            for (l = h - 1; l >= r; l--) {
                c = t[l];
                u = e[l];
                a = i(c, n);
                if (a > 0) {
                    t[l + 1] = c;
                    e[l + 1] = u;
                } else {
                    break;
                }
            }
            t[l + 1] = n;
            e[l + 1] = o;
        }
    }
    function quickSort(t, e, r, s, i) {
        let n = 0, o = 0;
        let c, u, a;
        let h, l, f;
        let p, d, b;
        let w, v;
        let g, y, C, O;
        let A, x, S, m;
        while (true) {
            if (s - r <= 10) {
                insertionSort(t, e, r, s, i);
                return;
            }
            n = r + (s - r >> 1);
            c = t[r];
            h = e[r];
            u = t[s - 1];
            l = e[s - 1];
            a = t[n];
            f = e[n];
            p = i(c, u);
            if (p > 0) {
                w = c;
                v = h;
                c = u;
                h = l;
                u = w;
                l = v;
            }
            d = i(c, a);
            if (d >= 0) {
                w = c;
                v = h;
                c = a;
                h = f;
                a = u;
                f = l;
                u = w;
                l = v;
            } else {
                b = i(u, a);
                if (b > 0) {
                    w = u;
                    v = l;
                    u = a;
                    l = f;
                    a = w;
                    f = v;
                }
            }
            t[r] = c;
            e[r] = h;
            t[s - 1] = a;
            e[s - 1] = f;
            g = u;
            y = l;
            C = r + 1;
            O = s - 1;
            t[n] = t[C];
            e[n] = e[C];
            t[C] = g;
            e[C] = y;
            t: for (o = C + 1; o < O; o++) {
                A = t[o];
                x = e[o];
                S = i(A, g);
                if (S < 0) {
                    t[o] = t[C];
                    e[o] = e[C];
                    t[C] = A;
                    e[C] = x;
                    C++;
                } else if (S > 0) {
                    do {
                        O--;
                        if (O == o) {
                            break t;
                        }
                        m = t[O];
                        S = i(m, g);
                    } while (S > 0);
                    t[o] = t[O];
                    e[o] = e[O];
                    t[O] = A;
                    e[O] = x;
                    if (S < 0) {
                        A = t[o];
                        x = e[o];
                        t[o] = t[C];
                        e[o] = e[C];
                        t[C] = A;
                        e[C] = x;
                        C++;
                    }
                }
            }
            if (s - O < C - r) {
                quickSort(t, e, O, s, i);
                s = C;
            } else {
                quickSort(t, e, r, C, i);
                r = O;
            }
        }
    }
    const i = Array.prototype;
    const n = [ "push", "unshift", "pop", "shift", "splice", "reverse", "sort" ];
    let o;
    function overrideArrayPrototypes() {
        const e = i.push;
        const c = i.unshift;
        const u = i.pop;
        const a = i.shift;
        const h = i.splice;
        const l = i.reverse;
        const f = i.sort;
        o = {
            push: function(...t) {
                const s = r.get(this);
                if (s === void 0) {
                    return e.apply(this, t);
                }
                const i = this.length;
                const n = t.length;
                if (n === 0) {
                    return i;
                }
                this.length = s.indexMap.length = i + n;
                let o = i;
                while (o < this.length) {
                    this[o] = t[o - i];
                    s.indexMap[o] = -2;
                    o++;
                }
                s.notify();
                return this.length;
            },
            unshift: function(...t) {
                const e = r.get(this);
                if (e === void 0) {
                    return c.apply(this, t);
                }
                const s = t.length;
                const i = new Array(s);
                let n = 0;
                while (n < s) {
                    i[n++] = -2;
                }
                c.apply(e.indexMap, i);
                const o = c.apply(this, t);
                e.notify();
                return o;
            },
            pop: function() {
                const t = r.get(this);
                if (t === void 0) {
                    return u.call(this);
                }
                const e = t.indexMap;
                const s = u.call(this);
                const i = e.length - 1;
                if (e[i] > -1) {
                    e.deletedIndices.push(e[i]);
                    e.deletedItems.push(s);
                }
                u.call(e);
                t.notify();
                return s;
            },
            shift: function() {
                const t = r.get(this);
                if (t === void 0) {
                    return a.call(this);
                }
                const e = t.indexMap;
                const s = a.call(this);
                if (e[0] > -1) {
                    e.deletedIndices.push(e[0]);
                    e.deletedItems.push(s);
                }
                a.call(e);
                t.notify();
                return s;
            },
            splice: function(...t) {
                const e = t[0];
                const s = t[1];
                const i = r.get(this);
                if (i === void 0) {
                    return h.apply(this, t);
                }
                const n = this.length;
                const o = e | 0;
                const c = o < 0 ? Math.max(n + o, 0) : Math.min(o, n);
                const u = i.indexMap;
                const a = t.length;
                const l = a === 0 ? 0 : a === 1 ? n - c : s;
                let f = c;
                if (l > 0) {
                    const t = f + l;
                    while (f < t) {
                        if (u[f] > -1) {
                            u.deletedIndices.push(u[f]);
                            u.deletedItems.push(this[f]);
                        }
                        f++;
                    }
                }
                f = 0;
                if (a > 2) {
                    const t = a - 2;
                    const r = new Array(t);
                    while (f < t) {
                        r[f++] = -2;
                    }
                    h.call(u, e, s, ...r);
                } else {
                    h.apply(u, t);
                }
                const p = h.apply(this, t);
                if (l > 0 || f > 0) {
                    i.notify();
                }
                return p;
            },
            reverse: function() {
                const t = r.get(this);
                if (t === void 0) {
                    l.call(this);
                    return this;
                }
                const e = this.length;
                const s = e / 2 | 0;
                let i = 0;
                while (i !== s) {
                    const r = e - i - 1;
                    const s = this[i];
                    const n = t.indexMap[i];
                    const o = this[r];
                    const c = t.indexMap[r];
                    this[i] = o;
                    t.indexMap[i] = c;
                    this[r] = s;
                    t.indexMap[r] = n;
                    i++;
                }
                t.notify();
                return this;
            },
            sort: function(e) {
                const s = r.get(this);
                if (s === void 0) {
                    f.call(this, e);
                    return this;
                }
                let i = this.length;
                if (i < 2) {
                    return this;
                }
                quickSort(this, s.indexMap, 0, i, preSortCompare);
                let n = 0;
                while (n < i) {
                    if (this[n] === void 0) {
                        break;
                    }
                    n++;
                }
                if (e === void 0 || !t.isFunction(e)) {
                    e = sortCompare;
                }
                quickSort(this, s.indexMap, 0, n, e);
                let o = false;
                for (n = 0, i = s.indexMap.length; i > n; ++n) {
                    if (s.indexMap[n] !== n) {
                        o = true;
                        break;
                    }
                }
                if (o || v) {
                    s.notify();
                }
                return this;
            }
        };
        for (const t of n) {
            s(o[t], "observing", {
                value: true,
                writable: false,
                configurable: false,
                enumerable: false
            });
        }
    }
    let c = false;
    const h = "__au_arr_on__";
    function enableArrayObservation() {
        if (o === undefined) {
            overrideArrayPrototypes();
        }
        if (!(u(h, Array) ?? false)) {
            a(true, Array, h);
            for (const t of n) {
                if (i[t].observing !== true) {
                    rtDefineHiddenProp(i, t, o[t]);
                }
            }
        }
    }
    class ArrayObserverImpl {
        constructor(t) {
            this.type = f;
            if (!c) {
                c = true;
                enableArrayObservation();
            }
            this.indexObservers = {};
            this.collection = t;
            this.indexMap = createIndexMap(t.length);
            this.lenObs = void 0;
            r.set(t, this);
        }
        notify() {
            const t = this.subs;
            t.notifyDirty();
            const e = this.indexMap;
            if (v) {
                addCollectionBatch(t, this.collection, e);
                return;
            }
            const r = this.collection;
            const s = r.length;
            this.indexMap = createIndexMap(s);
            t.notifyCollection(r, e);
        }
        getLengthObserver() {
            return this.lenObs ??= new CollectionLengthObserver(this);
        }
        getIndexObserver(t) {
            return this.indexObservers[t] ??= new ArrayIndexObserverImpl(this, t);
        }
    }
    (() => {
        g(ArrayObserverImpl, null);
    })();
    class ArrayIndexObserverImpl {
        constructor(t, e) {
            this.owner = t;
            this.index = e;
            this.doNotCache = true;
            this.value = this.getValue();
        }
        getValue() {
            return this.owner.collection[this.index];
        }
        setValue(t) {
            if (t === this.getValue()) {
                return;
            }
            const e = this.owner;
            const r = this.index;
            const s = e.indexMap;
            if (s[r] > -1) {
                s.deletedIndices.push(s[r]);
            }
            s[r] = -2;
            e.collection[r] = t;
            e.notify();
        }
        handleDirty() {
            if (this.value !== this.getValue()) {
                this.subs.notifyDirty();
            }
        }
        handleCollectionChange(t, e) {
            const r = this.index;
            const s = e[r] === r;
            if (s) {
                return;
            }
            const i = this.value;
            const n = this.value = this.getValue();
            if (i !== n) {
                this.subs.notify(n, i);
            }
        }
        subscribe(t) {
            if (this.subs.add(t) && this.subs.count === 1) {
                this.owner.subscribe(this);
            }
        }
        unsubscribe(t) {
            if (this.subs.remove(t) && this.subs.count === 0) {
                this.owner.unsubscribe(this);
            }
        }
    }
    (() => {
        g(ArrayIndexObserverImpl, null);
    })();
    return function getArrayObserver(t) {
        let e = r.get(t);
        if (e === void 0) {
            r.set(t, e = new ArrayObserverImpl(t));
            enableArrayObservation();
        }
        return e;
    };
})();

const C = /*@__PURE__*/ (() => {
    const t = Symbol.for("__au_set_obs__");
    const e = Set[t] ?? rtDefineHiddenProp(Set, t, new WeakMap);
    const {add: r, clear: s, delete: i} = Set.prototype;
    const n = [ "add", "clear", "delete" ];
    const o = {
        add: function(t) {
            const s = e.get(this);
            if (s === undefined) {
                r.call(this, t);
                return this;
            }
            const i = this.size;
            r.call(this, t);
            const n = this.size;
            if (n === i) {
                return this;
            }
            s.indexMap[i] = -2;
            s.notify();
            return this;
        },
        clear: function() {
            const t = e.get(this);
            if (t === undefined) {
                return s.call(this);
            }
            const r = this.size;
            if (r > 0) {
                const e = t.indexMap;
                let r = 0;
                for (const t of this.keys()) {
                    if (e[r] > -1) {
                        e.deletedIndices.push(e[r]);
                        e.deletedItems.push(t);
                    }
                    r++;
                }
                s.call(this);
                e.length = 0;
                t.notify();
            }
            return undefined;
        },
        delete: function(t) {
            const r = e.get(this);
            if (r === undefined) {
                return i.call(this, t);
            }
            const s = this.size;
            if (s === 0) {
                return false;
            }
            let n = 0;
            const o = r.indexMap;
            for (const e of this.keys()) {
                if (e === t) {
                    if (o[n] > -1) {
                        o.deletedIndices.push(o[n]);
                        o.deletedItems.push(e);
                    }
                    o.splice(n, 1);
                    const s = i.call(this, t);
                    if (s === true) {
                        r.notify();
                    }
                    return s;
                }
                n++;
            }
            return false;
        }
    };
    function enableSetObservation(t) {
        for (const e of n) {
            rtDefineHiddenProp(t, e, o[e]);
        }
    }
    class SetObserverImpl {
        constructor(t) {
            this.type = f;
            this.collection = t;
            this.indexMap = createIndexMap(t.size);
            this.lenObs = void 0;
        }
        notify() {
            const t = this.subs;
            t.notifyDirty();
            const e = this.indexMap;
            if (v) {
                addCollectionBatch(t, this.collection, e);
                return;
            }
            const r = this.collection;
            const s = r.size;
            this.indexMap = createIndexMap(s);
            t.notifyCollection(r, e);
        }
        getLengthObserver() {
            return this.lenObs ??= new CollectionSizeObserver(this);
        }
    }
    g(SetObserverImpl, null);
    return function getSetObserver(t) {
        let r = e.get(t);
        if (r === void 0) {
            e.set(t, r = new SetObserverImpl(t));
            enableSetObservation(t);
        }
        return r;
    };
})();

const O = /*@__PURE__*/ (() => {
    const t = Symbol.for("__au_map_obs__");
    const e = Map[t] ?? rtDefineHiddenProp(Map, t, new WeakMap);
    const {set: r, clear: s, delete: i} = Map.prototype;
    const n = [ "set", "clear", "delete" ];
    const o = {
        set: function(t, s) {
            const i = e.get(this);
            if (i === undefined) {
                r.call(this, t, s);
                return this;
            }
            const n = this.get(t);
            const o = this.size;
            r.call(this, t, s);
            const c = this.size;
            if (c === o) {
                let e = 0;
                for (const r of this.entries()) {
                    if (r[0] === t) {
                        if (r[1] !== n) {
                            i.indexMap.deletedIndices.push(i.indexMap[e]);
                            i.indexMap.deletedItems.push(r);
                            i.indexMap[e] = -2;
                            i.notify();
                        }
                        return this;
                    }
                    e++;
                }
                return this;
            }
            i.indexMap[o] = -2;
            i.notify();
            return this;
        },
        clear: function() {
            const t = e.get(this);
            if (t === undefined) {
                return s.call(this);
            }
            const r = this.size;
            if (r > 0) {
                const e = t.indexMap;
                let r = 0;
                for (const t of this.keys()) {
                    if (e[r] > -1) {
                        e.deletedIndices.push(e[r]);
                        e.deletedItems.push(t);
                    }
                    r++;
                }
                s.call(this);
                e.length = 0;
                t.notify();
            }
            return undefined;
        },
        delete: function(t) {
            const r = e.get(this);
            if (r === undefined) {
                return i.call(this, t);
            }
            const s = this.size;
            if (s === 0) {
                return false;
            }
            let n = 0;
            const o = r.indexMap;
            for (const e of this.keys()) {
                if (e === t) {
                    if (o[n] > -1) {
                        o.deletedIndices.push(o[n]);
                        o.deletedItems.push(e);
                    }
                    o.splice(n, 1);
                    const s = i.call(this, t);
                    if (s === true) {
                        r.notify();
                    }
                    return s;
                }
                ++n;
            }
            return false;
        }
    };
    function enableMapObservation(t) {
        for (const e of n) {
            rtDefineHiddenProp(t, e, o[e]);
        }
    }
    class MapObserverImpl {
        constructor(t) {
            this.type = f;
            this.collection = t;
            this.indexMap = createIndexMap(t.size);
            this.lenObs = void 0;
        }
        notify() {
            const t = this.subs;
            t.notifyDirty();
            const e = this.indexMap;
            if (v) {
                addCollectionBatch(t, this.collection, e);
                return;
            }
            const r = this.collection;
            const s = r.size;
            this.indexMap = createIndexMap(s);
            t.notifyCollection(r, e);
        }
        getLengthObserver() {
            return this.lenObs ??= new CollectionSizeObserver(this);
        }
    }
    g(MapObserverImpl, null);
    return function getMapObserver(t) {
        let r = e.get(t);
        if (r === void 0) {
            e.set(t, r = new MapObserverImpl(t));
            enableMapObservation(t);
        }
        return r;
    };
})();

const A = /*@__PURE__*/ (() => {
    class BindingObserverRecord {
        constructor(t) {
            this.version = 0;
            this.count = 0;
            this.o = new Map;
            this.b = t;
        }
        add(t) {
            if (!this.o.has(t)) {
                t.subscribe(this.b);
                ++this.count;
            }
            this.o.set(t, this.version);
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
    function unsubscribeAll(t, e) {
        e.unsubscribe(this.b);
    }
    function unsubscribeStale(t, e) {
        if (this.version !== t) {
            e.unsubscribe(this.b);
            this.o.delete(e);
        }
    }
    function getObserverRecord() {
        return rtDefineHiddenProp(this, "obs", new BindingObserverRecord(this));
    }
    function observe(t, e) {
        this.obs.add(this.oL.getObserver(t, e));
    }
    function observeCollection(e) {
        let r;
        if (t.isArray(e)) {
            r = y(e);
        } else if (t.isSet(e)) {
            r = C(e);
        } else if (t.isMap(e)) {
            r = O(e);
        } else {
            throw createMappedError(210, e);
        }
        this.obs.add(r);
    }
    function subscribeTo(t) {
        this.obs.add(t);
    }
    function noopHandleChange() {
        throw createMappedError(99, "handleChange");
    }
    function noopHandleCollectionChange() {
        throw createMappedError(99, "handleCollectionChange");
    }
    return function connectableDecorator(t, e) {
        const r = t.prototype;
        ensureProto(r, "observe", observe);
        ensureProto(r, "observeCollection", observeCollection);
        ensureProto(r, "subscribeTo", subscribeTo);
        s(r, "obs", {
            get: getObserverRecord
        });
        ensureProto(r, "handleChange", noopHandleChange);
        ensureProto(r, "handleCollectionChange", noopHandleCollectionChange);
        return t;
    };
})();

function connectable(t, e) {
    return t == null ? A : A(t, e);
}

let x = null;

const S = [];

let m = false;

function pauseConnecting() {
    m = false;
}

function resumeConnecting() {
    m = true;
}

function currentConnectable() {
    return x;
}

function enterConnectable(t) {
    if (t == null) {
        throw createMappedError(206);
    }
    if (x == null) {
        x = t;
        S[0] = x;
        m = true;
        return;
    }
    if (x === t) {
        throw createMappedError(207);
    }
    S.push(t);
    x = t;
    m = true;
}

function exitConnectable(t) {
    if (t == null) {
        throw createMappedError(208);
    }
    if (x !== t) {
        throw createMappedError(209);
    }
    S.pop();
    x = S.length > 0 ? S[S.length - 1] : null;
    m = x != null;
}

const R = /*@__PURE__*/ n({
    get current() {
        return x;
    },
    get connecting() {
        return m;
    },
    enter: enterConnectable,
    exit: exitConnectable,
    pause: pauseConnecting,
    resume: resumeConnecting
});

const P = Reflect.get;

const I = Object.prototype.toString;

const _ = new WeakMap;

const D = "__au_nw__";

const k = "__au_nw";

function canWrap(t) {
    switch (I.call(t)) {
      case "[object Object]":
        return t.constructor[D] !== true;

      case "[object Array]":
      case "[object Map]":
      case "[object Set]":
        return true;

      default:
        return false;
    }
}

const M = "__raw__";

function wrap(t) {
    return canWrap(t) ? getProxy(t) : t;
}

function getProxy(t) {
    return _.get(t) ?? createProxy(t);
}

function getRaw(t) {
    return t[M] ?? t;
}

function unwrap(t) {
    return canWrap(t) && t[M] || t;
}

function doNotCollect(t, e) {
    return e === "constructor" || e === "__proto__" || e === "$observers" || e === Symbol.toPrimitive || e === Symbol.toStringTag || t.constructor[`${k}_${o(e)}__`] === true;
}

function createProxy(e) {
    const r = t.isArray(e) ? V : t.isMap(e) || t.isSet(e) ? N : L;
    const s = new Proxy(e, r);
    _.set(e, s);
    _.set(s, s);
    return s;
}

const L = {
    get(t, e, r) {
        if (e === M) {
            return t;
        }
        const s = currentConnectable();
        if (!m || doNotCollect(t, e) || s == null) {
            return P(t, e, r);
        }
        s.observe(t, e);
        return wrap(P(t, e, r));
    }
};

const V = {
    get(t, e, r) {
        if (e === M) {
            return t;
        }
        if (!m || doNotCollect(t, e) || x == null) {
            return P(t, e, r);
        }
        switch (e) {
          case "length":
            x.observe(t, "length");
            return t.length;

          case "map":
            return wrappedArrayMap;

          case "includes":
            return wrappedArrayIncludes;

          case "indexOf":
            return wrappedArrayIndexOf;

          case "lastIndexOf":
            return wrappedArrayLastIndexOf;

          case "every":
            return wrappedArrayEvery;

          case "filter":
            return wrappedArrayFilter;

          case "find":
            return wrappedArrayFind;

          case "findIndex":
            return wrappedArrayFindIndex;

          case "flat":
            return wrappedArrayFlat;

          case "flatMap":
            return wrappedArrayFlatMap;

          case "join":
            return wrappedArrayJoin;

          case "push":
            return wrappedArrayPush;

          case "pop":
            return wrappedArrayPop;

          case "reduce":
            return wrappedReduce;

          case "reduceRight":
            return wrappedReduceRight;

          case "reverse":
            return wrappedArrayReverse;

          case "shift":
            return wrappedArrayShift;

          case "unshift":
            return wrappedArrayUnshift;

          case "slice":
            return wrappedArraySlice;

          case "splice":
            return wrappedArraySplice;

          case "some":
            return wrappedArraySome;

          case "sort":
            return wrappedArraySort;

          case "keys":
            return wrappedKeys;

          case "values":
          case Symbol.iterator:
            return wrappedValues;

          case "entries":
            return wrappedEntries;
        }
        x.observe(t, e);
        return wrap(P(t, e, r));
    },
    ownKeys(t) {
        currentConnectable()?.observe(t, "length");
        return Reflect.ownKeys(t);
    }
};

function wrappedArrayMap(t, e) {
    const r = getRaw(this);
    const s = r.map(((r, s) => unwrap(t.call(e, wrap(r), s, this))));
    observeCollection(x, r);
    return wrap(s);
}

function wrappedArrayEvery(t, e) {
    const r = getRaw(this);
    const s = r.every(((r, s) => t.call(e, wrap(r), s, this)));
    observeCollection(x, r);
    return s;
}

function wrappedArrayFilter(t, e) {
    const r = getRaw(this);
    const s = r.filter(((r, s) => unwrap(t.call(e, wrap(r), s, this))));
    observeCollection(x, r);
    return wrap(s);
}

function wrappedArrayIncludes(t) {
    const e = getRaw(this);
    const r = e.includes(unwrap(t));
    observeCollection(x, e);
    return r;
}

function wrappedArrayIndexOf(t) {
    const e = getRaw(this);
    const r = e.indexOf(unwrap(t));
    observeCollection(x, e);
    return r;
}

function wrappedArrayLastIndexOf(t) {
    const e = getRaw(this);
    const r = e.lastIndexOf(unwrap(t));
    observeCollection(x, e);
    return r;
}

function wrappedArrayFindIndex(t, e) {
    const r = getRaw(this);
    const s = r.findIndex(((r, s) => unwrap(t.call(e, wrap(r), s, this))));
    observeCollection(x, r);
    return s;
}

function wrappedArrayFind(t, e) {
    const r = getRaw(this);
    const s = r.find(((e, r) => t(wrap(e), r, this)), e);
    observeCollection(x, r);
    return wrap(s);
}

function wrappedArrayFlat() {
    const t = getRaw(this);
    observeCollection(x, t);
    return wrap(t.flat());
}

function wrappedArrayFlatMap(t, e) {
    const r = getRaw(this);
    observeCollection(x, r);
    return getProxy(r.flatMap(((r, s) => wrap(t.call(e, wrap(r), s, this)))));
}

function wrappedArrayJoin(t) {
    const e = getRaw(this);
    observeCollection(x, e);
    return e.join(t);
}

function wrappedArrayPop() {
    return wrap(getRaw(this).pop());
}

function wrappedArrayPush(...t) {
    return getRaw(this).push(...t);
}

function wrappedArrayShift() {
    return wrap(getRaw(this).shift());
}

function wrappedArrayUnshift(...t) {
    return getRaw(this).unshift(...t);
}

function wrappedArraySplice(...t) {
    return wrap(getRaw(this).splice(...t));
}

function wrappedArrayReverse(...t) {
    const e = getRaw(this);
    const r = e.reverse();
    observeCollection(x, e);
    return wrap(r);
}

function wrappedArraySome(t, e) {
    const r = getRaw(this);
    const s = r.some(((r, s) => unwrap(t.call(e, wrap(r), s, this))));
    observeCollection(x, r);
    return s;
}

function wrappedArraySort(t) {
    const e = getRaw(this);
    const r = e.sort(t);
    observeCollection(x, e);
    return wrap(r);
}

function wrappedArraySlice(t, e) {
    const r = getRaw(this);
    observeCollection(x, r);
    return getProxy(r.slice(t, e));
}

function wrappedReduce(t, e) {
    const r = getRaw(this);
    const s = r.reduce(((e, r, s) => t(e, wrap(r), s, this)), e);
    observeCollection(x, r);
    return wrap(s);
}

function wrappedReduceRight(t, e) {
    const r = getRaw(this);
    const s = r.reduceRight(((e, r, s) => t(e, wrap(r), s, this)), e);
    observeCollection(x, r);
    return wrap(s);
}

const N = {
    get(e, r, s) {
        if (r === M) {
            return e;
        }
        const i = currentConnectable();
        if (!m || doNotCollect(e, r) || i == null) {
            return P(e, r, s);
        }
        switch (r) {
          case "size":
            i.observe(e, "size");
            return e.size;

          case "clear":
            return wrappedClear;

          case "delete":
            return wrappedDelete;

          case "forEach":
            return wrappedForEach;

          case "add":
            if (t.isSet(e)) {
                return wrappedAdd;
            }
            break;

          case "get":
            if (t.isMap(e)) {
                return wrappedGet;
            }
            break;

          case "set":
            if (t.isMap(e)) {
                return wrappedSet;
            }
            break;

          case "has":
            return wrappedHas;

          case "keys":
            return wrappedKeys;

          case "values":
            return wrappedValues;

          case "entries":
            return wrappedEntries;

          case Symbol.iterator:
            return t.isMap(e) ? wrappedEntries : wrappedValues;
        }
        return wrap(P(e, r, s));
    }
};

function wrappedForEach(t, e) {
    const r = getRaw(this);
    observeCollection(x, r);
    return r.forEach(((r, s) => {
        t.call(e, wrap(r), wrap(s), this);
    }));
}

function wrappedHas(t) {
    const e = getRaw(this);
    observeCollection(x, e);
    return e.has(unwrap(t));
}

function wrappedGet(t) {
    const e = getRaw(this);
    observeCollection(x, e);
    return wrap(e.get(unwrap(t)));
}

function wrappedSet(t, e) {
    return wrap(getRaw(this).set(unwrap(t), unwrap(e)));
}

function wrappedAdd(t) {
    return wrap(getRaw(this).add(unwrap(t)));
}

function wrappedClear() {
    return wrap(getRaw(this).clear());
}

function wrappedDelete(t) {
    return wrap(getRaw(this).delete(unwrap(t)));
}

function wrappedKeys() {
    const t = getRaw(this);
    observeCollection(x, t);
    const e = t.keys();
    return {
        next() {
            const t = e.next();
            const r = t.value;
            const s = t.done;
            return s ? {
                value: void 0,
                done: s
            } : {
                value: wrap(r),
                done: s
            };
        },
        [Symbol.iterator]() {
            return this;
        }
    };
}

function wrappedValues() {
    const t = getRaw(this);
    observeCollection(x, t);
    const e = t.values();
    return {
        next() {
            const t = e.next();
            const r = t.value;
            const s = t.done;
            return s ? {
                value: void 0,
                done: s
            } : {
                value: wrap(r),
                done: s
            };
        },
        [Symbol.iterator]() {
            return this;
        }
    };
}

function wrappedEntries() {
    const t = getRaw(this);
    observeCollection(x, t);
    const e = t.entries();
    return {
        next() {
            const t = e.next();
            const r = t.value;
            const s = t.done;
            return s ? {
                value: void 0,
                done: s
            } : {
                value: [ wrap(r[0]), wrap(r[1]) ],
                done: s
            };
        },
        [Symbol.iterator]() {
            return this;
        }
    };
}

const observeCollection = (t, e) => t?.observeCollection(e);

const H = /*@__PURE__*/ n({
    getProxy: getProxy,
    getRaw: getRaw,
    wrap: wrap,
    unwrap: unwrap,
    rawKey: M
});

class ComputedObserver {
    constructor(t, e, r, s, i) {
        this.type = f;
        this.v = void 0;
        this.ir = false;
        this.D = false;
        this.cb = void 0;
        this.h = void 0;
        this.C = void 0;
        this.o = t;
        this.O = i ? wrap(t) : t;
        this.$get = e;
        this.$set = r;
        this.oL = s;
    }
    init(t) {
        this.v = t;
        this.D = false;
    }
    getValue() {
        if (this.subs.count === 0) {
            return this.$get.call(this.o, this.o, this);
        }
        if (this.D) {
            this.compute();
            this.D = false;
        }
        return this.v;
    }
    setValue(e) {
        if (t.isFunction(this.$set)) {
            if (this.h !== void 0) {
                e = this.h.call(null, e, this.C);
            }
            if (!t.areEqual(e, this.v)) {
                this.ir = true;
                this.$set.call(this.o, e);
                this.ir = false;
                this.run();
            }
        } else {
            throw createMappedError(221);
        }
    }
    useCoercer(t, e) {
        this.h = t;
        this.C = e;
        return true;
    }
    useCallback(t) {
        this.cb = t;
        return true;
    }
    handleDirty() {
        if (!this.D) {
            this.D = true;
            this.subs.notifyDirty();
        }
    }
    handleChange() {
        this.D = true;
        if (this.subs.count > 0) {
            this.run();
        }
    }
    handleCollectionChange() {
        this.D = true;
        if (this.subs.count > 0) {
            this.run();
        }
    }
    subscribe(t) {
        if (this.subs.add(t) && this.subs.count === 1) {
            this.compute();
            this.D = false;
        }
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && this.subs.count === 0) {
            this.D = true;
            this.obs.clearAll();
        }
    }
    run() {
        if (this.ir) {
            return;
        }
        const e = this.v;
        const r = this.compute();
        this.D = false;
        if (!t.areEqual(r, e)) {
            this.cb?.(r, e);
            this.subs.notify(this.v, e);
        }
    }
    compute() {
        this.ir = true;
        this.obs.version++;
        try {
            enterConnectable(this);
            return this.v = unwrap(this.$get.call(this.O, this.O, this));
        } finally {
            this.obs.clear();
            this.ir = false;
            exitConnectable(this);
        }
    }
}

(() => {
    connectable(ComputedObserver, null);
    g(ComputedObserver, null);
})();

const E = /*@__PURE__*/ c("IDirtyChecker", void 0);

const F = {
    timeoutsPerCheck: 25,
    disabled: false,
    throw: false,
    resetToDefault() {
        this.timeoutsPerCheck = 6;
        this.disabled = false;
        this.throw = false;
    }
};

class DirtyChecker {
    static register(e) {
        e.register(t.Registration.singleton(this, this), t.Registration.aliasTo(this, E));
    }
    constructor() {
        this.tracked = [];
        this.A = null;
        this.R = 0;
        this.p = t.resolve(t.IPlatform);
        this.check = () => {
            if (F.disabled) {
                return;
            }
            if (++this.R < F.timeoutsPerCheck) {
                return;
            }
            this.R = 0;
            const t = this.tracked;
            const e = t.length;
            let r;
            let s = 0;
            for (;s < e; ++s) {
                r = t[s];
                if (r.isDirty()) {
                    r.flush();
                }
            }
        };
        g(DirtyCheckProperty, null);
    }
    createProperty(t, e) {
        if (F.throw) {
            throw createMappedError(218, e);
        }
        return new DirtyCheckProperty(this, t, e);
    }
    addProperty(t) {
        this.tracked.push(t);
        if (this.tracked.length === 1) {
            this.A = this.p.taskQueue.queueTask(this.check, {
                persistent: true
            });
        }
    }
    removeProperty(t) {
        this.tracked.splice(this.tracked.indexOf(t), 1);
        if (this.tracked.length === 0) {
            this.A.cancel();
            this.A = null;
        }
    }
}

class DirtyCheckProperty {
    constructor(t, e, r) {
        this.obj = e;
        this.key = r;
        this.type = l;
        this.ov = void 0;
        this.P = t;
    }
    getValue() {
        return this.obj[this.key];
    }
    setValue(t) {
        throw createMappedError(219, this.key);
    }
    isDirty() {
        return this.ov !== this.obj[this.key];
    }
    flush() {
        const t = this.ov;
        const e = this.getValue();
        this.ov = e;
        this.subs.notify(e, t);
    }
    subscribe(t) {
        if (this.subs.add(t) && this.subs.count === 1) {
            this.ov = this.obj[this.key];
            this.P.addProperty(this);
        }
    }
    unsubscribe(t) {
        if (this.subs.remove(t) && this.subs.count === 0) {
            this.P.removeProperty(this);
        }
    }
}

class PrimitiveObserver {
    get doNotCache() {
        return true;
    }
    constructor(t, e) {
        this.type = l;
        this.o = t;
        this.k = e;
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
        this.type = l;
    }
    getValue(t, e) {
        return t[e];
    }
    setValue(t, e, r) {
        e[r] = t;
    }
}

class SetterObserver {
    constructor(t, e) {
        this.type = f;
        this.v = void 0;
        this.iO = false;
        this.cb = void 0;
        this.h = void 0;
        this.C = void 0;
        this.o = t;
        this.k = e;
    }
    getValue() {
        return this.v;
    }
    setValue(e) {
        if (this.h !== void 0) {
            e = this.h.call(void 0, e, this.C);
        }
        if (this.iO) {
            if (t.areEqual(e, this.v)) {
                return;
            }
            j = this.v;
            this.v = e;
            this.subs.notifyDirty();
            this.subs.notify(e, j);
            if (t.areEqual(e, this.v)) {
                this.cb?.(e, j);
            }
        } else {
            this.v = this.o[this.k] = e;
            this.cb?.(e, j);
        }
    }
    useCallback(t) {
        this.cb = t;
        this.start();
        return true;
    }
    useCoercer(t, e) {
        this.h = t;
        this.C = e;
        this.start();
        return true;
    }
    subscribe(t) {
        if (this.iO === false) {
            this.start();
        }
        this.subs.add(t);
    }
    start() {
        if (this.iO === false) {
            this.iO = true;
            this.v = this.o[this.k];
            s(this.o, this.k, {
                enumerable: true,
                configurable: true,
                get: i((() => this.getValue()), {
                    getObserver: () => this
                }),
                set: t => {
                    this.setValue(t);
                }
            });
        }
        return this;
    }
    stop() {
        if (this.iO) {
            s(this.o, this.k, {
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

(() => {
    g(SetterObserver, null);
})();

let j = void 0;

const z = new PropertyAccessor;

const $ = /*@__PURE__*/ c("IObserverLocator", (t => t.singleton(ObserverLocator)));

const B = /*@__PURE__*/ c("INodeObserverLocator", (t => t.cachedCallback((t => new DefaultNodeObserverLocator))));

class DefaultNodeObserverLocator {
    handles() {
        return false;
    }
    getObserver() {
        return z;
    }
    getAccessor() {
        return z;
    }
}

const W = /*@__PURE__*/ c("IComputedObserverLocator", (t => t.singleton(class DefaultLocator {
    getObserver(t, e, r, n) {
        const o = new ComputedObserver(t, r.get, r.set, n, true);
        s(t, e, {
            enumerable: r.enumerable,
            configurable: true,
            get: i((() => o.getValue()), {
                getObserver: () => o
            }),
            set: t => {
                o.setValue(t);
            }
        });
        return o;
    }
})));

class ObserverLocator {
    constructor() {
        this.I = [];
        this.P = t.resolve(E);
        this._ = t.resolve(B);
        this.M = t.resolve(W);
    }
    addAdapter(t) {
        this.I.push(t);
    }
    getObserver(e, r) {
        if (e == null) {
            throw createMappedError(199, r);
        }
        if (!t.isObject(e)) {
            return new PrimitiveObserver(e, t.isFunction(r) ? "" : r);
        }
        if (t.isFunction(r)) {
            return new ComputedObserver(e, r, void 0, this, true);
        }
        const s = getObserverLookup(e);
        let i = s[r];
        if (i === void 0) {
            i = this.createObserver(e, r);
            if (!i.doNotCache) {
                s[r] = i;
            }
        }
        return i;
    }
    getAccessor(t, e) {
        const r = t.$observers?.[e];
        if (r !== void 0) {
            return r;
        }
        if (this._.handles(t, e, this)) {
            return this._.getAccessor(t, e, this);
        }
        return z;
    }
    getArrayObserver(t) {
        return y(t);
    }
    getMapObserver(t) {
        return O(t);
    }
    getSetObserver(t) {
        return C(t);
    }
    createObserver(e, s) {
        if (this._.handles(e, s, this)) {
            return this._.getObserver(e, s, this);
        }
        switch (s) {
          case "length":
            if (t.isArray(e)) {
                return y(e).getLengthObserver();
            }
            break;

          case "size":
            if (t.isMap(e)) {
                return O(e).getLengthObserver();
            } else if (t.isSet(e)) {
                return C(e).getLengthObserver();
            }
            break;

          default:
            if (t.isArray(e) && t.isArrayIndex(s)) {
                return y(e).getIndexObserver(Number(s));
            }
            break;
        }
        let i = K(e, s);
        if (i === void 0) {
            let t = q(e);
            while (t !== null) {
                i = K(t, s);
                if (i === void 0) {
                    t = q(t);
                } else {
                    break;
                }
            }
        }
        if (i !== void 0 && !r.call(i, "value")) {
            let t = this.L(e, s, i);
            if (t == null) {
                t = (i.get?.getObserver)?.(e);
            }
            return t == null ? i.configurable ? this.M.getObserver(e, s, i, this) : this.P.createProperty(e, s) : t;
        }
        return new SetterObserver(e, s);
    }
    L(t, e, r) {
        if (this.I.length > 0) {
            for (const s of this.I) {
                const i = s.getObserver(t, e, r, this);
                if (i != null) {
                    return i;
                }
            }
        }
        return null;
    }
}

const getCollectionObserver = e => {
    let r;
    if (t.isArray(e)) {
        r = y(e);
    } else if (t.isMap(e)) {
        r = O(e);
    } else if (t.isSet(e)) {
        r = C(e);
    }
    return r;
};

const q = Object.getPrototypeOf;

const K = Object.getOwnPropertyDescriptor;

const getObserverLookup = e => {
    let r = e.$observers;
    if (r === void 0) {
        s(e, "$observers", {
            enumerable: false,
            value: r = t.createLookup()
        });
    }
    return r;
};

const T = /*@__PURE__*/ c("IObservation", (t => t.singleton(Observation)));

class Observation {
    static get inject() {
        return [ $ ];
    }
    constructor(t) {
        this.oL = t;
        this.V = {
            immediate: true
        };
    }
    run(t) {
        const e = new RunEffect(this.oL, t);
        e.run();
        return e;
    }
    watch(t, e, r, s = this.V) {
        let i = undefined;
        let n = false;
        const o = this.oL.getObserver(t, e);
        const c = {
            handleChange: (t, e) => r(t, i = e)
        };
        const run = () => {
            if (n) return;
            r(o.getValue(), i);
        };
        const stop = () => {
            n = true;
            o.unsubscribe(c);
        };
        o.subscribe(c);
        if (s.immediate) {
            run();
        }
        return {
            run: run,
            stop: stop
        };
    }
}

class RunEffect {
    constructor(t, e) {
        this.oL = t;
        this.fn = e;
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
            throw createMappedError(225);
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
        } finally {
            this.obs.clear();
            this.running = false;
            exitConnectable(this);
        }
        if (this.queued) {
            if (this.runCount > this.maxRunCount) {
                this.runCount = 0;
                throw createMappedError(226);
            }
            this.run();
        } else {
            this.runCount = 0;
        }
    }
    stop() {
        this.stopped = true;
        this.obs.clearAll();
    }
}

(() => {
    connectable(RunEffect, null);
})();

const U = /*@__PURE__*/ (() => {
    function getObserversLookup(t) {
        if (t.$observers === void 0) {
            s(t, "$observers", {
                value: {}
            });
        }
        return t.$observers;
    }
    const e = {};
    function observable(r, i) {
        if (!SetterNotifier.mixed) {
            SetterNotifier.mixed = true;
            g(SetterNotifier, null);
        }
        let n = false;
        let c;
        if (typeof r === "object") {
            c = r;
        } else if (r != null) {
            c = {
                name: r
            };
            n = true;
        } else {
            c = t.emptyObject;
        }
        if (arguments.length === 0) {
            return function(t, e) {
                if (e.kind !== "field") throw createMappedError(224);
                return createFieldInitializer(e);
            };
        }
        if (i?.kind === "field") return createFieldInitializer(i);
        if (n) {
            return function(t, r) {
                createDescriptor(t, c.name, (() => e), true);
            };
        }
        return function(t, r) {
            switch (r.kind) {
              case "field":
                return createFieldInitializer(r);

              case "class":
                return createDescriptor(t, c.name, (() => e), true);

              default:
                throw createMappedError(224);
            }
        };
        function createFieldInitializer(t) {
            let e;
            t.addInitializer((function() {
                createDescriptor(this, t.name, (() => e), false);
            }));
            return function(t) {
                return e = t;
            };
        }
        function createDescriptor(t, e, r, i) {
            const n = c.callback || `${o(e)}Changed`;
            const u = c.set;
            const observableGetter = function() {
                const t = getNotifier(this, e, n, r, u);
                currentConnectable()?.subscribeTo(t);
                return t.getValue();
            };
            observableGetter.getObserver = function(t) {
                return getNotifier(t, e, n, r, u);
            };
            const a = {
                enumerable: true,
                configurable: true,
                get: observableGetter,
                set(t) {
                    getNotifier(this, e, n, r, u).setValue(t);
                }
            };
            if (i) s(t.prototype, e, a); else s(t, e, a);
        }
    }
    function getNotifier(t, r, s, i, n) {
        const o = getObserversLookup(t);
        let c = o[r];
        if (c == null) {
            const u = i();
            c = new SetterNotifier(t, s, n, u === e ? void 0 : u);
            o[r] = c;
        }
        return c;
    }
    class SetterNotifier {
        constructor(e, r, s, i) {
            this.type = f;
            this.v = void 0;
            this.ov = void 0;
            this.o = e;
            this.S = s;
            this.hs = t.isFunction(s);
            const n = e[r];
            this.cb = t.isFunction(n) ? n : void 0;
            this.v = i;
        }
        getValue() {
            return this.v;
        }
        setValue(e) {
            if (this.hs) {
                e = this.S(e);
            }
            if (!t.areEqual(e, this.v)) {
                this.ov = this.v;
                this.v = e;
                this.subs.notifyDirty();
                this.subs.notify(this.v, this.ov);
                if (t.areEqual(e, this.v)) {
                    this.cb?.call(this.o, this.v, this.ov);
                }
            }
        }
    }
    SetterNotifier.mixed = false;
    return observable;
})();

typeof SuppressedError === "function" ? SuppressedError : function(t, e, r) {
    var s = new Error(r);
    return s.name = "SuppressedError", s.error = t, s.suppressed = e, s;
};

function nowrap(t, e) {
    return arguments.length === 0 ? decorator : decorator(t, e);
    function decorator(t, e) {
        switch (e.kind) {
          case "class":
            rtDefineHiddenProp(t, D, true);
            break;

          case "field":
            e.addInitializer((function() {
                const t = this.constructor;
                const r = `${k}_${o(e.name)}__`;
                if (r in t) return;
                rtDefineHiddenProp(t, r, true);
            }));
            break;
        }
    }
}

exports.AccessorType = b;

exports.CollectionLengthObserver = CollectionLengthObserver;

exports.CollectionSizeObserver = CollectionSizeObserver;

exports.ComputedObserver = ComputedObserver;

exports.ConnectableSwitcher = R;

exports.DirtyCheckProperty = DirtyCheckProperty;

exports.DirtyCheckSettings = F;

exports.DirtyChecker = DirtyChecker;

exports.ICoercionConfiguration = h;

exports.IComputedObserverLocator = W;

exports.IDirtyChecker = E;

exports.INodeObserverLocator = B;

exports.IObservation = T;

exports.IObserverLocator = $;

exports.Observation = Observation;

exports.ObserverLocator = ObserverLocator;

exports.PrimitiveObserver = PrimitiveObserver;

exports.PropertyAccessor = PropertyAccessor;

exports.ProxyObservable = H;

exports.SetterObserver = SetterObserver;

exports.batch = batch;

exports.cloneIndexMap = cloneIndexMap;

exports.connectable = connectable;

exports.copyIndexMap = copyIndexMap;

exports.createIndexMap = createIndexMap;

exports.getCollectionObserver = getCollectionObserver;

exports.getObserverLookup = getObserverLookup;

exports.isIndexMap = isIndexMap;

exports.nowrap = nowrap;

exports.observable = U;

exports.subscriberCollection = g;
//# sourceMappingURL=index.cjs.map
