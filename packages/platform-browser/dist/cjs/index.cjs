"use strict";

Object.defineProperty(exports, "__esModule", {
    value: true
});

var t = require("@aurelia/platform");

const s = new Map;

class BrowserPlatform extends t.Platform {
    constructor(s, e = {}) {
        super(s, e);
        this.t = false;
        this.i = -1;
        this.h = false;
        this.u = -1;
        ("Node Element HTMLElement CustomEvent CSSStyleSheet ShadowRoot MutationObserver " + "window document customElements").split(" ").forEach((t => this[t] = t in e ? e[t] : s[t]));
        "fetch requestAnimationFrame cancelAnimationFrame".split(" ").forEach((t => this[t] = t in e ? e[t] : s[t]?.bind(s) ?? i(t)));
        this.flushDomRead = this.flushDomRead.bind(this);
        this.flushDomWrite = this.flushDomWrite.bind(this);
        this.domReadQueue = new t.TaskQueue(this, this.requestDomRead.bind(this), this.cancelDomRead.bind(this));
        this.domWriteQueue = new t.TaskQueue(this, this.requestDomWrite.bind(this), this.cancelDomWrite.bind(this));
    }
    static getOrCreate(t, i = {}) {
        let e = s.get(t);
        if (void 0 === e) s.set(t, e = new BrowserPlatform(t, i));
        return e;
    }
    static set(t, i) {
        s.set(t, i);
    }
    requestDomRead() {
        this.t = true;
        if (-1 === this.u) this.u = this.requestAnimationFrame(this.flushDomWrite);
    }
    cancelDomRead() {
        this.t = false;
        if (this.i > -1) {
            this.clearTimeout(this.i);
            this.i = -1;
        }
        if (false === this.h && this.u > -1) {
            this.cancelAnimationFrame(this.u);
            this.u = -1;
        }
    }
    flushDomRead() {
        this.i = -1;
        if (true === this.t) {
            this.t = false;
            this.domReadQueue.flush();
        }
    }
    requestDomWrite() {
        this.h = true;
        if (-1 === this.u) this.u = this.requestAnimationFrame(this.flushDomWrite);
    }
    cancelDomWrite() {
        this.h = false;
        if (this.u > -1 && (false === this.t || this.i > -1)) {
            this.cancelAnimationFrame(this.u);
            this.u = -1;
        }
    }
    flushDomWrite() {
        this.u = -1;
        if (true === this.h) {
            this.h = false;
            this.domWriteQueue.flush();
        }
        if (true === this.t && -1 === this.i) this.i = this.setTimeout(this.flushDomRead, 0);
    }
}

const i = t => () => {
    throw new Error(`The PLATFORM did not receive a valid reference to the global function '${t}'.`);
};

exports.BrowserPlatform = BrowserPlatform;
//# sourceMappingURL=index.cjs.map
