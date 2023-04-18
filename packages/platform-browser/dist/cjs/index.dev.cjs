'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var platform = require('@aurelia/platform');

const lookup = new Map();
class BrowserPlatform extends platform.Platform {
    constructor(g, overrides = {}) {
        super(g, overrides);
        /** @internal */ this._domReadRequested = false;
        /** @internal */ this._domReadHandle = -1;
        /** @internal */ this._domWriteRequested = false;
        /** @internal */ this._domWriteHandle = -1;
        ('Node Element HTMLElement CustomEvent CSSStyleSheet ShadowRoot MutationObserver '
            + 'window document customElements')
            .split(' ')
            // eslint-disable-next-line
            .forEach(prop => this[prop] = prop in overrides ? overrides[prop] : g[prop]);
        'fetch requestAnimationFrame cancelAnimationFrame'.split(' ').forEach(prop => 
        // eslint-disable-next-line
        this[prop] = prop in overrides ? overrides[prop] : (g[prop]?.bind(g) ?? notImplemented(prop)));
        this.flushDomRead = this.flushDomRead.bind(this);
        this.flushDomWrite = this.flushDomWrite.bind(this);
        this.domReadQueue = new platform.TaskQueue(this, this.requestDomRead.bind(this), this.cancelDomRead.bind(this));
        this.domWriteQueue = new platform.TaskQueue(this, this.requestDomWrite.bind(this), this.cancelDomWrite.bind(this));
        /* eslint-enable @typescript-eslint/no-unnecessary-type-assertion */
    }
    static getOrCreate(g, overrides = {}) {
        let platform = lookup.get(g);
        if (platform === void 0) {
            lookup.set(g, platform = new BrowserPlatform(g, overrides));
        }
        return platform;
    }
    static set(g, platform) {
        lookup.set(g, platform);
    }
    requestDomRead() {
        this._domReadRequested = true;
        // Yes, this is intentional: the timing of the read can only be "found" by doing a write first.
        // The flushDomWrite queues the read.
        // If/when requestPostAnimationFrame is implemented in browsers, we can use that instead.
        if (this._domWriteHandle === -1) {
            this._domWriteHandle = this.requestAnimationFrame(this.flushDomWrite);
        }
    }
    cancelDomRead() {
        this._domReadRequested = false;
        if (this._domReadHandle > -1) {
            this.clearTimeout(this._domReadHandle);
            this._domReadHandle = -1;
        }
        if (this._domWriteRequested === false && this._domWriteHandle > -1) {
            this.cancelAnimationFrame(this._domWriteHandle);
            this._domWriteHandle = -1;
        }
    }
    flushDomRead() {
        this._domReadHandle = -1;
        if (this._domReadRequested === true) {
            this._domReadRequested = false;
            this.domReadQueue.flush();
        }
    }
    requestDomWrite() {
        this._domWriteRequested = true;
        if (this._domWriteHandle === -1) {
            this._domWriteHandle = this.requestAnimationFrame(this.flushDomWrite);
        }
    }
    cancelDomWrite() {
        this._domWriteRequested = false;
        if (this._domWriteHandle > -1 &&
            // if dom read is requested and there is no readHandle yet, we need the rAF to proceed regardless.
            // The domWriteRequested=false will prevent the read flush from happening.
            (this._domReadRequested === false || this._domReadHandle > -1)) {
            this.cancelAnimationFrame(this._domWriteHandle);
            this._domWriteHandle = -1;
        }
    }
    flushDomWrite() {
        this._domWriteHandle = -1;
        if (this._domWriteRequested === true) {
            this._domWriteRequested = false;
            this.domWriteQueue.flush();
        }
        if (this._domReadRequested === true && this._domReadHandle === -1) {
            this._domReadHandle = this.setTimeout(this.flushDomRead, 0);
        }
    }
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const notImplemented = (name) => {
    return () => {
        throw new Error(`The PLATFORM did not receive a valid reference to the global function '${name}'.`); // TODO: link to docs describing how to fix this issue
    };
};

exports.BrowserPlatform = BrowserPlatform;
//# sourceMappingURL=index.dev.cjs.map
