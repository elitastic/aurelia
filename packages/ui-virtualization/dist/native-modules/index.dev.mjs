import { DI, IContainer, Registration } from '../../../kernel/dist/native-modules/index.mjs';
import { BindingBehaviorExpression, ValueConverterExpression, Scope, BindingContext, getCollectionObserver } from '../../../runtime/dist/native-modules/index.mjs';
import { customAttribute, IRenderLocation, IInstruction, IController, IViewFactory, IPlatform } from '../../../runtime-html/dist/native-modules/index.mjs';

const IDomRenderer = DI.createInterface('IDomRenderer');
const IScrollerObsererLocator = DI.createInterface('IScrollerObsererLocator');
const ICollectionStrategyLocator = DI.createInterface('ICollectionStrategyLocator');

function unwrapExpression(expression) {
    let unwrapped = false;
    while (expression instanceof BindingBehaviorExpression) {
        expression = expression.expression;
    }
    while (expression instanceof ValueConverterExpression) {
        expression = expression.expression;
        unwrapped = true;
    }
    return unwrapped ? expression : null;
}

const getScrollerElement = (element) => {
    let current = element.parentNode;
    while (current !== null && current !== document.body) {
        if (hasOverflowScroll(current)) {
            return current;
        }
        current = current.parentNode;
    }
    throw new Error('Unable to find a scroller');
};
const hasOverflowScroll = (element) => {
    const style = window.getComputedStyle(element);
    return style && (style.overflowY === 'scroll' || style.overflow === 'scroll' || style.overflowY === 'auto' || style.overflow === 'auto');
};
const getStyleValues = (element, ...styles) => {
    const currentStyle = window.getComputedStyle(element);
    let value = 0;
    let styleValue = 0;
    for (let i = 0, ii = styles.length; ii > i; ++i) {
        styleValue = parseFloat(currentStyle[styles[i]]);
        value += isNaN(styleValue) ? 0 : styleValue;
    }
    return value;
};
const calcOuterHeight = (element) => {
    let height = element.getBoundingClientRect().height;
    height += getStyleValues(element, 'marginTop', 'marginBottom');
    return height;
};
const getDistanceToScroller = (child, scroller) => {
    const offsetParent = child.offsetParent;
    const childOffsetTop = child.offsetTop;
    if (offsetParent === null || offsetParent === scroller) {
        return childOffsetTop;
    }
    if (offsetParent.contains(scroller)) {
        return childOffsetTop - scroller.offsetTop;
    }
    return childOffsetTop + getDistanceToScroller(offsetParent, scroller);
};

const noScrollInfo = {
    height: 0,
    scrollTop: 0,
    scroller: null,
    width: 0
};
class VirtualRepeat {
    constructor(location, instruction, parent, _factory, _container, platform) {
        this.location = location;
        this.instruction = instruction;
        this.parent = parent;
        this._factory = _factory;
        this._container = _container;
        this.items = void 0;
        this.views = [];
        this.task = null;
        this._currScrollerInfo = noScrollInfo;
        this.itemHeight = 0;
        this.minViewsRequired = 0;
        this.dom = null;
        this.scrollerObserver = null;
        const iteratorInstruction = instruction.props[0];
        const forOf = iteratorInstruction.from;
        const iterable = this.iterable = unwrapExpression(forOf.iterable) ?? forOf.iterable;
        const hasWrapExpression = this._hasWrapExpression = forOf.iterable !== iterable;
        this._obsMediator = new CollectionObservationMediator(this, hasWrapExpression ? 'handleInnerCollectionChange' : 'handleCollectionChange');
        this.local = forOf.declaration.name;
        this.taskQueue = platform.domWriteQueue;
    }
    static get inject() {
        return [IRenderLocation, IInstruction, IController, IViewFactory, IContainer, IPlatform];
    }
    attaching() {
        const container = this._container;
        const collectionStrategyLocator = container.get(ICollectionStrategyLocator);
        const collectionStrategy = this.collectionStrategy = collectionStrategyLocator.getStrategy(this.items);
        const itemCount = collectionStrategy.count();
        if (itemCount === 0) {
            return;
        }
        const repeatDom = this.dom = container.get(IDomRenderer).render(this.location);
        const firstView = this._createAndActivateFirstView();
        (this.scrollerObserver = container.get(IScrollerObsererLocator).getObserver(repeatDom.scroller))
            .subscribe(this);
        this._initCalculation(firstView);
        this.itemsChanged(this.items);
    }
    detaching() {
        this.task?.cancel();
        this._resetCalculation();
        this.dom.dispose();
        this.scrollerObserver.unsubscribe(this);
        this.dom
            = this.scrollerObserver
                = this.task
                    = null;
    }
    _initCalculation(firstView) {
        const itemHeight = calcOuterHeight(firstView.nodes.firstChild);
        const scrollerInfo = this.scrollerObserver.getValue();
        const calculation = this._calculate(scrollerInfo, this.collectionStrategy.count(), itemHeight);
        if (calculation.signals & 1) {
            this._resetCalculation();
        }
        if ((calculation.signals & 2) === 0) {
            return;
        }
        this.itemHeight = itemHeight;
        this.minViewsRequired = calculation.minViews;
    }
    _calculate(scrollerInfo, itemCount, itemHeight) {
        if (itemCount === 0) {
            return Calculation.reset;
        }
        if (itemHeight === 0) {
            return Calculation.none;
        }
        const minViewsRequired = Math.floor(scrollerInfo.height / itemHeight);
        return Calculation.from(2, minViewsRequired);
    }
    _resetCalculation() {
        this.minViewsRequired = 0;
        this.itemHeight = 0;
    }
    itemsChanged(items) {
        const controller = this.$controller;
        const collectionStrategy = this.collectionStrategy = this._container.get(ICollectionStrategyLocator).getStrategy(items);
        const itemCount = collectionStrategy.count();
        const views = this.views;
        const maxViewsRequired = this.minViewsRequired * 2;
        let i = 0;
        let currViewCount = views.length;
        let view = null;
        if (itemCount === 0) {
            for (i = 0; currViewCount > i; ++i) {
                view = views[i];
                void view.deactivate(controller, controller, 2);
                view.nodes.remove();
            }
            views.length = 0;
            this._resetCalculation();
            return;
        }
        if (currViewCount > maxViewsRequired) {
            while (currViewCount > maxViewsRequired) {
                view = views[currViewCount - 1];
                void view.deactivate(controller, controller, 2);
                view.nodes.remove();
                --currViewCount;
            }
            views.length = currViewCount;
        }
        if (currViewCount > itemCount) {
            while (currViewCount > itemCount) {
                view = views[currViewCount - 1];
                void view.deactivate(controller, controller, 2);
                view.nodes.remove();
                --currViewCount;
            }
            views.length = itemCount;
        }
        currViewCount = views.length;
        const realViewCount = Math.min(maxViewsRequired, itemCount);
        for (i = currViewCount; i < realViewCount; i++) {
            views.push(this._factory.create());
        }
        const itemHeight = this.itemHeight;
        const local = this.local;
        const { firstIndex, topCount, botCount, } = this.measureBuffer(this.scrollerObserver.getValue(), views.length, itemCount, itemHeight);
        let idx = 0;
        let item;
        let prevView;
        let scope;
        for (i = 0; realViewCount > i; ++i) {
            idx = firstIndex + i;
            item = collectionStrategy.item(idx);
            view = views[i];
            prevView = views[i - 1];
            if (view.isActive) {
                scope = view.scope;
                scope.bindingContext[local] = item;
                scope.overrideContext.$index = idx;
                scope.overrideContext.$length = itemCount;
            }
            else {
                view.nodes.insertBefore(prevView.nodes.firstChild.nextSibling);
                scope = Scope.fromParent(controller.scope, new BindingContext(local, collectionStrategy.item(idx)));
                scope.overrideContext.$index = idx;
                scope.overrideContext.$length = itemCount;
                enhanceOverrideContext(scope.overrideContext);
                void view.activate(controller, controller, 1, scope);
            }
        }
        this._obsMediator.start(items);
        this.dom.update(topCount * itemHeight, botCount * itemHeight);
    }
    calcRealScrollTop(scrollerInfo) {
        const scroller_scroll_top = scrollerInfo.scrollTop;
        const top_buffer_distance = getDistanceToScroller(this.dom.top, scrollerInfo.scroller);
        const real_scroll_top = Math.max(0, scroller_scroll_top === 0
            ? 0
            : (scroller_scroll_top - top_buffer_distance));
        return real_scroll_top;
    }
    measureBuffer(scrollerInfo, viewCount, collectionSize, itemHeight) {
        const real_scroll_top = this.calcRealScrollTop(scrollerInfo);
        let first_index_after_scroll_adjustment = real_scroll_top === 0
            ? 0
            : Math.floor(real_scroll_top / itemHeight);
        if (first_index_after_scroll_adjustment + viewCount >= collectionSize) {
            first_index_after_scroll_adjustment = Math.max(0, collectionSize - viewCount);
        }
        const top_buffer_item_count_after_scroll_adjustment = first_index_after_scroll_adjustment;
        const bot_buffer_item_count_after_scroll_adjustment = Math.max(0, collectionSize - top_buffer_item_count_after_scroll_adjustment - viewCount);
        return {
            firstIndex: first_index_after_scroll_adjustment,
            topCount: top_buffer_item_count_after_scroll_adjustment,
            botCount: bot_buffer_item_count_after_scroll_adjustment,
        };
    }
    handleScrollerChange(scrollerInfo) {
        const task = this.task;
        this.task = this.taskQueue.queueTask(() => {
            this.task = null;
            this.handleScroll(scrollerInfo);
        });
        task?.cancel();
    }
    handleScroll(scrollerInfo) {
        if (this.itemHeight === 0) {
            return;
        }
        const prevScrollerInfo = this._currScrollerInfo;
        const local = this.local;
        const itemHeight = this.itemHeight;
        const repeatDom = this.dom;
        const views = this.views;
        const collectionStrategy = this.collectionStrategy;
        const viewCount = views.length;
        const collectionSize = collectionStrategy.count();
        const prevFirstIndex = views[0].scope.overrideContext.$index;
        const { firstIndex: currFirstIndex, topCount: topCount1, botCount: botCount1 } = this.measureBuffer(scrollerInfo, viewCount, collectionSize, itemHeight);
        const isScrollingDown = scrollerInfo.scrollTop > prevScrollerInfo.scrollTop;
        const isJumping = isScrollingDown
            ? currFirstIndex >= prevFirstIndex + viewCount
            : currFirstIndex + viewCount <= prevFirstIndex;
        this._currScrollerInfo = scrollerInfo;
        if (currFirstIndex === prevFirstIndex) {
            return;
        }
        let view = null;
        let scope = null;
        let idx = 0;
        let viewsToMoveCount = 0;
        let idxIncrement = 0;
        let i = 0;
        if (isJumping) {
            for (i = 0; viewCount > i; ++i) {
                idx = currFirstIndex + i;
                scope = views[i].scope;
                scope.bindingContext[local] = collectionStrategy.item(idx);
                scope.overrideContext.$index = idx;
                scope.overrideContext.$length = collectionSize;
            }
        }
        else if (isScrollingDown) {
            viewsToMoveCount = currFirstIndex - prevFirstIndex;
            while (viewsToMoveCount > 0) {
                view = views.shift();
                idx = views[views.length - 1].scope.overrideContext['$index'] + 1;
                views.push(view);
                scope = view.scope;
                scope.bindingContext[local] = collectionStrategy.item(idx);
                scope.overrideContext.$index = idx;
                scope.overrideContext.$length = collectionSize;
                view.nodes.insertBefore(repeatDom.bottom);
                ++idxIncrement;
                --viewsToMoveCount;
            }
        }
        else {
            viewsToMoveCount = prevFirstIndex - currFirstIndex;
            while (viewsToMoveCount > 0) {
                idx = prevFirstIndex - (idxIncrement + 1);
                view = views.pop();
                scope = view.scope;
                scope.bindingContext[local] = collectionStrategy.item(idx);
                scope.overrideContext.$index = idx;
                scope.overrideContext.$length = collectionSize;
                view.nodes.insertBefore(views[0].nodes.firstChild);
                views.unshift(view);
                ++idxIncrement;
                --viewsToMoveCount;
            }
        }
        if (isScrollingDown) {
            if (collectionStrategy.isNearBottom(currFirstIndex + (viewCount - 1))) ;
        }
        else {
            if (collectionStrategy.isNearTop(views[0].scope.overrideContext['$index'])) ;
        }
        repeatDom.update(topCount1 * itemHeight, botCount1 * itemHeight);
    }
    getDistances() {
        return this.dom?.distances ?? [0, 0];
    }
    getViews() {
        return this.views.slice(0);
    }
    handleCollectionChange(_collection, _indexmap) {
        this.itemsChanged(this.items);
    }
    handleInnerCollectionChange() {
        const newItems = this.iterable.evaluate(this.parent.scope, this._container, null);
        const oldItems = this.items;
        this.items = newItems;
        if (newItems === oldItems) {
            this.itemsChanged(newItems);
        }
    }
    _createAndActivateFirstView() {
        const firstView = this.getOrCreateFirstView();
        const repeatController = this.$controller;
        const collectionStrategy = this.collectionStrategy;
        const parentScope = repeatController.scope;
        const itemScope = Scope.fromParent(parentScope, new BindingContext(this.local, collectionStrategy.first()));
        itemScope.overrideContext.$index = 0;
        itemScope.overrideContext.$length = collectionStrategy.count();
        enhanceOverrideContext(itemScope.overrideContext);
        firstView.nodes.insertBefore(this.dom.bottom);
        void firstView.activate(repeatController, repeatController, 0, itemScope);
        return firstView;
    }
    getOrCreateFirstView() {
        const views = this.views;
        if (views.length > 0) {
            return views[0];
        }
        const view = this._factory.create();
        views.push(view);
        return view;
    }
}
customAttribute({
    isTemplateController: true,
    name: 'virtual-repeat',
    bindables: {
        local: { property: 'local' },
        items: { property: 'items', primary: true }
    }
})(VirtualRepeat);
class CollectionObservationMediator {
    constructor(repeat, key) {
        this.repeat = repeat;
        this.key = key;
    }
    handleCollectionChange(collection, indexMap) {
        this.repeat[this.key](collection, indexMap);
    }
    start(c) {
        if (this._collection === c) {
            return;
        }
        this.stop();
        if (c != null) {
            getCollectionObserver(this._collection = c)?.subscribe(this);
        }
    }
    stop() {
        getCollectionObserver(this._collection)?.unsubscribe(this);
    }
}
var SizingSignals;
(function (SizingSignals) {
    SizingSignals[SizingSignals["none"] = 0] = "none";
    SizingSignals[SizingSignals["reset"] = 1] = "reset";
    SizingSignals[SizingSignals["has_sizing"] = 2] = "has_sizing";
})(SizingSignals || (SizingSignals = {}));
class Calculation {
    constructor(signals, minViews) {
        this.signals = signals;
        this.minViews = minViews;
    }
    static from(signals, minViews) {
        return new Calculation(signals, minViews);
    }
}
Calculation.reset = new Calculation(1, 0);
Calculation.none = new Calculation(0, 0);
const enhancedContextCached = new WeakSet();
function enhanceOverrideContext(context) {
    const ctx = context;
    if (enhancedContextCached.has(ctx)) {
        return;
    }
    Object.defineProperties(ctx, {
        $first: createGetterDescriptor($first),
        $last: createGetterDescriptor($last),
        $middle: createGetterDescriptor($middle),
        $even: createGetterDescriptor($even),
        $odd: createGetterDescriptor($odd),
    });
}
function createGetterDescriptor(getter) {
    return { configurable: true, enumerable: true, get: getter };
}
function $even() {
    return this.$index % 2 === 0;
}
function $odd() {
    return this.$index % 2 !== 0;
}
function $first() {
    return this.$index === 0;
}
function $last() {
    return this.$index === this.$length - 1;
}
function $middle() {
    return this.$index > 0 && this.$index < (this.$length - 1);
}

class CollectionStrategyLocator {
    static register(container) {
        return Registration.singleton(ICollectionStrategyLocator, this).register(container);
    }
    getStrategy(items) {
        if (items == null) {
            return new NullCollectionStrategy();
        }
        if (items instanceof Array) {
            return new ArrayCollectionStrategy(items);
        }
        throw new Error(`Unable to find a strategy for collection type: ${typeof items}`);
    }
}
class ArrayCollectionStrategy {
    constructor(val) {
        this.val = val;
    }
    count() {
        return this.val.length;
    }
    first() {
        return this.val.length > 0 ? this.val[0] : null;
    }
    last() {
        return this.val.length > 0 ? this.val[this.val.length - 1] : null;
    }
    item(index) {
        return this.val[index] ?? null;
    }
    range(start, end) {
        const val = this.val;
        const len = val.length;
        if (len > start && end > start) {
            return val.slice(start, end);
        }
        return [];
    }
    isNearTop(index) {
        return index < 5;
    }
    isNearBottom(index) {
        return index > this.val.length - 5;
    }
}
class NullCollectionStrategy {
    get val() {
        return null;
    }
    count() {
        return 0;
    }
    isNearTop() {
        return false;
    }
    isNearBottom() {
        return false;
    }
    first() {
        return null;
    }
    last() {
        return null;
    }
    item() {
        return null;
    }
    range() {
        return [];
    }
}

class ScrollerObserverLocator {
    constructor(p) {
        this.cache = new WeakMap();
        this.p = p;
    }
    static get inject() {
        return [IPlatform];
    }
    static register(container) {
        return Registration.singleton(IScrollerObsererLocator, this).register(container);
    }
    getObserver(scroller) {
        const cache = this.cache;
        let observer = cache.get(scroller);
        if (!cache.has(scroller)) {
            cache.set(scroller, observer = new ScrollerObserver(this.p, scroller));
        }
        return observer;
    }
}
class ScrollerObserver {
    constructor(p, scroller) {
        this.p = p;
        this.scroller = scroller;
        this.subs = new Set();
        this.geo = null;
    }
    start() {
        this.scroller.addEventListener('scroll', this);
        const ResizeObserverCtor = getResizeObserverClass(this.p);
        if (typeof ResizeObserverCtor === 'function') {
            (this.sizeObs = new ResizeObserverCtor((entries) => {
                const oldGeo = this.geo;
                const newGeo = new ElementGeometry(entries[0].contentRect);
                if (!newGeo.equals(oldGeo)) {
                    this.geo = newGeo;
                    this.notify();
                }
            }))
                .observe(this.scroller);
        }
    }
    stop() {
        this.scroller.removeEventListener('scroll', this);
        this.sizeObs?.disconnect();
        this.sizeObs = void 0;
    }
    notify() {
        this.subs.forEach(notifySubscriber, this.getValue());
    }
    setValue() {
        throw new Error('scroller info is readonly');
    }
    getValue() {
        const scroller = this.scroller;
        const rect = scroller.getBoundingClientRect();
        return new ScrollerInfo(scroller, scroller.scrollTop, rect.width, rect.height);
    }
    handleEvent(_e) {
        this.notify();
    }
    subscribe(sub) {
        if (this.subs.size === 0) {
            this.start();
        }
        this.subs.add(sub);
    }
    unsubscribe(sub) {
        const subs = this.subs;
        if (subs.has(sub) && subs.size === 1) {
            this.stop();
        }
        subs.delete(sub);
    }
}
function notifySubscriber(sub) {
    sub.handleScrollerChange(this);
}
class ElementGeometry {
    constructor(domRect) {
        this.t = domRect.top;
        this.l = domRect.left;
        this.w = domRect.width;
        this.h = domRect.height;
    }
    equals(geo) {
        if (geo == null) {
            return false;
        }
        return this.t === geo.t
            && this.l === geo.l
            && this.w === geo.w
            && this.h === geo.h;
    }
}
class ScrollerInfo {
    constructor(scroller, scrollTop, width, height) {
        this.scroller = scroller;
        this.scrollTop = scrollTop;
        this.width = width;
        this.height = height;
    }
}
const getResizeObserverClass = (p) => p.window.ResizeObserver;

class DefaultDomRenderer {
    constructor(p) {
        this.p = p;
    }
    static get inject() { return [IPlatform]; }
    static register(container) {
        return Registration.singleton(IDomRenderer, this).register(container);
    }
    render(target) {
        const doc = this.p.document;
        const parent = target.parentNode;
        if (parent === null) {
            throw new Error('Invalid render target');
        }
        let bufferEls;
        switch (parent.tagName) {
            case 'TBODY':
            case 'THEAD':
            case 'TFOOT':
            case 'TABLE':
                bufferEls = insertBefore(doc, 'tr', target);
                return new TableDom(parent.closest('table'), target, bufferEls[0], bufferEls[1]);
            case 'UL':
            case 'OL':
                bufferEls = insertBefore(doc, 'div', target);
                return new ListDom(parent, target, bufferEls[0], bufferEls[1]);
            default:
                bufferEls = insertBefore(doc, 'div', target);
                return new DefaultDom(target, bufferEls[0], bufferEls[1]);
        }
    }
}
class DefaultDom {
    constructor(anchor, top, bottom) {
        this.anchor = anchor;
        this.top = top;
        this.bottom = bottom;
        this.tH = 0;
        this.bH = 0;
    }
    get scroller() {
        return getScrollerElement(this.anchor);
    }
    get distances() {
        return [this.tH, this.bH];
    }
    update(top, bot) {
        this.top.style.height = `${this.tH = top}px`;
        this.bottom.style.height = `${this.bH = bot}px`;
    }
    dispose() {
        this.top.remove();
        this.bottom.remove();
    }
}
class ListDom extends DefaultDom {
    constructor(list, anchor, top, bottom) {
        super(anchor, top, bottom);
        this.list = list;
    }
    get scroller() {
        return getScrollerElement(this.list);
    }
}
class TableDom extends DefaultDom {
    constructor(table, anchor, top, bottom) {
        super(anchor, top, bottom);
        this.table = table;
    }
    get scroller() {
        return getScrollerElement(this.table);
    }
}
function insertBefore(doc, el, target) {
    const parent = target.parentNode;
    return [
        parent.insertBefore(doc.createElement(el), target),
        parent.insertBefore(doc.createElement(el), target),
    ];
}

const DefaultVirtualRepeatConfiguration = {
    register(container) {
        return container.register(ScrollerObserverLocator, CollectionStrategyLocator, DefaultDomRenderer, VirtualRepeat);
    }
};

export { CollectionStrategyLocator, DefaultDomRenderer, DefaultVirtualRepeatConfiguration, ICollectionStrategyLocator, IDomRenderer, IScrollerObsererLocator, ScrollerObserver, ScrollerObserverLocator, VirtualRepeat };
//# sourceMappingURL=index.dev.mjs.map
