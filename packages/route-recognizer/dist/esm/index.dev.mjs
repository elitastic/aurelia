class Parameter {
    constructor(name, isOptional, isStar) {
        this.name = name;
        this.isOptional = isOptional;
        this.isStar = isStar;
    }
}
class ConfigurableRoute {
    constructor(path, caseSensitive, handler) {
        this.path = path;
        this.caseSensitive = caseSensitive;
        this.handler = handler;
    }
}
class Endpoint {
    constructor(route, params) {
        this.route = route;
        this.params = params;
    }
}
class RecognizedRoute {
    constructor(endpoint, params) {
        this.endpoint = endpoint;
        this.params = params;
    }
}
class Candidate {
    constructor(chars, states, skippedStates, result) {
        this.chars = chars;
        this.states = states;
        this.skippedStates = skippedStates;
        this.result = result;
        this.head = states[states.length - 1];
        this.endpoint = this.head?.endpoint;
    }
    advance(ch) {
        const { chars, states, skippedStates, result } = this;
        let stateToAdd = null;
        let matchCount = 0;
        const state = states[states.length - 1];
        function $process(nextState, skippedState) {
            if (nextState.isMatch(ch)) {
                if (++matchCount === 1) {
                    stateToAdd = nextState;
                }
                else {
                    result.add(new Candidate(chars.concat(ch), states.concat(nextState), skippedState === null ? skippedStates : skippedStates.concat(skippedState), result));
                }
            }
            if (state.segment === null && nextState.isOptional && nextState.nextStates !== null) {
                if (nextState.nextStates.length > 1) {
                    throw createError(`${nextState.nextStates.length} nextStates`);
                }
                const separator = nextState.nextStates[0];
                if (!separator.isSeparator) {
                    throw createError(`Not a separator`);
                }
                if (separator.nextStates !== null) {
                    for (const $nextState of separator.nextStates) {
                        $process($nextState, nextState);
                    }
                }
            }
        }
        if (state.isDynamic) {
            $process(state, null);
        }
        if (state.nextStates !== null) {
            for (const nextState of state.nextStates) {
                $process(nextState, null);
            }
        }
        if (stateToAdd !== null) {
            states.push(this.head = stateToAdd);
            chars.push(ch);
            if (stateToAdd.endpoint !== null) {
                this.endpoint = stateToAdd.endpoint;
            }
        }
        if (matchCount === 0) {
            result.remove(this);
        }
    }
    finalize() {
        function collectSkippedStates(skippedStates, state) {
            const nextStates = state.nextStates;
            if (nextStates !== null) {
                if (nextStates.length === 1 && nextStates[0].segment === null) {
                    collectSkippedStates(skippedStates, nextStates[0]);
                }
                else {
                    for (const nextState of nextStates) {
                        if (nextState.isOptional && nextState.endpoint !== null) {
                            skippedStates.push(nextState);
                            if (nextState.nextStates !== null) {
                                for (const $nextState of nextState.nextStates) {
                                    collectSkippedStates(skippedStates, $nextState);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
        collectSkippedStates(this.skippedStates, this.head);
    }
    getParams() {
        const { states, chars, endpoint } = this;
        const params = {};
        for (const param of endpoint.params) {
            params[param.name] = void 0;
        }
        for (let i = 0, ii = states.length; i < ii; ++i) {
            const state = states[i];
            if (state.isDynamic) {
                const name = state.segment.name;
                if (params[name] === void 0) {
                    params[name] = chars[i];
                }
                else {
                    params[name] += chars[i];
                }
            }
        }
        return params;
    }
    compareTo(b) {
        const statesA = this.states;
        const statesB = b.states;
        for (let iA = 0, iB = 0, ii = Math.max(statesA.length, statesB.length); iA < ii; ++iA) {
            let stateA = statesA[iA];
            if (stateA === void 0) {
                return 1;
            }
            let stateB = statesB[iB];
            if (stateB === void 0) {
                return -1;
            }
            let segmentA = stateA.segment;
            let segmentB = stateB.segment;
            if (segmentA === null) {
                if (segmentB === null) {
                    ++iB;
                    continue;
                }
                if ((stateA = statesA[++iA]) === void 0) {
                    return 1;
                }
                segmentA = stateA.segment;
            }
            else if (segmentB === null) {
                if ((stateB = statesB[++iB]) === void 0) {
                    return -1;
                }
                segmentB = stateB.segment;
            }
            if (segmentA.kind < segmentB.kind) {
                return 1;
            }
            if (segmentA.kind > segmentB.kind) {
                return -1;
            }
            ++iB;
        }
        const skippedStatesA = this.skippedStates;
        const skippedStatesB = b.skippedStates;
        const skippedStatesALen = skippedStatesA.length;
        const skippedStatesBLen = skippedStatesB.length;
        if (skippedStatesALen < skippedStatesBLen) {
            return 1;
        }
        if (skippedStatesALen > skippedStatesBLen) {
            return -1;
        }
        for (let i = 0; i < skippedStatesALen; ++i) {
            const skippedStateA = skippedStatesA[i];
            const skippedStateB = skippedStatesB[i];
            if (skippedStateA.length < skippedStateB.length) {
                return 1;
            }
            if (skippedStateA.length > skippedStateB.length) {
                return -1;
            }
        }
        return 0;
    }
}
function hasEndpoint(candidate) {
    return candidate.head.endpoint !== null;
}
function compareChains(a, b) {
    return a.compareTo(b);
}
class RecognizeResult {
    get isEmpty() {
        return this.candidates.length === 0;
    }
    constructor(rootState) {
        this.candidates = [];
        this.candidates = [new Candidate([''], [rootState], [], this)];
    }
    getSolution() {
        const candidates = this.candidates.filter(hasEndpoint);
        if (candidates.length === 0) {
            return null;
        }
        for (const candidate of candidates) {
            candidate.finalize();
        }
        candidates.sort(compareChains);
        return candidates[0];
    }
    add(candidate) {
        this.candidates.push(candidate);
    }
    remove(candidate) {
        this.candidates.splice(this.candidates.indexOf(candidate), 1);
    }
    advance(ch) {
        const candidates = this.candidates.slice();
        for (const candidate of candidates) {
            candidate.advance(ch);
        }
    }
}
const RESIDUE = '$$residue';
class RouteRecognizer {
    constructor() {
        this.rootState = new State(null, null, '');
        this.cache = new Map();
        this.endpointLookup = new Map();
    }
    add(routeOrRoutes, addResidue = false) {
        let params;
        if (routeOrRoutes instanceof Array) {
            for (const route of routeOrRoutes) {
                params = this.$add(route, false).params;
                if (!addResidue || (params[params.length - 1]?.isStar ?? false))
                    continue;
                this.$add({ ...route, path: `${route.path}/*${RESIDUE}` }, true);
            }
        }
        else {
            params = this.$add(routeOrRoutes, false).params;
            if (addResidue && !(params[params.length - 1]?.isStar ?? false)) {
                this.$add({ ...routeOrRoutes, path: `${routeOrRoutes.path}/*${RESIDUE}` }, true);
            }
        }
        this.cache.clear();
    }
    $add(route, addResidue) {
        const path = route.path;
        const lookup = this.endpointLookup;
        if (lookup.has(path))
            throw createError(`Cannot add duplicate path '${path}'.`);
        const $route = new ConfigurableRoute(path, route.caseSensitive === true, route.handler);
        const parts = path === '' ? [''] : path.split('/').filter(isNotEmpty);
        const params = [];
        let state = this.rootState;
        for (const part of parts) {
            state = state.append(null, '/');
            switch (part.charAt(0)) {
                case ':': {
                    const isOptional = part.endsWith('?');
                    const name = isOptional ? part.slice(1, -1) : part.slice(1);
                    if (name === RESIDUE)
                        throw new Error(`Invalid parameter name; usage of the reserved parameter name '${RESIDUE}' is used.`);
                    params.push(new Parameter(name, isOptional, false));
                    state = new DynamicSegment(name, isOptional).appendTo(state);
                    break;
                }
                case '*': {
                    const name = part.slice(1);
                    let kind;
                    if (name === RESIDUE) {
                        if (!addResidue)
                            throw new Error(`Invalid parameter name; usage of the reserved parameter name '${RESIDUE}' is used.`);
                        kind = 1;
                    }
                    else {
                        kind = 2;
                    }
                    params.push(new Parameter(name, true, true));
                    state = new StarSegment(name, kind).appendTo(state);
                    break;
                }
                default: {
                    state = new StaticSegment(part, $route.caseSensitive).appendTo(state);
                    break;
                }
            }
        }
        const endpoint = new Endpoint($route, params);
        state.setEndpoint(endpoint);
        lookup.set(path, endpoint);
        return endpoint;
    }
    recognize(path) {
        let result = this.cache.get(path);
        if (result === void 0) {
            this.cache.set(path, result = this.$recognize(path));
        }
        return result;
    }
    $recognize(path) {
        path = decodeURI(path);
        if (!path.startsWith('/')) {
            path = `/${path}`;
        }
        if (path.length > 1 && path.endsWith('/')) {
            path = path.slice(0, -1);
        }
        const result = new RecognizeResult(this.rootState);
        for (let i = 0, ii = path.length; i < ii; ++i) {
            const ch = path.charAt(i);
            result.advance(ch);
            if (result.isEmpty) {
                return null;
            }
        }
        const candidate = result.getSolution();
        if (candidate === null) {
            return null;
        }
        const { endpoint } = candidate;
        const params = candidate.getParams();
        return new RecognizedRoute(endpoint, params);
    }
    getEndpoint(path) {
        return this.endpointLookup.get(path) ?? null;
    }
}
class State {
    constructor(prevState, segment, value) {
        this.prevState = prevState;
        this.segment = segment;
        this.value = value;
        this.nextStates = null;
        this.endpoint = null;
        switch (segment?.kind) {
            case 3:
                this.length = prevState.length + 1;
                this.isSeparator = false;
                this.isDynamic = true;
                this.isOptional = segment.optional;
                break;
            case 2:
            case 1:
                this.length = prevState.length + 1;
                this.isSeparator = false;
                this.isDynamic = true;
                this.isOptional = false;
                break;
            case 4:
                this.length = prevState.length + 1;
                this.isSeparator = false;
                this.isDynamic = false;
                this.isOptional = false;
                break;
            case undefined:
                this.length = prevState === null ? 0 : prevState.length;
                this.isSeparator = true;
                this.isDynamic = false;
                this.isOptional = false;
                break;
        }
    }
    append(segment, value) {
        let state;
        let nextStates = this.nextStates;
        if (nextStates === null) {
            state = void 0;
            nextStates = this.nextStates = [];
        }
        else if (segment === null) {
            state = nextStates.find(s => s.value === value);
        }
        else {
            state = nextStates.find(s => s.segment?.equals(segment));
        }
        if (state === void 0) {
            nextStates.push(state = new State(this, segment, value));
        }
        return state;
    }
    setEndpoint(endpoint) {
        if (this.endpoint !== null) {
            throw createError(`Cannot add ambiguous route. The pattern '${endpoint.route.path}' clashes with '${this.endpoint.route.path}'`);
        }
        this.endpoint = endpoint;
        if (this.isOptional) {
            this.prevState.setEndpoint(endpoint);
            if (this.prevState.isSeparator && this.prevState.prevState !== null) {
                this.prevState.prevState.setEndpoint(endpoint);
            }
        }
    }
    isMatch(ch) {
        const segment = this.segment;
        switch (segment?.kind) {
            case 3:
                return !this.value.includes(ch);
            case 2:
            case 1:
                return true;
            case 4:
            case undefined:
                return this.value.includes(ch);
        }
    }
}
function isNotEmpty(segment) {
    return segment.length > 0;
}

class StaticSegment {
    get kind() { return 4; }
    constructor(value, caseSensitive) {
        this.value = value;
        this.caseSensitive = caseSensitive;
    }
    appendTo(state) {
        const { value, value: { length } } = this;
        if (this.caseSensitive) {
            for (let i = 0; i < length; ++i) {
                state = state.append(this, value.charAt(i));
            }
        }
        else {
            for (let i = 0; i < length; ++i) {
                const ch = value.charAt(i);
                state = state.append(this, ch.toUpperCase() + ch.toLowerCase());
            }
        }
        return state;
    }
    equals(b) {
        return (b.kind === 4 &&
            b.caseSensitive === this.caseSensitive &&
            b.value === this.value);
    }
}
class DynamicSegment {
    get kind() { return 3; }
    constructor(name, optional) {
        this.name = name;
        this.optional = optional;
    }
    appendTo(state) {
        state = state.append(this, '/');
        return state;
    }
    equals(b) {
        return (b.kind === 3 &&
            b.optional === this.optional &&
            b.name === this.name);
    }
}
class StarSegment {
    constructor(name, kind) {
        this.name = name;
        this.kind = kind;
    }
    appendTo(state) {
        state = state.append(this, '');
        return state;
    }
    equals(b) {
        return ((b.kind === 2 || b.kind === 1) &&
            b.name === this.name);
    }
}
const createError = (msg) => new Error(msg);

export { ConfigurableRoute, Endpoint, Parameter, RESIDUE, RecognizedRoute, RouteRecognizer };
//# sourceMappingURL=index.dev.mjs.map
