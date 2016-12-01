'use strict';

const _ = require('lodash');

class MethodNotImplementedError extends Error {
    constructor() {
        super('This method was not implemented.');
        this.name = 'MethodNotImplementedError';
    }
};

class Engine {

    constructor(core) {
        this.core = core;

        this.startArgs = {};

        this.middleware = {
            prePull: {},
            preStart: {}
        };

        this.containers = {};

        this.initialize();
    }

    initialize() {
        throw new MethodNotImplementedError();
    }

    addPreStartMiddleware(name, fn) {
        this.middleware.preStart[name] = fn;
    }

    addPrePullMiddleware(name, fn) {
        this.middleware.prePull[name] = fn;
    }

    setStartArgument(key, value) {
        this.startArgs[key] = value;
    }

    getContainers() {
        return this.containers;
    }

    start(options) {
        throw new MethodNotImplementedError();
    }

    stop(options) {
        throw new MethodNotImplementedError();
    }

    reconcile() {
        throw new MethodNotImplementedError();
    }
}

module.exports = Engine;
