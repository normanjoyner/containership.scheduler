'use strict';

class Constraint {
    constructor(constraint, core) {
        this.name = constraint;
        this.core = core;
    }

    harmonize(options, callback) {
        return callback();
    }

    deploy(options, callback) {
        return callback();
    }

    remove(options, callback) {
        return callback();
    }
}

module.exports = Constraint;
