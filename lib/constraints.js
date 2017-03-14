'use strict';

const constraints = require('./constraints/index');

const _ = require('lodash');
const async = require('async');

module.exports = {

    initialize(core) {
        constraints.initialize(core);
    },

    get_list() {
        return constraints.list;
    },

    get_constraints(container) {
        if(container.tags.constraints) {
            return _.intersection(_.keys(container.tags.constraints), _.keys(constraints.list));
        } else {
            return [];
        }
    },

    enforce(core, options, callback) {
        async.eachSeries(this.get_constraints(options.container), (constraint, callback) => {
            constraints.list[constraint].deploy(options, callback);
        }, (err) => {
            return callback(err, options.available_hosts);
        });
    },

    enforce_remove(core, options, callback) {
        async.eachSeries(this.get_constraints(options.application), (constraint, callback) => {
            constraints.list[constraint].remove(options, callback);
        }, (err) => {
            return callback(err, options.available_containers);
        });
    }

};
