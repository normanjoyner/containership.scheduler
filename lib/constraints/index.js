'use strict';
const MaxConstraint = require('./max');
const MinConstraint = require('./min');
const PartitionConstraint = require('./partition');
const PerHostConstraint = require('./per_host');

module.exports = {

    list: {},

    initialize(core) {
        this.list = {
            max: new MaxConstraint(core),
            min: new MinConstraint(core),
            partition: new PartitionConstraint(core),
            per_host: new PerHostConstraint(core)
        };
    }
};
