'use strict';

const Constraint = require('./constraint');

const _ = require('lodash');

class MaxConstraint extends Constraint {
    constructor(core) {
        super('max', core);
    }

    harmonize(options, callback) {
        this.core.cluster.myriad.persistence.keys([this.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, '*'].join(this.core.constants.myriad.DELIMITER), (err, containers) => {
            if(err) {
                return callback(err);
            }

            const difference = containers.length - options.application.tags.constraints.max;

            if(difference > 0) {
                this.core.applications.remove_containers(options.application_name, difference, callback);
            } else {
                return callback();
            }
        });
    }

    deploy(options, callback) {
        const max_containers = options.container.tags.constraints.max;

        this.core.cluster.myriad.persistence.keys([this.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, '*'].join(this.core.constants.myriad.DELIMITER), (err, containers) => {
            if(err) {
                return callback(err);
            }

            if((containers.length === max_containers && _.isUndefined(options.container.id)) || (containers.length > options.container.tags.constraints.max)) {
                const err = new Error('Max containers constraint already met');
                err.should_redeploy_container = false;
                return callback(err);
            } else {
                return callback();
            }
        });
    }
}

module.exports = MaxConstraint;
