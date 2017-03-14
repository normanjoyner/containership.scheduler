'use strict';

const Constraint = require('./constraint');

const _ = require('lodash');
const async = require('async');

class MinConstraint extends Constraint {
    constructor(core) {
        super('min', core);
    }

    harmonize(options, callback) {
        this.core.cluster.myriad.persistence.keys([this.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, '*'].join(this.core.constants.myriad.DELIMITER), (err, containers) => {
            if(err) {
                return callback(err);
            }

            const difference = options.application.tags.constraints.min - containers.length;

            if(difference > 0) {
                async.times(difference, (index, callback) => {
                    this.core.applications.deploy_container(options.application_name, {}, () => {
                        return callback();
                    });
                }, callback);
            } else {
                return callback();
            }
        });
    }

    remove(options, callback) {
        let to_remove = options.application.tags.constraints.min;

        if(options.total_containers - options.num_containers < options.application.tags.constraints.min) {
            to_remove = options.total_containers - options.application.tags.constraints.min;
        }

        const available_containers = options.available_containers;

        const available_containers_by_status = _.defaults(_.groupBy(available_containers, 'status'), {
            loaded: [],
            loading: [],
            unloaded: []
        });

        options.available_containers = _.take(_.flatten([
            available_containers_by_status.unloaded,
            available_containers_by_status.loading,
            available_containers_by_status.loaded
        ]), to_remove);

        return callback();
    }

}

module.exports = MinConstraint;
