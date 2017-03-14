'use strict';

const engines = require('../engines');
const persistence = require('./persistence');
const utils = require('./utils');

const _ = require('lodash');

module.exports = (core) => {

    // initialize available engines
    _.forEach(engines, (engine, engine_name) => {
        engines[engine_name] = engine(core);
    });

    // register middleware
    _.forEach(engines, (engine) => {

        // set CS_PROC_OPTS environment variable
        engine.addPreStartMiddleware('CS_PROC_OPTS', (options, callback) => {
            const application_name = options.application_name;
            const container = _.omit(options, [
                'application_name',
                'start_args'
            ]);

            const env_vars = container.env_vars;

            env_vars.CS_PROC_OPTS = JSON.stringify(core.options);
            container.env_vars = env_vars;

            const myriad_key = [core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER);
            core.cluster.myriad.persistence.set(myriad_key, JSON.stringify(container), callback);
        });

        // set codexd middleware
        persistence.codexd.register_middleware(core, engine);
    });

    return {

        container: {
            start(configuration) {
                const container = configuration.container;
                container.application_name = configuration.application;

                if(container.engine && engines[container.engine]) {
                    engines[container.engine].start(container);
                } else {
                    core.loggers['containership.scheduler'].log('error', `Could not start application ${container.application_name} with container id ${container.id}. Engine ${container.engine} not found!`);

                    // unload the container
                    utils.setContainerMyriadStateUnloaded(core, {
                        application_name: container.application_name,
                        container_id: container.id
                    });
                }
            },

            stop(configuration) {
                const container = {
                    application_name: configuration.application,
                    id: configuration.container_id,
                    engine: configuration.engine
                };

                if(container.engine && engines[container.engine]) {
                    engines[container.engine].stop(container);
                } else {
                    core.loggers['containership.scheduler'].log('error', `Could not stop application ${container.application_name} with container id ${container.id}. Engine ${container.engine} not found!`);
                }
            },

            reconcile(leader) {
                _.forEach(engines, (engine, engine_name) => {
                    engines[engine_name].reconcile(leader);
                });
            },

            add_pre_start_middleware(engine_name, middleware_name, callback) {
                if(engines[engine_name]) {
                    engines[engine_name].addPreStartMiddleware(middleware_name, callback);
                }
            },

            add_pre_pull_middleware(engine_name, middleware_name, callback) {
                if(engines[engine_name]) {
                    engines[engine_name].addPrePullMiddleware(middleware_name, callback);
                }
            },

            set_start_arguments(engine_name, key, value) {
                if(engines[engine_name]) {
                    engines[engine_name].setStartArgument(key, value);
                }
            }
        }

    };

};
