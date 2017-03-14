'use strict';

const _ = require('lodash');
const mkdirp = require('mkdirp');

function createApplicationLogDirectory(core, options, callback) {
    const log_dir = `${core.options['base-log-dir']}/applications/${options.application_name}/${options.container_id}`;

    return mkdirp(log_dir, (err) => {
        return callback(err, {
            directory: log_dir
        });
    });
}

function updateContainerMyriadState(core, options, callback) {
    if(_.has(options, 'respawn') && !options.respawn) {
        deleteContainerMyriadState(core, options, callback);
    } else {
        // get container definition from controlling leader
        core.cluster.myriad.persistence.get(_.join([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, options.container_id], core.constants.myriad.DELIMITER), {local: false}, (err, container) => {
            if(err) {
                return  callback(err);
            }

            try {
                container = JSON.parse(container);

                // update container properties
                container = _.merge(container, _.pick(options, [
                    'container_port',
                    'engine',
                    'host',
                    'host_port',
                    'start_time',
                    'status',
                    'tags'
                ]));

                container.host_port = options.status === 'unloaded' && container.random_host_port ? null : container.host_port;

                // persist container definition
                core.cluster.myriad.persistence.set(_.join([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, options.container_id], core.constants.myriad.DELIMITER), JSON.stringify(container), callback);
            } catch(err) {
                return callback(err);
            }
        });
    }
}

function deleteContainerMyriadState(core, options, callback) {
    core.cluster.myriad.persistence.delete(_.join([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, options.container_id], core.constants.myriad.DELIMITER), callback);
}

function setContainerMyriadStateUnloaded(core, options, callback) {
    updateContainerMyriadState(core, {
        application_name: options.application_name,
        container_id: options.container_id,
        host: null,
        respawn: options.respawn,
        start_time: null,
        status: 'unloaded'
    }, (err) => {
        if(err) {
            core.loggers['containership.scheduler'].log('warn', `Failed to unload ${options.application_name} container: ${options.container_id}`);
            core.loggers['containership.scheduler'].log('error', err.message);
        }

        if(callback) {
            return callback(err);
        }
    });
}

function safeJsonParse(value) {
    if(typeof value !== 'string') {
        return value;
    }

    try {
        value = JSON.parse(value);
    } catch(err) { /* ignore */ }

    return value;
}

module.exports = {
    createApplicationLogDirectory,
    deleteContainerMyriadState,
    safeJsonParse,
    setContainerMyriadStateUnloaded,
    updateContainerMyriadState
};
