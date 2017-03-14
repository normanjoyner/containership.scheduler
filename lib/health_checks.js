'use strict';

const _ = require('lodash');
const async = require('async');
const constants = require('containership.core.constants');
const net = require('net');
const MyriadKVClient = require('myriad-kv-client');

const myriad = new MyriadKVClient({
    host: process.env.MYRIAD_HOST,
    port: process.env.MYRIAD_PORT
});

class HealthCheck {

    constructor() {
        this.application_name = process.env.APPLICATION_NAME;
        this.scope = process.env.LEGIOND_SCOPE;

        this.hosts = {};
        this.health_checks = [];
        this.health_check_intervals = [];
        this.application;

        this.get_hosts(() => {
            this.get_application((err) => {
                if(!err) {
                    this.get_containers();
                }
            });
        });

        const application_subscriber = myriad.subscribe([constants.myriad.APPLICATION_PREFIX, this.application_name].join(constants.myriad.DELIMITER));
        const containers_subscriber = myriad.subscribe([constants.myriad.CONTAINERS_PREFIX, this.application_name, '*'].join(constants.myriad.DELIMITER));

        application_subscriber.on('message', (message) => {
            if(message.type === 'data') {
                this.get_application();
            }
        });

        containers_subscriber.on('message', (message) => {
            if(message.type === 'data') {
                this.get_containers();
            }
        });
    }

    get_hosts(callback) {
        return myriad.stat((err, stats) => {
            if(err) {
                return callback && callback(err);
            }
            const hosts = {};
            _.forEach(stats.hosts, (value, key) => {
                hosts[key] = value.address[this.scope];
            });

            this.hosts = hosts;

            return callback && callback();
        });
    }

    get_application(callback) {
        return myriad.get([constants.myriad.APPLICATION_PREFIX, this.application_name].join(constants.myriad.DELIMITER), (err, app) => {
            if(err) {
                return callback && callback(err);
            }

            try {
                this.application = JSON.parse(app);
            } catch(err) {
                return callback && callback(err);
            }

            // reset health check intervals
            this.health_checks = this.application.health_checks;
            _.forEach(this.health_check_intervals, (interval) => {
                clearInterval(interval);
            });
            this.health_check_intervals = [];

            _.forEach(this.health_checks, (config) => {
                const interval = setInterval(() => {
                    this.do_health_check(config);
                }, config.interval);
                this.health_check_intervals.push(interval);
            });

            return callback && callback();
        });
    }

    get_containers(callback) {
        return myriad.keys([constants.myriad.CONTAINERS_PREFIX, this.application_name, '*'].join(constants.myriad.DELIMITER), (err, containers) => {
            if(err) {
                return callback && callback(err);
            }

            return async.map(containers || [], (container_id, callback) => {
                return myriad.get(container_id, (err, container) => {
                    if(err) {
                        return callback(err);
                    }

                    try {
                        container = JSON.parse(container);

                        const EMPTY_CONTAINER_ID = [constants.myriad.CONTAINERS_PREFIX, this.application_name, ''].join(constants.myriad.DELIMITER);
                        const isEmptyContainerId = EMPTY_CONTAINER_ID === container_id;

                        // If we have arrived at a state where the myriad-kv key contains an ID for the container, but the
                        // actual container data does not contain an ID, set the container data id to the value of the myriad kv id
                        if (!isEmptyContainerId && !container.id) {
                            container.id = container_id.substring(EMPTY_CONTAINER_ID.length);
                        }

                        return callback(null, container);
                    } catch(err) {
                        return callback(err);
                    }
                });
            }, (err, containers) => {
                if(!err) {
                    this.application.containers = containers;
                }

                return callback && callback(null, containers);
            });
        });
    }

    get_host_ip(host_id, callback) {
        let host_ip = this.hosts[host_id];

        if(!host_ip) {
            return this.get_hosts(() => {
                host_ip = this.hosts[host_id];

                if(!host_ip) {
                    return callback(new Error('Error getting host ip'));
                }

                return callback(null, host_ip);
            });
        }

        return callback(null, host_ip);
    }

    do_health_check(config) {
        _.forEach(this.application && this.application.containers, (container) => {
            this.get_host_ip(container.host, (err, host) => {
                if(err || (!config.port && !container.host_port)) {
                    // if container is unloaded don't check for connection
                    return;
                }

                const options = {
                    host: host,
                    port: config.port || container.host_port
                };

                const client = net.connect(options, () => {
                    if(config.type === 'http') {
                        return client.write(`GET ${config.path} HTTP/1.0\r\n\r\n`);
                    } else {
                        return client.end();
                    }
                });

                const enforce_health_check_timeout = setTimeout(() => {
                    client.destroy(new Error('Health check timeout!'));
                }, config.timeout);

                // catch errors: required or error is caught by global error handler,
                // crashing the health check process
                client.on('error', () => {});

                const http_response = [];

                client.on('data', (data) => {
                    http_response.push(data.toString());
                });

                client.on('close', (did_error) => {
                    clearTimeout(enforce_health_check_timeout);

                    // any error should mark check as failed
                    if(did_error) {
                        return this.failed(container, config);
                    }

                    // if health check type is http, ensure status code is valid
                    if(config.type === 'http') {

                        const status_line = http_response.join().split('\n')[0];
                        const status_start = status_line.indexOf(' ', status_line.indexOf('HTTP') + 1);
                        const status_code = status_line.substring(status_start, status_line.indexOf(' ', status_start + 1)).trim();

                        if(status_code === config.status_code.toString()) {
                            this.successful(container, config);
                        } else {
                            this.failed(container, config);
                        }

                    // otherwise the check was successful
                    } else {
                        this.successful(container, config);
                    }
                });
            });
        });
    }

    successful(container, config) {
        const health_check_id = config.id;

        let container_health_check_results = container.tags && container.tags.metadata && container.tags.metadata.health_checks[health_check_id];

        if(!container_health_check_results) {
            return;
        }

        let successes = container_health_check_results.consecutive_successes || 0;
        successes++;

        container.tags.metadata.health_checks[health_check_id].consecutive_successes = successes;
        container.tags.metadata.health_checks[health_check_id].consecutive_failures = 0;
        if(successes < config.healthy_threshold) {
            return myriad.set({ key: [constants.myriad.CONTAINERS_PREFIX,  this.application_name, container.id].join(constants.myriad.DELIMITER), value: JSON.stringify(container)}, (err) => {
                if(!err) {
                    process.send({
                        type: 'log',
                        log: {
                            message: `${this.application_name} container ${container.id} passed ${successes}/${config.healthy_threshold} health checks`,
                            level: 'debug'
                        }
                    });
                }
            });
        } else if(successes === config.healthy_threshold) {
            container.tags.metadata.health_checks[health_check_id].is_healthy = true;

            myriad.set({ key: [constants.myriad.CONTAINERS_PREFIX,  this.application_name, container.id].join(constants.myriad.DELIMITER), value: JSON.stringify(container)}, (err) => {
                if(!err) {
                    process.send({
                        type: 'health_check',
                        container : container.id,
                        status: 'success'
                    });

                    process.send({
                        type: 'log',
                        log: {
                            message: `${this.application_name} container ${container.id} passed ${successes}/${config.healthy_threshold} health checks`,
                            level: 'debug'
                        }
                    });

                    process.send({
                        type: 'log',
                        log: {
                            message: `${this.application_name} container ${container.id} is healthy`,
                            level: 'info'
                        }
                    });
                }
            });
        }
    }

    failed(container, config) {
        const health_check_id = config.id;

        let container_health_check_results = container.tags && container.tags.metadata && container.tags.metadata.health_checks[health_check_id];

        if(!container_health_check_results) {
            return;
        }

        let failures = container_health_check_results.consecutive_failures || 0;
        failures++;

        if(failures < config.unhealthy_threshold) {
            container.tags.metadata.health_checks[health_check_id].consecutive_failures = failures;
            container.tags.metadata.health_checks[health_check_id].consecutive_successes = 0;

            return myriad.set({ key: [constants.myriad.CONTAINERS_PREFIX, this.application_name, container.id].join(constants.myriad.DELIMITER), value: JSON.stringify(container)}, () => {
                process.send({
                    type: 'log',
                    log: {
                        message: `${this.application_name} container ${container.id} failed ${failures} consecutive times (max consecutive failures allowed: ${config.unhealthy_threshold})`,
                        level: 'debug'
                    }
                });
            });
        } else if(failures === config.unhealthy_threshold) {
            // redeploy container send message to parent process
            process.send({
                type: 'log',
                log: {
                    message: `${this.application_name} container ${container.id} is unhealthy. Container will be killed and redeployed`,
                    level: 'info'
                }
            });

            process.send({
                type: 'health_check',
                container : container.id,
                status: 'failed'
            });
        }
    }
}

new HealthCheck();
