'use strict';

const constraints = require('./constraints');

const _ = require('lodash');
const async = require('async');
const child_process = require('child_process');
const constants = require('containership.core.constants');
const flat = require('flat');

let health_check_workers = {};

module.exports = (core) => {

    constraints.initialize(core);

    return {

        container: {

            harmonize: (callback) => {
                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, (err, applications) => {
                    if(err) {
                        core.loggers['containership.scheduler'].log('error', `Failed to run harmonization: ${err.message}`);
                        return callback();
                    }

                    async.each(applications, (application_name, callback) => {
                        core.cluster.myriad.persistence.get(application_name, (err, application) => {
                            if(err) {
                                return callback(err);
                            }

                            application_name = _.last(application_name.split(core.constants.myriad.DELIMITER));

                            try{
                                application = JSON.parse(application);
                            } catch(err) {
                                return callback(err);
                            }

                            async.each(_.keys(application.tags.constraints), (constraint, callback) => {
                                const constraints_list = constraints.get_list();
                                constraints_list[constraint].harmonize({
                                    application: application,
                                    application_name: application_name
                                }, callback);
                            }, (err) => {
                                if(err) {
                                    core.loggers['containership.scheduler'].log('error', `Failed to run harmonization: ${err.message}`);
                                    return callback();
                                }

                                core.applications.get_containers(application_name, (err, containers) => {
                                    if(err) {
                                        core.loggers['containership.scheduler'].log('error', `Failed to run harmonization: ${err.message}`);
                                        return callback();
                                    }

                                    async.each(containers, (container, callback) => {
                                        // if this container does not have an ID, we need to remove the container
                                        // state from myriad. Let it continue, it will be assigned an ID in the
                                        // core.applications.deploy function when it comes back null later on but
                                        // since its state is removed, it should prevent from container loop happening
                                        // on future harmonizes
                                        if(!container.id) {
                                            const invalid_container_id = [core.constants.myriad.CONTAINERS_PREFIX, application_name, ''].join(core.constants.myriad.DELIMITER);

                                            return core.cluster.myriad.persistence.delete(invalid_container_id, (err) => {
                                                if(err) {
                                                    core.loggers['containership.scheduler'].log('error', `Failed to remove container ${invalid_container_id}`);
                                                }

                                                if(container.status === 'loaded' && _.isNull(container.host)) {
                                                    core.applications.unload_containers(application_name, callback);
                                                } else {
                                                    return callback();
                                                }
                                            });
                                        } else {
                                            if(container.status === 'loaded' && _.isNull(container.host)) {
                                                core.applications.unload_containers(application_name, callback);
                                            } else {
                                                return callback();
                                            }
                                        }
                                    }, () => {
                                        containers = _.groupBy(containers, 'status');

                                        if(containers.unloaded) {
                                            core.loggers['containership.scheduler'].log('info', `Attempting to replace ${containers.unloaded.length} unloaded containers for ${application.id}`);

                                            async.each(containers.unloaded, (container, callback) => {
                                                container = _.omit(container, [
                                                    'host',
                                                    'start_time'
                                                ]);

                                                core.applications.deploy_container(application.id, container, (err) => {
                                                    if(err) {
                                                        core.loggers['containership.scheduler'].log('error', err.message);
                                                    }

                                                    return callback();
                                                });
                                            }, callback);
                                        } else {
                                            return callback();
                                        }
                                    });
                                });
                            });
                        });
                    }, callback);
                });
            },

            deploy: (application_name, container, callback) => {
                const find_available_host = (container, callback) => {
                    const containership_container_id = `${application_name}-${container.id}`;

                    // get all available peers
                    const available_hosts = core.cluster.legiond.get_peers();

                    // add self
                    available_hosts.push(core.cluster.legiond.get_attributes());

                    const resources = {
                        cpus: parseFloat(container.cpus),
                        memory: _.parseInt(container.memory)
                    };

                    async.waterfall([
                        // filter out non-follower hosts
                        (callback) => {
                            filter.by_mode(available_hosts, callback);
                        },
                        // filter out hosts that do not match requested tags
                        (hosts, callback) => {
                            filter.by_tag(hosts, container.tags, callback);
                        },
                        // filter out hosts without sufficient resources
                        (hosts, callback) => {
                            filter.by_vacancy(hosts, resources, callback);
                        },
                        // filter out hosts that do not match requested constraints
                        (hosts, callback) => {
                            filter.by_constraints(hosts, callback);
                        }
                    ], (err, hosts) => {
                        if(err) {
                            core.loggers['containership.scheduler'].log('debug', `${err.message} ${containership_container_id}`);
                            return callback();
                        }

                        return callback(null, _.sample(hosts));
                    });
                };

                const filter = {
                    // get hosts running in follower mode
                    by_mode: (filtered_hosts, callback) => {
                        const hosts = _.filter(filtered_hosts, (host) => {
                            return host.mode === 'follower';
                        });

                        if(_.isEmpty(hosts)) {
                            return callback(new Error('No follower nodes detected when deploying'));
                        } else {
                            return callback(null, hosts);
                        }
                    },

                    // filter hosts by tag
                    by_tag: (filtered_hosts, tags, callback) => {
                        tags = _.cloneDeep(tags);

                        delete tags.constraints;
                        delete tags.metadata;

                        tags = flat.flatten(tags);

                        const hosts = _.filter(filtered_hosts, (host) => {
                            let matched = 0;
                            host.tags = flat.flatten(host.tags);
                            _.forEach(tags, (tag_value, tag_name) => {
                                if(host.tags[tag_name] && host.tags[tag_name] === tag_value) {
                                    matched++;
                                }
                            });

                            return matched === _.keys(tags).length;
                        });

                        if(_.isEmpty(hosts)) {
                            return callback(new Error('No follower nodes met the given tag criteria when deploying'));
                        } else {
                            return callback(null, hosts);
                        }
                    },

                    // filter hosts by vacancy
                    by_vacancy: (filtered_hosts, resources, callback) => {
                        const hosts = {};

                        _.forEach(filtered_hosts, (host) => {
                            hosts[host.id] = host;
                            hosts[host.id].available_cpus = host.cpus;
                            hosts[host.id].available_memory = host.memory;
                        });

                        core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, '*', '*'].join(core.constants.myriad.DELIMITER), (err, containers) => {
                            if(err) {
                                return callback(new Error('No follower nodes had sufficient resources when deploying'));
                            }

                            async.each(containers, (container_name, callback) => {
                                core.cluster.myriad.persistence.get(container_name, (err, container) => {
                                    if(err) {
                                        return callback();
                                    }

                                    try{
                                        container = JSON.parse(container);

                                        if(hosts[container.host]) {
                                            hosts[container.host].available_cpus -= parseFloat(container.cpus);
                                            hosts[container.host].available_memory -= _.parseInt(container.memory) * (1024 * 1024);
                                        }
                                    } catch(err) { /* empty */ }

                                    return callback();
                                });
                            }, () => {
                                const vacant_hosts = _.filter(filtered_hosts, (host) => {
                                    const available_cpus = host.available_cpus.toFixed(2);
                                    delete host.available_cpus;

                                    const available_memory = host.available_memory / (1024 * 1024);
                                    delete host.available_memory;

                                    return (available_memory - resources.memory >= 0) && (available_cpus - resources.cpus >= 0);
                                });

                                if(_.isEmpty(vacant_hosts)) {
                                    return callback(new Error('No follower nodes had sufficient resources when deploying'));
                                } else {
                                    return callback(null, vacant_hosts);
                                }
                            });
                        });
                    },

                    // filter hosts by constraints
                    by_constraints: (filtered_hosts, callback) => {
                        constraints.enforce(core, {
                            available_hosts: filtered_hosts,
                            container: container,
                            application_name: application_name
                        }, (err, hosts) => {
                            if(err) {
                                return callback(err);
                            } else if(_.isEmpty(hosts)) {
                                return callback(new Error('Could not meet constraints when deploying'));
                            } else {
                                return callback(null, hosts);
                            }
                        });
                    }
                };

                const get_host_port = (host, callback) => {
                    core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, (err, application_names) => {
                        if(err) {
                            return callback();
                        }

                        async.map(application_names, (application_name, callback) => {
                            application_name = _.last(application_name.split(core.constants.myriad.DELIMITER));
                            core.applications.get_containers(application_name, callback);
                        }, (err, containers) => {
                            if(err) {
                                return callback(err);
                            }

                            containers = _.flatten(containers);

                            const containers_on_host = _.filter(containers, (container) => {
                                return container.host === host.id;
                            });

                            let host_port;
                            const utilized_ports = _.map(containers_on_host, 'host_port');

                            if(container.network_mode === 'host' && container.container_port) {
                                if(_.includes(utilized_ports, container.container_port)) {
                                    return callback(new Error('Requested port already in use'));
                                } else {
                                    return callback(null, {
                                        host: host,
                                        host_port: container.container_port
                                    });
                                }
                            }

                            if(container.host_port) {
                                host_port = _.parseInt(container.host_port);
                            }

                            if(_.isUndefined(host_port)) {
                                // no ports left on host
                                if(containers_on_host.length >= (core.scheduler.options.container.max_port - core.scheduler.options.container.min_port)) {
                                    return callback(new Error('No ports left to allocate on desired host'));
                                }

                                // generate a random host port
                                host_port = _.random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);
                            } else if(!_.includes(utilized_ports, host_port)) {
                                return callback(null, {
                                    host: host,
                                    host_port: host_port
                                });
                            }

                            // host_port taken, attempt to regenerate
                            while(_.includes(utilized_ports, host_port)) {
                                host_port = _.random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);
                            }

                            return callback(null, {
                                host: host,
                                host_port: host_port
                            });
                        });
                    });
                };

                const env_vars = {
                    clear_defaults: () => {
                        _.forEach(container.env_vars, (value, name) => {
                            if(name.indexOf('CS_') === 0) {
                                delete container.env_vars[name];
                            }
                        });
                    },

                    set_defaults: (host, callback) => {
                        _.defaults(container.env_vars, {
                            CS_CONTAINER_ID: container.id,
                            CS_APPLICATION: application_name,
                            CS_CLUSTER_ID: core.cluster_id,
                            CS_FOLLOWER_HOST_ID: host.id,
                            CS_FOLLOWER_HOSTNAME: host.host_name,
                            CS_FOLLOWER_HOST_NAME: host.host_name
                        });

                        async.series([
                            (callback) => {
                                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, (err, applications) => {
                                    if(err) {
                                        return callback(err);
                                    }

                                    async.each(applications, (application_name, callback) => {
                                        core.cluster.myriad.persistence.get(application_name, (err, configuration) => {
                                            if(err) {
                                                return callback(err);
                                            }

                                            try{
                                                configuration = JSON.parse(configuration);
                                            } catch(err) {
                                                return callback(err);
                                            }

                                            application_name = _.last(application_name.split(core.constants.myriad.DELIMITER));

                                            const discovery_port_env_var_name = `CS_DISCOVERY_PORT_${application_name.toUpperCase()}`;
                                            container.env_vars[discovery_port_env_var_name] = configuration.discovery_port;

                                            const application_name_env_var_name = `CS_ADDRESS_${application_name.toUpperCase()}`;
                                            container.env_vars[application_name_env_var_name] = `${application_name}.${core.cluster_id}.containership`;

                                            return callback();
                                        });
                                    }, callback);
                                });
                            },

                            (callback) => {
                                core.cluster.myriad.persistence.keys(core.constants.myriad.VARIABLES, (err, variables) => {
                                    if(err) {
                                        return callback(err);
                                    }

                                    async.each(variables, (variable_name, callback) => {
                                        core.cluster.myriad.persistence.get(variable_name, (err, value) => {
                                            if(err) {
                                                return callback(err);
                                            }

                                            variable_name = _.last(variable_name.split(core.constants.myriad.DELIMITER));
                                            const name = `CS_MYRIAD_${variable_name.toUpperCase()}`;
                                            container.env_vars[name] = value;

                                            return callback();
                                        });
                                    }, callback);
                                });
                            }
                        ], callback);
                    }
                };

                // clear default environment variables
                env_vars.clear_defaults();

                // check if application should be deployed to certain host
                if(container.host) {
                    const hosts = _.keyBy(core.cluster.legiond.get_peers(), 'id');

                    const attributes = core.cluster.legiond.get_attributes();
                    hosts[attributes.id] = attributes;

                    const host = hosts[container.host];

                    if(!host) {
                        return callback();
                    }

                    env_vars.set_defaults(host, () => {
                        return get_host_port(host, callback);
                    });
                } else {
                    find_available_host(container, (err, host) => {
                        if(err && err.should_redeploy_container === false) {
                            return callback(err);
                        } else if(_.isUndefined(host)) {
                            return callback();
                        } else {
                            env_vars.set_defaults(host, () => {
                                return get_host_port(host, callback);
                            });
                        }
                    });
                }
            }
        },

        application: {

            get_loadbalancer_port: (port, callback) => {
                if(port) {
                    port = _.parseInt(port);
                }

                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, (err, application_names) => {
                    if(err) {
                        return callback(err);
                    }

                    async.map(application_names, (application_name, callback) => {
                        core.cluster.myriad.persistence.get(application_name, (err, application) => {
                            if(err) {
                                return callback(err);
                            }

                            try{
                                application = JSON.parse(application);
                            } catch(err) {
                                return callback(err);
                            }

                            return callback(null, application.discovery_port);
                        });
                    }, (err, ports) => {
                        if(err) {
                            return callback(err);
                        }

                        if(!port) {
                            // no ports left to allocate
                            if(ports.length >= (core.scheduler.options.loadbalancer.max_port - core.scheduler.options.loadbalancer.min_port)) {
                                return callback(new Error('No discovery ports left to allocate'));
                            }

                            // allocate a random port
                            port = _.random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);
                        } else if(port < core.scheduler.options.loadbalancer.min_port || port > core.scheduler.options.loadbalancer.max_port) {
                            return callback(new Error('Invalid port specified'));
                        }

                        // port taken, attempt to regenerate
                        while(_.includes(ports, port)) {
                            port = _.random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);
                        }

                        return callback(null, port);
                    });
                });
            },

            remove_containers: (application_name, num_containers, callback) => {
                const all_containers = [];

                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, application_name, '*'].join(core.constants.myriad.DELIMITER), (err, containers) => {
                    if(err) {
                        return callback(err);
                    }

                    async.each(containers, (container_id, callback) => {
                        core.cluster.myriad.persistence.get(container_id, (err, container) => {
                            if(err) {
                                return callback(err);
                            }

                            try{
                                container = JSON.parse(container);
                                all_containers.push(container);
                            } catch(err) { /* empty */ }

                            return callback();
                        });
                    }, (err) => {
                        if(err) {
                            return callback(err);
                        }

                        if(all_containers.length <= num_containers) {
                            return callback(null, _.map(all_containers, 'id'));
                        } else {
                            core.cluster.myriad.persistence.get([core.constants.myriad.APPLICATION_PREFIX, application_name].join(core.constants.myriad.DELIMITER), (err, application) => {
                                if(err) {
                                    return callback(err);
                                }

                                try{
                                    application = JSON.parse(application);
                                } catch(err) {
                                    return callback(err);
                                }

                                constraints.enforce_remove(core, {
                                    num_containers: num_containers,
                                    total_containers: all_containers.length,
                                    available_containers: all_containers,
                                    application: application
                                }, (err, available_containers) => {
                                    if(err) {
                                        return callback(err);
                                    } else {
                                        if(available_containers.length > num_containers) {
                                            let available_containers_by_status = _.defaults(_.groupBy(available_containers, 'status'), {
                                                loaded: [],
                                                loading: [],
                                                unloaded: []
                                            });

                                            available_containers = _.take(_.flatten([
                                                available_containers_by_status.unloaded,
                                                available_containers_by_status.loading,
                                                available_containers_by_status.loaded
                                            ]), num_containers);
                                        }

                                        return callback(err, _.map(available_containers, 'id'));
                                    }
                                });
                            });
                        }
                    });
                });
            },

            stop_all_health_checks: () => {
                _.forEach(health_check_workers, (child) => {
                    child.kill();
                });

                health_check_workers = {};
            },

            start_all_health_checks: (callback) => {
                return core.cluster.myriad.persistence.keys(constants.myriad.APPLICATIONS, (err, applications) => {
                    if(err) {
                        return callback && callback(err);
                    }

                    async.each(applications, (application, callback) => {
                        core.scheduler.leader.application.start_health_check(_.last(application.split(constants.myriad.DELIMITER)), (err) => {
                            return callback && callback(err);
                        });
                    }, callback);
                });
            },

            stop_health_check: (application_name) => {
                const child = health_check_workers[application_name];

                if(child) {
                    delete health_check_workers[application_name];
                    child.kill();
                }
            },

            start_health_check: (application_name, callback) => {
                if(!core.cluster.praetor.is_controlling_leader()) {
                    return callback && callback(new Error('You can not start health checks on a host that is not the controlling leader'));
                }

                const attributes = core.cluster.legiond.get_attributes();
                const legiond_address = attributes.address;
                const legiond_scope = core.options['legiond-scope'];
                const myriad_host = legiond_address[legiond_scope] || '127.0.0.1';

                if(!health_check_workers[application_name]) {
                    const child = child_process.fork(`${__dirname}/health_checks`, {
                        env : {
                            APPLICATION_NAME : application_name,
                            MYRIAD_HOST : myriad_host,
                            MYRIAD_PORT : process.env.MYRIAD_PORT || 2666,
                            LEGIOND_SCOPE: legiond_scope
                        },
                        execArgv: []
                    });

                    health_check_workers[application_name] = child;

                    child.on('message', (message) => {
                        if(message.type === 'log') {
                            core.loggers['containership.scheduler'].log(message.log.level, message.log.message);
                        } else if(message.type === 'health_check' && message.status === 'failed') {
                            return core.applications.redeploy_container(application_name, message.container, (err) => {
                                if(err) {
                                    core.loggers['containership.scheduler'].log('error', err.message);
                                }
                            });
                        }
                    });

                    return callback && callback();
                } else {
                    return callback && callback(new Error(`Health checks already running for application: ${application_name}`));
                }
            }
        }
    };

};
