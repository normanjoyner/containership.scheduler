'use strict';

const Constraint = require('./constraint');

const _ = require('lodash');
const async = require('async');

class PerHostConstraint extends Constraint {
    constructor(core) {
        super('per_host', core);
    }

    harmonize(options, callback) {
        this.core.cluster.myriad.persistence.keys([this.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, '*'].join(this.core.constants.myriad.DELIMITER), (err, containers) => {
            if(err) {
                return callback(err);
            }

            async.map(containers, (container_name, callback) => {
                this.core.cluster.myriad.persistence.get(container_name, (err, container) => {
                    if(err) {
                        return callback(err);
                    } else {
                        try {
                            container = JSON.parse(container);
                        } catch(err) {
                            return callback(err);
                        }

                        return callback(null, container);
                    }
                });
            }, (err, containers) => {
                if(err) {
                    return callback(err);
                }

                containers = _.compact(containers);

                // group containers by host
                const containers_by_host = _.groupBy(containers, 'host');
                let container_hosts = _.keys(containers_by_host);

                // enumerate all follower nodes
                const peers = this.core.cluster.legiond.get_peers();
                peers.push(this.core.cluster.legiond.get_attributes());

                const peers_by_mode = _.groupBy(peers, 'mode');
                const followers = peers_by_mode.follower || [];

                async.each(followers, (follower, callback) => {
                    const host_id = follower.id;

                    container_hosts = _.without(container_hosts, host_id);

                    // containers for the application already running on specified host
                    if(containers_by_host[host_id]) {
                        const containers_running_on_host = containers_by_host[host_id].length;

                        // we're already running the correct number of containers for the application on this host
                        if(containers_running_on_host === options.application.tags.constraints.per_host) {
                            return callback();

                        // we're not running enough containers for the application on this host
                        } else if(containers_running_on_host < options.application.tags.constraints.per_host) {
                            const difference = options.application.tags.constraints.per_host - containers_running_on_host;

                            // deploy containers
                            async.times(difference, (index, callback) => {
                                this.core.applications.deploy_container(options.application_name, { tags: { host: host_id } }, () => {
                                    return callback();
                                });
                            }, callback);

                        // we're running too many containers for the application on this host
                        } else {
                            const difference = containers_running_on_host - options.application.tags.constraints.per_host;

                            // remove containers
                            async.times(difference, (index, callback) => {
                                this.core.applications.remove_container(options.application_name, containers_by_host[host_id][index].id, callback);
                            }, callback);
                        }

                    // no containers for the application running yet
                    } else {
                        async.times(options.application.tags.constraints.per_host, (index, callback) => {
                            this.core.applications.deploy_container(options.application_name, { tags: { host: host_id } }, () => {
                                return callback();
                            });
                        }, callback);
                    }
                }, () => {
                    // clean up application containers on invalid hosts
                    async.each(container_hosts, (id, callback) => {
                        async.each(containers_by_host[id], (container, callback) => {
                            this.core.applications.remove_container(options.application_name, container.id, callback);
                        }, () => {
                            return callback();
                        });
                    }, () => {
                        return callback();
                    });
                });
            });
        });
    }

    deploy(options, callback) {
        const hosts = [];

        const desired_per_host = options.container.tags.constraints.per_host;

        this.core.applications.get_containers(options.application_name, (err, containers) => {
            if(err) {
                return callback(err);
            }

            _.forEach(options.available_hosts, (host) => {
                let host_containers = _.filter(containers, (container) => {
                    return container.host === host.id;
                });

                host_containers = _.groupBy(host_containers, 'application');

                if(!host_containers[options.application_name] || host_containers[options.application_name].length < desired_per_host) {
                    hosts.push(host);
                }
            });

            options.available_hosts = hosts;
            return callback();
        });
    }

}

module.exports = PerHostConstraint;
