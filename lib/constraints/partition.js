'use strict';

const Constraint = require('./constraint');

const _ = require('lodash');
const flat = require('flat');

class PartitionConstraint extends Constraint {
    constructor(core) {
        super('partition', core);
    }

    deploy(options, callback) {
        const peers = this.core.cluster.legiond.get_peers();
        peers.push(this.core.cluster.legiond.get_attributes());

        const peers_by_id = _.keyBy(peers, 'id');

        const partition = options.container.tags.constraints.partition;

        const available_hosts = _.map(options.available_hosts, (host) => {
            host.tags = flat.flatten(host.tags);
            return host;
        });

        // gather all possible tag values
        const all_tag_values = _.groupBy(available_hosts, (host) => {
            return host.tags[partition];
        });

        const partition_mapping = {};

        // initialize possible tag values to 0
        _.forEach(_.uniq(_.keys(all_tag_values)), (tag_value) => {
            partition_mapping[tag_value] = 0;
        });

        this.core.applications.get_containers(options.application_name, (err, containers) => {
            if(err) {
                return callback(err);
            }

            _.forEach(containers, (container) => {
                if(container.status !== 'unloaded') {
                    const peer_tags = flat.flatten(peers_by_id[container.host].tags);
                    if(partition_mapping[peer_tags[partition]] !== undefined) {
                        partition_mapping[peer_tags[partition]]++;
                    }
                }
            });

            let min;
            let partition_val;

            _.forEach(partition_mapping, (num_containers, partition_value) => {
                if(_.isUndefined(min) || num_containers < min) {
                    partition_val = partition_value;
                    min = num_containers;
                }
            });

            options.available_hosts = all_tag_values[partition_val];
            return callback();
        });
    }

    remove(options, callback) {
        const available_containers = options.available_containers;
        options.available_containers = [];

        const partition = options.application.tags.constraints.partition;

        this.core.applications.get_containers(options.application.id, (err, containers) => {
            if(err) {
                return callback(err, available_containers);
            }

            _.forEach(containers, (container) => {
                container.tags = flat.flatten(container.tags);
            });

            const available_containers_by_partition = _.groupBy(containers, (container) => {
                return container.tags[partition];
            });

            let available_values = _.values(available_containers_by_partition);

            while(available_values.length > 0) {
                available_values = _.sortBy(available_values, (value) => {
                    return -value.length;
                });

                options.available_containers.push(_.first(available_values).shift());

                if(_.isEmpty(_.first(available_values))) {
                    available_values.shift();
                }
            }

            return callback();
        });
    }
}

module.exports = PartitionConstraint;
