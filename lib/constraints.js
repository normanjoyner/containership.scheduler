var _ = require("lodash");
var async = require("async");

module.exports = {

    get_constraints: function(container){
        if(_.has(container.tags, "constraints"))
            return _.intersection(_.keys(container.tags.constraints), _.keys(this.constraints));
        else
            return [];
    },

    enforce: function(core, options){
        var available_hosts = options.available_hosts;

        _.each(this.get_constraints(options.container), function(constraint){
            available_hosts = this.constraints[constraint].deploy(core, options, available_hosts);
        }, this);

        return available_hosts;
    },

    list: {

        per_host: {
            harmonize: function(core, options, fn){
                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, "*"].join("::"), function(err, containers){
                    var peers = _.groupBy(core.cluster.legiond.get_peers(), "mode");

                    var followers = peers.follower || [];

                    var difference = options.application.tags.constraints.per_host * followers.length - containers.length;

                    if(difference > 0){
                        core.cluster.myriad.persistence.get([core.constants.myriad.APPLICATION_PREFIX, options.application_name].join("::"), function(err, application){
                            async.times(difference, function(fn){
                                var config = _.defaults({ status: "unloaded", random_host_port: true }, _.cloneDeep(application));
                                delete config.id;

                                core.applications.deploy_container(options.application_name, config, fn);
                            }, fn);
                        });
                    }
                    else if(difference < 0){
                        difference = Math.abs(difference);

                        var unloaded_containers = _.where(containers, {
                            status: "unloaded"
                        });

                        async.times(difference, function(fn){
                            var container = _.sample(unloaded_containers);

                            if(_.isUndefined(container))
                                var container = _.sample(containers);

                            if(!_.isUndefined(container))
                                core.applications.remove_container(options.application_name, container.id, fn);
                            else
                                return fn();
                        }, fn);
                    }
                    else
                        return fn();
                });
            },

            deploy: function(core, options, available_hosts){
                var hosts = [];

                var desired = options.container.tags.constraints.per_host;

                core.applications.get_containers(options.application_name, function(err, containers){
                    _.each(available_hosts, function(host){
                        var host_containers = _.filter(containers, function(container){
                            return container.host = host.id;
                        });

                        host_containers = _.groupBy(host_containers, "application");

                        if(!_.has(host_conatiners, options.application_name) || host_containers[options.application_name].length < desired)
                            hosts.push(host);
                    });

                    return hosts;
                });
            }
        }

    }

}
