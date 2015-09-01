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
                        var followers_by_id = _.indexBy(followers, "id");

                        async.each(containers, function(container_name, fn){
                            core.cluster.myriad.persistence.get(container_name, function(err, container){
                                if(err)
                                    return fn();

                                if(_.has(followers_by_id, container.host))
                                    delete followers_by_id[container.host];

                                return fn();
                            });
                        }, function(){
                            async.each(_.values(followers_by_id), function(host, fn){
                                core.applications.deploy_container(options.application_name, { tags: { host: host.id } }, function(){
                                        return fn();
                                });
                            }, fn);
                        });
                    }
                    else if(difference < 0){
                        difference = Math.abs(difference);

                        var all_containers = [];

                        async.each(containers, function(container_name, fn){
                       	    core.cluster.myriad.persistence.get(container_name, function(err, container){
                                if(err)
                                    return fn();

                                try{
                                    container = JSON.parse(container);
                                    all_containers.push(container);
                                }
                                catch(err){};
                                return fn();
                            });
                        }, function(){
                            var unloaded_containers = _.where(all_containers, {
                                status: "unloaded"
                            });

                            async.times(Math.abs(difference), function(index, fn){
                                var container = _.sample(unloaded_containers);

                                if(_.isUndefined(container)){
                                    var container = _.sample(all_containers);
                                    if(!_.isUndefined(container))
                                        all_containers = _.without(all_containers, container);
                                }
                                else
                                    unloaded_containers = _.without(unloaded_containers, container);

                                if(!_.isUndefined(container))
                                    core.applications.remove_container(options.application_name, container.id, fn);
                                else
                                    return fn();
                            }, fn);
                        });
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
