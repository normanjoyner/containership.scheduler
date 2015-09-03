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
                    async.map(containers, function(container_name, fn){
                        core.cluster.myriad.persistence.get(container_name, function(err, container){
                            if(err)
                                return fn();
                            else{
                                try{
                                    container = JSON.parse(container);
                                    return fn(err, container);
                                }
                                catch(e){
                                    return fn();
                                }
                            }
                        });
                    }, function(err, containers){
                        containers = _.compact(containers);
                        containers = _.groupBy(containers, "host");
                        var container_hosts = _.keys(containers);

                        var peers = _.groupBy(core.cluster.legiond.get_peers(), "mode");
                        var followers = peers.follower || [];

                        async.each(followers, function(follower, fn){
                            var id = follower.id;
                            container_hosts = _.without(container_hosts, id);
                            if(_.has(containers, id)){
                                var running = containers[id].length;
                                if(running == options.application.tags.constraints.per_host)
                                    return fn();
                                else if(running < options.application.tags.constraints.per_host){
                                    var difference = options.application.tags.constraints.per_host - running;
                                    async.times(difference, function(index, fn){
                                        core.applications.deploy_container(options.application_name, { tags: { host: id } }, function(){
                                            return fn();
                                        });
                                    }, fn);
                                }
                                else{
                                    var difference = running - options.application.tags.constraints.per_host;
                                    async.times(difference, function(index, fn){
                                        core.applications.remove_container(options.application_name, containers[id][index].id, fn);
                                    }, fn);
                                }
                            }
                            else{
                                var difference = options.application.tags.constraints.per_host;
                                async.times(difference, function(index, fn){
                                    core.applications.deploy_container(options.application_name, { tags: { host: id } }, function(){
                                        return fn();
                                    });
                                }, fn);
                            }
                        }, function(){
                            async.each(container_hosts, function(id, fn){
                                async.each(containers[id], function(container, fn){
                                    core.applications.remove_container(options.application_name, container.id, fn);
                                }, function(){
                                    return fn();
                                });
                            }, function(){
                                return fn();
                            });
                        });
                    });
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
