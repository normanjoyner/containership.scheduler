var _ = require("lodash");
var async = require("async");
var constraints = require([__dirname, "constraints"].join("/"));
var core;

module.exports = function(core){

    return {

        container: {

            harmonize: function(fn){
                var self = this;
                var peers = _.where(core.cluster.legiond.get_peers(), function(peer){
                    return peer.mode == "follower";
                });

                async.each(_.values(core.applications.list), function(application, next_app){
                    if(_.contains(constraints.get_constraints(application), "per_host")){
                        var difference = application.tags.constraints.per_host * peers.length - _.keys(application.containers).length;
                        if(difference > 0){
                            _.times(difference, function(){
                                var config = _.defaults({ status: "unloaded", random_host_port: false }, _.cloneDeep(application));
                                delete config.id;

                                var new_container = core.applications.list[application.id].add_container(config);
                            });
                        }
                        else if(difference < 0){
                            difference = Math.abs(difference);
                            var unloaded_containers = _.where(application.serialize().containers, {
                                status: "unloaded"
                            });
                            _.times(difference, function(){
                                var container = _.sample(unloaded_containers);
                                if(!_.isUndefined(container))
                                    core.applications.list[application.id].remove_container(container.id);
                            });
                        }
                    }

                    _.each(application.serialize().containers, function(container){
                        if(container.status == "loaded" && _.isNull(container.host))
                            core.applications.list[application.id].unload_container(container.id);
                    });

                    var unloaded_containers = _.where(application.serialize().containers, {
                        status: "unloaded"
                    });

                    if(unloaded_containers.length > 0){
                        core.loggers["containership.scheduler"].log("info", ["Attempting to replace", unloaded_containers.length, "containers for", application.id].join(" "));

                        async.eachSeries(unloaded_containers, function(container, next_container){
                            container = _.omit(container, ["start_time", "host"]);

                            core.applications.deploy_container(application.id, container, function(err){
                                if(_.isUndefined(err)){
                                    core.applications.sync(function(){
                                        return next_container();
                                    });
                                }
                                else{
                                    core.loggers["containership.scheduler"].log("error", err.message);
                                    return next_container();
                                }
                            });
                        }, function(){
                            var remaining_containers = _.where(application.serialize().containers, {
                                status: "unloaded"
                            });

                            if(remaining_containers.length == 0)
                                core.loggers["containership.scheduler"].log("info", ["Successfully replaced all containers for", application.id].join(" "));
                            else
                                core.loggers["containership.scheduler"].log("warn", ["Failed to replace", remaining_containers.length, "containers for", application.id].join(" "));

                            return next_app();
                        });
                    }
                    else
                        return next_app();
                }, function(){
                    return fn();
                });
            },

            deploy: function(application_name, container, fn){
                var place_container = function(container){
                    var success = true;
                    var cid = [application_name, container.id].join("-");

                    // get available hosts
                    var available_hosts = _.values(core.hosts.get_all());

                    // find hosts running in follower mode
                    available_hosts = filter.by_mode(available_hosts);

                    if(_.isEmpty(available_hosts)){
                        success = false;
                        core.loggers["containership.scheduler"].log("debug", ["No follower nodes detected when deploying", cid].join(" "));
                    }

                    // find hosts with specified tags
                    if(_.keys(container.tags).length > 0)
                        available_hosts = filter.by_tag(available_hosts, container.tags);

                    if(_.isEmpty(available_hosts) && success){
                        success = false;
                        core.loggers["containership.scheduler"].log("debug", ["No follower nodes met the given tag criteria when deploying", cid].join(" "));
                    }

                    // find hosts with available resources
                    var resources = {
                        cpus: parseFloat(container.cpus),
                        memory: _.parseInt(container.memory)
                    }

                    available_hosts = filter.by_vacancy(available_hosts, resources);

                    if(_.isEmpty(available_hosts) && success){
                        success = false;
                        core.loggers["containership.scheduler"].log("debug", ["No follower nodes had sufficient resources when deploying", cid].join(" "));
                    }

                    // find hosts that meet constraints
                    available_hosts = filter.by_constraints(available_hosts);

                    if(_.isEmpty(available_hosts) && success){
                        success = false;
                        core.loggers["containership.scheduler"].log("debug", ["Could not meet constraints when deploying", cid].join(" "));
                    }


                    // grab a random host
                    return _.sample(available_hosts);
                }

                var filter = {
                    // get hosts running in follower mode
                    by_mode: function(filtered_hosts){
                        return _.filter(filtered_hosts, function(host){
                            return host.mode == "follower";
                        });
                    },

                    // filter hosts by tag
                    by_tag: function(filtered_hosts, tags){
                        delete tags.constraints;
                        delete tags.metadata;

                        return _.filter(filtered_hosts, function(host){
                            var matched = 0;
                            _.each(tags, function(value, tag){
                                if(_.has(host.tags, tag) && host.tags[tag] == value)
                                    matched++;
                            });

                            return matched == _.keys(tags).length;
                        });
                    },

                    // filter hosts by vacancy
                    by_vacancy: function(filtered_hosts, resources){
                        var overhead = 32;
                        return _.filter(filtered_hosts, function(host){
                            var available_cpus = host.cpus;
                            var available_memory = host.memory / (1024 * 1024);
                            _.each(core.hosts.get_containers(host.id), function(container){
                                available_memory -= _.parseInt(container.memory) + overhead;
                                available_cpus -= parseFloat(container.cpus);
                            });

                            available_memory -= overhead;

                            return (available_memory - resources.memory >= 0) && (available_cpus - resources.cpus >= 0);
                        });
                    },

                    // filter hosts by constraints
                    by_constraints: function(filtered_hosts){
                        return constraints.enforce(core, {
                            available_hosts: filtered_hosts,
                            container: container,
                            application_name: application_name
                        });
                    }
                }

                // check if application should be deployed to certain host
                if(_.has(container, "host") && container.host != null)
                    var host = core.hosts.get(container.host);

                // check if host is tagged
                else if(_.has(container.tags, "host"))
                    var host = core.hosts.get(container.tags.host);

                // get a random host
                else{
                    var application = core.applications.list[application_name].serialize();
                    var new_container = _.defaults(_.cloneDeep(container), _.omit(application, ["id", "containers"]));
                    var host = place_container(new_container);
                }

                var containers = core.hosts.get_containers(container.host);
                var port;
                var ports = _.map(containers, function(host_container){
                    if(host_container.id != container.id)
                        return host_container.host_port;
                    else
                        return 0;
                });

                if(_.has(container, "host_port") && !_.isNull(container.host_port) && !_.isUndefined(container.host_port))
                    port = _.parseInt(container.host_port);

                if(containers.length >= (core.scheduler.options.container.max_port - core.scheduler.options.container.min_port))
                    return fn();

                if(_.isUndefined(port))
                    port = _.random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);
                else if(_.contains(ports, port) || port < core.scheduler.options.container.min_port || port > core.scheduler.options.container.max_port)
                    return fn();
                else
                    return fn(host, port);

                while(_.contains(ports, port))
                    port = _.random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);

                return fn(host, port);
            },

            remove: function(application_name, container_id){
                var containers = {};
                _.each(core.applications.get_containers(application_name), function(container){
                    containers[container.id] = container;
                });

                if(_.isUndefined(container_id) && _.keys(containers).length > 0){
                    var sorted_containers = _.sortBy(_.values(containers), "start_time");
                    return sorted_containers[0];
                }
                else if(_.has(containers, container_id))
                    return containers[container_id];
                else if(!_.has(containers, container_id))
                    return;
            }

        },

        application: {

            get_loadbalancer_port: function(port){
                if(!_.isUndefined(port))
                    port = _.parseInt(port);

                var ports = _.map(core.applications.list, function(application, application_name){
                    return application.serialize().port;
                });

                if(_.isUndefined(port))
                    port = _.random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);
                else if(_.contains(ports, port) || port < core.scheduler.options.loadbalancer.min_port || port > core.scheduler.options.loadbalancer.max_port)
                    return;

                if(_.keys(core.applications.list).length >= (core.scheduler.options.loadbalancer.max_port - core.scheduler.options.loadbalancer.min_port))
                    return;

                while(_.contains(ports, port))
                    port = _.random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);

                return port;
            }

        }

    }

}
