var _ = require("lodash");
var async = require("async");
var flat = require("flat");
var constraints = require([__dirname, "constraints"].join("/"));

module.exports = function(core){

    return {

        container: {

            harmonize: function(fn){
                var self = this;

                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, applications){
                    if(err)
                        return fn();

                    async.each(applications, function(application_name, fn){
                        core.cluster.myriad.persistence.get(application_name, function(err, application){
                            application_name = _.last(application_name.split("::"));

                            try{
                                application = JSON.parse(application);
                            }
                            catch(err){
                                return fn(err);
                            }

                            async.each(_.keys(application.tags.constraints), function(constraint, fn){

                                constraints.list[constraint].harmonize(core, {
                                    application: application,
                                    application_name: application_name
                                }, fn);
                            }, function(){
                                core.applications.get_containers(application_name, function(err, containers){
                                    async.each(containers, function(container, fn){
                                        if(container.status == "loaded" && _.isNull(container.host))
                                            core.applications.unload_containers(application_name, fn);
                                        else
                                            return fn();
                                    }, function(){
                                        containers = _.groupBy(containers, "status");
                                        if(_.has(containers, "unloaded")){
                                            core.loggers["containership.scheduler"].log("info", ["Attempting to replace", containers.unloaded.length, "containers for", application.id].join(" "));
                                            async.each(containers.unloaded, function(container, fn){
                                                container = _.omit(container, ["start_time", "host"]);

                                                core.applications.deploy_container(application.id, container, function(err){
                                                    if(err)
                                                        core.loggers["containership.scheduler"].log("error", err.message);

                                                    return fn();
                                                });
                                            }, fn);
                                        }
                                        else
                                            return fn();
                                    });
                                });
                            });
                        });
                    }, fn);
                });
            },

            deploy: function(application_name, container, fn){
                var place_container = function(container, fn){
                    var cid = [application_name, container.id].join("-");

                    // get available hosts
                    var available_hosts = core.cluster.legiond.get_peers();
                    available_hosts.push(core.cluster.legiond.get_attributes());

                    var resources = {
                        cpus: parseFloat(container.cpus),
                        memory: _.parseInt(container.memory)
                    }

                    async.waterfall([
                        function(fn){
                            filter.by_mode(available_hosts, fn);
                        },
                        function(hosts, fn){
                            if(_.keys(container.tags).length > 0)
                                filter.by_tag(hosts, _.cloneDeep(container.tags), fn);
                            else
                                return fn(null, hosts);
                        },
                        function(hosts, fn){
                            filter.by_vacancy(hosts, resources, fn);
                        },
                        function(hosts, fn){
                            filter.by_constraints(hosts, fn);
                        }
                    ], function(err, hosts){
                        if(err){
                            core.loggers["containership.scheduler"].log("debug", [err.message, cid].join(" "));
                            return fn();
                        }
                        else
                            return fn(_.sample(hosts));
                    });
                }

                var filter = {
                    // get hosts running in follower mode
                    by_mode: function(filtered_hosts, fn){
                        var hosts = _.filter(filtered_hosts, function(host){
                            return host.mode == "follower";
                        });

                        if(_.isEmpty(hosts))
                            return fn(new Error("No follower nodes detected when deploying"));
                        else
                            return fn(null, hosts);
                    },

                    // filter hosts by tag
                    by_tag: function(filtered_hosts, tags, fn){
                        delete tags.constraints;
                        delete tags.metadata;
                        tags = flat.flatten(tags);

                        var hosts = _.filter(filtered_hosts, function(host){
                            var matched = 0;
                            host.tags = flat.flatten(host.tags);
                            _.each(tags, function(tag_value, tag_name){
                                if(_.has(host.tags, tag_name) && host.tags[tag_name] == tag_value)
                                    matched++;
                            });

                            return matched == _.keys(tags).length;
                        });

                        if(_.isEmpty(hosts))
                            return fn(new Error("No follower nodes met the given tag criteria when deploying"));
                        else
                            return fn(null, hosts);
                    },

                    // filter hosts by vacancy
                    by_vacancy: function(filtered_hosts, resources, fn){
                        var overhead = 32;
                        var hosts = {};

                        _.each(filtered_hosts, function(host){
                            hosts[host.id] = host;
                            hosts[host.id].available_cpus = host.cpus;
                            hosts[host.id].available_memory = host.memory;
                        });

                        core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, "*", "*"].join("::"), function(err, containers){
                            if(err)
                                return fn(new Error("No follower nodes had sufficient resources when deploying"));

                            async.each(containers, function(container_name, fn){
                                core.cluster.myriad.persistence.get(container_name, function(err, container){
                                    if(err)
                                        return fn();

                                    try{
                                        container = JSON.parse(container);
                                        if(_.contains(_.keys(hosts), container.host)){
                                            hosts[container.host].available_cpus -= parseFloat(container.cpus);
                                            hosts[container.host].available_memory -= _.parseInt(container.memory) + overhead;
                                        }
                                    }
                                    catch(err){}
                                    return fn();
                                });
                            }, function(){
                                var hosts = _.filter(filtered_hosts, function(host){
                                    var available_cpus = host.available_cpus.toFixed(2);
                                    delete host.available_cpus;
                                    var available_memory = host.available_memory / (1024 * 1024);
                                    delete host.available_memory;
                                    available_memory -= overhead;
                                    return (available_memory - resources.memory >= 0) && (available_cpus - resources.cpus >= 0);
                                });

                                if(_.isEmpty(hosts))
                                    return fn(new Error("No follower nodes had sufficient resources when deploying"));
                                else
                                    return fn(null, hosts);
                            });
                        });
                    },

                    // filter hosts by constraints
                    by_constraints: function(filtered_hosts, fn){
                        constraints.enforce(core, {
                            available_hosts: filtered_hosts,
                            container: container,
                            application_name: application_name
                        }, function(err, hosts){
                            if(_.isEmpty(hosts))
                                return fn(new Error("Could not meet constraints when deploying"));
                            else
                                return fn(null, hosts);
                        });
                    }
                }

                var get_host_port = function(host, fn){
                    core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, application_names){
                        if(err)
                            return fn();

                        async.map(application_names, function(application_name, fn){
                            application_name = _.last(application_name.split("::"));
                            core.applications.get_containers(application_name, fn);
                        }, function(err, containers){
                            if(err)
                                return fn(err);

                            containers = _.flatten(containers);

                            var containers_on_host = _.filter(containers, function(container){
                                return container.host == host.id;
                            });

                            var host_port;
                            var utilized_ports = _.pluck(containers_on_host, "host_port");

                            if(container.network_mode == "host" && _.has(container, "container_port")){
                                if(_.contains(utilized_ports, container.container_port))
                                    return fn(new Error("Requested port already in use"));
                                else{
                                    return fn(null, {
                                        host: host,
                                        host_port: container.container_port
                                    });
                                }
                            }

                            if(_.has(container, "host_port") && !_.isNull(container.host_port) && !_.isUndefined(container.host_port))
                                host_port = _.parseInt(container.host_port);

                            if(_.isUndefined(host_port)){
                                if(containers_on_host.length >= (core.scheduler.options.container.max_port - core.scheduler.options.container.min_port))
                                    return fn(new Error("No ports left to allocate on desired host"));

                                host_port = _.random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);
                            }
                            else if(host_port < core.scheduler.options.container.min_port || host_port > core.scheduler.options.container.max_port)
                                return fn(new Error("Invalid port specified"));
                            else if(!_.contains(utilized_ports, host_port)){
                                return fn(null, {
                                    host: host,
                                    host_port: host_port
                                });
                            }
                            while(_.contains(utilized_ports, host_port))
                                host_port = _.random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);

                            return fn(null, {
                                host: host,
                                host_port: host_port
                            });
                        });
                    });
                }

                // check if application should be deployed to certain host
                if(container.host != null){
                    var host = core.hosts.get(container.host);
                    return get_host_port(host, fn);
                }

                // check if host is tagged
                else if(_.has(container.tags, "host")){
                    var peers = core.cluster.legiond.get_peers();
                    peers = _.indexBy(peers, "id");
                    var host = peers[container.tags.host];
                    return get_host_port(host, fn);
                }

                // get a random host
                else{
                    place_container(container, function(host){
                        // short circuit if no host is found
                        if(_.isUndefined(host))
                            return fn(new Error("No host available"));
                        else
                            return get_host_port(host, fn);
                    });
                }
            }
        },

        application: {

            get_loadbalancer_port: function(port, fn){
                if(!_.isUndefined(port))
                    port = _.parseInt(port);

                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, application_names){
                    async.map(application_names, function(application_name, fn){
                        core.cluster.myriad.persistence.get(application_name, function(err, application){
                            try{
                                return fn(null, JSON.parse(application).discovery_port)
                            }
                            catch(err){
                                return fn(err);
                            }
                        });
                    }, function(err, ports){
                        if(_.isUndefined(port)){
                            if(ports.length >= (core.scheduler.options.loadbalancer.max_port - core.scheduler.options.loadbalancer.min_port))
                                return fn(new Error("No ports left to allocate"));

                            port = _.random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);
                        }
                        else if(port < core.scheduler.options.loadbalancer.min_port || port > core.scheduler.options.loadbalancer.max_port)
                            return fn(new Error("Invalid port specified"));

                        while(_.contains(ports, port))
                            port = _.random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);

                        return fn(null, port);
                    });
                });

            }

        }

    }

}
