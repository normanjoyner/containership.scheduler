var _ = require("lodash");
var async = require("async");
var mkdirp = require("mkdirp");
var forever = require("forever-monitor");
var Docker = require("dockerode");
var docker = new Docker({socketPath: "/var/run/docker.sock"});

module.exports = {

    initialize: function(core){
        var self = this;
        this.core = core;

        docker.version(function(err, info){
            if(_.isNull(err)){
                var attributes = self.core.cluster.legiond.get_attributes();
                var tags = attributes.tags;

                if(!_.has(tags.metadata, "engines"))
                    tags.engines = {};

                tags.metadata.engines.docker = {
                    client_version: info.Version,
                    api_version: info.ApiVersion,
                    go_version: info.GoVersion
                }

                self.core.cluster.legiond.set_attributes(tags);
            }
        });

        this.start_args = {};

        this.middleware = {
            pre_pull: {},
            pre_start: {}
        }

        self.reconcile();
    },

    // add pre start middleware
    add_pre_start_middleware: function(name, fn){
        this.middleware.pre_start[name] = fn;
    },

    // add pre pull middleware
    add_pre_pull_middleware: function(name, fn){
        this.middleware.pre_pull[name] = fn;
    },

    // set standard start arguments
    set_start_arguments: function(key, value){
        this.start_args[key] = value;
    },

    // start container
    start: function(options){
        var self = this;

        var node = this.core.cluster.legiond.get_attributes();

        var pre_pull_middleware = _.map(self.middleware.pre_pull, function(middleware, middleware_name){
            return function(fn){
                middleware(options, fn);
            }
        });

        options.cpus = Math.floor(1024 * options.cpus);

        async.parallel(pre_pull_middleware, function(err){
            if(err){
                self.core.cluster.legiond.send("container.unloaded", {
                    id: options.id,
                    application_name: options.application_name,
                    host: node.id,
                    error: err
                });
            }
            else{
                commands.pull(options.image, options.auth || [{}], function(err){
                    if(err){
                        var error = new Error("Docker pull failed");
                        error.details = err.message;

                        self.core.cluster.legiond.send("container.unloaded", {
                            id: options.id,
                            application_name: options.application_name,
                            host: node.id,
                            error: error
                        });
                        self.core.loggers["containership.scheduler"].log("warn", ["Failed to pull", options.image].join(" "));
                        self.core.loggers["containership.scheduler"].log("errror", err.message);
                    }
                    options.start_args = self.start_args;

                    var pre_start_middleware = _.map(self.middleware.pre_start, function(middleware, middleware_name){
                        return function(fn){
                            middleware(options, fn);
                        }
                    });

                    async.parallel(pre_start_middleware, function(err){
                        if(err){
                            self.core.cluster.legiond.send("container.unloaded", {
                                id: options.id,
                                application_name: options.application_name,
                                host: node.id,
                                error: err
                            });
                        }
                        else
                            commands.start(self.core, options);
                    });
                });
            }
        });
    },

    // stop container
    stop: function(options){
        commands.stop(this.core, options);
    },

    // get containeres
    get_containers: function(){
        return containers;
    },

    // reconcile containers
    reconcile: function(){
        var self = this;

        var node = this.core.cluster.legiond.get_attributes();

        docker.listContainers({all: true}, function(err, all_containers){
            if(_.isNull(all_containers))
                all_containers = [];

            async.each(all_containers, function(container, fn){
                docker.getContainer(container.Id).inspect(function(err, info){
                    var name = container.Names[0].slice(1);
                    var parts = name.split("-");

                    var container_port;
                    var host_port;

                    if(parts.length > 0){
                        var application_name = _.initial(parts).join("-");
                        var container_id = _.last(parts);

                        if(info.HostConfig.NetworkMode == "bridge"){
                            _.each(info.HostConfig.PortBindings, function(bindings, binding){
                                host_port = bindings[0].HostPort;
                                binding = binding.split("/")[0];
                                if(binding != host_port)
                                    container_port = binding;
                            });
                        }
                        else{
                            _.each(info.Config.Env, function(env_var){
                                if(env_var.indexOf("PORT=") == 0)
                                    host_port = env_var.split("=")[1];
                            });
                        }

                        if(!info.State.Running && !info.State.Restarting){
                            docker.getContainer(container.Id).remove(function(err){
                                if(_.isNull(err))
                                    self.core.loggers["containership.scheduler"].log("verbose", ["Cleaned up dead", application_name, "container:", container_id].join(" "));
                            });
                        }
                        else if(!_.has(containers, container_id)){
                            var args = [
                                "wait",
                                "--container", container.Id
                            ]

                            var base_log_dir = [self.core.scheduler.options["container-log-dir"], application_name, container_id].join("/");

                            containers[container_id] = new(forever.Monitor)([__dirname, "..", "executors", "docker"].join("/"), {
                                silent: false,
                                max: 1,
                                minUptime: 5000,
                                args: args,
                                uid: container_id,
                                killSignal: "SIGTERM",
                                outFile: [base_log_dir, "stdout"].join("/"),
                                errFile: [base_log_dir, "stderr"].join("/")
                            });

                            containers[container_id].on("start", function(){
                                self.core.loggers["containership.scheduler"].log("info", ["Reconciled running", application_name, "container:", container_id].join(" "));
                                commands.update_container({
                                    core: self.core,
                                    application_name: application_name,
                                    container_id: container_id,
                                    status: "loaded",
                                    host: node.id,
                                    start_time: new Date(info.Created).valueOf(),
                                    host_port: host_port,
                                    container_port: container_port,
                                    engine: "docker"
                                }, function(err){
                                    if(err){
                                        docker.getContainer(container.Id).remove(function(err){
                                            if(_.isNull(err))
                                                self.core.loggers["containership.scheduler"].log("verbose", ["Cleaned up dead", application_name, "container:", container_id].join(" "));
                                        });
                                    }
                                });
                            });

                            containers[container_id].on("exit", function(){
                                self.core.loggers["containership.scheduler"].log("info", ["Unloading", application_name, "container:", container_id].join(" "));
                                commands.update_container({
                                    application_name: application_name,
                                    container_id: container_id,
                                    status: "unloaded",
                                    host: null,
                                    start_time: null,
                                    core: self.core
                                }, function(err){
                                    if(err){
                                        core.loggers["containership.scheduler"].log("warn", ["Failed to stop", options.application, "container:", options.id].join(" "));
                                        core.loggers["containership.scheduler"].log("warn", err.message);
                                    }
                                });
                            });

                            containers[container_id].start();
                        }
                        else{
                            self.core.loggers["containership.scheduler"].log("info", ["Reconciled running", application_name, "container:", container_id].join(" "));
                            commands.update_container({
                                core: self.core,
                                application_name: application_name,
                                container_id: container_id,
                                status: "loaded",
                                host: node.id,
                                start_time: new Date(info.Created).valueOf(),
                                host_port: host_port,
                                container_port: container_port,
                                engine: "docker"
                            }, function(err){
                                if(err){
                                    docker.getContainer(container.Id).remove(function(err){
                                        if(_.isNull(err))
                                            self.core.loggers["containership.scheduler"].log("verbose", ["Cleaned up dead", application_name, "container:", container_id].join(" "));
                                    });
                                }
                            });
                        }

                        return fn();
                    }
                });
            }, function(){});
        });
    }
}

var containers = {};

var commands = {

    // pull docker image
    pull: function(image, auth, fn){
        async.eachSeries(auth, function(authentication, fn){
            docker.pull(image, authentication, function(err, stream){
                if(err)
                    return fn(err);

                docker.modem.followProgress(stream, onFinished, onProgress);

                function onFinished(err, output){
                    return fn(err);
                }
                function onProgress(){}
            });
        }, function(){
            return fn();
        });
    },

    // start process with forever
    start: function(core, options){
        var self = this;

        var args = [
            "start",
            "--Cmd", options.command,
            "--CpuShares", options.cpus,
            "--Memory", options.memory,
            "--Image", options.image,
            "--name", [options.application_name, options.id].join("-"),
            "--host-port", options.host_port,
            "--HostConfig.NetworkMode", options.network_mode,
            "--HostConfig.Privileged", options.privileged
        ]

        if(!_.isEmpty(options.volumes)){
            args.push("--HostConfig.Binds");

            var volumes = _.map(options.volumes, function(volume){
                return [volume.host, volume.container].join(":");
            });

            args.push(volumes.join(" "));
        }

        _.each(options.start_args, function(val, key){
            args.push(["--", key].join(""));
            if(_.isFunction(val))
                args.push(val(options));
            else
                args.push(val);
        });

        _.each(options.env_vars, function(val, key){
            args.push("--Env");
            val = val.toString();

            if(val.indexOf("$") == 0 && _.has(options.env_vars, val.substring(1, val.length)))
                val = options.env_vars[val.substring(1, val.length)];

            args.push([key, val].join("="));
        });

        if(_.has(options, "container_port") && !_.isNull(options.container_port)){
            args.push("--Env");
            args.push(["PORT", options.container_port].join("="));
            args.push("--Env");
            args.push(["PORT0", options.container_port].join("="));
            args.push("--container-port");
            args.push(options.container_port);
        }
        else{
            args.push("--Env");
            args.push(["PORT", options.host_port].join("="));
            args.push("--Env");
            args.push(["PORT0", options.host_port].join("="));
            args.push("--container-port");
            args.push(options.host_port);
        }

        var base_log_dir = [core.scheduler.options["container-log-dir"], options.application_name, options.id].join("/");

        mkdirp(base_log_dir, function(){
            containers[options.id] = new(forever.Monitor)([__dirname, "..", "executors", options.engine].join("/"), {
                silent: false,
                max: 1,
                minUptime: 5000,
                args: args,
                uid: options.id,
                killSignal: "SIGTERM",
                outFile: [base_log_dir, "stdout"].join("/"),
                errFile: [base_log_dir, "stderr"].join("/")
            });

            containers[options.id].on("start", function(){
                core.loggers["containership.scheduler"].log("info", ["Loading", options.application_name, "container:", options.id].join(" "));

                self.update_container({
                    application_name: options.application_name,
                    container_id: options.id,
                    status: "loaded",
		    core: core
                }, function(err){
                    if(err){
                        core.loggers["containership.scheduler"].log("warn", ["Failed to load", options.application_name, "container:", options.id].join(" "));
                        core.loggers["containership.scheduler"].log("warn", err.message);
                    }
                });
            });

            containers[options.id].on("exit", function(){
                core.loggers["containership.scheduler"].log("info", ["Unloading", options.application_name, "container:", options.id].join(" "));
                core.loggers["containership.scheduler"].log("verbose", [options.id, "exited after", ((new Date() - options.start_time) / 1000), "seconds"].join(" "));

                self.update_container({
                    application_name: options.application_name,
                    container_id: options.id,
                    status: "unloaded",
                    host: null,
                    start_time: null,
                    core: core
                }, function(err){
                    if(err){
                        core.loggers["containership.scheduler"].log("warn", ["Failed to stop", options.application_name, "container:", options.id].join(" "));
                        core.loggers["containership.scheduler"].log("warn", err.message);
                    }
                });
            });

            containers[options.id].start();
        });
    },

    // stop process
    stop: function(core, options){
        this.delete_container({
            application_name: options.application,
            container_id: options.container_id,
            core: core
        }, function(err){
            if(err){
                core.loggers["containership.scheduler"].log("warn", ["Failed to delete", options.application, "container:", options.container_id].join(" "));
                core.loggers["containership.scheduler"].log("warn", err.message);
            }

            containers[options.container_id].stop();

            if(_.contains(containers[options.container_id].args, "wait")){
                docker.listContainers({all: true}, function(err, all_containers){
                    if(_.isNull(all_containers))
                        all_containers = [];

                    _.each(all_containers, function(container){
                        docker.getContainer(container.Id).inspect(function(err, info){
                            var name = container.Names[0].slice(1);
                            if(name == [options.application, options.container_id].join("-"))
                                docker.getContainer(container.Id).kill(function(err, data){});
                        });
                    });
                });
            }
        });
    },

    // update container status
    update_container: function(options, fn){
        options.core.cluster.myriad.persistence.get([options.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, options.container_id].join("::"), { local: false }, function(err, container){
            if(err)
                return fn(err);

            try{
                container = JSON.parse(container);
                container.status = options.status;

                if(_.has(options, "host"))
                    container.host = options.host;

                if(_.has(options, "start_time"))
                    container.start_time = options.start_time;

                if(_.has(options, "engine"))
                    container.engine = options.engine;

                if(_.has(options, "host_port"))
                    container.host_port = options.host_port;

                if(_.has(options, "container_port"))
                    container.container_port = options.container_port;

                if(options.status == "unloaded" && container.random_host_port)
                    container.host_port = null;

                options.core.cluster.myriad.persistence.set([options.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, options.container_id].join("::"), JSON.stringify(container), fn);
            }
            catch(err){
                return fn(err);
            }
        });
    },

    // delete container
    delete_container: function(options, fn){
        options.core.cluster.myriad.persistence.delete([options.core.constants.myriad.CONTAINERS_PREFIX, options.application_name, options.container_id].join("::"), fn);
    }
}
