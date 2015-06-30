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
                var engine_metadata = {
                    engines: {
                        docker: {
                            client_version: info.Version,
                            api_version: info.ApiVersion,
                            go_version: info.GoVersion
                        }
                    }
                }
                self.core.cluster.legiond.set_attributes(engine_metadata);
            }
        });

        this.start_args = {};
        this.middleware = {
            pre_start: {}
        }
        self.reconcile();
    },

    // add pre start middleware
    add_pre_start_middleware: function(name, fn){
        this.middleware.pre_start[name] = fn;
    },

    // set standard start arguments
    set_start_arguments: function(key, value){
        this.start_args[key] = value;
    },

    // start container
    start: function(options){
        var self = this;
        var node = this.core.cluster.legiond.get_attributes();

        options.cpus = Math.floor(1024 * options.cpus);
        commands.pull(options.image, function(err){
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
    },

    // stop container
    stop: function(id){
        commands.stop(id);
    },

    // get containeres
    get_containers: function(){
        return containers;
    },

    // reconcile containers
    reconcile: function(leader){
        var self = this;

        var node = this.core.cluster.legiond.get_attributes();
        var applications = {};

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

                        if(!_.has(applications, application_name))
                            applications[application_name] = [];

                        applications[application_name].push({
                            id: container_id,
                            host: node.id,
                            start_time: new Date(info.Created).valueOf(),
                            host_port: host_port,
                            container_port: container_port,
                            engine: "docker"
                        });

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
                                silent: self.core.options["log-level"] != "silly",
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
                            });

                            containers[container_id].on("exit", function(){
                                self.core.loggers["containership.scheduler"].log("info", ["Unloading", application_name, "container:", container_id].join(" "));
                                self.core.cluster.legiond.send("container.unloaded", {
                                    id: container_id,
                                    application_name: application_name,
                                    host: self.core.cluster.legiond.get_attributes().id
                                });
                            });

                            containers[container_id].start();
                        }
                        else
                            self.core.loggers["containership.scheduler"].log("info", ["Reconciled running", application_name, "container:", container_id].join(" "));

                        return fn();
                    }
                });
            }, function(){
                if(_.isUndefined(leader))
                    var leader = self.core.cluster.praetor.get_controlling_leader();

                if(!_.isUndefined(leader))
                    self.core.cluster.legiond.send("applications.reconciled", applications, leader);
            });
        });
    }
}

var containers = {};

var commands = {

    // pull docker image
    pull: function(image, fn){
        docker.pull(image, function(err, stream){
            if(err)
                return fn(err);
            else{
                stream.on("data", function(){});

                stream.on("error", function(err){
                    return fn(err);
                });

                stream.on("end", function(){
                    return fn();
                });
            }
        });
    },

    // start process with forever
    start: function(core, options){
        var args = [
            "start",
            "--Cmd", options.command,
            "--CpuShares", options.cpus,
            "--Memory", options.memory,
            "--Image", options.image,
            "--name", [options.application_name, options.id].join("-"),
            "--host-port", options.host_port,
            "--HostConfig.NetworkMode", options.network_mode
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
                silent: core.options["log-level"] != "silly",
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
                core.cluster.legiond.send("container.loaded", {
                    id: options.id,
                    application_name: options.application_name,
                    host: core.cluster.legiond.get_attributes().id
                });
            });

            containers[options.id].on("exit", function(){
                core.loggers["containership.scheduler"].log("info", ["Unloading", options.application_name, "container:", options.id].join(" "));
                core.loggers["containership.scheduler"].log("verbose", [options.id, "exited after", ((new Date() - options.start_time) / 1000), "seconds"].join(" "));
                core.cluster.legiond.send("container.unloaded", {
                    id: options.id,
                    application_name: options.application_name,
                    host: core.cluster.legiond.get_attributes().id
                });
            });

            containers[options.id].start();
        });
    },

    // stop process
    stop: function(id){
        containers[id].stop();

        if(_.contains(containers[id].args, "wait")){
            docker.listContainers({all: true}, function(err, all_containers){
                if(_.isNull(all_containers))
                    all_containers = [];

                _.each(all_containers, function(container){
                    docker.getContainer(container.Id).inspect(function(err, info){
                        var name = container.Names[0].slice(1);
                        var parts = name.split("-");
                        if(_.last(parts) == id)
                            docker.getContainer(container.Id).kill(function(err, data){});
                    });
                });
            });
        }
    }

}

