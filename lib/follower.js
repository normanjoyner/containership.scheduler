var fs = require("fs");
var _ = require("lodash");

module.exports = function(core){

    var engines = {};
    var available_engines = fs.readdirSync([__dirname, "..", "engines"].join("/"));
    _.each(available_engines, function(engine){
        var engine_name = engine.split(".")[0];
        engines[engine_name] = require([__dirname, "..", "engines", engine].join("/"));
        engines[engine_name].initialize(core);
    });

    return {

        container: {
            start: function(configuration){
                var container = configuration.container;
                container.application_name = configuration.application;

                if(_.has(container, "engine") && _.has(engines, container.engine))
                    engines[container.engine].start(container);
                else{
                    var error = new Error("Unsupported engine provided");
                    error.details = ["Engine", container.engine, "not found!"].join(" ");
                    core.loggers["containership.scheduler"].log("error", error.details);
                    containers.status = "unloaded";
                    core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, container.application_name, container.id].join("::"), function(err){
                        if(err){
                            var error = new Error("Unable to set container status");
                            error.details = ["Could not set status for", container.application_name, "container", container.id, "to 'unloaded'"].join(" ");
                            core.loggers["containership.scheduler"].log("error", error.details);
                        }
                    });
                }
            },

            stop: function(configuration){
                engines[configuration.engine].stop(configuration);
            },

            reconcile: function(leader){
                _.each(engines, function(engine, engine_name){
                    engines[engine_name].reconcile(leader);
                });
            },

            add_pre_start_middleware: function(engine_name, middleware_name, fn){
                if(_.has(engines, engine_name))
                    engines[engine_name].add_pre_start_middleware(middleware_name, fn);
            },

            add_pre_pull_middleware: function(engine_name, middleware_name, fn){
                if(_.has(engines, engine_name))
                    engines[engine_name].add_pre_pull_middleware(middleware_name, fn);
            },

            set_start_arguments: function(engine_name, key, value){
                if(_.has(engines, engine_name))
                    engines[engine_name].set_start_arguments(key, value);
            }
        }

    }

}
