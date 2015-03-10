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
            start: function(container){
                if(_.has(container, "engine") && _.has(engines, container.engine))
                    engines[container.engine].start(container);
                else{
                    var error = new Error("Unsupported engine provided");
                    error.details = ["Engine", container.engine, "not found!"].join(" ");

                    self.core.cluster.legiond.send("container.unloaded", {
                        id: container.id,
                        application_name: container.application_name,
                        host: self.core.cluster.legiond.get_attributes().id,
                        error: error
                    });

                    core.loggers["containership.scheduler"].log("error", error.details);
                }
            },

            stop: function(container){
                engines[container.engine].stop(container.id);
            },

            reconcile: function(leader){
                _.each(engines, function(engine, engine_name){
                    engines[engine_name].reconcile(leader);
                });
            },

            set_start_arguments: function(engine_name, key, value){
                if(_.has(engines, engine_name))
                    engines[engine_name].set_start_arguments(key, value);
            }
        }

    }

}
