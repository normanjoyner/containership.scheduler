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
                    engines[container.engine].start(container, core.cluster.legiond.get_attributes());
                else
                    core.loggers["containership.scheduler"].log("error", ["Engine", container.engine, "not found!"].join(" "));
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
