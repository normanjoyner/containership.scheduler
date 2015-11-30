var fs = require("fs");
var _ = require("lodash");
var async = require("async");

module.exports = function(core){

    var engines = {};
    var available_engines = fs.readdirSync([__dirname, "..", "engines"].join("/"));
    _.each(available_engines, function(engine){
        var engine_name = engine.split(".")[0];
        engines[engine_name] = require([__dirname, "..", "engines", engine].join("/"));
        engines[engine_name].initialize(core);
    });


    // register codexd middleware
    _.each(engines, function(engine, engine_name){
        engine.add_pre_start_middleware("codexd", function(options, fn){

            var application_name = options.application_name;
            var container = _.omit(options, ["application_name", "start_args"]);

            var volumes = _.groupBy(container.volumes, function(volume){
                return volume.host;
            });

            var codexd_volumes_metadata = {};

            async.each(volumes.undefined || [], function(volume, fn){
                var uuid = core.cluster.codexd.create_uuid();

                core.cluster.codexd.create_volume({
                    id: uuid
                }, function(err){
                    if(err)
                        return fn();

                    codexd_volumes_metadata[uuid] = volume.container;
                    return fn();
                });
            }, function(err){
                var codexd_volumes_inverse = _.invert(codexd_volumes_metadata);
                container.volumes = _.map(container.volumes, function(volume){
                    if(volume.host == undefined && _.has(codexd_volumes_inverse, volume.container)){
                        return {
                            host: [core.cluster.codexd.options.base_path, codexd_volumes_inverse[volume.container]].join("/"),
                            container: volume.container
                        }
                    }
                    else
                        return volume;
                });

                if(!_.has(container.tags, "metadata"))
                    container.tags.metadata = {};

                if(!_.has(container.tags.metadata, "codexd"))
                    container.tags.metadata.codexd = {};

                if(!_.has(container.tags.metadata.codexd, "volumes")){
                    container.tags.metadata.codexd.volumes = codexd_volumes_metadata;
                    _.merge(options, container);
                    core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER), JSON.stringify(container), function(err){
                        return fn();
                    });
                }
                else{
                    var codexd_errs = [];

                    var host_volumes = core.cluster.codexd.get_volumes();

                    async.each(_.keys(container.tags.metadata.codexd.volumes), function(uuid, fn){
                        if(!_.has(host_volumes, uuid)){
                            core.cluster.codexd.get_snapshot(uuid, function(err){
                                if(err)
                                    codexd_errs.push(err);
                                else{
                                    core.legiond.send({
                                        event: core.cluster.codexd.constants.REMOVE_SNAPSHOT,
                                        data: {
                                          id: uuid
                                        }
                                    });
                                }
                                return fn();
                            });
                        }
                        else
                            return fn();
                    }, function(){
                        container.tags.metadata.codexd.volumes = _.defaults(container.tags.metadata.codexd.volumes, codexd_volumes_metadata);
                        _.merge(options, container);
                        core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER), JSON.stringify(container), function(err){
                            if(_.isEmpty(codexd_errs))
                                return fn();
                            else
                                return fn(_.first(codexd_errs));
                        });
                    });
                }
            });
        });
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
