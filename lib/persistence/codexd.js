'use strict';

const _ = require('lodash');
const async = require('async');

module.exports = {
    register_middleware(core, engine) {
        _.forEach(this.middleware, (middleware) => {
            middleware(core, engine);
        });
    },

    middleware: {
        pre_start: (core, engine) => {
            engine.addPreStartMiddleware('codexd', (options, callback) => {
                const application_name = options.application_name;
                const container = _.omit(options, [
                    'application_name',
                    'start_args'
                ]);

                const volumes = _.groupBy(container.volumes, (volume) => {
                    return volume.host;
                });

                const codexd_volumes_metadata = {};

                async.each(volumes.undefined || [], (volume, callback) => {
                    const uuid = core.cluster.codexd.create_uuid();

                    core.cluster.codexd.create_volume({
                        id: uuid
                    }, (err) => {
                        if(err) {
                            return callback();
                        }

                        codexd_volumes_metadata[uuid] = volume.container;
                        return callback();
                    });
                }, (err) => {
                    if(err) {
                        return callback(err);
                    }

                    const codexd_volumes_inverse = _.invert(codexd_volumes_metadata);
                    container.volumes = _.map(container.volumes, (volume) => {
                        if(volume.host === undefined && codexd_volumes_inverse[volume.container]) {
                            return {
                                host: [core.cluster.codexd.options.base_path, codexd_volumes_inverse[volume.container]].join('/'),
                                container: volume.container
                            };
                        } else {
                            return volume;
                        }
                    });

                    if(!container.tags.metadata) {
                        container.tags.metadata = {};
                    }

                    if(!container.tags.metadata.codexd) {
                        container.tags.metadata.codexd = {};
                    }

                    if(!container.tags.metadata.codexd.volumes) {
                        container.tags.metadata.codexd.volumes = codexd_volumes_metadata;
                        _.merge(options, container);
                        core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER), JSON.stringify(container), () => {
                            return callback();
                        });
                    } else{
                        const codexd_errs = [];

                        const host_volumes = core.cluster.codexd.get_volumes();

                        async.each(_.keys(container.tags.metadata.codexd.volumes), (uuid, callback) => {
                            if(!host_volumes[uuid]) {
                                core.cluster.codexd.get_snapshot(uuid, (err) => {
                                    if(err && err.name !== 'ESNAPSHOTNOTFOUND') {
                                        codexd_errs.push(err);
                                    } else if(!err) {
                                        core.cluster.legiond.send({
                                            event: core.cluster.codexd.constants.REMOVE_SNAPSHOT,
                                            data: {
                                                id: uuid
                                            }
                                        });
                                    }

                                    return callback();
                                });
                            } else {
                                return callback();
                            }
                        }, () => {
                            container.tags.metadata.codexd.volumes = _.defaults(container.tags.metadata.codexd.volumes, codexd_volumes_metadata);
                            _.merge(options, container);
                            core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER), JSON.stringify(container), () => {
                                if(_.isEmpty(codexd_errs)) {
                                    return callback();
                                } else {
                                    return callback(_.head(codexd_errs));
                                }
                            });
                        });
                    }
                });
            });
        }
    }
};
