'use strict';

const DockerEngine = require('./docker-engine');

module.exports = {
    docker: (core) => new DockerEngine(core)
};
