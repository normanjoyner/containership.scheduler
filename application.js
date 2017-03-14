'use strict';

const ContainershipScheduler = require('./containership-scheduler');
const options = require('./options');
const pkg = require('./package');

// instantiate new Containership Scheduler
module.exports = function() {
    const scheduler = new ContainershipScheduler();
    scheduler.version = pkg.version;
    scheduler.options = options;
    return scheduler;
};
