var ContainershipScheduler = require([__dirname, "containership-scheduler"].join("/"));
var pkg = require([__dirname, "package"].join("/"));
var options = require([__dirname, "options"].join("/"));

// instantiate new Containership Scheduler
module.exports = function(){
    var scheduler = new ContainershipScheduler();
    scheduler.version = pkg.version;
    scheduler.options = options;
    return scheduler;
}
