var _ = require("lodash");
var leader = require([__dirname, "lib", "leader"].join("/"));
var follower = require([__dirname, "lib", "follower"].join("/"));

// define ContainershipScheduler
function ContainershipScheduler(core){}

ContainershipScheduler.prototype.load_options = function(options){
    this.options = _.defaults(options || {}, {
        loadbalancer: {
            min_port: 10000,
            max_port: 11023
        },
        container: {
            min_port: 11024,
            max_port: 22047
        }
    });
}

ContainershipScheduler.prototype.load_core = function(core){
    this.core = core;
    this.core.logger.register("containership.scheduler");

    if(core.options.mode == "leader")
        this.leader = leader(core);
    else
        this.follower = follower(core);
}

ContainershipScheduler.prototype.harmonize = function(){
    var self = this;
    self.leader.container.harmonize(function(){
        self.core.loggers["containership.scheduler"].log("info", "Completed application harmonization");

        this.harmonizer = setInterval(function(){
            self.leader.container.harmonize(function(){
                self.core.loggers["containership.scheduler"].log("info", "Completed application harmonization");
            });
        }, self.options["harmonization-interval"]);
    });
}

ContainershipScheduler.prototype.deharmonize = function(){
    clearInterval(this.harmonizer);
}

module.exports = ContainershipScheduler;
