var _ = require("lodash");

module.exports = {

    get_constraints: function(container){
        if(_.has(container.tags, "constraints"))
            return _.intersection(_.keys(container.tags.constraints), _.keys(this.constraints));
        else
            return [];
    },

    enforce: function(core, options){
        var available_hosts = options.available_hosts;

        if(_.isEmpty(options.container.tags))
            var c = core.applications.list[options.application_name];
        else
            var c = options.container;

        _.each(this.get_constraints(c), function(constraint){
            available_hosts = this.constraints[constraint](core, options, available_hosts);
        }, this);

        return available_hosts;
    },

    constraints: {

        per_host: function(core, options, available_hosts){
            var hosts = [];
            if(_.isEmpty(options.container.tags))
                var desired = core.applications.list[options.application_name].tags.constraints.per_host;
            else
                var desired = options.container.tags.constraints.per_host;

            _.each(available_hosts, function(host){
                var containers = _.groupBy(core.hosts.get_containers(host.id), "application");
                if(!_.has(containers, options.application_name) || containers[options.application_name].length < desired){
                    hosts.push(host);
                    return false;
                }
                else
                    return true;
            });

            return hosts;
        }

    }

}
