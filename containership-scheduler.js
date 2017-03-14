'use strict';

const _ = require('lodash');

// define ContainershipScheduler
class ContainershipScheduler {
    load_options(options) {
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

    load_core(core) {
        this.core = core;
        this.core.logger.register('containership.scheduler');

        if(core.options.mode === 'leader') {
            this.leader = require('./lib/leader')(core);
        } else {
            this.follower = require('./lib/follower')(core);
        }
    }

    harmonize() {
        if(this.leader) {
            this.leader.container.harmonize(() => {
                this.core.loggers['containership.scheduler'].log('info', 'Completed application harmonization');

                this.harmonizer = setInterval(() => {
                    this.leader.container.harmonize(() => {
                        this.core.loggers['containership.scheduler'].log('info', 'Completed application harmonization');
                    });
                }, this.options['harmonization-interval']);
            });
        } else {
            this.core.loggers['containership.scheduler'].log('error', 'Harmonization can only be run on leader nodes!');
        }
    }

    deharmonize() {
        clearInterval(this.harmonizer);
    }
}

module.exports = ContainershipScheduler;
