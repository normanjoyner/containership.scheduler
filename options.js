'use strict';

module.exports = {

    'harmonization-interval': {
        help: 'Interval on which container harmonization occurs',
        metavar: 'INTERVAL',
        default: 15000
    },

    'scheduler-available-memory' : {
        help: 'Amount of memory the containership scheduler can allocate to running containers (MiB)',
        metavar: 'SCHEDULER_AVAILABLE_MEMORY'
    },

    'scheduler-available-cpus' : {
        help: 'Number of CPUs the containership scheduler can allocate to running containers',
        metavar: 'SCHEDULER_AVAILABLE_CPUS'
    },

    'max-docker-container-age': {
        help: 'Amount of time before a killed / stopped docker container is cleaned up on disk(hours)',
        metavar: 'HOURS',
        default: 6
    },

    'max-docker-image-age': {
        help: 'Amount of time before an unused docker image is cleaned up on disk(hours)',
        metavar: 'HOURS',
        default: 6
    }

};
