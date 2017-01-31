module.exports = {

    "harmonization-interval": {
        help: "Interval on which container harmonization occurs",
        metavar: "INTERVAL",
        default: 15000
    },

    "scheduler-available-memory" : {
        help: 'Amount of memory the containership scheduler can allocate to running containers (MiB)',
        metavar: 'SCHEDULER_AVAILABLE_MEMORY'
    },

     "scheduler-available-cpus" : {
         help: 'Number of CPUs the containership scheduler can allocate to running containers',
         metavar: 'SCHEDULER_AVAILABLE_CPUS'
     }

}
