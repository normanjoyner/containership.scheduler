module.exports = {

    "container-log-dir": {
        help: "Container log directory",
        metavar: "PATH",
        default: ["", "var", "log", "containership", "applications"].join("/")
    },

    "harmonization-interval": {
        help: "Interval on which container harmonization occurs",
        metavar: "INTERVAL",
        default: 15000
    }

}
