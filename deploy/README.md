# RumbleDB on AWS EMR Query Execution Tutorial

In order to execute the HEP ADL benchmark queries with RumbleDB on AWS EMR one needs to follow the next steps:

1. Run the `./deploy.sh` script
    * The 1st parameter of the script is the number of instances to be spawned
    * The 2nd parameter is the type of the instances
    * The 3rd parameter is the port offset when doing port forwarding.  
1. Execute the `./run_experiments.sh` script (feel free to change what queries this runs; by default all queries shown in the paper are executed)
1. The query execution data is dumped by default in `./experiments/rumbledb/experiment_yyyy-MM-dd-HH-mm-ss`. In this directory you will find:
    * A set of folders having the name `run_yyyy-MM-dd-HH-mm-ss.SSS` each containing detailed information on each of the queries executed during the experiment. 
    * The `summary.json` file which is obtained via the `summarize_experiment.py` source. This aggregates all the individual query statistics into one `.json` type file. 
1. Study or plot the above-mentioned  `summary.json` file to see the query execution times
    * For the query execution time, one can scan the `runtime` entries
    * For the number of read bytes, one can scan the `inputBytes` or `bytesRead` entry 
1. Terminate the cluster by running the `terminate.sh` script. Note that at one point you will be prompted to specify the new cluster state. You can press `^C` to exit this state.

## Preparing the data for plotting

To prepare the summary data for plotting, you'll have to use the following command in order to copy the `summary.json` file to the plotting directory. You can do this via the following command:

```
lastexp=$(ls -d experiments/rumbledb/experiment_* | sort | tail -n1)
cat "$lastexp/summary.json" >> "$(git rev-parse --show-toplevel)"/plots/rumble.json
```

Note that `lastexp=$(ls -d experiments/rumbledb/experiment_* | sort | tail -n1)` will fetch the path to the latest experiment directory. If you wish to pick another experiment directory make sure to change the path in `cat` accordingly.