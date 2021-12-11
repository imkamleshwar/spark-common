# spark-common
Spark Initialisation and Argument Parsing


### How to run [SDC2Job](src/main/scala/jobs/SDC2Job.scala) in local:
* Pass below configuration as program argument:

        -DjobConfigs=jobsConf/SDC2Job.conf -Dspark.master=local[2]
