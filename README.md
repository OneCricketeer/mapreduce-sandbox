mapreduce-sandbox
===

Sandbox for running Hadoop MapReduce programs

To run a specific task, do `./gradlew :<task>`

The following classes are available

|Main class|Input File|Gradle task|
|---|---|---|
|`CustomerDriver`|`inFiles/customer`|`runCustomerDriver`|
|`DateGrouperDriver`|`inFiles/dates`|`runDateGrouperDriver`|
|`StoreSumDriver`|`inFiles/stores`|`runStoreSumDriver`|
|`TempMinMaxAvgDriver`|`inFiles/temp`|`runTempMinMaxAvgDriver`|
|`WeatherMultiDriver`|`inFiles/weather`|`runWeatherMultiDriver`|

Each task will write into (and overwrite) the `outFiles/` directory.
