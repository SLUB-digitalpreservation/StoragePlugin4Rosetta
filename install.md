## Installation

Both, compilation and deployment have to take place on the target system due to installation 
requirements 
### Prerequisites
* Java OpenJDK 1.8
* git
* gcc, make, ...
* To compile the plugin, ensure that the NFSStoragePlugin.jar is located at /exlibris/dps/d4_1/system.dir/bundled_plugins/NFSStoragePlugin.jar
* Rosetta PluginSDK has to be found at /exlibris/dps/d4_1/system.dir/dps-sdk-${ROSETTAVERSION}/lib/
* Check makefile for appropriate directories, file locations and *Rosetta version*

### Compile 

* Clone Repository from git: git clone https://github.com/SLUB-digitalpreservation/StoragePlugin4Rosetta.git
* cd to newly created directory: cd StoragePlugin4Rosetta
* Check Makefile! vi Makefile
* run make

### Deploy

* create dir /operational_shared/plugins/custom if not exists
* create dir /operational_shared/plugins/custom/deploy if not exists
* copy SLUBStoragePlugin.jar to /operational_shared/plugins/custom/
* copy SLUBStoragePlugin.jar to /operational_shared/plugins/custom/deploy 
* use dps user to restart Rosetta: su dps; dps_restart
