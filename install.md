## Installation
### Prerequisites
* Java SDK 1.8
* git
* gcc, make, ...
* To compile the plugin, ensure that the SDK and NFSStoragePlugin.jar is located in "/exlibris" as described in the Makefile.
* Check makefile for appropriate directories, file locations and Rosetta version

### Compile 

* Clone Repository from git: git clone https://github.com/SLUB-digitalpreservation/StoragePlugin4Rosetta.git
* cd to newly created directory: cd StoragePlugin4Rosetta
* Check Makefile! vi Makefile
* run make

### Deploy

* copy SLUBStoragePlugin.jar to /operational_shared/plugins/custom/ on target
  system
* copy SLUBStoragePlugin.jar to /operational_shared/plugins/custom/deploy on
  target system
* use dps user to restart Rosetta: su dps; dps_restart
