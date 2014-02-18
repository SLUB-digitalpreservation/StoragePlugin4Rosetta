#!/bin/bash
# Andreas Romeyke, SLUB Dresden
# erzeugt Submission-Application, die vorbereitete Verzeichnisse per Java SDK
# von ExLibris an Rosetta übergibt.

# Pfad zu Java 6
JAVAPATH=$(wildcard /usr/lib/jvm/java-1.6.0-openjdk-*/bin/)

# Verwendete Rosetta-Version
ROSETTAVERSION=3.2.2

# Pfad zum Rosetta-SDK
ROSETTASDK=/exlibris/dps/d4_1/system.dir/dps-sdk-${ROSETTAVERSION}/lib/
# Pfad zum Rosetta-SDK, Deposit-Module
ROSETTASDKDEPOSIT=${ROSETTASDK}/../dps-sdk-projects/dps-sdk-deposit/lib
ROSETTASDKPLUGINS=${ROSETTASDK}/../../bundled_plugins/


# classpath
JUNITCLASSPATH=/usr/share/java/junit4.jar
#SOURCESCLASSPATH=org/slub/rosetta/dps/repository/plugin/storage/nfs
CLASSPATH=./java:${ROSETTASDKDEPOSIT}/../src/:${ROSETTASDKDEPOSIT}/xmlbeans-2.3.0.jar:${ROSETTASDKDEPOSIT}/dps-sdk-${ROSETTAVERSION}.jar:${ROSETTASDKDEPOSIT}/log4j-1.2.14.jar:${ROSETTASDKPLUGINS}/NFSStoragePlugin.jar

# sources

#SOURCES=java/org/slub/rosetta/dps/repository/plugin/storage/nfs/SLUBStoragePlugin.java\
#	java/org/slub/rosetta/dps/repository/plugin/storage/nfs/testSLUBStoragePlugin.java
SOURCES=java/org/slub/rosetta/dps/repository/plugin/storage/nfs/SLUBStoragePlugin.java
OBJS=$(SOURCES:.java=.class)


all: SLUBStoragePlugin.jar

help:
	@echo "erzeugt Storage-Plugin für Rosetta von Exlibris"
	@echo ""
	@echo "Das Argument 'clean' löscht temporäre Dateien, 'help' gibt diese Hilfe aus und"
	@echo "'compile' erzeugt ein JAR-File und ein Bash-Script welches das Java-Programm"
	@echo "aufruft."

jarclean:
	@rm -Rf build

test:   $(OBJS) 
	java -cp ${CLASSPATH}:$(JUNITCLASSPATH) org.junit.runner.JUnitCore org.slub.rosetta.dps.repository.plugin.storage.nfs.testSLUBStoragePlugin

clean: jarclean
	@rm -Rf doc/
	find ./java/org/ -name "*.class" -exec rm -f \{\} \;
	@rm -Rf SLUBStoragePlugin.jar

distclean: clean
	find ./ -name "*~" -exec rm -f \{\} \;
	@rm -Rf null

SLUBStoragePlugin.jar: $(OBJS)
	@mkdir build;
	@cp -r PLUGIN-INF/ build/
	@cp -r META-INF/ build/
	@cd java; find ./ -name "*.class" -print -exec cp --parents -r \{\} $(PWD)/build \; ; cd ..
	@cp /exlibris/dps/d4_1/system.dir/bundled_plugins/NFSStoragePlugin.jar build/
	#cd build; ${JAVAPATH}/jar xfz /exlibris/dps/d4_1/system.dir/bundled_plugins/NFSStoragePlugin.jar
	@cd build; ${JAVAPATH}/jar cfvM ../$@ ./* ; cd ..

%.class: %.java
	${JAVAPATH}/javac -classpath ${CLASSPATH}:${JUNITCLASSPATH} -Xlint:deprecation $< 

doc: $(SOURCES)
	javadoc -d doc/ $^

check_prerequisites:
	@echo -n "### Checking java path: $(JAVAPATH) ...."
	@if [ -e $(JAVAPATH) ]; then echo "fine :)"; else echo " not found! :("; fi
	@echo -n "### Checking Exlibris Rosetta SDK path: $(ROSETTASDK) ...."
	@if [ -e $(ROSETTASDK) ]; then echo "fine :)"; else echo " not found! :("; fi

.PHONY: help clean distclean jarclean test

