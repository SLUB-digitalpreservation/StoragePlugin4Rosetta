#!/bin/bash
# Andreas Romeyke, SLUB Dresden
# erzeugt Submission-Application, die vorbereitete Verzeichnisse per Java SDK
# von ExLibris an Rosetta übergibt.

# Pfad zu Java 6
JAVAPATH=$(wildcard /usr/lib/jvm/java-1.6.0-openjdk-*/bin/)

# Verwendete Rosetta-Version
ROSETTAVERSION=3.2.0

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

SOURCES=java/org/slub/rosetta/dps/repository/plugin/storage/nfs/SLUBStoragePlugin.java\
	java/org/slub/rosetta/dps/repository/plugin/storage/nfs/testSLUBStoragePlugin.java
OBJS=$(SOURCES:.java=.class)


all: $(OBJS)

help:
	@echo "erzeugt Storage-Plugin für Rosetta von Exlibris"
	@echo ""
	@echo "Das Argument 'clean' löscht temporäre Dateien, 'help' gibt diese Hilfe aus und"
	@echo "'compile' erzeugt ein JAR-File und ein Bash-Script welches das Java-Programm"
	@echo "aufruft."

jarclean: 
	@rm -Rf  \
	com/  \
	org/ gov/ srw/ uk/ nbnDe11112004033116 PLUGIN_INF repackage  \
	schemaorg_apache_xmlbeans META-INF NOTICE.txt \
	dnx_profile.xls ExLibMessageFile.properties LICENSE.txt manifest.txt

test:   $(OBJS) 
	java -cp ${CLASSPATH}:$(JUNITCLASSPATH) org.junit.runner.JUnitCore org.slub.rosetta.dps.repository.plugin.storage.nfs.testSLUBStoragePlugin

clean: jarclean
	@rm -Rf doc/
	find ./ -name "*.class" -exec rm -f \{\} \;

distclean: clean
	find ./ -name "*~" -exec rm -f \{\} \;
	@rm -Rf null

.PRECIOUS: %.sh %.jar

%.jar: %.class
	# setze temporären Link zu kompilierten Files des Rosetta-SDK, Deposit-Module
	@${JAVAPATH}/jar xf ${ROSETTASDKDEPOSIT}/xmlbeans-2.3.0.jar
	@${JAVAPATH}/jar xf ${ROSETTASDKDEPOSIT}/dps-sdk-${ROSETTAVERSION}.jar
	@${JAVAPATH}/jar xf ${ROSETTASDKDEPOSIT}/log4j-1.2.14.jar
	@cp -a  ${ROSETTASDKDEPOSIT}/../src/com .
	@echo "Main-Class: $(basename $@)" > manifest.txt
	# Komprimiere alle class-Files zusammen
	@${JAVAPATH}/jar cfm $@ manifest.txt \
	com/ *.class \
	org/ gov/ srw/ uk/ nbnDe11112004033116 PLUGIN_INF repackage  \
	schemaorg_apache_xmlbeans META-INF \
	dnx_profile.xls ExLibMessageFile.properties LICENSE.txt
	# Lösche temporären Link
	@rm -Rf  \
	com/  \
	org/ gov/ srw/ uk/ nbnDe11112004033116 PLUGIN_INF repackage  \
	schemaorg_apache_xmlbeans META-INF NOTICE.txt \
	dnx_profile.xls ExLibMessageFile.properties LICENSE.txt manifest.txt

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

