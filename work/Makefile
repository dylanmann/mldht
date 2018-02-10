all:
	touch shutdown
	cd .. && mvn -Dmaven.test.skip=true package 
	../bin/mldht-daemon
