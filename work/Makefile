all:
	touch shutdown
	cd .. && mvn -Dmaven.test.skip=true package dependency:copy-dependencies appassembler:assemble && mvn antrun:run
	../bin/mldht-daemon
