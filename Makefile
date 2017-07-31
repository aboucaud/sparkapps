.PHONY: clean compile package

clean:
	sbt clean && rm -rf target project/target project/project

compile:
	sbt compile

package:
	sbt package
