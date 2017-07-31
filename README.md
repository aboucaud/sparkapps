## Spark Example Applications

Sandbox for exploring the use of Scala/Spark for astrophysics projects.

### Package

This package is called `sparkapps` and contains the following programs, that can be run using the sequence:

1. Launch the sbt console
   ```bash
   $ sbt
   ```
2. Compile and run the program
   ```sbt
   > compile
   > runMain sparkapps.<ProgramName>
   ```

#### `FitsReader`

Scala program using the java library [nom-tam-fits](http://nom-tam-fits.github.io/nom-tam-fits/) to read and extract information for a FITS image file.

#### `GeoSpark`

Set of methods using the [GeoSpark](http://geospark.datasyslab.org/) framework to perform spatial indexing, as well as spatial distance joins and queries.

### Installation

This project is meant to be compiled and run with [sbt][sbt].

It can be imported using the right Scala plugins in [Intellij IDEA][intell] and [Eclipse][eclipse] with [Scala IDE][scalaide], as well as [SublimeText][subl] or [Atom][atom] with [ENSIME][ensime].

### Credits

I took inspiration from the [sbt][sbt] project template: https://github.com/Ludwsam/SparkTemplate/


[sbt]: http://www.scala-sbt.org
[intell]: https://www.jetbrains.com/idea/
[eclipse]: http://www.eclipse.org/
[scalaide]: http://scala-ide.org/
[subl]: http://www.sublimetext.com/
[atom]: https://atom.io/
[ensime]: http://ensime.org/
