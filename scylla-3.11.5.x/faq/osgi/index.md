```eval_rst
:orphan:
```

# Frequently Asked Questions - OSGi

## How to use the Java driver in an OSGi environment?

We have complete examples demonstrating usage of the driver in an [OSGi](https://www.osgi.org)
environment; please refer to our [OSGi examples repository](https://github.com/datastax/java-driver-examples-osgi).

## How to override Guava’s version?

The driver is compatible and tested with all versions of Guava in the range
`[16.0.1,26.0-jre)`.

If using Maven, you can force a more specific version by re-declaring
the Guava dependency in your project, e.g.:

```none
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>19.0</version>
</dependency>
```

Make sure that your project’s manifest is importing the right version
of Guava’s packages, e.g. for 19.0:

```none
Import-Package: com.google.common.base;version="[19.0,20)"
```

## How to enable compression?

First, read our [manual page on compression]()
to understand how to enable compression for the Java driver.

OSGi projects can use both Snappy or LZ4 compression algorithms.

For Snappy, include the following Maven dependency:

```none
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
    <version>1.1.2.6</version>
</dependency>
```

For LZ4, include the following Maven dependency:

```none
<dependency>
    <groupId>net.jpountz.lz4</groupId>
    <artifactId>lz4</artifactId>
    <version>1.3.0</version>
</dependency>
```

**IMPORTANT**: versions of LZ4 library below 1.3.0 cannot be used
because they are *not* valid OSGi bundles.

Because compression libraries are *optional runtime dependencies*,
most manifest generation tools (such as [BND](http://bnd.bndtools.org/) and the [Maven bundle plugin](https://cwiki.apache.org/confluence/display/FELIX/Apache+Felix+Maven+Bundle+Plugin+%28BND%29))
will not reference these libraries in your project’s manifest.
This is correct, but could be a problem for some OSGi provisioning tools,
and notably for [Tycho](https://eclipse.org/tycho/), because it does not consider such
dependencies when computing the target platform.

If you are facing provisioning issues related to compression libraries,
you might need to either add them explicitly to your OSGi runtime,
or explicitly reference them in your project’s manifest.
With the [Maven bundle plugin](https://cwiki.apache.org/confluence/display/FELIX/Apache+Felix+Maven+Bundle+Plugin+%28BND%29), this second option can be achieved with the following
[BND](http://bnd.bndtools.org/) `Import-Package` instruction (the example below is for
LZ4, but the same applies to Snappy as well):

```none
<Import-Package>net.jpountz.lz4,*</Import-Package>
```

With Tycho, another option is to explicitly declare an “extra requirement”
to the compression library in the target platform definition:

```none
<plugin>
    <groupId>org.eclipse.tycho</groupId>
    <artifactId>target-platform-configuration</artifactId>
    <version>0.25.0</version>
    <configuration>
        <dependency-resolution>
            <extraRequirements>
                <requirement>
                    <id>lz4-java</id>
                    <versionRange>1.3.0</versionRange>
                    <type>eclipse-plugin</type>
                </requirement>
            </extraRequirements>
        </dependency-resolution>
        ...
    </configuration>
</plugin>
```

Note that the requirement id is its bundle symbolic name,
*not* its Maven artifact id.

## How to use the driver shaded jar?

The driver [shaded jar]() can be used
in any OSGi application, although the same limitations explained in
the manual apply.

## How to get proper logs?

The driver uses [SLF4j](http://www.slf4j.org/) for [logging]().

You OSGi runtime should therefore include the SLF4J API bundle, and
one valid implementation bundle, such as [Logback](http://logback.qos.ch/).

For Maven-based projects, this can be achieved with the following
dependencies:

```none
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.25</version>
</dependency>

<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.2.3</version>
    <scope>runtime</scope>
</dependency>
```

Some OSGi containers might require additional configuration.
Please consult their documentation for further details.

## I’m getting the error: “Could not load JNR C Library”

The driver is able to perform native system calls through JNR in some cases,
for example to achieve microsecond resolution when
[generating timestamps]().

Unfortunately, some of the JNR artifacts available from Maven
are not valid OSGi bundles and cannot be used in OSGi applications.

[JAVA-1127](https://datastax-oss.atlassian.net/browse/JAVA-1127) has been created to track this issue, and there
is currently no simple workaround.

Note that if you use Maven and include any JNR dependency
in your pom, *these will be silently ignored by Pax Exam when
running integration tests*, and most likely, your tests will
fail with provisioning errors.

Because native calls are not available,
it is also normal to see the following log lines when starting the driver:

```none
INFO - Could not load JNR C Library, native system calls through this library will not be available
INFO - Using java.lang.System clock to generate timestamps.
```
