# Types and Codecs

How to port row access, temporal types, UDTs, and custom codecs to 4.x. See [CQL to Java type
mappings](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#cql-to-java-type-mappings)
in the migration guide.

## Temporal Types

4.x maps `timestamp`, `date`, and `time` to `java.time` types, as listed in [CQL to Java type
mappings](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#cql-to-java-type-mappings),
and has no codec for `java.util.Date`. `duration` moves from `com.datastax.driver.core.Duration` to
`com.datastax.oss.driver.api.core.data.CqlDuration`.

### Wrong

```java
// Compiles, then fails at runtime:
// CodecNotFoundException: Codec not found for requested operation: [null <-> java.util.Date]
session.execute("INSERT INTO events (id, ts) VALUES (?, ?)", id, new Date());
```

### Right

```java
session.execute("INSERT INTO events (id, ts) VALUES (?, ?)", id, Instant.now());

// If the rest of the application still needs Date, convert at the driver boundary only.
// Date.from() and toInstant() throw on null; 3.x getTimestamp() and setTimestamp() did not.
Instant read = row.getInstant("ts");
Date date = read == null ? null : Date.from(read);
session.execute("INSERT INTO events (id, ts) VALUES (?, ?)", id, date == null ? null : date.toInstant());
```

Positional values passed to `execute(String, Object...)` or `SimpleStatement.newInstance` are
checked only at runtime. After porting, grep for `new Date(`, `java.util.Date`, and `.getTime()`
near driver calls.

## Getter and Setter Renames

The temporal getters follow the types above, as listed in [CQL to Java type
mappings](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#cql-to-java-type-mappings).
The guide does not list the other renames below. They apply to `Row`, `BoundStatement`, `UdtValue`,
and `TupleValue`, and setters follow the same names (`setBigInteger`, `setUuid`, and so on).

| 3.x | 4.x |
| --- | --- |
| `getBytes` | `getByteBuffer` |
| `getVarint` | `getBigInteger` |
| `getDecimal` | `getBigDecimal` |
| `getInet` | `getInetAddress` |
| `getUUID` | `getUuid` |
| `getUDTValue` | `getUdtValue` |
| `getBool` | `getBoolean` (`getBool` still exists as an alias) |

`getString`, `getInt`, `getLong`, `getDouble`, `getFloat`, `getList(name, Class)`, `getSet`,
`getMap`, `getTupleValue`, and `isNull` keep their names. `getObject` does too, but returns the 4.x
types: a `timestamp` comes back as `Instant`, so a `(Date)` cast compiles and throws
`ClassCastException`.

## Utility Classes

| 3.x | 4.x |
| --- | --- |
| `com.datastax.driver.core.utils.UUIDs.timeBased()` | `com.datastax.oss.driver.api.core.uuid.Uuids.timeBased()` |
| `com.datastax.driver.core.utils.Bytes.toHexString` / `fromHexString` | `com.datastax.oss.driver.api.core.data.ByteUtils.toHexString` / `fromHexString` |
| `cluster.getMetadata().getKeyspace(ks).getUserType("address")` | `session.getMetadata().getKeyspace(ks).flatMap(k -> k.getUserDefinedType("address"))`, which returns an `Optional` |
| `cluster.getMetadata().newTupleType(DataType.cint(), DataType.text())` | `DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT)` |

## Custom Codecs

In 4.x `TypeCodec` is an interface with `getJavaType()`, `getCqlType()`, `encode`, `decode`,
`format`, and `parse`. 3.x codecs that extend the abstract class must be rewritten; the logic
usually carries over.

Registration:

```java
// 3.x
CodecRegistry.DEFAULT_INSTANCE.register(new MyCodec());

// 4.x: at build time
CqlSession session = CqlSession.builder().addTypeCodecs(new MyCodec()).build();

// 4.x: at runtime
((MutableCodecRegistry) session.getContext().getCodecRegistry()).register(new MyCodec());
```

## `scylla-driver-extras` Codecs

Most extras codecs have a ready replacement in
`com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs`, in `java-driver-core`. Register them
as shown above.

| 3.x extras codec | 4.x |
| --- | --- |
| `jdk8.InstantCodec`, `jdk8.LocalDateCodec`, `jdk8.LocalTimeCodec` | Not needed: these are the default mappings |
| `jdk8.LocalDateTimeCodec` (UTC) | `ExtraTypeCodecs.LOCAL_TIMESTAMP_UTC` |
| `jdk8.ZonedDateTimeCodec` (`tuple<timestamp, varchar>`) | `ExtraTypeCodecs.ZONED_TIMESTAMP_PERSISTED` |
| `jdk8.OptionalCodec` | `ExtraTypeCodecs.optionalOf(innerCodec)` |
| `enums.EnumNameCodec` / `EnumOrdinalCodec` | `ExtraTypeCodecs.enumNamesOf(MyEnum.class)` / `enumOrdinalsOf(MyEnum.class)` |
| `json.JacksonJsonCodec` | `ExtraTypeCodecs.json(MyPojo.class)`, or `ExtraTypeCodecs.json(MyPojo.class, objectMapper)` if the 3.x codec got a configured `ObjectMapper`; keep that mapper so the stored JSON format does not change |
| `arrays.IntArrayCodec`, `LongArrayCodec`, `FloatArrayCodec`, `DoubleArrayCodec` | `ExtraTypeCodecs.INT_LIST_TO_ARRAY`, `LONG_LIST_TO_ARRAY`, `FLOAT_LIST_TO_ARRAY`, `DOUBLE_LIST_TO_ARRAY` |
| `arrays.ObjectArrayCodec` | `ExtraTypeCodecs.listToArrayOf(elementCodec)` |
| `date.SimpleTimestampCodec` (`timestamp` as epoch millis) | `ExtraTypeCodecs.TIMESTAMP_MILLIS_UTC` |
| `jdk8.ZoneIdCodec`, `date.SimpleDateCodec`, `json.Jsr353JsonCodec`, `guava.OptionalCodec`, all `joda.*` codecs | No replacement. Write a custom codec, or convert at the call site. |

Enum and JSON codecs are the ones most often registered in 3.x code. If they are missing in 4.x,
reads and writes of those columns fail with `CodecNotFoundException` at runtime, not at compile
time.
