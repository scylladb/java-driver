# Object Mapper

How to port code that uses the 3.x mapper (`com.datastax.driver.mapping`) to the 4.x mapper. The 4.x
mapper is a different design: it generates code at compile time from `@Entity`, `@Dao`, and
`@Mapper` interfaces, so it needs the `java-driver-mapper-processor` annotation processor (see
[coordinates-and-build](coordinates-and-build.md)). The [4.x mapper
manual](https://java-driver.docs.scylladb.com/stable/manual/mapper/index.md) covers the API; this
file covers what changes.

## Annotation Mapping

| 3.x | 4.x |
| --- | --- |
| `@Table(keyspace = "ks", name = "users")` | `@Entity(defaultKeyspace = "ks")` + `@CqlName("users")` on the class, or pass the keyspace through `@DaoKeyspace`. `readConsistency` and `writeConsistency` move to `@StatementAttributes(consistencyLevel = "...")` on each DAO method; without it the default consistency applies. `caseSensitiveKeyspace` and `caseSensitiveTable` become quoted names, as for columns below. |
| `@UDT(name = "address")` | `@Entity` + `@CqlName("address")`. UDT classes are entities too. |
| `@Column(name = "x")`, `@Field(name = "x")` | `@CqlName("x")` |
| `@Column(name = "x", caseSensitive = true)`, same on `@Field` | `@CqlName("\"x\"")`. Without the inner quotes the name is lowercased. |
| `@PartitionKey(n)`, `@ClusteringColumn(n)` | Same names, in `com.datastax.oss.driver.api.mapper.annotations` |
| `@Transient` | `@Transient`, or `@TransientProperties` on the class |
| `@Computed("writetime(v)")` | `@Computed("writetime(v)")` |
| `@Frozen`, `@FrozenKey`, `@FrozenValue` | Delete them; 4.x does not need them. |
| `MappingManager`, `manager.mapper(User.class)` | A `@Mapper` interface and its generated `<Name>Builder` |
| `mapper.get(id)`, `save(u)`, `delete(u)` | `@Select`, `@Insert`, `@Delete` methods on a `@Dao` interface |
| `@Accessor` interface with `@Query` methods | `@Query` methods on a `@Dao` interface |
| `Result<User>` | `PagingIterable<User>` |
| `ListenableFuture<User>` from `getAsync` | `CompletionStage<User>` from an `@Select` method that returns one |
| `Mapper.Option.ttl(n)`, `Option.consistencyLevel(cl)` | `@Insert(ttl = ":ttl")` with a `ttl` parameter, `@StatementAttributes(consistencyLevel = "...")` |

## Two Behavior Changes

Both compile cleanly. The first fails at startup; the second silently changes the data written.

**Column names.** 3.x mapped a property to its name in lowercase: `firstName` to `firstname`.
The 4.x default is `SNAKE_CASE_INSENSITIVE`: `firstName` to `first_name`. The mapper checks each
entity against its table when it creates a DAO, so a missed annotation fails with
`The CQL ks.table: ks.users has missing columns: [first_name]`. To keep the 3.x mapping, annotate
every entity:

```java
@NamingStrategy(convention = NamingConvention.CASE_INSENSITIVE)
```

Explicit `@CqlName` values are used as they are, so properties that had `@Column(name = ...)` in 3.x
keep their columns.

**Null fields.** 3.x `mapper.save(user)` wrote `null` fields as nulls by default, which cleared
those columns. 4.x `@Insert`, `@Update`, and `@Query` default to `NullSavingStrategy.DO_NOT_SET`,
which leaves them unchanged. For a ported `@Accessor` query, a `null` parameter in an `UPDATE` no
longer clears the column. To keep the 3.x behavior, annotate the DAO, which covers all three, or set
`nullSavingStrategy` on a single method:

```java
@DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
```

Ask the user before keeping `SET_TO_NULL`: writing nulls creates tombstones, and many 3.x
applications kept the default without knowing it.

## Example

### 3.x

```java
@Table(keyspace = "ks", name = "users")
public class User {
  @PartitionKey private UUID id;
  @Column(name = "first_name") private String firstName;
  private String email;                       // column "email"
  // no-arg constructor, getters and setters
}

MappingManager manager = new MappingManager(session);
Mapper<User> mapper = manager.mapper(User.class);
User u = mapper.get(id);
mapper.save(u);
```

### 4.x

```java
@Entity(defaultKeyspace = "ks")
@CqlName("users")
@NamingStrategy(convention = NamingConvention.CASE_INSENSITIVE)
public class User {
  @PartitionKey private UUID id;
  @CqlName("first_name") private String firstName;
  private String email;
  // no-arg constructor, getters and setters: same as 3.x
}

@Dao
@DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL) // only if the user confirms
public interface UserDao {
  @Select User findById(UUID id);             // returns null when the row is missing
  @Insert void save(User user);
  @Delete void delete(User user);
  @Query("SELECT * FROM ${qualifiedTableId} WHERE id IN :ids")
  PagingIterable<User> findByIds(List<UUID> ids);
}

@Mapper
public interface UserMapper {
  @DaoFactory UserDao userDao();
}

UserMapper mapper = new UserMapperBuilder(session).build(); // generated at compile time
UserDao dao = mapper.userDao();
```

- `UserMapperBuilder` does not exist until the annotation processor runs. If the IDE reports it as
  missing, build the project first.
- In `@Query`, use `${keyspaceId}`, `${tableId}`, or `${qualifiedTableId}` instead of a hard-coded
  keyspace, and named markers (`:ids`) that match the method's parameter names. The table comes from
  the entity the method returns. A `@Query` that returns no entity (`ResultSet`, `long`, `boolean`)
  takes it from a `@DaoTable` parameter of the `@DaoFactory` method; without one, creating the DAO
  fails with `Cannot substitute ${tableId} ... the DAO wasn't built with a table`. Keep the
  qualified table name (`ks.users`) in such queries, or add the factory parameter. The placeholders
  in such a query also take their keyspace from the factory: add a `@DaoKeyspace` parameter, or
  `${qualifiedTableId}` is the bare table name and works only if the session has a default keyspace.
- Port 3.x `@Accessor` query strings as they are, except the markers: replace each anonymous `?`
  with a named marker that matches the parameter name, for example `WHERE id = :userId` for
  `findById(UUID userId)`. 4.x binds `@Query` parameters by name, so a `?` works only when the
  parameter happens to have the column's name; otherwise the call fails at runtime with
  `IllegalArgumentException: userId is not a variable in this bound statement`. A 3.x `@Param("x")`
  becomes `@CqlName("x")` on the parameter.
- If an `@Accessor` query uses `ALLOW FILTERING`, point it out to the user instead of rewriting it;
  see `scylladb-data-modeling`.
