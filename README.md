# dsx - Datastore Extended

A type-safe, generic wrapper for Google Cloud Datastore in Go. Provides a fluent API for building queries and performing CRUD operations with compile-time type checking.

## Features

- **Type-safe generics** - Compile-time type checking for all operations
- **Fluent API** - Chainable methods for building queries
- **Explicit contexts** - Every call that performs I/O takes its own `context.Context`
- **Honest not-found** - Lookups report `ErrNotFound` instead of a nil entity
- **Namespaces** - Multi-tenant partitioning per connection or per query
- **Automatic batching** - Batch reads, writes and deletes are chunked to Datastore's limits
- **Streaming deletes** - Query-based delete uses constant memory regardless of match count
- **Pagination support** - Both offset and cursor-based pagination
- **Filter operators** - Type-safe enum for query operators
- **Aggregation queries** - Efficient count operations without loading entities
- **Auto-generated keys** - Insert entities with Datastore-assigned keys
- **Projection queries** - Fetch only specific fields for efficiency
- **Transaction support** - Atomic multi-entity operations

## Installation

```bash
go get github.com/louvri/dsx
```

## Quick Start

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/louvri/dsx"
)

type User struct {
    Name      string
    Email     string
    Status    string
    CreatedAt time.Time
}

func main() {
    ctx := context.Background()

    // Connect to Datastore
    db, err := dsx.Connect(ctx, "my-project", "")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Query users
    users, err := dsx.Query[User](db, "User").
        WithFilter("Status", dsx.OpEqual, "active").
        WithOrderDesc("CreatedAt").
        WithLimit(10).
        Select(ctx)
    if err != nil {
        log.Fatal(err)
    }

    for _, user := range users {
        log.Printf("User: %s (%s)", user.Name, user.Email)
    }

    // Look one up
    user, err := dsx.GetByKey[User](ctx, db, "User", "user-123")
    switch {
    case errors.Is(err, dsx.ErrNotFound):
        log.Println("no such user")
    case err != nil:
        log.Fatal(err)
    default:
        log.Printf("found %s", user.Name)
    }
}
```

## Migrating from v0.0.x to v0.1.0

v0.1.0 is a breaking release. The changes are mechanical and the compiler finds all of them except the `ErrNotFound` one, which is listed first because it is the only change that compiles cleanly and behaves differently.

| Before | After | Why |
| --- | --- | --- |
| `user, err := ...Get()`; `if user == nil` | `if errors.Is(err, dsx.ErrNotFound)` | A missing entity was indistinguishable from a successful empty result, so every caller had to remember a nil check. `Get` and `GetByKey` now return `ErrNotFound`. |
| `dsx.Query[User](db, ctx, "User")` | `dsx.Query[User](db, "User")` | The context belongs to the call that does the I/O, not to the builder. |
| `.Select()` / `.Get()` / `.Count()` | `.Select(ctx)` / `.Get(ctx)` / `.Count(ctx)` | |
| `.Upsert(key, &u)` | `.Upsert(ctx, key, &u)` | |
| `.UpsertMulti(m)` / `.Delete()` | `.UpsertMulti(ctx, m)` / `.Delete(ctx)` | |
| `.InsertWithAutoKey(&u)` | `.InsertWithAutoKey(ctx, &u)` | |
| `.InsertMultiWithAutoKey(s)` | `.InsertMultiWithAutoKey(ctx, s)` | |
| `dsx.GetMulti[User](db, ctx, ...)` | `dsx.GetMulti[User](ctx, db, ...)` | Context first, as Go convention requires. |
| `dsx.GetByKey[User](db, ctx, ...)` | `dsx.GetByKey[User](ctx, db, ...)` | |
| `dsx.DeleteByKey(db, ctx, ...)` | `dsx.DeleteByKey(ctx, db, ...)` | |
| `dsx.DeleteMultiByKey(db, ctx, ...)` | `dsx.DeleteMultiByKey(ctx, db, ...)` | |
| `dsx.RunInTransaction(db, ctx, fn)` | `commit, err := dsx.RunInTransaction(ctx, db, fn)` | Context first, and the commit is now returned: it is the only way to resolve the pending key of an auto-ID insert made inside the transaction. |
| `dsx.Connect(ctx, project, database, credJSON)` | `dsx.Connect(ctx, project, database, dsx.WithCredentialsJSON(credJSON))` | Options leave room for namespaces and client settings without another signature change. |
| `db.ProjectId()` / `db.DatabaseId()` | `db.ProjectID()` / `db.DatabaseID()` | Go initialisms are capitalised throughout; v0.1.0 is the moment to fix it, since these can never be renamed cheaply again. |
| `.KeysOnly().Select(ctx)` | `.SelectKeys(ctx)` | `KeysOnly` + `Select` returned zero entities and a nil error: Datastore skips entity loading for a keys-only query, so there was nothing to decode. `SelectKeys` returns the keys instead. |
| `dsx.SetLogger(l)`, `dsx.Logger` | *(removed)* | A library should not log. Errors are wrapped with the operation and kind, so the caller logs them once, with its own logger and fields. |
| `errors.New("query defined to use cursor")` | `dsx.ErrPaginationConflict` | Comparable with `errors.Is`. |

These fixes need no migration but change behavior:

- **`OpNotIn` works at all now.** It was defined as `"not in"` where Datastore spells the operator `"not-in"`, so every `WithFilter(field, dsx.OpNotIn, ...)` failed. If you worked around it, you can drop the workaround.
- **An unknown `FilterOperator` is now rejected by the builder.** Previously `Count` silently dropped a filter the Datastore client refused and returned a count over more rows than you asked for.
- **An undecodable cursor is now an error.** It used to be logged and ignored, which silently served page 1. A service that accepts a cursor from a client will now return an error where it previously returned the first page.
- **An empty kind is rejected.** `dsx.Query[User](db, "")` was a *kindless* query matching every entity of every kind, so `Delete` would have emptied the namespace rather than the kind.
- **An empty `OpIn` / `OpNotIn` slice, an empty order field and an empty projected field are rejected** instead of being dropped silently by `Count`.
- **`OpIn` and `OpNotIn` now accept any slice.** `WithFilter("Status", dsx.OpIn, []string{"active", "pending"})` was documented but rejected by Datastore, which only accepts `[]any`. dsx now converts the slice for you.
- **Batch operations chunk automatically.** `UpsertMulti`, `InsertMultiWithAutoKey` and `GetMulti` previously sent everything in one request and failed above Datastore's limits. Each now splits into requests of 500 (writes) or 1000 (reads).

## API Reference

### Connecting

```go
// Using default credentials (GOOGLE_APPLICATION_CREDENTIALS)
db, err := dsx.Connect(ctx, "project-id", "")

// Using a specific database
db, err := dsx.Connect(ctx, "project-id", "database-id")

// Using explicit credentials JSON
db, err := dsx.Connect(ctx, "project-id", "", dsx.WithCredentialsJSON(credentialsJSON))

// Scoped to a namespace, with an extra client option
db, err := dsx.Connect(ctx, "project-id", "",
    dsx.WithNamespace("tenant-42"),
    dsx.WithClientOptions(option.WithEndpoint("localhost:8081")))

// Always close when done
defer db.Close()
```

### Namespaces

Datastore namespaces partition data inside one database, which is the usual way to isolate tenants. A namespace can be set for the whole connection, derived per request, or overridden for a single query.

```go
// Per connection
db, err := dsx.Connect(ctx, "project-id", "", dsx.WithNamespace("tenant-42"))

// Per request - shares the underlying client, so this is free
tenant := db.WithNamespace(tenantIDFromRequest)
user, err := dsx.GetByKey[User](ctx, tenant, "User", "user-123")

// Per query
users, err := dsx.Query[User](db, "User").
    WithNamespace("tenant-42").
    Select(ctx)
```

The namespace applies to queries *and* to the keys dsx builds from a name - the keys it writes, deletes, and filters on. `WithNamespace` may appear anywhere in the chain, including after a key filter. An empty namespace means the default namespace.

Keys **you** build are used exactly as you give them, namespace included. That covers `WithAncestorKey`, a `*datastore.Key` passed to `WithFilter`, and anything inside `RunInTransaction`. Datastore rejects a query whose ancestor or key filter sits in a different partition from the query, so set the namespace yourself:

```go
companyKey := datastore.NameKey("Company", "acme", nil)
companyKey.Namespace = db.Namespace()
```

`db.WithNamespace` returns a copy that shares the underlying client, so it must not be closed separately - closing any copy closes the connection for all of them.

`db.Namespace()` reports the namespace in effect.

### Querying

#### Basic Select

```go
users, err := dsx.Query[User](db, "User").Select(ctx)
```

#### With Filters

```go
// Single filter
users, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Select(ctx)

// Multiple filters (AND)
users, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    WithFilter("Age", dsx.OpGreaterEqual, 18).
    Select(ctx)

// Membership - any slice type works
users, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpIn, []string{"active", "pending"}).
    Select(ctx)

// Filter by key
users, err := dsx.Query[User](db, "User").
    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
    Select(ctx)

// Several keys at once
users, err := dsx.Query[User](db, "User").
    WithFilter(dsx.FieldKey, dsx.OpIn, []string{"user-1", "user-2"}).
    Select(ctx)
```

#### Available Filter Operators

| Operator | Datastore | Meaning |
| --- | --- | --- |
| `dsx.OpEqual` | `=` | Equal |
| `dsx.OpNotEqual` | `!=` | Not equal |
| `dsx.OpGreater` | `>` | Greater than |
| `dsx.OpGreaterEqual` | `>=` | Greater than or equal |
| `dsx.OpLess` | `<` | Less than |
| `dsx.OpLessEqual` | `<=` | Less than or equal |
| `dsx.OpIn` | `in` | Member of a slice |
| `dsx.OpNotIn` | `not-in` | Not a member of a slice |

#### Ordering

```go
users, err := dsx.Query[User](db, "User").
    WithOrder("Status").          // ascending
    WithOrderDesc("CreatedAt").   // descending
    Select(ctx)
```

#### Get Single Entity

```go
user, err := dsx.Query[User](db, "User").
    WithFilter("Email", dsx.OpEqual, "john@example.com").
    Get(ctx)
if errors.Is(err, dsx.ErrNotFound) {
    // no such user
}
```

`Get` limits the query to one entity for you; there is no need to add `WithLimit(1)`.

#### Get Single Entity by Key

```go
user, err := dsx.GetByKey[User](ctx, db, "User", "user-123")
if errors.Is(err, dsx.ErrNotFound) {
    // no such user
}
```

#### Get Multiple Entities by Key

```go
users, err := dsx.GetMulti[User](ctx, db, "User", []string{"user-1", "user-2", "user-3"})
```

The result keeps the order of the requested keys, and requests are split into chunks of 1000 automatically. Entities that do not exist are left **zero-valued** rather than reported, so use `GetByKey` when you need to tell a missing entity from an empty one.

### Counting Entities

```go
count, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Count(ctx)
```

Uses an aggregation query, so no entities are loaded. Datastore's count aggregation is limited to roughly one million entities.

### Pagination

#### Offset-based (Simple)

```go
users, err := dsx.Query[User](db, "User").
    WithOffset(20).
    WithLimit(10).
    Select(ctx)
```

Datastore caps offsets at 1000 and still scans everything it skips. Use cursors beyond the first few pages.

#### Cursor-based (Efficient)

```go
cursor := ""
for {
    users, nextCursor, err := dsx.Query[User](db, "User").
        WithFilter("Status", dsx.OpEqual, "active").
        WithLimit(100).
        WithCursor(cursor).
        SelectWithCursor(ctx)
    if err != nil {
        return err
    }

    process(users)

    if len(users) < 100 {
        break // last page
    }
    cursor = nextCursor
}
```

Offset and cursor pagination cannot be combined. Mixing them yields `dsx.ErrPaginationConflict`.

### Upserting

#### Single Entity

```go
user := User{Name: "John", Email: "john@example.com", Status: "active"}
err := dsx.Query[User](db, "User").Upsert(ctx, "user-123", &user)
```

An empty key name is rejected. Datastore would otherwise treat it as an incomplete key and commit an auto-ID insert, creating a new entity on every call; use `InsertWithAutoKey` when that is what you want.

#### Multiple Entities

```go
users := map[string]*User{
    "user-1": {Name: "Alice", Status: "active"},
    "user-2": {Name: "Bob", Status: "active"},
}
err := dsx.Query[User](db, "User").UpsertMulti(ctx, users)
```

Split into commits of 500 automatically. Keys are written in sorted order, so batch boundaries are stable across runs and a failure names the first and last key of the batch that failed. Each commit is independent, so a failure part-way through leaves the batches before it applied.

#### Insert with Auto-generated Key

```go
order := Order{CustomerID: "cust-123", Total: 99.99, CreatedAt: time.Now()}
key, err := dsx.Query[Order](db, "Order").InsertWithAutoKey(ctx, &order)
if err != nil {
    return err
}
fmt.Printf("Created order with key ID: %d\n", key.ID)
```

#### Batch Insert with Auto-generated Keys

```go
orders := []*Order{
    {CustomerID: "cust-1", Total: 10.00},
    {CustomerID: "cust-2", Total: 20.00},
}
keys, err := dsx.Query[Order](db, "Order").InsertMultiWithAutoKey(ctx, orders)
```

Returns the complete keys in input order, committing in batches of 500. If a batch fails, the keys of the batches already committed are returned **alongside** the error - those entities exist, and this is the only way to learn the IDs Datastore assigned them.

### Deleting

```go
// Delete everything matching a query
err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "inactive").
    Delete(ctx)

// Delete one entity by key
err := dsx.DeleteByKey(ctx, db, "User", "user-123")

// Delete many entities by key
err := dsx.DeleteMultiByKey(ctx, db, "User", []string{"user-1", "user-2"})
```

Query-based `Delete` streams keys from a keys-only query and deletes them in batches of 500, so memory stays constant no matter how many entities match.

**Warning:** `Delete` without filters removes every entity of the kind.

### Advanced Features

#### Ancestor Queries

```go
companyKey := datastore.NameKey("Company", "acme", nil)
companyKey.Namespace = db.Namespace() // keys you build carry their own namespace

employees, err := dsx.Query[Employee](db, "Employee").
    WithAncestorKey(companyKey).
    Select(ctx)
```

#### Projection Queries

```go
users, err := dsx.Query[User](db, "User").
    WithProject("Name", "Email").
    Select(ctx)
```

Projected fields must be indexed; fields tagged `noindex` cannot be projected.

#### Transactions

```go
_, err := dsx.RunInTransaction(ctx, db, func(tx *datastore.Transaction) error {
    key := datastore.NameKey("User", "user-123", nil)
    key.Namespace = db.Namespace()

    var user User
    if err := tx.Get(key, &user); err != nil {
        return err
    }
    user.Balance += 100
    _, err := tx.Put(key, &user)
    return err
})
```

The returned `*datastore.Commit` resolves the pending key of an auto-ID insert made inside the transaction:

```go
var pending *datastore.PendingKey
commit, err := dsx.RunInTransaction(ctx, db, func(tx *datastore.Transaction) error {
    var err error
    pending, err = tx.Put(datastore.IncompleteKey("Order", nil), &order)
    return err
})
if err != nil {
    return err
}
key := commit.Key(pending)
```

Datastore transactions are limited to 25 entity groups and 270 seconds.

#### Distinct Results

```go
users, err := dsx.Query[User](db, "User").
    WithProject("Status").
    WithDistinct().
    Select(ctx)
```

#### Keys Only

```go
keys, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "inactive").
    SelectKeys(ctx)
```

Returns `[]*datastore.Key` without loading the entities - the cheap way to check what matches, or to hand keys to `RunInTransaction`. Every matching key is held in memory, so use `WithLimit` on a kind that may match a very large number of entities; `Delete` streams instead and needs no bound.

#### Access Underlying Client

```go
client := db.Client()
```

## Indexing

Datastore requires indexes for queries. Simple single-property filters use built-in indexes, but composite queries need explicit indexes in `index.yaml`:

```yaml
indexes:
- kind: User
  properties:
  - name: Status
  - name: CreatedAt
    direction: desc
```

This index supports:

```go
dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    WithOrderDesc("CreatedAt").
    Select(ctx)
```

## Best Practices

### Use Count() Instead of Loading Entities

```go
// Good - uses an aggregation query, no data loaded
count, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Count(ctx)

// Bad - loads all entities just to count them
users, err := dsx.Query[User](db, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Select(ctx)
count := len(users)
```

### Use GetMulti for Multiple Known Keys

```go
// Good - batched lookups
users, err := dsx.GetMulti[User](ctx, db, "User", []string{"user-1", "user-2", "user-3"})

// Bad - one round trip per key
for _, key := range keys {
    user, err := dsx.GetByKey[User](ctx, db, "User", key)
}
```

### Use Cursors for Deep Pagination

```go
// Good - efficient at any depth
users, cursor, err := dsx.Query[User](db, "User").
    WithLimit(50).
    WithCursor(cursor).
    SelectWithCursor(ctx)

// Bad - expensive for large offsets, and Datastore caps them at 1000
users, err := dsx.Query[User](db, "User").
    WithLimit(50).
    WithOffset(5000).
    Select(ctx)
```

### Batch Operations for Multiple Entities

```go
// Good - batched commits
err := dsx.Query[User](db, "User").UpsertMulti(ctx, usersMap)

// Bad - one round trip per entity
for key, user := range usersMap {
    err := dsx.Query[User](db, "User").Upsert(ctx, key, user)
}
```

### Use noindex for Non-Queryable Fields

In your struct, mark fields you don't query to save on index writes:

```go
type User struct {
    Name      string
    Email     string
    Status    string
    Biography string `datastore:",noindex"` // Won't be indexed
}
```

## Concurrency

A `DB` is safe for concurrent use and is meant to be created once and shared; `db.WithNamespace` returns a copy that shares the same client.

A `QueryBuilder` is mutable and is **not** safe for concurrent use. Build one per operation - they are cheap.

Filters, ordering, projection and pagination describe a query, so they apply to `Select`, `SelectKeys`, `SelectWithCursor`, `Get`, `Count` and `Delete`. The write terminals - `Upsert`, `UpsertMulti`, `InsertWithAutoKey`, `InsertMultiWithAutoKey` - address entities by key and ignore them; only the kind and namespace apply.

## Error Handling

dsx does not log. Every error is wrapped with the operation and kind that produced it and returned to the caller, so your own logger reports it once, with your own fields:

```
dsx: select User: rpc error: code = PermissionDenied ...
dsx: upsert User/user-123: rpc error: ...
dsx: get-multi User [1000:2000]: rpc error: ...
dsx: delete User: scan keys: rpc error: ...
```

The underlying error is preserved, so `errors.Is` and `errors.As` still reach the Datastore and gRPC error values beneath.

### Sentinel errors

| Error | Returned by | Meaning |
| --- | --- | --- |
| `dsx.ErrNotFound` | `Get`, `GetByKey` | No entity matched |
| `dsx.ErrPaginationConflict` | any terminal call | Offset and cursor pagination were combined |

```go
user, err := dsx.GetByKey[User](ctx, db, "User", "user-123")
switch {
case errors.Is(err, dsx.ErrNotFound):
    return handleMissing()
case err != nil:
    return fmt.Errorf("load user: %w", err)
}
```

### Deferred build errors

Builder methods never return an error. The first problem found while building the query - an undecodable cursor, a key filter given the wrong type, offset combined with a cursor - is recorded and returned by the terminal call, so a chain stays readable:

```go
users, err := dsx.Query[User](db, "User").
    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
    WithCursor(cursor).
    SelectWithCursor(ctx)
```

`QueryBuilder.Err()` exposes the same error earlier if you want to check before executing.

## Releasing

Pushing to `main` tags a release automatically. The bump level is read from every commit since the last release tag, so it does not matter whether a pull request is squashed, merged or rebased. Write the subject as a [conventional commit](https://www.conventionalcommits.org/):

| Commit | Below v1.0.0 | v1.0.0 and above |
| --- | --- | --- |
| any type with `!` (`feat!:`, `fix!:`, `refactor(api)!:`), or a `BREAKING CHANGE:` / `BREAKING-CHANGE:` line in the body | minor | major |
| `feat:` / `feat(scope):` | patch | minor |
| anything else (`fix:`, `docs:`, `chore:`, ...) | patch | patch |

When a pull request is squashed, the generated body lists the original commit subjects as `* subject` lines, and those count as subjects too - so a squash keeps the right level even if its title is not a conventional commit.

Below v1.0.0, semver keeps breaking changes in the minor position, which is why this release is v0.1.0 rather than v1.0.0.

To set the level explicitly, add a `Release-As:` trailer on its own line in the commit message:

```
Release-As: minor
```

A change that reaches no consumer - a workflow, a README - can skip the release entirely:

```
Release-As: skip
```

An explicit trailer always wins, and has to be a whole line, so prose that merely mentions a level cannot trigger a release. Surrounding whitespace is fine; a trailer whose level is not one of the four logs a warning and is ignored.

`skip` is read from the merged commit only, unlike the three release levels which are read from every commit since the last tag. Skipping creates no tag, so a skip found anywhere in the range would still be there on the next push and would disable releases permanently. Reading only the commit that was merged makes a skip **defer** rather than suppress: the next release goes out normally and carries the skipped commits with it. If several commits in the range carry different trailers, the highest level is used.

## License

MIT
