# dsx - Datastore Extended

A type-safe, generic wrapper for Google Cloud Datastore in Go. Provides a fluent API for building queries and performing CRUD operations with compile-time type checking.

## Features

- **Type-safe generics** - Compile-time type checking for all operations
- **Fluent API** - Chainable methods for building queries
- **Pagination support** - Both offset and cursor-based pagination
- **Batch operations** - Efficient multi-entity get, upsert, and delete
- **Filter operators** - Type-safe enum for query operators
- **Aggregation queries** - Efficient count operations without loading entities
- **Auto-generated IDs** - Insert entities with Datastore-assigned IDs

## Installation

```bash
go get github.com/yourusername/dsx
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/yourusername/dsx"
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
    db, err := dsx.Connect(ctx, "my-project", "", "")
    if err != nil {
        log.Fatal(err)
    }

    // Query users
    users, err := dsx.Query[User](db, ctx, "User").
        WithFilter("Status", dsx.OpEqual, "active").
        WithOrderDesc("CreatedAt").
        WithLimit(10).
        Select()
    if err != nil {
        log.Fatal(err)
    }

    for _, user := range users {
        log.Printf("User: %s (%s)", user.Name, user.Email)
    }
}
```

## API Reference

### Connecting

```go
// Using default credentials (GOOGLE_APPLICATION_CREDENTIALS)
db, err := dsx.Connect(ctx, "project-id", "", "")

// Using specific database
db, err := dsx.Connect(ctx, "project-id", "database-id", "")

// Using explicit credentials JSON
db, err := dsx.Connect(ctx, "project-id", "", credentialsJSON)
```

### Querying

#### Basic Select

```go
users, err := dsx.Query[User](db, ctx, "User").Select()
```

#### With Filters

```go
// Single filter
users, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Select()

// Multiple filters (AND logic)
users, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    WithFilter("Age", dsx.OpGreaterEqual, 18).
    Select()

// IN filter
users, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpIn, []string{"active", "pending"}).
    Select()

// Filter by entity key
users, err := dsx.Query[User](db, ctx, "User").
    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
    Select()
```

#### Available Filter Operators

| Operator | Description |
|----------|-------------|
| `dsx.OpEqual` | Equal (=) |
| `dsx.OpGreater` | Greater than (>) |
| `dsx.OpGreaterEqual` | Greater than or equal (>=) |
| `dsx.OpLess` | Less than (<) |
| `dsx.OpLessEqual` | Less than or equal (<=) |
| `dsx.OpIn` | In list |
| `dsx.OpNotIn` | Not in list |

#### Ordering

```go
// Ascending
users, err := dsx.Query[User](db, ctx, "User").
    WithOrder("Name").
    Select()

// Descending
users, err := dsx.Query[User](db, ctx, "User").
    WithOrderDesc("CreatedAt").
    Select()

// Multiple orders
users, err := dsx.Query[User](db, ctx, "User").
    WithOrder("Status").
    WithOrderDesc("CreatedAt").
    Select()
```

#### Get Single Entity

```go
user, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Email", dsx.OpEqual, "john@example.com").
    WithLimit(1).
    Get()

if user == nil {
    // Not found
}
```

#### Get Multiple Entities by ID

```go
users, err := dsx.GetMulti[User](db, ctx, "User", []string{"user-1", "user-2", "user-3"})
```

Entities that don't exist will be zero-valued in the result slice. The result slice maintains the same order as the input keys.

### Counting Entities

Use `Count()` to efficiently count entities matching a query without loading them into memory.

```go
// Count all users
total, err := dsx.Query[User](db, ctx, "User").Count()

// Count with filters
activeCount, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Count()
```

> **Note:** Datastore count aggregations have a default limit of approximately 1 million entities.

### Pagination

#### Offset-based (Simple)

```go
// Page 1
users, err := dsx.Query[User](db, ctx, "User").
    WithLimit(50).
    Select()

// Page 2
users, err := dsx.Query[User](db, ctx, "User").
    WithLimit(50).
    WithOffset(50).
    Select()
```

> **Note:** Datastore has a maximum offset of 1000. For deeper pagination, use cursors.

#### Cursor-based (Efficient)

```go
// First page
users, cursor, err := dsx.Query[User](db, ctx, "User").
    WithLimit(50).
    SelectWithCursor()

// Next page
users, cursor, err = dsx.Query[User](db, ctx, "User").
    WithLimit(50).
    WithCursor(cursor).
    SelectWithCursor()

// Iterate through all pages
cursor := ""
for {
    users, nextCursor, err := dsx.Query[User](db, ctx, "User").
        WithFilter("Status", dsx.OpEqual, "active").
        WithLimit(100).
        WithCursor(cursor).
        SelectWithCursor()
    if err != nil {
        return err
    }

    // Process users...

    if len(users) < 100 {
        break // Last page
    }
    cursor = nextCursor
}
```

### Upserting

#### Single Entity

```go
user := User{
    Name:      "John Doe",
    Email:     "john@example.com",
    Status:    "active",
    CreatedAt: time.Now(),
}

err := dsx.Query[User](db, ctx, "User").Upsert("user-123", &user)
```

#### Multiple Entities

```go
users := map[string]*User{
    "user-1": {Name: "Alice", Email: "alice@example.com", Status: "active"},
    "user-2": {Name: "Bob", Email: "bob@example.com", Status: "active"},
    "user-3": {Name: "Charlie", Email: "charlie@example.com", Status: "pending"},
}

err := dsx.Query[User](db, ctx, "User").UpsertMulti(users)
```

> **Note:** Datastore limits batch operations to 500 entities.

#### Insert with Auto-generated ID

Use `InsertWithAutoID` when you want Datastore to generate a unique numeric ID and need to know the ID after insertion.

```go
order := Order{
    CustomerID: "cust-123",
    Total:      99.99,
    CreatedAt:  time.Now(),
}

key, err := dsx.Query[Order](db, ctx, "Order").InsertWithAutoID(&order)
if err != nil {
    return err
}
fmt.Printf("Created order with ID: %d\n", key.ID)
```

### Deleting

```go
// Delete by filter
err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "inactive").
    Delete()

// Delete specific entity
err := dsx.Query[User](db, ctx, "User").
    WithFilter(dsx.FieldKey, dsx.OpEqual, "user-123").
    Delete()
```

> **Warning:** Calling `Delete()` without filters will delete ALL entities of that kind.

### Advanced Features

#### Ancestor Queries

```go
companyKey := datastore.NameKey("Company", "acme", nil)

employees, err := dsx.Query[Employee](db, ctx, "Employee").
    WithAncestorKey(companyKey).
    Select()
```

#### Distinct Results

```go
users, err := dsx.Query[User](db, ctx, "User").
    WithDistinct().
    Select()
```

#### Keys Only

```go
qb := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    KeysOnly()
```

#### Access Underlying Client

```go
// For operations not covered by dsx
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
dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    WithOrderDesc("CreatedAt").
    Select()
```

## Best Practices

### Use Limit with Get()

```go
// Good - efficient
user, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Email", dsx.OpEqual, "john@example.com").
    WithLimit(1).
    Get()

// Works but fetches all matches first
user, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Email", dsx.OpEqual, "john@example.com").
    Get()
```

### Use Count() Instead of Loading Entities

```go
// Good - uses aggregation query, no data loaded
count, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Count()

// Bad - loads all entities just to count them
users, err := dsx.Query[User](db, ctx, "User").
    WithFilter("Status", dsx.OpEqual, "active").
    Select()
count := len(users)
```

### Use GetMulti for Multiple Known IDs

```go
// Good - single API call
users, err := dsx.GetMulti[User](db, ctx, "User", []string{"user-1", "user-2", "user-3"})

// Bad - multiple API calls
for _, id := range ids {
    user, err := dsx.Query[User](db, ctx, "User").
        WithFilter(dsx.FieldKey, dsx.OpEqual, id).
        Get()
}
```

### Use Cursors for Deep Pagination

```go
// Good - efficient at any depth
users, cursor, err := dsx.Query[User](db, ctx, "User").
    WithLimit(50).
    WithCursor(cursor).
    SelectWithCursor()

// Bad - expensive for large offsets, max 1000
users, err := dsx.Query[User](db, ctx, "User").
    WithLimit(50).
    WithOffset(5000). // This will fail!
    Select()
```

### Batch Operations for Multiple Entities

```go
// Good - single API call
err := dsx.Query[User](db, ctx, "User").UpsertMulti(usersMap)

// Bad - multiple API calls
for id, user := range usersMap {
    err := dsx.Query[User](db, ctx, "User").Upsert(id, user)
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

## Error Handling

The package logs errors with context before returning them:

```
datastore User select-error <error details>
datastore User upsert-error <error details>
datastore User delete get-all error <error details>
datastore get-multi User error <error details>
datastore Order insert-with-auto-id-error <error details>
```

Common errors:

- **"query defined to use offset instead of cursor"** - Can't use `SelectWithCursor()` after `WithOffset()`
- **"query defined to use cursor"** - Can't use `Select()` after `WithCursor()`

## License

MIT License - see LICENSE file for details.