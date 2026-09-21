package dsx_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/louvri/dsx"
)

type User struct {
	Name      string
	Email     string
	Status    string
	Balance   int
	CreatedAt time.Time
}

// newDB is a stand-in for however your service holds its connection.
func newDB() *dsx.DB { return nil }

func ExampleConnect() {
	ctx := context.Background()

	db, err := dsx.Connect(ctx, "my-project", "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	fmt.Println(db.ProjectID())
}

func ExampleConnect_options() {
	ctx := context.Background()

	db, err := dsx.Connect(ctx, "my-project", "my-database",
		dsx.WithCredentialsJSON(`{"type":"service_account"}`),
		dsx.WithNamespace("tenant-42"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	fmt.Println(db.Namespace())
}

func ExampleQuery() {
	ctx, db := context.Background(), newDB()

	users, err := dsx.Query[User](db, "User").
		WithFilter("Status", dsx.OpEqual, "active").
		WithOrderDesc("CreatedAt").
		WithLimit(50).
		Select(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, user := range users {
		fmt.Println(user.Name)
	}
}

// OpIn and OpNotIn take any slice, not only a []any.
func ExampleQueryBuilder_WithFilter_membership() {
	ctx, db := context.Background(), newDB()

	users, err := dsx.Query[User](db, "User").
		WithFilter("Status", dsx.OpIn, []string{"active", "pending"}).
		Select(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(users))
}

func ExampleQueryBuilder_SelectKeys() {
	ctx, db := context.Background(), newDB()

	// Returns the matching keys without loading the entities.
	keys, err := dsx.Query[User](db, "User").
		WithFilter("Status", dsx.OpEqual, "inactive").
		SelectKeys(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, key := range keys {
		fmt.Println(key.Name)
	}
}

func ExampleQueryBuilder_Get() {
	ctx, db := context.Background(), newDB()

	user, err := dsx.Query[User](db, "User").
		WithFilter("Email", dsx.OpEqual, "john@example.com").
		Get(ctx)
	switch {
	case errors.Is(err, dsx.ErrNotFound):
		fmt.Println("no such user")
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Println(user.Name)
	}
}

func ExampleGetByKey() {
	ctx, db := context.Background(), newDB()

	user, err := dsx.GetByKey[User](ctx, db, "User", "user-123")
	if errors.Is(err, dsx.ErrNotFound) {
		fmt.Println("no such user")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(user.Name)
}

func ExampleGetMulti() {
	ctx, db := context.Background(), newDB()

	// Absent entities stay zero-valued; the order matches the requested keys.
	users, err := dsx.GetMulti[User](ctx, db, "User", []string{"user-1", "user-2"})
	if err != nil {
		log.Fatal(err)
	}
	for _, user := range users {
		fmt.Println(user.Name)
	}
}

func ExampleQueryBuilder_Count() {
	ctx, db := context.Background(), newDB()

	count, err := dsx.Query[User](db, "User").
		WithFilter("Status", dsx.OpEqual, "active").
		Count(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(count)
}

func ExampleQueryBuilder_SelectWithCursor() {
	ctx, db := context.Background(), newDB()

	cursor := ""
	for {
		users, next, err := dsx.Query[User](db, "User").
			WithFilter("Status", dsx.OpEqual, "active").
			WithLimit(100).
			WithCursor(cursor).
			SelectWithCursor(ctx)
		if err != nil {
			log.Fatal(err)
		}

		for _, user := range users {
			fmt.Println(user.Name)
		}
		if len(users) < 100 {
			break
		}
		cursor = next
	}
}

func ExampleQueryBuilder_UpsertMulti() {
	ctx, db := context.Background(), newDB()

	// Split into commits of 500 automatically, however large the map is.
	users := map[string]*User{
		"user-1": {Name: "Alice", Status: "active"},
		"user-2": {Name: "Bob", Status: "active"},
	}
	if err := dsx.Query[User](db, "User").UpsertMulti(ctx, users); err != nil {
		log.Fatal(err)
	}
}

func ExampleQueryBuilder_InsertWithAutoKey() {
	ctx, db := context.Background(), newDB()

	key, err := dsx.Query[User](db, "User").
		InsertWithAutoKey(ctx, &User{Name: "Alice", CreatedAt: time.Now()})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(key.ID)
}

func ExampleQueryBuilder_Delete() {
	ctx, db := context.Background(), newDB()

	// Keys are streamed and deleted in batches, so memory stays constant.
	if err := dsx.Query[User](db, "User").
		WithFilter("Status", dsx.OpEqual, "inactive").
		Delete(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_WithNamespace() {
	ctx, db := context.Background(), newDB()

	// A per-request view of the same connection; it shares the client, so it
	// costs nothing to create and must not be closed separately.
	tenant := db.WithNamespace("tenant-42")

	user, err := dsx.GetByKey[User](ctx, tenant, "User", "user-123")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(user.Name)
}

func ExampleQueryBuilder_WithNamespace() {
	ctx, db := context.Background(), newDB()

	users, err := dsx.Query[User](db, "User").
		WithNamespace("tenant-42").
		WithFilter(dsx.FieldKey, dsx.OpIn, []string{"user-1", "user-2"}).
		Select(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(users))
}

func ExampleRunInTransaction() {
	ctx, db := context.Background(), newDB()

	err := dsx.RunInTransaction(ctx, db, func(tx *datastore.Transaction) error {
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
	if err != nil {
		log.Fatal(err)
	}
}
