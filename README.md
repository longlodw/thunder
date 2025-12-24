# Thunder

Thunder is a Go library that provides a lightweight, persistent, and datalog-like database interface.
It leverages `bolt` for underlying storage and supports serialization via `MessagePack`, `JSON`, `Gob`, or custom marshalers/unmarshalers.

This library is designed for Go applications needing an embedded database with capabilities for schema definitions, indexing, filtering, and complex query operations, including recursive queries for hierarchical data.

## Features

- **Embedded Database:** Built on top of `bolt` for reliable, file-based persistence.
- **Relational Operations:** Supports creating persistent "relations" (tables), inserting data, and querying with filters.
- **Flexible Schema:** Define column specifications with optional indexing.
- **Datalog-like Queries:**
  - Support for recursive queries (e.g., finding all descendants in a tree structure).
  - Projections and joins (implicit in query construction).
- **Flexible Serialization:** Uses pluggable marshalers/unmarshalers (MessagePack, JSON, Gob, or custom).
- **Transaction Support:** Full support for ACID transactions (Read-Only and Read-Write).

## Installation

```bash
go get github.com/longlodw/thunder
```

## Usage

### Basic Usage

This example shows how to open a database, define a schema, insert data, and perform a basic query.

```go
package main

import (
	"fmt"
	"os"

	"github.com/longlodw/thunder"
)

func main() {
	// 1. Open the database
	// We use MsgpackMaUn for MessagePack marshaling/unmarshaling
	db, err := thunder.OpenDB(&thunder.MsgpackMaUn, "my.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Start a Read-Write Transaction
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	// 3. Define Schema (Create a Relation)
	users, err := tx.CreatePersistent("users", map[string]thunder.ColumnSpec{
		"id":       {},
		"username": {Indexed: true},
		"role":     {},
	})
	if err != nil {
		panic(err)
	}

	// 4. Insert Data
	err = users.Insert(map[string]any{"id": "1", "username": "alice", "role": "admin"})
	if err != nil {
		panic(err)
	}

	// Commit changes
	if err := tx.Commit(); err != nil {
		panic(err)
	}

	// 5. Query Data (Read-Only Transaction)
	tx, _ = db.Begin(false)
	defer tx.Rollback()

	users, _ = tx.LoadPersistent("users")

	// Filter for username "alice"
	filter, _ := thunder.Filter(thunder.Eq("username", "alice"))
	
	// Execute Select
	results, _ := users.Select(filter)

	for row, _ := range results {
		fmt.Printf("User: %s, Role: %s\n", row["username"], row["role"])
	}
}
```

### Recursive Queries

Thunder supports recursive Datalog-style queries, useful for traversing hierarchical data like organizational charts or file systems.

```go
// Example: Find all descendants of a manager
// Assume 'employees' table exists with 'id' and 'manager_id'

// Define a recursive query "path" with columns "ancestor" and "descendant"
qPath, _ := tx.CreateQuery("path", []string{"ancestor", "descendant"}, true)

// Rule 1: Direct reports (Base case)
baseProj, _ := employees.Project(map[string]string{
    "manager_id": "ancestor",
    "id":         "descendant",
})
qPath.AddBody(baseProj)

// Rule 2: Indirect reports (Recursive step)
// Join employees(manager=a, id=b) with path(ancestor=b, descendant=c)
edgeProj, _ := employees.Project(map[string]string{
    "manager_id": "ancestor", // a
    "id":         "join_key", // b
})
pathProj, _ := qPath.Project(map[string]string{
    "ancestor":   "join_key",   // b
    "descendant": "descendant", // c
})
qPath.AddBody(edgeProj, pathProj)

// Execute query to find descendants of ID "1"
filter, _ := thunder.Filter(thunder.Eq("ancestor", "1"))
results, _ := qPath.Select(filter)
```

## License

See the [LICENSE](LICENSE) file for details.
