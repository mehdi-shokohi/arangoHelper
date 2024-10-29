package arangoHelper

import (
	"context"

	driver "github.com/arangodb/go-driver/v2/arangodb"
	// "github.com/arangodb/go-driver/v2/arangodb"
)

type AQL map[string]interface{}
type SORT map[string]string
type ArangoContainer[T any] struct {
	Model          T
	Ctx            context.Context
	Connection     driver.Client
	DatabaseName   string
	CollectionName string
}

// type TXStore struct {
// 	Tx      driver.Transaction
// 	TxContext context.Context
// 	Db        arangodb.Database
// }
