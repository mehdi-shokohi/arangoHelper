package arangoHelper

import (
	"context"

	driver "github.com/arangodb/go-driver"
)

type AQL map[string]interface{}

type ArangoContainer[T any] struct {
	Model          T
	Ctx            context.Context
	Connection     driver.Client
	DatabaseName   string
	CollectionName string
}

type TXStore struct{
	TxId driver.TransactionID
	TxContext context.Context
	Db driver.Database
}
