package arangoHelper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	driver "github.com/arangodb/go-driver"
)


func NewArango[T any](ctx context.Context, clientId, dbName, collection string, model T) ArangoContainer[T] {
	m := ArangoContainer[T]{}
	RWdb, exist := GetClientById(clientId)
	if !exist {
		panic(errors.New("before this func define connection by AddNewConnection"))
	}
	m.Connection = RWdb
	m.Model = model
	m.Ctx = ctx
	m.DatabaseName = dbName
	m.CollectionName = collection
	return m
}

func (m *ArangoContainer[T]) FindOne(filter map[string]interface{}) (*T, error) {
	db, err := m.Connection.Database(m.Ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}
	for k := range filter {
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, k))
	}
	if len(exp) > 0 {
		querystring += fmt.Sprintf("FILTER %s", strings.Join(exp, " && "))
	}
	querystring += " LIMIT 0,1 RETURN doc"

	filter["@collection"] = m.CollectionName
	fmt.Println(querystring)
	cursor, err := db.Query(m.Ctx, querystring, filter)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	doc := new(T)
	for {
		_, err := cursor.ReadDocument(m.Ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			// handle other errors
			continue
		}
	}

	return doc, nil
}

func (m *ArangoContainer[T]) FindAll(filter map[string]interface{}, sort map[string]string, offset, limit uint64) ([]T, error) {
	db, err := m.Connection.Database(m.Ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}

	for k := range filter {
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, k))
	}
	if len(exp) > 0 {
		querystring += fmt.Sprintf("FILTER %s", strings.Join(exp, " && "))
	}
	if sort != nil {
		sortExp := []string{}
		for k, v := range sort {
			sortExp = append(sortExp, fmt.Sprintf("doc.%s %s", k, v))
		}
		querystring += fmt.Sprintf(" SORT %s", strings.Join(sortExp, ","))
	} else {
		querystring += fmt.Sprintf(" SORT %s", "null")
	}
	querystring += " LIMIT @offset,@limit RETURN doc"

	filter["limit"] = limit
	filter["offset"] = offset
	filter["@collection"] = m.CollectionName

	fmt.Println(querystring)
	cursor, err := db.Query(m.Ctx, querystring, filter)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	docs := make([]T, 0)
	for {
		var doc T
		_, err := cursor.ReadDocument(m.Ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			continue
		}
		docs = append(docs, doc)
	}

	return docs, nil
}
func (m *ArangoContainer[T]) Update(filter map[string]interface{}, data interface{}, limit uint64) ([]T, error) {
	db, err := m.Connection.Database(m.Ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}

	for k := range filter {
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, k))
	}
	if len(exp) > 0 {
		querystring += fmt.Sprintf("FILTER %s", strings.Join(exp, " && "))
		if limit > 0 {
			querystring += fmt.Sprintf(" LIMIT 0,%d", limit)
		}
	}
	querystring += " update { _key: doc._key } with @data in @@collection"

	querystring += " RETURN NEW"
	fmt.Println(querystring)
	filter["data"] = data
	filter["@collection"] = m.CollectionName
	cursor, err := db.Query(m.Ctx, querystring, filter)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	docs := make([]T, 0)
	for {
		var doc T
		_, err := cursor.ReadDocument(m.Ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			continue
		}
		docs = append(docs, doc)
	}

	return docs, nil

}
func (m *ArangoContainer[T]) RawQuery(query string) ([]T, error) {
	db, err := m.Connection.Database(m.Ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	cursor, err := db.Query(m.Ctx, query, nil)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	docs := make([]T, 0)
	for {
		var doc T
		_, err := cursor.ReadDocument(m.Ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			continue
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

func (m *ArangoContainer[T]) Insert() (map[string]interface{}, error) {
	db, err := m.Connection.Database(m.Ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	querystring := "Insert @data into @@collection return NEW"

	cursor, err := db.Query(m.Ctx, querystring, map[string]interface{}{"@collection": m.CollectionName, "data": m.Model})
	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	doc := make(map[string]interface{}, 0)
	for {
		_, err := cursor.ReadDocument(m.Ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			continue
		}
	}

	return doc, nil
}
/*
Use returned Context on Queries and finally transaction be done with
db.AbortTransaction() or db.CommitTransaction()

*/
func (m *ArangoContainer[T])NewTransactionContext()(context.Context,error){
	db, err := m.Connection.Database(m.Ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	trxid, err := db.BeginTransaction(m.Ctx, driver.TransactionCollections{Exclusive: []string{m.CollectionName}}, nil)
	if err != nil {
		return nil,err
	}

	tctx := driver.WithTransactionID(m.Ctx, trxid)
	return tctx,nil
}

func CreateDatabaseIfNotExist(ctx context.Context, clientId, dbName string) error {
	client, ok := GetClientById(clientId)
	if !ok {
		return errors.New("client not found")
	}
	if ok, _ := client.DatabaseExists(ctx, dbName); ok {
		return nil
	}
	_, err := client.CreateDatabase(ctx, dbName, nil)

	return err
}

func CreateCollectionIfNotExist(ctx context.Context, clientId, dbName, collectionName string) error {
	client, ok := GetClientById(clientId)
	if !ok {
		return errors.New("client not found")
	}
	db, err := client.Database(ctx, dbName)
	if err != nil {
		return err
	}
	if ok, _ := db.CollectionExists(ctx, collectionName); ok {
		return nil

	}
	_, err = db.CreateCollection(ctx, collectionName, nil)
	return err
}

