package arangoHelper

import (
	"context"
	"errors"
	"fmt"

	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/arangodb/go-driver/v2/arangodb"
)

type AuthOptions struct {
	Url      []string
	Username string
	Password string
}

func NewArango[T any](ctx context.Context, clientId, dbName, collection string, model T) ArangoContainer[T] {
	m := ArangoContainer[T]{}
	RWdb, exist := GetClientById(clientId)
	if !exist {
		panic(errors.New("before this func define connection by AddNewConnection"))
	}
	m.Connection = RWdb
	m.Model = model
	m.Ctx = ctx
	m.Database, _ = m.Connection.GetDatabase(m.Ctx, dbName, &arangodb.GetDatabaseOptions{SkipExistCheck: true})
	m.CollectionName = collection
	return m
}

func (m *ArangoContainer[T]) CreateCollection() error {

	found, err := m.Database.CollectionExists(m.Ctx, m.CollectionName)
	if err != nil || found {
		return err
	}
	_, err = m.Database.CreateCollection(m.Ctx, m.CollectionName, &arangodb.CreateCollectionProperties{})
	return err
}
func (m *ArangoContainer[T]) Delete(filter AQL) (*T, error) {
	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}
	for k, v := range filter {
		if strings.HasPrefix(k, "__") {
			continue
		}
		scapedKey := "__" + strings.ReplaceAll(k, ".", "_")
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, scapedKey))
		filter[scapedKey] = v
		delete(filter, k)
	}
	if len(exp) > 0 {
		querystring += fmt.Sprintf("FILTER %s", strings.Join(exp, " && "))
	}
	querystring += " remove {_key: doc._key} in @@collection  "

	filter["@collection"] = m.CollectionName
	fmt.Println(querystring)
	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: filter})

	if err != nil {
		fmt.Printf("Query failed: %v", err)
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
func (m *ArangoContainer[T]) FindOne(filter AQL) (*T, error) {
	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}
	for k, v := range filter {
		if strings.HasPrefix(k, "__") {
			continue
		}
		scapedKey := "__" + strings.ReplaceAll(k, ".", "_")
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, scapedKey))
		filter[scapedKey] = v
		delete(filter, k)
	}
	if len(exp) > 0 {
		querystring += fmt.Sprintf("FILTER %s", strings.Join(exp, " && "))
	}
	querystring += " LIMIT 0,1 RETURN doc"

	filter["@collection"] = m.CollectionName
	fmt.Println(querystring)
	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: filter})

	if err != nil {
		fmt.Printf("Query failed: %v", err)
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

func (m *ArangoContainer[T]) FindAll(filter AQL, sort SORT, offset, limit uint64) ([]T, error) {

	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}

	for k, v := range filter {
		if strings.HasPrefix(k, "__") {
			continue
		}
		scapedKey := "__" + strings.ReplaceAll(k, ".", "_")
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, scapedKey))
		filter[scapedKey] = v
		delete(filter, k)
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
	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: filter})

	if err != nil {
		fmt.Printf("Query failed: %v", err)
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
func (m *ArangoContainer[T]) Update(filter AQL, data interface{}, limit uint64) ([]T, error) {
	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}

	for k, v := range filter {
		if strings.HasPrefix(k, "__") {
			continue
		}
		scapedKey := "__" + strings.ReplaceAll(k, ".", "_")
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, scapedKey))
		filter[scapedKey] = v
		delete(filter, k)
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
	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: filter})
	if err != nil {
		fmt.Printf("Query failed: %v", err)
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

func (m *ArangoContainer[T]) UpdateExpr(filter AQL, expression string, limit uint64) ([]T, error) {

	if filter == nil {
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection "
	exp := []string{}

	for k, v := range filter {
		if strings.HasPrefix(k, "__") {
			continue
		}
		scapedKey := "__" + strings.ReplaceAll(k, ".", "_")
		exp = append(exp, fmt.Sprintf("doc.%s == @%s", k, scapedKey))
		filter[scapedKey] = v
		delete(filter, k)

	}
	if len(exp) > 0 {
		querystring += fmt.Sprintf("FILTER %s", strings.Join(exp, " && "))
		if limit > 0 {
			querystring += fmt.Sprintf(" LIMIT 0,%d", limit)
		}
	}
	querystring += fmt.Sprintf(" update { _key: doc._key } with %s in @@collection", expression)

	querystring += " RETURN NEW"
	fmt.Println(querystring)
	filter["@collection"] = m.CollectionName
	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: filter})
	if err != nil {
		fmt.Printf("Query failed: %v", err)
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

// opt BindVars
// opt TransactionId used for running Query as Member of Transaction
// opt Count
func (m *ArangoContainer[T]) RawQuery(query string, opt *arangodb.QueryOptions) ([]T, error) {

	cursor, err := m.Database.Query(m.Ctx, query, opt)
	if err != nil {
		fmt.Printf("Query failed: %v", err)
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
func (m *ArangoContainer[T]) Upsert(filter AQL, data interface{}) ([]T, error) {
	querystring := "UPSERT @filter INSERT @data UPDATE @data INTO @@collection RETURN NEW"
	fmt.Println(querystring)
	bind := make(map[string]interface{})
	bind["filter"] = filter
	bind["data"] = data

	bind["@collection"] = m.CollectionName
	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: bind})
	if err != nil {
		fmt.Printf("Query failed: %v", err)
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

	querystring := "insert @data into @@collection return NEW"

	cursor, err := m.Database.Query(m.Ctx, querystring, &arangodb.QueryOptions{BindVars: AQL{"@collection": m.CollectionName, "data": m.Model}})
	if err != nil {
		fmt.Printf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	doc := make(map[string]interface{}, 1)
	_, err = cursor.ReadDocument(m.Ctx, &doc)
	return doc, err
}

func NewTransactionContext(ctx context.Context, clientId, dbName string, collectionContribute []string) (arangodb.Transaction, error) {
	cli, exist := GetClientById(clientId)
	if !exist {
		return nil, errors.New("before this func define connection by AddNewConnection")
	}

	db, err := cli.Database(ctx, dbName)
	if err != nil {
		return nil, err
	}

	trx, err := db.BeginTransaction(ctx, arangodb.TransactionCollections{Read: collectionContribute, Write: collectionContribute}, &arangodb.BeginTransactionOptions{})
	if err != nil {
		return nil, err
	}

	// tctx := trx.(trx)
	// tx:=TXStore{Tx: trx,TxContext: ctx,Db: db}
	return trx, nil
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

func CreateIndex(ctx context.Context, clientId, dbName, collectionName,indexName string, fields []string, sparse, unique bool) error {
	client, ok := GetClientById(clientId)
	if !ok {
		return errors.New("client not found")
	}
	db, err := client.Database(ctx, dbName)
	if err != nil {
		return err
	}
	col, err := db.Collection(ctx, collectionName)
	if err != nil {
		return err
	}
	_, _, err = col.EnsurePersistentIndex(ctx, fields, &arangodb.CreatePersistentIndexOptions{
		Name: indexName,
		Unique: &unique, // Set to true if you need a unique index
		Sparse: &sparse, // Set to true if null values should not be indexed
	})
	if err != nil {
		fmt.Printf("Failed to create index: %v\n", err)
		return err
	}
	return nil
}

// func (t *TXStore)Commit()error{
// 	return t.Tx.Commit(t.TxContext,&arangodb.CommitTransactionOptions{})
// }

// func (t *TXStore)Abort()error{
// 	return t.Tx.Abort(t.TxContext,&arangodb.AbortTransactionOptions{})
// }
