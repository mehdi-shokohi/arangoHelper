package arangoHelper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	driver "github.com/arangodb/go-driver"
)

type ArangoContainer[T any] struct {
	Model          T
	Ctx            context.Context
	Connection     driver.Client
	DatabaseName   string
	CollectionName string
}

func NewArango[T any](ctx context.Context, clientId, dbName,collection string, model T) ArangoContainer[T] {
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

func (m *ArangoContainer[T]) FindOne(ctx context.Context, filter map[string]interface{}) (*T, error) {
	db, err := m.Connection.Database(ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	if filter == nil{
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection " 
	exp:=[]string{}
	for k:=range filter{
		exp = append(exp, fmt.Sprintf("doc.%s == @%s",k,k))
	}
	if len(exp)>0{
		querystring += fmt.Sprintf("FILTER %s",strings.Join(exp," && "))
	}
	querystring+=" LIMIT 0,1 RETURN doc"
//map[string]interface{}{"@collection": m.CollectionName}
	// querystring = fmt.Sprintf(querystring,conditions)
	filter["@collection"] = m.CollectionName
	fmt.Println(querystring)
	cursor, err := db.Query(ctx, querystring, filter)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	doc := new(T)
	for {
		_, err := cursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			// handle other errors
			continue
		}
	}

	return doc, nil
}



func (m *ArangoContainer[T]) FindAll(ctx context.Context, filter map[string]interface{},offset,limit  uint64) ([]T, error) {
	db, err := m.Connection.Database(ctx, m.DatabaseName)
	if err != nil {
		return nil, err
	}
	if filter == nil{
		filter = make(map[string]interface{})
	}
	querystring := "FOR doc IN @@collection " 
	exp:=[]string{}
	
	for k:=range filter{
		exp = append(exp, fmt.Sprintf("doc.%s == @%s",k,k))
	}
	if len(exp)>0{
		querystring += fmt.Sprintf("FILTER %s",strings.Join(exp," && "))
	}
	querystring+=" LIMIT @offset,@limit RETURN doc"

	filter["limit"] = limit
	filter["offset"] = offset
	filter["@collection"] = m.CollectionName

	fmt.Println(querystring)
	cursor, err := db.Query(ctx, querystring, filter)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
		return nil, err
	}
	defer cursor.Close()
	docs := make([]T,0)
	for {
		var doc T
		_, err := cursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			continue
		}
		docs = append(docs, doc)
	}

	return docs, nil
}