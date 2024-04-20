package arangoHelper

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/arangodb/go-driver"
)


type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}
func TestConnection(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	client,ok:=GetClientById("defaultdb")
	var db driver.Database
	var  coll_exists bool
	if ok{
		db_exists, err := client.DatabaseExists(nil, "example")

		if !db_exists {
			db, err = client.CreateDatabase(nil, "example", nil)
	
			if err != nil {
				log.Fatalf("Failed to create database: %v", err)
			}
		} else {
			fmt.Println("That db exists already")
	
			db, err = client.Database(nil, "example")
	
			if err != nil {
				log.Fatalf("Failed to open existing database: %v", err)
			}
	
		}
	
		// Create collection
	
		coll_exists, err = db.CollectionExists(nil, "users")
	
		if coll_exists {
			fmt.Println("That collection exists already")
			PrintCollection(db, "users")
	}else{
		var col driver.Collection
		col, err = db.CreateCollection(nil, "users", nil)

		if err != nil {
			log.Fatalf("Failed to create collection: %v", err)
		}

		// Create documents
		users := []User{
			User{
				Name: "John",
				Age:  65,
			},
			User{
				Name: "Tina",
				Age:  25,
			},
			User{
				Name: "George",
				Age:  31,
			},
		}
		metas, errs, err := col.CreateDocuments(nil, users)

		if err != nil {
			log.Fatalf("Failed to create documents: %v", err)
		} else if err := errs.FirstNonNil(); err != nil {
			log.Fatalf("Failed to create documents: first error: %v", err)
		}

		fmt.Printf("Created documents with keys '%s' in collection '%s' in database '%s'\n", strings.Join(metas.Keys(), ","), col.Name(), db.Name())
	}
}

}

func PrintCollection(db driver.Database, name string) {

	var err error
	var cursor driver.Cursor

	querystring := "FOR doc IN users LIMIT 10 RETURN doc"

	cursor, err = db.Query(nil, querystring, nil)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		 doc:=make(map[string]interface{}) 
		var metadata driver.DocumentMeta

		metadata, err = cursor.ReadDocument(nil, &doc)

		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("Doc returned: %v", err)
		} else {
			fmt.Print("Dot doc ", metadata, doc, "\n")
		}
	}
}
type Terminology struct {
	ID            string `json:"id"   bson:"_id,omitempty"`
	NameSpace     string              `json:"ns" bson:"ns"`
	TerminologyID string              `json:"terminologyId"   bson:"terminologyId"`
	Code          interface{}         `json:"code"   bson:"code"`
	Value         interface{}         `json:"value"   bson:"value"`
	Description   string              `json:"description"   bson:"description"`
}
func TestArangoContainer(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","items",Terminology{})
	ter,err:=db.FindOne(AQL{"code":222})
	if err!=nil{
		fmt.Println(err)
	}
	fmt.Println(ter)

	results,err:=db.FindAll(AQL{"terminologyId":"ICD10CM"},nil,0,100)
	if err==nil{
		for _,v:=range results{
			fmt.Println(v)
		}
	}
}

func TestUpdate(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","items",Terminology{})
	results,err:=db.Update(AQL{"terminologyId":"ICD10-FA"},AQL{"coding_fa":"grade-baa"},20)
	if err==nil{
		for _,v:=range results{
			fmt.Println(v)
		}
	}

}

func TestUpdateFunc(t *testing.T) {
	AddNewConnection("defaultdb",[]string{"http://localhost:8530"},driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","users",AQL{})
	results,err:=db.UpdateExpr(AQL{"name":"mehdi"},`{spec:APPEND(doc.spec,"elem")}`,20)
	if err==nil{
		for _,v:=range results{
			fmt.Println(v)
		}
	}

}
func TestUpsert(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","items",Terminology{})
	results,err:=db.Upsert(AQL{"terminologyId":"mate_test"},Terminology{NameSpace: "ns003",TerminologyID: "mate_test",Code: "alt nelson mertin kent",Value: "okkkk_new"})
	if err==nil{
		for _,v:=range results{
			fmt.Println(v)
		}
	}

}

func TestRawQueryWithoutBindVar(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","items",Terminology{})
	results,err:=db.RawQuery("for doc in items sort doc._id desc limit 0,10 return doc",nil)
	if err==nil{
		for _,v:=range results{
			fmt.Println(v)
		}
	}

}
func TestRawQuery(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","items",Terminology{})
	results,err:=db.RawQuery("for doc in items filter doc.terminologyId == @termId sort doc._id desc limit 0,10 return doc",AQL{"termId":"ICPC2P"})
	if err==nil{
		for _,v:=range results{
			fmt.Println(v)
		}
	}

}
func TestInser(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))
	db:=NewArango(context.Background(),"defaultdb","_system","items",Terminology{TerminologyID: "TEST_ID",Code: 4420,Value: "okayyy"})
	results,err:=db.Insert()
	if err==nil{
			fmt.Println(results)
		
	}

}

func TestCreateDb(t *testing.T) {
	AddNewConnection("defaultdb",GetDefaultLocalUri(),driver.BasicAuthentication("root", "mate123"))

	err:=CreateDatabaseIfNotExist(context.Background(),"defaultdb","expOne")
	if err!=nil{
		fmt.Println(err)
	}
	err=CreateCollectionIfNotExist(context.Background(),"defaultdb","expOne","users")
	if err!=nil{
		fmt.Println(err)
	}
}

func TestTransaction(t *testing.T) {
	const defaultStore = "defaultdb"
	const defaultDb = "_system"
	AddNewConnection(defaultStore,[]string{"http://localhost:8530"},driver.BasicAuthentication("root", "mate123"))
	CreateCollectionIfNotExist(context.Background(),defaultStore,defaultDb,"users")
	CreateCollectionIfNotExist(context.Background(),defaultStore,defaultDb,"info")

	ctx:=context.Background()
	tx,err:=NewTransactionContext(ctx,defaultStore,defaultDb,[]string{"users","info"})
	if err!=nil{
		panic(err)
	}
	user:=make(map[string]interface{})
	user["name"]="mate"
	user["age"]=38

	dbUser:=NewArango(tx.TxContext,defaultStore,defaultDb,"users",user)
	dbUser.Insert()

	info:=make(map[string]interface{})
	info["score"] = 10
	info["user"] = user["name"]
	dbInfo:=NewArango(tx.TxContext,defaultStore,defaultDb,"info",info)
	dbInfo.Insert()
	tx.Commit()
	

}