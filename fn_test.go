package arangoHelper

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"

	v2 "github.com/arangodb/go-driver"

	driver "github.com/arangodb/go-driver/v2/arangodb"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestConnection(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Username: "root", Password: "mate123"})
	client, ok := GetClientById("defaultdb")
	var db driver.Database
	var coll_exists bool
	if ok {
		db_exists, err := client.DatabaseExists(nil, "example")

		if !db_exists {
			db, err = client.CreateDatabase(context.Background(), "example", nil)

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
		} else {
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
			_, err := col.CreateDocuments(nil, users)

			if err != nil {
				log.Fatalf("Failed to create documents: %v", err)
			}

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
		doc := make(map[string]interface{})
		var metadata driver.DocumentMeta
		metadata, err = cursor.ReadDocument(nil, &doc)

		if v2.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("Doc returned: %v", err)
		} else {
			fmt.Print("Dot doc ", metadata, doc, "\n")
		}
	}
}

type Terminology struct {
	ID            string      `json:"id"   bson:"_id,omitempty"`
	NameSpace     string      `json:"ns" bson:"ns"`
	TerminologyID string      `json:"terminologyId"   bson:"terminologyId"`
	Code          interface{} `json:"code"   bson:"code"`
	Value         interface{} `json:"value"   bson:"value"`
	Description   string      `json:"description"   bson:"description"`
}

func TestArangoContainer(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Username: "root", Password: "mate123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "items", Terminology{})
	ter, err := db.FindOne(AQL{"code": 222})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(ter)

	results, err := db.FindAll(AQL{"terminologyId": "ICD10CM"}, nil, 0, 100)
	if err == nil {
		for _, v := range results {
			fmt.Println(v)
		}
	}
}

func TestUpdate(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Username: "root", Password: "mate123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "items", Terminology{})
	results, err := db.Update(AQL{"terminologyId": "ICD10-FA"}, AQL{"coding_fa": "grade-baa"}, 20)
	if err == nil {
		for _, v := range results {
			fmt.Println(v)
		}
	}

}

func TestUpdateFunc(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:8530"}, Username: "root", Password: "mate123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "users", AQL{})
	results, err := db.UpdateExpr(AQL{"name": "mehdi"}, `{spec:APPEND(doc.spec,"elem")}`, 20)
	if err == nil {
		for _, v := range results {
			fmt.Println(v)
		}
	}

}
func TestUpsert(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:8530"}, Username: "root", Password: "mate123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "cves_new", map[string]string{})
	data := make(map[string]interface{})
	json.Unmarshal([]byte(jsonCVE), &data)
	results, err := db.Upsert(AQL{"cveMetadata": AQL{"cveId": "CVE-2023-5752"}}, data)
	if err == nil {
		for _, v := range results {
			fmt.Println(v)
		}
	}

}

func TestRawQueryWithoutBindVar(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Username: "root", Password: "mate123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "items", Terminology{})
	results, err := db.RawQuery("for doc in items sort doc._id desc limit 0,10 return doc", nil)
	if err == nil {
		for _, v := range results {
			fmt.Println(v)
		}
	}

}
func TestRawQuery(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:27020"}, Username: "root", Password: "m123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "items", Terminology{})
	results, err := db.RawQuery("for doc in items filter doc.terminologyId == @termId sort doc._id desc limit 0,10 return doc", &driver.QueryOptions{BindVars:AQL{"termId": "ICPC2P"}})
	if err == nil {
		for _, v := range results {
			fmt.Println(v)
		}
	}

}
func TestCreateCollection(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:27020"}, Username: "root", Password: "m123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "expOne", Terminology{TerminologyID: "TEST_ID", Code: 4420, Value: "okayyy"})
	db.CreateCollection()
}

func TestInser(t *testing.T) {
	AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:27020"}, Username: "root", Password: "m123"})
	db := NewArango(context.Background(), "defaultdb", "_system", "expOne", Terminology{TerminologyID: "TTTTTTTTTTTTTTTTTTT", Code: 4420, Value: "okayyy"})
	results, err := db.Insert()
	if err == nil {
		fmt.Println(results)

	}

}

func TestCreateDb(t *testing.T) {
	cli := AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:27020"}, Username: "root", Password: "m123"})
	fmt.Println(cli.Version(context.Background()))
	err := CreateDatabaseIfNotExist(context.Background(), "defaultdb", "expOne")
	if err != nil {
		fmt.Println(err)
	}
	err = CreateCollectionIfNotExist(context.Background(), "defaultdb", "expOne", "users")
	if err != nil {
		fmt.Println(err)
	}
}

func TestTransaction(t *testing.T) {
	const defaultStore = "defaultdb"
	const defaultDb = "_system"
	AddNewConnection("defaultdb", AuthOptions{Url: []string{"http://localhost:27020"}, Username: "root", Password: "m123"})
	CreateCollectionIfNotExist(context.Background(), defaultStore, defaultDb, "users")
	CreateCollectionIfNotExist(context.Background(), defaultStore, defaultDb, "info")

	ctx := context.Background()
	trx, err := NewTransactionContext(ctx, defaultStore, defaultDb, []string{"users", "info"})
	if err != nil {
		panic(err)
	}
	user := make(map[string]interface{})
	user["name"] = "mate"
	user["age"] = 38

	querystring := "insert @data into @@collection return NEW"
	dbUser:=NewArango(ctx,defaultStore,defaultDb,"users",user)

	dbUser.RawQuery(querystring, &driver.QueryOptions{TransactionID: string(trx.ID()),BindVars: AQL{"@collection": "users", "data": user}})
	info := make(map[string]interface{})
	info["score"] = 10
	info["user"] = user["name"]

	dbInfo:=NewArango(ctx,defaultStore,defaultDb,"info",info)
	dbInfo.RawQuery( querystring, &driver.QueryOptions{TransactionID: string(trx.ID()),BindVars: AQL{"@collection": "info", "data": info}})
	// trx.Abort(ctx, &driver.AbortTransactionOptions{})
	trx.Commit(ctx, &driver.CommitTransactionOptions{})

}

const jsonCVE = `
{
	"containers": {
	  "cna": {
		"affected": [
		  {
			"collectionURL": "https://pypi.org",
			"defaultStatus": "affected",
			"packageName": "pip",
			"product": "pip",
			"repo": "https://github.com/pypa/pip",
			"vendor": "Pip maintainers",
			"versions": [
			  {
				"status": "unaffected",
				"version": "23.3"
			  }
			]
		  }
		],
		"credits": [
		  {
			"lang": "en",
			"type": "reporter",
			"user": "00000000-0000-4000-9000-000000000000",
			"value": "Paul Gerste"
		  }
		],
		"descriptions": [
		  {
			"lang": "en",
			"supportingMedia": [
			  {
				"base64": false,
				"type": "text/html",
				"value": "When installing a package from a Mercurial VCS URL  (ie \"pip install \nhg+...\") with pip prior to v23.3, the specified Mercurial revision could\n be used to inject arbitrary configuration options to the \"hg clone\" \ncall (ie \"--config\"). Controlling the Mercurial configuration can modify\n how and which repository is installed. This vulnerability does not \naffect users who aren't installing from Mercurial.<br>"
			  }
			],
			"value": "When installing a package from a Mercurial VCS URL  (ie \"pip install \nhg+...\") with pip prior to v23.3, the specified Mercurial revision could\n be used to inject arbitrary configuration options to the \"hg clone\" \ncall (ie \"--config\"). Controlling the Mercurial configuration can modify\n how and which repository is installed. This vulnerability does not \naffect users who aren't installing from Mercurial.\n"
		  }
		],
		"metrics": [
		  {
			"cvssV3_1": {
			  "attackComplexity": "LOW",
			  "attackVector": "LOCAL",
			  "availabilityImpact": "NONE",
			  "baseScore": 5.5,
			  "baseSeverity": "MEDIUM",
			  "confidentialityImpact": "NONE",
			  "integrityImpact": "HIGH",
			  "privilegesRequired": "LOW",
			  "scope": "UNCHANGED",
			  "userInteraction": "NONE",
			  "vectorString": "CVSS:3.1/AV:L/AC:L/PR:L/UI:N/S:U/C:N/I:H/A:N",
			  "version": "3.1"
			},
			"format": "CVSS",
			"scenarios": [
			  {
				"lang": "en",
				"value": "GENERAL"
			  }
			]
		  }
		],
		"problemTypes": [
		  {
			"descriptions": [
			  {
				"cweId": "CWE-77",
				"description": "CWE-77 Improper Neutralization of Special Elements used in a Command ('Command Injection')",
				"lang": "en",
				"type": "CWE"
			  }
			]
		  }
		],
		"providerMetadata": {
		  "dateUpdated": "2023-10-24T21:41:30.616Z",
		  "orgId": "28c92f92-d60d-412d-b760-e73465c3df22",
		  "shortName": "PSF"
		},
		"references": [
		  {
			"tags": [
			  "patch"
			],
			"url": "https://github.com/pypa/pip/pull/12306"
		  },
		  {
			"tags": [
			  "vendor-advisory"
			],
			"url": "https://mail.python.org/archives/list/security-announce@python.org/thread/F4PL35U6X4VVHZ5ILJU3PWUWN7H7LZXL/"
		  },
		  {
			"url": "https://lists.fedoraproject.org/archives/list/package-announce@lists.fedoraproject.org/message/YBSB3SUPQ3VIFYUMHPO3MEQI4BJAXKCZ/"
		  }
		],
		"source": {
		  "discovery": "UNKNOWN"
		},
		"title": "Mercurial configuration injectable in repo revision when installing via pip",
		"x_generator": {
		  "engine": "Vulnogram 0.1.0-dev"
		}
	  }
	},
	"search": [
	  {
		"K": "cveId",
		"V": "CVE-2023-5752"
	  },
	  {
		"K": "assignerShortName",
		"V": "PSF"
	  },
	  {
		"K": "product",
		"V": "pip"
	  },
	  {
		"K": "vendor",
		"V": "Pip maintainers"
	  }
	],
	"doc": "When installing a package from a Mercurial VCS URL  (ie \"pip install \nhg+...\") with pip prior to v23.3, the specified Mercurial revision could\n be used to inject arbitrary configuration options to the \"hg clone\" \ncall (ie \"--config\"). Controlling the Mercurial configuration can modify\n how and which repository is installed. This vulnerability does not \naffect users who aren't installing from Mercurial.\n\nMercurial configuration injectable in repo revision when installing via pip\n",
	"cveMetadata": {
	  "assignerOrgId": "28c92f92-d60d-412d-b760-e73465c3df22",
	  "assignerShortName": "PSF",
	  "cveId": "CVE-2023-5752",
	  "datePublished": 1698180965,
	  "dateReserved": 1698159841,
	  "dateUpdated": 1698183690,
	  "state": "PUBLISHED"
	}
  }
`
