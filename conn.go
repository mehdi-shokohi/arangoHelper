package arangoHelper

import (
	// "crypto/tls"
	// "net"
	// "time"
	// "net/http"
	driver "github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"
)

var Holder *ConnManager

func init(){
	if Holder==nil{
		Holder = new(ConnManager)
		Holder.holder = make(map[string]driver.Client)
	}
}

func connect(auth AuthOptions) driver.Client {
	if auth.Url==nil{
		auth.Url = GetDefaultLocalUri()
	}
	if auth.Username==""{auth.Username = "root"}
	authConfig:= connection.Http2Configuration{
		Endpoint:   connection.NewRoundRobinEndpoints(auth.Url),
		Authentication: connection.NewBasicAuth(auth.Username,auth.Password),
		ContentType: connection.ApplicationJSON,
		
		// Transport: &http.Transport{
		// 	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		// 	DialContext: (&net.Dialer{
		// 		Timeout:   30 * time.Second,
		// 		KeepAlive: 90 * time.Second,
		// 	}).DialContext,
		// 	MaxIdleConns:          100,
		// 	IdleConnTimeout:       90 * time.Second,
		// 	TLSHandshakeTimeout:   10 * time.Second,
		// 	ExpectContinueTimeout: 1 * time.Second,
		// },
	}
	
	return driver.NewClient(connection.NewHttp2Connection(authConfig,))
}
// func connect(auth AuthOptions)arangodb.Client{

// 	// endpoint := connection.NewRoundRobinEndpoints(auth.Url)
// 	conn := connection.NewHttpConnection(makeHttpConnection(auth))

// 	// Create a client
// 	return  arangodb.NewClient(conn)

// }

// func connect(uri []string,authConfig driver.Authentication)(arangodb.Client,error){
// 	conn, err := http.NewConnection(http.ConnectionConfig{
// 		Endpoints: uri,
// 		ConnLimit: 32,
// 	})
// 	if err!=nil{
// 		return nil,err
// 	}
// 	client, err := driver.NewClient(driver.ClientConfig{Connection: conn,Authentication: authConfig})

// 	return client , err
// }

func GetClientById(key string)(driver.Client,bool){
	return Holder.read(key)
}
func AddNewConnection(Id string,auth AuthOptions) driver.Client {

	c:=connect(auth)
	if c!=nil{
		Holder.write(Id,c)
	}
	return c
}

func GetDefaultLocalUri()[]string{
	return []string{"http://localhost:8529"}
}