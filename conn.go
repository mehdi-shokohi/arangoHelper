package arangoHelper


import (


	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

var Holder *ConnManager

func init(){
	if Holder==nil{
		Holder = new(ConnManager)
		Holder.holder = make(map[string]driver.Client)
	}
}

func connect(uri []string,authConfig driver.Authentication)(driver.Client,error){
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: uri,
		ConnLimit: 32,
	})
	if err!=nil{
		return nil,err
	}
	client, err := driver.NewClient(driver.ClientConfig{Connection: conn,Authentication: authConfig})

	return client , err
}

func GetClientById(key string)(driver.Client,bool){
	return Holder.read(key)
}
func AddNewConnection(Id string,Uri []string,authConfig driver.Authentication) driver.Client {

	if authConfig==nil{
		authConfig = driver.BasicAuthentication("root", "")

	}
	c,err:=connect(Uri,authConfig)
	if err==nil{
		Holder.write(Id,c)
	}
	return c
}

func GetDefaultLocalUri()[]string{
	return []string{"http://localhost:8529"}
}