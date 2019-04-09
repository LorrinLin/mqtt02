package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"time"
	"log"
	"sync"
	"strconv"
)

var(
	start time.Time
)
//This application is combined a publisher and so many consumers(100,1000),
//The publisher publish message to the consumers, and then test the using time
func main(){
	//iot.eclipse.org
	//142.93.161.16
	uri := "142.93.161.16:1883"
	topic := "test"
	t := 100
	var wg, wg2 sync.WaitGroup
	publisher := connect("pub",uri)
	
	for i:= 0; i<t;i++{
		wg.Add(1)
		go listen(uri, topic, i, &wg, &wg2)
	}
	wg2.Add(t)
	wg.Wait()
	log.Println("------ before publish")
	start = time.Now()

	go publishMessage(publisher, topic)
	
	wg2.Wait()
	duration := time.Since(start)
	
	log.Println("---- 100 consumers got message duration:",duration)
	log.Println("---- average time duration:",duration/100)
	log.Println("---------end---------")
}

func publishMessage(publisher mqtt.Client, topic string){
	if publisher.IsConnectionOpen(){
		token := publisher.Publish(topic, 0, false, "hello")
		if token.Error() != nil{
			log.Println("err in publish..",token.Error())
		}
	}
}

func listen(uri string, topic string,i int,  wg *sync.WaitGroup, wg2 *sync.WaitGroup){
	conId := "sub"+strconv.Itoa(i)
	consumer := connect(conId,uri)
	consumer.Subscribe("test", 0, func(client mqtt.Client, msg mqtt.Message){
			log.Println("-----message from publisher:",string(msg.Payload()),i)
			wg2.Done()
	})
	wg.Done()
}

func connect(clientId string, uri string) mqtt.Client{
	opts := mqtt.NewClientOptions()
	opts.AddBroker(uri)
	opts.SetClientID(clientId)
	
	client := mqtt.NewClient(opts)
	
	token := client.Connect()
	
	for !token.WaitTimeout(3 * time.Second){
	
	}
	
	if err := token.Error();err != nil{
		log.Fatal(err)
	}
	return client
	
}