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
//This application is combined a mqtt consumer and a publisher,
//The publisher publish so many messages(100,1000) to the consumer, and then calculate the using time
func main(){
	
	var wg sync.WaitGroup
	
	start = time.Now()
	uri := "iot.eclipse.org:1883"
	
	topic := "testTimeTopic"
	wg.Add(1)
	go listen(uri, topic,&wg)
	
	publisher := connect("pub",uri)
	for i:=0; i<100; i++{
		payload := "msg:"
		payload += strconv.Itoa(i) 
		publisher.Publish(topic, 0, false, payload)
	}
	wg.Wait()
	
}

func listen(uri string, topic string,wg *sync.WaitGroup){
	consumer := connect("sub",uri)
	consumer.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message){
		//log.Print("---message:", string(msg.Payload()))
		
	})
	duration := time.Since(start)
	log.Println("----100 messages duration:",duration)
	log.Println("----average time duration:",duration/100)
	log.Println("---------end---------")
	
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