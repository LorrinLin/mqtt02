package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"time"
	"log"
	"sync"
)

var(
	
	start time.Time
)
//This application is combined a publisher and so many consumers(100,1000),
//The publisher publish message to the consumers, and then test the using time
func main(){
	start = time.Now()
	uri := "iot.eclipse.org:1883"
	
	topic := "testTimeTopic"
	var wg sync.WaitGroup
	publisher := connect("pub",uri)
	//publisher.Publish(topic, 0, false, "hello")
	for i:= 0; i<10;i++{
		log.Println("-----------",i)
		wg.Add(1)
		go listen(uri, topic, &wg)
	
	}
	//wg.Wait()
	
	publisher.Publish(topic, 0, false, "hello")
	wg.Wait()
	duration := time.Since(start)
	log.Println("-------------------",duration)
	
}

func listen(uri string, topic string, wg *sync.WaitGroup){
	log.Println("-----------------")
	consumer := connect("sub",uri)
	consumer.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message){
			log.Println("subscribe callback function..")
		log.Print("---message from publisher:", string(msg.Payload()))
		//wg.Done()
	})
	log.Println("listen finished..")
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