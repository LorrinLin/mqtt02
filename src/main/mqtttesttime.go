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
	
	var wg,wg2,wg3 sync.WaitGroup
	
	uri := "iot.eclipse.org:1883"
	topic := "testTimeTopic"
	wg.Add(1)
	
	go listen(uri, topic, &wg, &wg2)
	wg.Wait()
	publisher := connect("pub",uri)
	
	start = time.Now()
	//set a timestamp when it ready to publish
	
	for i:=0; i<100; i++{
		payload := "msg:"
		payload += strconv.Itoa(i) 
		wg2.Add(1)
		wg3.Add(1)
		go publushMessage(publisher, topic, payload, &wg3)
	}
	wg3.Wait()
	wg2.Wait()
	//after the consumer received the messages, stop time
	duration := time.Since(start)
	log.Println("---- 100 messages duration:",duration)
	log.Println("---- average time duration:",duration/100)
	log.Println("---------end---------")
	
}

func publushMessage(publisher mqtt.Client, topic string, payload string, wg3 *sync.WaitGroup){
	publisher.Publish(topic, 0, false, payload)
	wg3.Done()
}

func listen(uri string, topic string,wg *sync.WaitGroup, wg2 *sync.WaitGroup){
	consumer := connect("sub",uri)
	consumer.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message){
		log.Print("--- message:", string(msg.Payload()))
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
	
	for !token.WaitTimeout(1 * time.Second){
	
	}
	
	if err := token.Error();err != nil{
		log.Fatal(err)
	}
	return client
}