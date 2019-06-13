package mqtt

import (
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const _default string = "default"

var store sync.Map

func connect(opts *mqtt.ClientOptions, timeout ...time.Duration) (mqtt.Client, error) {
	client := mqtt.NewClient(opts)
	token := client.Connect()
	_timeout := time.Duration(3)
	if len(timeout) > 0 {
		_timeout = timeout[0]
	}

	for !token.WaitTimeout(_timeout * time.Second) {
		fmt.Println("waiting", token.Error())
	}

	if err := token.Error(); err != nil {
		log.Fatal(err)
	}

	if !client.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	return client, nil
}

func createClientOptions(uri *url.URL, clientId ...string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("%s://%s", uri.Scheme, uri.Host))
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)

	_clientId := time.Now().Format(time.RFC3339Nano)
	if len(clientId) > 0 && clientId[0] != "" {
		_clientId = clientId[0]
	}
	log.Println("connected mqtt client id", _clientId)
	opts.SetClientID(_clientId)

	return opts
}

// New Create new mqtt connection
func New(uri string, timeout time.Duration, key ...string) (mqtt.Client, error) {
	uriUrl, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	_key := _default
	if len(key) > 0 && key[0] != "" {
		_key = key[0]
	}

	opts := createClientOptions(uriUrl, key...)
	client, err := connect(opts, timeout)
	if err != nil {
		return nil, err
	}
	store.Store(_key, client)

	return client, nil
}

type SubscribeHandler func([]byte)

// Subscribe mqqt Subscribe helper
func Subscribe(topic string, qos int, f SubscribeHandler, key ...string) error {
	client := Client(key...)
	if client == nil {
		return fmt.Errorf("mqtt is not connected")
	}

	client.Subscribe(topic, byte(qos), func(client mqtt.Client, msg mqtt.Message) {
		f(msg.Payload())
	})
	return nil
}

// Client Client return mqttt client connection
func Client(key ...string) mqtt.Client {
	_key := _default
	if len(key) > 0 {
		_key = key[0]
	}

	if v, ok := store.Load(_key); ok {
		data := v.(mqtt.Client)
		return data
	}

	return nil
}

// Close Close is close all connection client
func Close(timeout time.Duration, key ...string) error {
	closeConn := func(client mqtt.Client) error {
		if client == nil {
			return fmt.Errorf("client not available")
		}

		if !client.IsConnectionOpen() {
			log.Println("client already disconnected")
		}

		client.Disconnect(uint(timeout))
		return nil
	}

	if len(key) > 0 {
		return closeConn(Client(key...))
	}

	store.Range(func(key interface{}, value interface{}) bool {
		log.Println("disconnect mqtt client ", key)
		closeConn(Client(key.(string)))
		return true
	})

	return nil
}

func init() {
	store = sync.Map{}
}
