package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

var (
	uri               = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	insecureTLS       = flag.Bool("insecure-tls", false, "Insecure TLS mode: don't check certificates")
	queue             = flag.String("queue", "", "AMQP queue name")
	ack               = flag.Bool("ack", false, "Acknowledge messages")
	maxMessages       = flag.Uint("max-messages", 5000, "Maximum number of messages to dump")
	outputDir         = flag.String("output-dir", ".", "Directory in which to save the dumped messages")
	jsonContent       = flag.Bool("json-content", true, "If the content of the message is a JSON object")
	verbose           = flag.Bool("verbose", false, "Print progress")
	messagesToAckFile = flag.String("messages-to-ack", "", "File with messages to ack")
	newExchange       = flag.String("new-exchange", "", "New exchange to move message to")
	newURI            = flag.String("new-uri", "", "AMQP URI")
	messagesToPostDir = flag.String("messages-to-post-dir", "", "Directory with messages to post")
)

type contain struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type messageToAck struct {
	RoutingKey     string    `json:"routingKey"`
	Contains       []contain `json:"contains"`
	ContainsString string    `json:"containsString"`
}

type messagesToAck struct {
	Messages []messageToAck `json:"messages"`
}

type messageProps struct {
	AppID           string `json:"app_id,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	ContentType     string `json:"content_type,omitempty"`
	CorrelationID   string `json:"correlation_id,omitempty"`
	DeliveryMode    uint8  `json:"delivery_mode,omitempty"`
	Expiration      string `json:"expiration,omitempty"`
	MessageID       string `json:"message_id,omitempty"`
	Timestamp       string `json:"timestamp,omitempty"`
	Priority        uint8  `json:"priority,omitempty"`
	ReplyTo         string `json:"reply_to,omitempty"`
	Type            string `json:"type,omitempty"`
	UserID          string `json:"user_id,omitempty"`
	Exchange        string `json:"exchange,omitempty"`
	RoutingKey      string `json:"routing_key,omitempty"`
}

type message struct {
	Content    interface{}  `json:"content"`
	Headers    amqp.Table   `json:"headers"`
	Properties messageProps `json:"properties"`
}

func main() {
	flag.Parse()
	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "Error: Unused command line arguments detected.\n")
		flag.Usage()
		os.Exit(2)
	}

	var err error

	if *messagesToPostDir != "" {
		err = postMessageToExchange(*uri, *messagesToPostDir, *newExchange)
	} else {
		err = dumpMessagesFromQueue(*uri, *queue, *maxMessages, *outputDir, *messagesToAckFile, *jsonContent, *ack, *newExchange, *newURI)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func dial(amqpURI string) (*amqp.Connection, error) {
	verboseLog(fmt.Sprintf("Dialing %q", amqpURI))
	if *insecureTLS && strings.HasPrefix(amqpURI, "amqps://") {
		tlsConfig := new(tls.Config)
		tlsConfig.InsecureSkipVerify = true
		conn, err := amqp.DialTLS(amqpURI, tlsConfig)
		return conn, err
	}
	conn, err := amqp.Dial(amqpURI)
	return conn, err
}

func readDir(dirPath string) ([]string, error) {
	var files []string
	fileInfo, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return files, err
	}
	for _, file := range fileInfo {
		fileName := file.Name()
		hidden, err := isHidden(fileName)

		if err != nil {
			return files, err
		}

		if !file.IsDir() && !hidden && strings.HasSuffix(fileName, ".json") {
			files = append(files, path.Join(dirPath, fileName))
		}
	}
	return files, nil
}

func postMessageToExchange(amqpURI string, dirToPost string, newExchange string) error {
	jsonFiles, err := readDir(dirToPost)
	if err != nil {
		return fmt.Errorf("Opening messages to ack file: %s", err)
	}

	conn, err := dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	defer func() {
		conn.Close()
		verboseLog("AMQP connection closed")
	}()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	defer func() {
		channel.Close()
		verboseLog("AMQP channel closed")
	}()

	timeLayout := "2006-01-02 15:04:05.999999999 -0700 MST"

	for _, filePath := range jsonFiles {
		jsonFile, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("Opening message file: %s", err)
		}

		var msg message

		byteValue, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			return fmt.Errorf("Reading message file: %s", err)
		}

		json.Unmarshal(byteValue, &msg)
		jsonFile.Close()
		byteValue = nil

		body, err := json.Marshal(msg.Content)
		if err != nil {
			return fmt.Errorf("Marshal message body: %s", err)
		}

		timestamp, err := time.Parse(timeLayout, msg.Properties.Timestamp)
		if err != nil {
			return fmt.Errorf("Time Parse: %s", err)
		}

		fmt.Println("Publishing Message")

		var exchange = msg.Properties.Exchange

		if newExchange != "" {
			exchange = newExchange
		}

		err = channel.Publish(exchange,
			msg.Properties.RoutingKey,
			false,
			false,
			amqp.Publishing{
				Headers:         msg.Headers,
				ContentType:     msg.Properties.ContentType,
				ContentEncoding: msg.Properties.ContentEncoding,
				DeliveryMode:    msg.Properties.DeliveryMode,
				Priority:        msg.Properties.Priority,
				CorrelationId:   msg.Properties.CorrelationID,
				ReplyTo:         msg.Properties.ReplyTo,
				Timestamp:       timestamp,
				Type:            msg.Properties.Type,
				UserId:          msg.Properties.UserID,
				AppId:           msg.Properties.AppID,
				Body:            body,
			})

		if err != nil {
			return fmt.Errorf("Publish %s: %s", filePath, err)
		}

		fmt.Printf("Message Published:%s\n", filePath)
	}

	return nil
}

func dumpMessagesFromQueue(amqpURI string, queueName string, maxMessages uint, outputDir string, messagesToAckFilePath string, isContentJSON bool, shouldAckMessage bool, newExchange string, newAmqpURI string) error {
	var msgsToAck messagesToAck
	var ackMessage bool
	searchToAck := false

	if messagesToAckFilePath != "" {
		jsonFile, err := os.Open(messagesToAckFilePath)
		if err != nil {
			return fmt.Errorf("Opening messages to ack file: %s", err)
		}

		byteValue, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			return fmt.Errorf("Reding sessages to ack file: %s", err)
		}

		json.Unmarshal(byteValue, &msgsToAck)
		jsonFile.Close()
		byteValue = nil
		searchToAck = true
		shouldAckMessage = false
	}

	if queueName == "" {
		return fmt.Errorf("Must supply queue name")
	}

	conn, err := dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}

	defer func() {
		conn.Close()
		verboseLog("AMQP connection closed")
	}()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	defer func() {
		channel.Close()
		verboseLog("AMQP channel closed")
	}()

	var publishChannel = channel
	var newExchangeConnection *amqp.Connection
	var newChannel *amqp.Channel

	if newAmqpURI != "" {
		newExchangeConnection, err = dial(newAmqpURI)

		if err != nil {
			return fmt.Errorf("New AMPQ URI Dial: %s", err)
		}

		defer func() {
			newExchangeConnection.Close()
			verboseLog("New AMQP connection closed")
		}()

		newChannel, err = newExchangeConnection.Channel()
		if err != nil {
			return fmt.Errorf("New channel: %s", err)
		}

		defer func() {
			newChannel.Close()
			verboseLog("New AMQP channel closed")
		}()

		publishChannel = newChannel
	}

	verboseLog(fmt.Sprintf("Pulling messages from queue %q", queueName))
	for messagesReceived := uint(0); messagesReceived < maxMessages; messagesReceived++ {
		ackMessage = shouldAckMessage
		msg, ok, err := channel.Get(queueName,
			false,
		)
		if err != nil {
			return fmt.Errorf("Queue get: %s", err)
		}

		if !ok {
			verboseLog("No more messages in queue")
			break
		}

		if newExchange != "" {
			err := publishChannel.Publish(newExchange,
				msg.RoutingKey,
				false,
				false,
				amqp.Publishing{
					Headers:         msg.Headers,
					ContentType:     msg.ContentType,
					ContentEncoding: msg.ContentEncoding,
					DeliveryMode:    msg.DeliveryMode,
					Priority:        msg.Priority,
					CorrelationId:   msg.CorrelationId,
					ReplyTo:         msg.ReplyTo,
					Timestamp:       msg.Timestamp,
					Type:            msg.Type,
					UserId:          msg.UserId,
					AppId:           msg.AppId,
					Body:            msg.Body,
				})

			if err != nil {
				fmt.Printf("Publish failed for msg-%04d: %s\n", messagesReceived, err)
			}
		} else if searchToAck {
			messageContent := string(msg.Body[:])
			for i := len(msgsToAck.Messages) - 1; i >= 0; i-- {
				if msg.RoutingKey == msgsToAck.Messages[i].RoutingKey {
					containsNeeded := len(msgsToAck.Messages[i].Contains)
					containsFound := 0
					for j := 0; j < containsNeeded; j++ {
						search := ""
						switch msgsToAck.Messages[i].Contains[j].Value.(type) {
						case string:
							search = "\"" + msgsToAck.Messages[i].Contains[j].Key + "\":\"" + msgsToAck.Messages[i].Contains[j].Value.(string) + "\""
						case float64:
							val := msgsToAck.Messages[i].Contains[j].Value.(float64)
							fmtp := "%." + strconv.Itoa(numDecPlaces(val)) + "f"
							search = "\"" + msgsToAck.Messages[i].Contains[j].Key + "\":" + fmt.Sprintf(fmtp, val)
						case bool:
							search = "\"" + msgsToAck.Messages[i].Contains[j].Key + "\":" + fmt.Sprintf("%t", msgsToAck.Messages[i].Contains[j].Value.(bool))
						}

						if search != "" && strings.Contains(messageContent, search) {
							containsFound++
						}
					}

					if msgsToAck.Messages[i].ContainsString != "" {
						containsNeeded++
						if strings.Contains(messageContent, msgsToAck.Messages[i].ContainsString) {
							containsFound++
						}
					}

					if containsNeeded == containsFound {
						ackMessage = true
						msgsToAck.Messages = append(msgsToAck.Messages[:i], msgsToAck.Messages[i+1:]...)
						break
					}
				}
			}
		}

		if ackMessage {
			msg.Ack(false)
			fmt.Printf("Acked msg-%04d\n", messagesReceived)
		}

		err = saveMessageToFile(msg, outputDir, messagesReceived, isContentJSON, ackMessage)
		if err != nil {
			return fmt.Errorf("Save message: %s", err)
		}
	}

	return nil
}

func getProperties(msg amqp.Delivery) map[string]interface{} {
	props := map[string]interface{}{
		"app_id":           msg.AppId,
		"content_encoding": msg.ContentEncoding,
		"content_type":     msg.ContentType,
		"correlation_id":   msg.CorrelationId,
		"delivery_mode":    msg.DeliveryMode,
		"expiration":       msg.Expiration,
		"message_id":       msg.MessageId,
		"priority":         msg.Priority,
		"reply_to":         msg.ReplyTo,
		"type":             msg.Type,
		"user_id":          msg.UserId,
		"exchange":         msg.Exchange,
		"routing_key":      msg.RoutingKey,
	}

	if !msg.Timestamp.IsZero() {
		props["timestamp"] = msg.Timestamp.String()
	}

	for k, v := range props {
		if v == "" {
			delete(props, k)
		}
	}

	return props
}

func saveMessageToFile(msg amqp.Delivery, outputDir string, counter uint, isContentJSON bool, wasAcked bool) error {
	extras := make(map[string]interface{})
	extras["properties"] = getProperties(msg)
	extras["headers"] = msg.Headers
	extras["acked"] = wasAcked
	if isContentJSON {
		var content interface{}
		json.Unmarshal(msg.Body, &content)
		extras["content"] = content
	} else {
		extras["content"] = string(msg.Body[:])
	}

	data, err := json.MarshalIndent(extras, "", "  ")
	if err != nil {
		return err
	}

	filePath := generateFilePath(outputDir, counter)
	err = ioutil.WriteFile(filePath, data, 0644)
	if err != nil {
		return err
	}

	fmt.Println(filePath)

	return nil
}

func generateFilePath(outputDir string, counter uint) string {
	return path.Join(outputDir, fmt.Sprintf("msg-%04d.json", counter))
}

func verboseLog(msg string) {
	if *verbose {
		fmt.Println("*", msg)
	}
}

func numDecPlaces(v float64) int {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	i := strings.IndexByte(s, '.')
	if i > -1 {
		return len(s) - i - 1
	}
	return 0
}
