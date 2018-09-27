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

	"github.com/streadway/amqp"
)

var (
	uri               = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	insecure_tls      = flag.Bool("insecure-tls", false, "Insecure TLS mode: don't check certificates")
	queue             = flag.String("queue", "", "AMQP queue name")
	ack               = flag.Bool("ack", false, "Acknowledge messages")
	maxMessages       = flag.Uint("max-messages", 1000, "Maximum number of messages to dump")
	outputDir         = flag.String("output-dir", ".", "Directory in which to save the dumped messages")
	jsonContent       = flag.Bool("json-content", true, "If the content of the message is a JSON object")
	verbose           = flag.Bool("verbose", false, "Print progress")
	messagesToAckFile = flag.String("messages-to-ack", "", "File with messages to ack")
)

type Messages struct {
	Messages []Message `json:"messages"`
}

type Message struct {
	RoutingKey     string    `json:"routingKey"`
	Contains       []Contain `json:"contains"`
	ContainsString string    `json:"containsString"`
}

type Contain struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func main() {
	flag.Parse()
	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "Error: Unused command line arguments detected.\n")
		flag.Usage()
		os.Exit(2)
	}
	err := DumpMessagesFromQueue(*uri, *queue, *maxMessages, *outputDir, *messagesToAckFile, *jsonContent)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func dial(amqpURI string) (*amqp.Connection, error) {
	VerboseLog(fmt.Sprintf("Dialing %q", amqpURI))
	if *insecure_tls && strings.HasPrefix(amqpURI, "amqps://") {
		tlsConfig := new(tls.Config)
		tlsConfig.InsecureSkipVerify = true
		conn, err := amqp.DialTLS(amqpURI, tlsConfig)
		return conn, err
	}
	conn, err := amqp.Dial(amqpURI)
	return conn, err
}

func DumpMessagesFromQueue(amqpURI string, queueName string, maxMessages uint, outputDir string, messagesToAckFilePath string, isContentJSON bool) error {
	var messages Messages
	var ackMessage bool
	searchToAck := false

	if messagesToAckFilePath != "" {
		jsonFile, err := os.Open(messagesToAckFilePath)
		if err != nil {
			return fmt.Errorf("Opening messages to ack file: %s", err)
		} else {
			byteValue, err := ioutil.ReadAll(jsonFile)
			if err != nil {
				return fmt.Errorf("Reding sessages to ack file: %s", err)
			}

			json.Unmarshal(byteValue, &messages)
			jsonFile.Close()
			byteValue = nil
			searchToAck = true
		}
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
		VerboseLog("AMQP connection closed")
	}()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	VerboseLog(fmt.Sprintf("Pulling messages from queue %q", queueName))
	for messagesReceived := uint(0); messagesReceived < maxMessages; messagesReceived++ {
		ackMessage = *ack
		msg, ok, err := channel.Get(queueName,
			ackMessage, // autoAck
		)
		if err != nil {
			return fmt.Errorf("Queue get: %s", err)
		}

		if !ok {
			VerboseLog("No more messages in queue")
			break
		}

		if searchToAck {
			message_content := string(msg.Body[:])
			for i := len(messages.Messages) - 1; i >= 0; i-- {
				if msg.RoutingKey == messages.Messages[i].RoutingKey {
					containsNeeded := len(messages.Messages[i].Contains)
					containsFound := 0
					for j := 0; j < containsNeeded; j++ {
						search := ""
						switch messages.Messages[i].Contains[j].Value.(type) {
						case string:
							search = "\"" + messages.Messages[i].Contains[j].Key + "\":\"" + messages.Messages[i].Contains[j].Value.(string) + "\""
						case float64:
							val := messages.Messages[i].Contains[j].Value.(float64)
							fmtp := "%." + strconv.Itoa(NumDecPlaces(val)) + "f"
							search = "\"" + messages.Messages[i].Contains[j].Key + "\":" + fmt.Sprintf(fmtp, val)
						case bool:
							search = "\"" + messages.Messages[i].Contains[j].Key + "\":" + fmt.Sprintf("%t", messages.Messages[i].Contains[j].Value.(bool))
						}

						if search != "" && strings.Contains(message_content, search) {
							containsFound++
						}
					}

					if messages.Messages[i].ContainsString != "" {
						containsNeeded++
						if strings.Contains(message_content, messages.Messages[i].ContainsString) {
							containsFound++
						}
					}

					if containsNeeded == containsFound {
						msg.Ack(true)
						fmt.Printf("Acked msg-%04d\n", messagesReceived)
						ackMessage = true
						messages.Messages = append(messages.Messages[:i], messages.Messages[i+1:]...)
					}
				}
			}
		}

		err = SaveMessageToFile(msg, outputDir, messagesReceived, isContentJSON, ackMessage)
		if err != nil {
			return fmt.Errorf("Save message: %s", err)
		}

	}

	return nil
}

func GetProperties(msg amqp.Delivery) map[string]interface{} {
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

func SaveMessageToFile(msg amqp.Delivery, outputDir string, counter uint, isContentJSON bool, wasAcked bool) error {
	extras := make(map[string]interface{})
	extras["properties"] = GetProperties(msg)
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

	filePath := GenerateFilePath(outputDir, counter)
	err = ioutil.WriteFile(filePath, data, 0644)
	if err != nil {
		return err
	}

	fmt.Println(filePath)

	return nil
}

func GenerateFilePath(outputDir string, counter uint) string {
	return path.Join(outputDir, fmt.Sprintf("msg-%04d.json", counter))
}

func VerboseLog(msg string) {
	if *verbose {
		fmt.Println("*", msg)
	}
}

func NumDecPlaces(v float64) int {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	i := strings.IndexByte(s, '.')
	if i > -1 {
		return len(s) - i - 1
	}
	return 0
}
