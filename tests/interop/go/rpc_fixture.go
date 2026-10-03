//go:build ignore

// rusty-mq interop fixture: Go amqp091-go client (one of the five target
// clients). Mirrors the pika/amqplib/java fixtures: handshake+auth, durable
// topology, confirmed persistent publish, manual-ack get with typed
// properties, and T25 request/reply over an exclusive server-named reply
// queue (§2.3 workload 4).
//
// Build/run (CI, module context provided by the workflow):
//
//	go mod init fixture && go get github.com/rabbitmq/amqp091-go@v1.10.0
//	go run rpc_fixture.go
//
// Requires AMQP_URL in the environment (amqp://user:pass@127.0.0.1:5672/%2F).
// Exit 0 = all assertions passed; prints PASS lines for the harness.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func fail(format string, args ...any) {
	log.Fatalf("FAIL "+format, args...)
}

func main() {
	url := os.Getenv("AMQP_URL")
	conn, err := amqp.Dial(url)
	if err != nil {
		fail("connect: %v", err)
	}
	defer conn.Close()
	fmt.Println("PASS connect handshake+auth")

	ch, err := conn.Channel()
	if err != nil {
		fail("channel: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		fail("confirm.select: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 16))

	if err := ch.ExchangeDeclare("go.ex", "topic", true, false, false, false, nil); err != nil {
		fail("exchange declare: %v", err)
	}
	if _, err := ch.QueueDeclare("go.q", true, false, false, false, nil); err != nil {
		fail("queue declare: %v", err)
	}
	if err := ch.QueueBind("go.q", "a.*.c", "go.ex", false, nil); err != nil {
		fail("bind: %v", err)
	}
	fmt.Println("PASS declare durable topology + bind")

	// Binary body: NUL byte, invalid UTF-8 sequence, high bytes.
	body := []byte{0, 1, 0xCE, 0, 0xE2, 0x9C, 0x93}
	err = ch.PublishWithContext(
		context.Background(),
		"go.ex",
		"a.b.c",
		false,
		false,
		amqp.Publishing{
			ContentType:     "application/json",
			DeliveryMode:    amqp.Persistent,
			Priority:        4,
			CorrelationId:   "go-corr",
			Headers:         amqp.Table{"x-n": int32(9), "x-s": "wörld", "x-b": true},
			Timestamp:       time.Now().Truncate(time.Second),
			MessageId:       "go-msg",
			Body:            body,
		},
	)
	if err != nil {
		fail("publish: %v", err)
	}
	if c := <-confirms; !c.Ack {
		fail("publish not confirmed")
	}
	fmt.Println("PASS confirmed persistent publish")

	g, ok, err := ch.Get("go.q", false)
	if err != nil {
		fail("basic.get: %v", err)
	}
	if !ok {
		fail("message missing")
	}
	if !bytes.Equal(g.Body, body) {
		fail("body mismatch")
	}
	if g.ContentType != "application/json" {
		fail("content_type lost: %q", g.ContentType)
	}
	if g.Priority != 4 {
		fail("priority lost: %d", g.Priority)
	}
	if g.CorrelationId != "go-corr" {
		fail("correlation lost: %q", g.CorrelationId)
	}
	switch n := g.Headers["x-n"].(type) {
	case int16:
		if n != 9 {
			fail("header x-n = %d", n)
		}
	case int32:
		if n != 9 {
			fail("header x-n = %d", n)
		}
	case int64:
		if n != 9 {
			fail("header x-n = %d", n)
		}
	default:
		fail("header x-n lost: %#v", g.Headers["x-n"])
	}
	if err := ch.Ack(g.DeliveryTag, false); err != nil {
		fail("ack: %v", err)
	}
	fmt.Println("PASS manual-ack get with typed properties")

	// Topic non-match discarded (confirmed first so a wrongly-routed copy
	// would already be visible to the get below).
	if err := ch.PublishWithContext(context.Background(), "go.ex", "a.b.d", false, false,
		amqp.Publishing{Body: []byte("no")}); err != nil {
		fail("non-match publish: %v", err)
	}
	if c := <-confirms; !c.Ack {
		fail("non-match publish not confirmed")
	}
	if _, ok, err := ch.Get("go.q", true); err != nil || ok {
		fail("a.b.d must not route through a.*.c (ok=%v err=%v)", ok, err)
	}
	fmt.Println("PASS topic non-match discarded")

	// T25: RPC over an exclusive server-named reply queue.
	replyQueue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		fail("reply queue declare: %v", err)
	}
	if replyQueue.Name == "" {
		fail("server must name the reply queue")
	}
	fmt.Println("PASS server-named exclusive reply queue")

	// Server side on a second connection echoes via the default exchange.
	server, err := amqp.Dial(url)
	if err != nil {
		fail("server connect: %v", err)
	}
	defer server.Close()
	sch, err := server.Channel()
	if err != nil {
		fail("server channel: %v", err)
	}
	if _, err := sch.QueueDeclare("go.rpc", true, false, false, false, nil); err != nil {
		fail("rpc queue declare: %v", err)
	}
	requests, err := sch.Consume("go.rpc", "rpc-server", false, false, false, false, nil)
	if err != nil {
		fail("server consume: %v", err)
	}
	go func() {
		for req := range requests {
			reply := amqp.Publishing{
				CorrelationId: req.CorrelationId,
				Body:          append([]byte("reply:"), req.Body...),
			}
			if err := sch.PublishWithContext(context.Background(), "", req.ReplyTo, false, false, reply); err != nil {
				log.Printf("reply publish: %v", err)
			}
			if err := sch.Ack(req.DeliveryTag, false); err != nil {
				log.Printf("server ack: %v", err)
			}
		}
	}()

	replies, err := ch.Consume(replyQueue.Name, "reply-consumer", true, true, false, false, nil)
	if err != nil {
		fail("reply consume: %v", err)
	}
	for i := 0; i < 3; i++ {
		corr := fmt.Sprintf("corr-%d", i)
		err := ch.PublishWithContext(context.Background(), "", "go.rpc", false, false, amqp.Publishing{
			ReplyTo:       replyQueue.Name,
			CorrelationId: corr,
			DeliveryMode:  amqp.Persistent,
			Body:          []byte(fmt.Sprintf("ping-%d", i)),
		})
		if err != nil {
			fail("rpc publish %d: %v", i, err)
		}
		select {
		case r := <-replies:
			if want := fmt.Sprintf("reply:ping-%d", i); string(r.Body) != want {
				fail("payload %q != %q", r.Body, want)
			}
			if r.CorrelationId != corr {
				fail("correlation %q != %q", r.CorrelationId, corr)
			}
		case <-time.After(5 * time.Second):
			fail("reply %d missing", i)
		}
	}
	fmt.Println("PASS 3 correlated request/reply roundtrips")

	if err := conn.Close(); err != nil {
		fail("connection close: %v", err)
	}
	if err := server.Close(); err != nil {
		fail("server connection close: %v", err)
	}
	fmt.Println("PASS connection close")
}
