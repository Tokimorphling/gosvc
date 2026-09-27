// Command kitex-example runs the Kitex echo server or client for manual testing.
//
//	go run ./examples/kitex/cmd -mode server -addr 127.0.0.1:9091
//	go run ./examples/kitex/cmd -mode client -addr 127.0.0.1:9091 -message hi
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"

	kitexexample "example.com/gosvc/examples/kitex"
	"example.com/gosvc/examples/kitex/api/echo/echoservice"
)

func main() {
	mode := flag.String("mode", "server", "server or client")
	addr := flag.String("addr", "127.0.0.1:9091", "listen or dial address")
	message := flag.String("message", "hello", "message to send in client mode")
	flag.Parse()

	switch *mode {
	case "server":
		if err := runServer(*addr); err != nil {
			log.Fatal(err)
		}
	case "client":
		if err := runClient(*addr, *message); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", *mode)
		os.Exit(2)
	}
}

func runServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("kitex echo server listening on %s", listener.Addr())

	svr := echoservice.NewServer(
		&kitexexample.EchoServiceImpl{Prefix: "echo: "},
		server.WithListener(listener),
	)
	return svr.Run()
}

func runClient(addr, message string) error {
	cli, err := echoservice.NewClient("echo", client.WithHostPorts(addr))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Echo(ctx, message)
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}
