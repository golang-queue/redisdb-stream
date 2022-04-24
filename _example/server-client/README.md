# Example with server and client

Please refer the following steps to build server and client.

## Build server

```sh
go build -o app server/main.go
```

## Build client

```sh
go build -o agent client/main.go
```

## Usage

Run the multiple agent. (open two console in the same terminal)

```sh
./agent
```

Publish the message.

```sh
./app
```
