all:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o pget

clean:
	rm ./pget
