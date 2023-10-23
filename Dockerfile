# Use the official Golang 1.21 image as the base image
FROM golang:1.21

# Set the working directory inside the container
WORKDIR /kv

# Installing dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the Go application
RUN make build

# Set the entry point to your Go application
CMD ["./bin/kv_http_server"]
