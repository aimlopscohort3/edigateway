# Use the official Golang image
FROM golang:1.20

# Set the working directory in the container
WORKDIR /app

# Copy dependency files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the Go application
RUN go build -o edi_gateway .

# Expose the application's port
EXPOSE 8086

# Run the Go application
CMD ["./edi_gateway"]