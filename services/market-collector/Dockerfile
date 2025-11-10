FROM golang:1.22-bullseye AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /market-collector ./...

FROM gcr.io/distroless/static-debian11
COPY --from=builder /market-collector /market-collector
ENTRYPOINT ["/market-collector"]