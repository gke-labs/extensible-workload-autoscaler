FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod .
COPY main.go .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o sample-app main.go

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/sample-app /sample-app
ENTRYPOINT ["/sample-app"]
