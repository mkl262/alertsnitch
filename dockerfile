FROM golang:1.24.4-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o alertsnitch

# Final stage
FROM scratch

COPY --from=builder /app/alertsnitch /alertsnitch

EXPOSE 9567

ENTRYPOINT ["/alertsnitch"]
