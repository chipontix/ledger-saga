FROM golang:1.26.6-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /ledgersaga ./cmd/ledgersaga

FROM alpine:3.22
COPY --from=build /ledgersaga /usr/local/bin/ledgersaga
ENTRYPOINT ["ledgersaga"]
