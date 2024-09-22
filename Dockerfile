FROM golang:1.23-alpine AS build-stage

WORKDIR /src

COPY go.* ./
RUN go mod download

COPY . .

RUN go build -o /app

FROM alpine AS release-stage

WORKDIR /

COPY --from=build-stage /app /app

EXPOSE 2002

ENTRYPOINT ["/app"]
