FROM golang:1.22-alpine AS build
WORKDIR /src

COPY go.mod ./
COPY main.go ./

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/example-service ./main.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/example-service /example-service

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/example-service"]
