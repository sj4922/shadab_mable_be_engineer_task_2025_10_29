# Build stage
FROM golang:alpine AS builder

WORKDIR /app

# Set build-time environment variables
ENV GOEXPERIMENT=greenteagc

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN cd benchmark && go build -o ../benchmark .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

# Set runtime environment variables
ENV GOMEMLIMIT=32GiB
ENV GOGC=100

# Pipeline hyperparameters (can be overridden)
ENV PIPELINE_NUM_WORKERS=4
ENV PIPELINE_BATCH_SIZE=100
ENV PIPELINE_CHANNEL_BUF_SIZE=10
ENV PIPELINE_COLLECT_MIN_BUF=10
ENV PIPELINE_COLLECT_MAX_BUF=500

WORKDIR /app

# Create data directory and copy default input file
RUN mkdir -p /app/data
COPY --from=builder /app/data/mable_event_sample.json /app/data/

COPY --from=builder /app/benchmark .

ENTRYPOINT ["./benchmark"]
