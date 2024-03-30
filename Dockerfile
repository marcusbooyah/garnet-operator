FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG TARGETARCH
WORKDIR /source

# Copy files
COPY . .
WORKDIR /source/main/GarnetServer

RUN dotnet restore -a $TARGETARCH
RUN dotnet build -a $TARGETARCH -c Release

# Copy and publish app and libraries
RUN dotnet publish -a $TARGETARCH -c Release -o /app --self-contained false -f net8.0

# Final stage/image
FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .

RUN apt update && apt install -y redis-tools

# Run GarnetServer with an index size of 128MB
ENTRYPOINT ["/app/GarnetServer", "-i", "128m"]