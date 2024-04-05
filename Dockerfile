FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /source

# Copy files
COPY . .
WORKDIR /source/src/garnet-operator

RUN dotnet restore
RUN dotnet build -c Release

# Copy and publish app and libraries
RUN dotnet publish -c Release -o /app --self-contained false -f net8.0

# Final stage/image
FROM mcr.microsoft.com/dotnet/aspnet:8.0.3-jammy-amd64
WORKDIR /app
COPY --from=build /app .

RUN apt update && apt install -y redis-tools

ENTRYPOINT ["dotnet", "garnet-operator.dll"]