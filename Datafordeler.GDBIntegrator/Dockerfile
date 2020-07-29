#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["Datafordeler.DBIntegrator/Datafordeler.DBIntegrator.csproj", "Datafordeler.DBIntegrator/"]
RUN dotnet restore "Datafordeler.DBIntegrator/Datafordeler.DBIntegrator.csproj"
COPY . .
WORKDIR "/src/Datafordeler.DBIntegrator"
RUN dotnet build "Datafordeler.DBIntegrator.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Datafordeler.DBIntegrator.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Datafordeler.DBIntegrator.dll"]