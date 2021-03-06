#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/sdk:5.0-focal AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0-focal AS build
WORKDIR /src
COPY ["Datafordeler.GDBIntegrator/Datafordeler.GDBIntegrator.csproj", "Datafordeler.GDBIntegrator/"]
RUN dotnet restore "Datafordeler.GDBIntegrator/Datafordeler.GDBIntegrator.csproj"
COPY . .
WORKDIR "/src/Datafordeler.GDBIntegrator"
RUN dotnet build "Datafordeler.GDBIntegrator.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Datafordeler.GDBIntegrator.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Datafordeler.GDBIntegrator.dll"]
