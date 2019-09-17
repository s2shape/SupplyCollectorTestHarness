FROM microsoft/dotnet
MAINTAINER burilovmv@gmail.com
RUN git clone https://github.com/s2shape/SupplyCollectorTestHarness
RUN cd SupplyCollectorTestHarness && dotnet restore && dotnet build && dotnet publish -r linux-x64 --self-contained true
ENV PATH $PATH:/SupplyCollectorTestHarness/bin/Debug/netcoreapp2.2/linux-x64/
RUN git clone https://github.com/s2shape/SupplyCollectorDataLoader
RUN cp -f SupplyCollectorDataLoader/SupplyCollectorDataLoader/SupplyCollectorDataLoader.csproj.nopkg SupplyCollectorDataLoader/SupplyCollectorDataLoader/SupplyCollectorDataLoader.csproj
RUN cd SupplyCollectorDataLoader && dotnet restore && dotnet build && dotnet publish -r linux-x64 --self-contained true
ENV PATH $PATH:/SupplyCollectorDataLoader/SupplyCollectorDataLoader/bin/Debug/netcoreapp2.2/linux-x64/
