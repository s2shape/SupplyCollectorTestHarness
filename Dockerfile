FROM microsoft/dotnet
MAINTAINER burilovmv@gmail.com
RUN git clone https://github.com/s2shape/SupplyCollectorTestHarness
RUN cd SupplyCollectorTestHarness && dotnet restore && dotnet build && dotnet publish -r linux-x64 --self-contained true
ENV PATH $PATH:/SupplyCollectorTestHarness/bin/Debug/netcoreapp2.2/linux-x64/
