# Development and Testing Guide <br/> Pip.Services Kafka for .NET

This document provides high-level instructions on how to build and test the microservice.

* [Environment Setup](#setup)
* [Installing](#install)
* [Building](#build)
* [Testing](#test)
* [Release](#release)
* [Contributing](#contrib) 

## <a name="setup"></a> Environment Setup

This is a .NET project with multiple build targets for .NET full and .NET core frameworks. 
To be able to develop and test it you need to install the following components:
- Visual Studio 2019 Professional or Community Edition: https://www.visualstudio.com 
- Core .NET SDK with Visual Studio extentions: https://www.microsoft.com/net/core 

To work with GitHub code repository you need to install Git from: https://git-scm.com/downloads

## <a name="install"></a> Installing

After your environment is ready you can check out source code from the Github repository:
```bash
git clone git@github.com:pip-services3-dotnet/pip-services3-kafka-dotnet.git
```

## <a name="build"></a> Building

Build the project from inside the Visual Studio. In the future we are planning to support
build process from command line. But today that option is not available

Command to compile binaries:
```bash
./build.sh
```

## <a name="test"></a> Testing

Before you execute tests you need to set configuration options in config.json file.
As a starting point you can use example from config.example.json:

```bash
copy config/config.example.yaml config/config.yaml
``` 

After that check all configuration options. Specifically, pay attention to connection options
for database and dependent microservices. For more information check [Configuration Guide](Configuration.md) 

Command to run unit tests:
```bash
./test.sh
```

## <a name="release"></a> Release

Detail description of the NuGet release publishing procedure 
is described at http://docs.nuget.org/ndocs/create-packages/publish-a-package

Before publishing a new release you shall register on NuGet site and get you API Key.
Then register your API Key as:

```bash
nuget setApiKey Your-API-Key
```

Update release notes in CHANGELOG. Update version number and release details in nuspec file.
After that compile and test the project. Then create a nuget package:

```bash
nuget pack pip-services3-kafka-dotnet.nuspec
```

Publish the package on nuget global repository

```bash
nuget push PipServices3.kafka.<version>.nuspec -Source https://www.nuget.org/api/v2/package
```

Or run command to pack and push:
```bash
./release.sh
```

## <a name="contrib"></a> Contributing

Developers interested in contributing should read the following instructions:

- [How to Contribute](http://www.pipservices.org/contribute/)
- [Guidelines](http://www.pipservices.org/contribute/guidelines)
- [Styleguide](http://www.pipservices.org/contribute/styleguide)
- [ChangeLog](../CHANGELOG.md)

> Please do **not** ask general questions in an issue. Issues are only to report bugs, request
  enhancements, or request new features. For general questions and discussions, use the
  [Contributors Forum](http://www.pipservices.org/forums/forum/contributors/).

It is important to note that for each release, the [ChangeLog](../CHANGELOG.md) is a resource that will
itemize all:

- Bug Fixes
- New Features
- Breaking Changes