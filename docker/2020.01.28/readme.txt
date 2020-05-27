
Build the execute file for linux in mac osx from source code by development engineer.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
go clean
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "./docker/2020.01.28/pump-plus-linux" -ldflags "-w"

build docker image file from execute file by operation and maintenance engineer.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker build -m 4g -t iotstream/pump-plus:01.28 -t iotstream/pump-plus:01.28.alpine -t iotstream/pump-plus:01.28.alpine.3.8 ./

docker container usage:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker run -d -it --name iotstream_pump iotstream/pump-plus:01.28.alpine.3.8

Mount Points:
/iotstream/config

User/Group
The image runs mbs under the iotstream user and group, which are created with a uid and gid of 1883.

Configuration:
When creating a container from the image, the default configuration values are used.
To use a custom configuration file, mount a local configuration file to /iotstream/config/pump-plus.ini

docker run -d -it --name iotstream_pump -v <absolute-path-to-configuration-file>:/iotstream/config/pump-plus.ini iotstream/pump-plus:<version>

example:
~~~~~~~~~~~~~~~~~~~~~
docker run -d -it --name iotstream_pump -v ~/IOTP_V20200128/pump-plus-release_20120128/docker/2020.01.28//conf/pump-plus-emqx-for-docker.ini:/iotstream/config/pump-plus.ini iotstream/pump-plus:01.28.alpine.3.8

See the log information from the container:
docker log -f iotstream_pump

Want to know more detail information about configuration and operation step
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Please see the notice.txt
