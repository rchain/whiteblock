# Setup

 * Enable `rchain/whiteblock` repository in Drone settings
 * Set up drone secrets:
    * ssh_host

```bash
drone secret add --repository rchain/whiteblock --name ssh_host --value ...
```

    * ssh_username

```bash
drone secret add --repository rchain/whiteblock --name ssh_username --value ...
```

    * ssh_key_b64

```bash
drone secret add --repository rchain/whiteblock --name ssh_key_b64 --value <(base64 --wrap=0 <WHITEBLOCK_SSH_PRIVATE_KEY_FILE)
```

# Whiteblock image

Whiteblock assumes the Docker image it uses comes with few preinstalled
packages.  They need to be added therefore to the official rchain Docker
image.

Substitute `622f50bec209e5576d3a1b0d4e6e30e2de8bf3be949033863d0ff78646d3cb72`
for the value you get from Docker of course:

```
$ docker pull rchain/rnode:dev
$ docker run --detach rchain/rnode:dev
622f50bec209e5576d3a1b0d4e6e30e2de8bf3be949033863d0ff78646d3cb72
$ docker exec -it 622f50bec209e5576d3a1b0d4e6e30e2de8bf3be949033863d0ff78646d3cb72 /bin/bash
root@622f50bec209:/opt/docker# apt update
root@622f50bec209:/opt/docker# apt-get install -y openssh-server procps
root@622f50bec209:/opt/docker# exit
$ docker commit 622f50bec209e5576d3a1b0d4e6e30e2de8bf3be949033863d0ff78646d3cb72 rchainops/rnode:whiteblock
$ docker stop 622f50bec209e5576d3a1b0d4e6e30e2de8bf3be949033863d0ff78646d3cb72
$ docker push rchainops/rnode:whiteblock
```
