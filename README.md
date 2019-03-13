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
