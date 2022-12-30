# Peering Over Yggdrasil

## Introduction

[Yggdrasil](https://yggdrasil-network.github.io/) is an overlay network 
implementation that provides end-to-end encrypted IPv6 routing between all 
network participants. Yggdrasil offers a desirable property for peer-to-peer 
software: it [facilitates NAT traversal](https://yggdrasil-network.github.io/faq.html#i-am-running-yggdrasil-from-behind-a-nat-will-this-affect-my-connectivity)
, making it easier to establish connections from behind a router or gateway.

In the context of Secure Scuttlebutt, the overlay and routing technology 
provided by Yggdrasil can aid us in making connections without intermediate 
pubs or room servers (the intermediaries are instead other nodes in the 
Yggdrasil meshwork). This short tutorial will demonstrate how to configure
Scuttlebutt Solar nodes to replicate over Yggdrasil.

## Yggdrasil Installation

Begin by [installing Yggdrasil](https://yggdrasil-network.github.io/installation.html) 
if you have not already done so.

## Yggdrasil Configuration

[Generate a new config file](https://yggdrasil-network.github.io/configuration.html#generating-a-new-config-file) 
if this is your first time running Yggdrasil.

Pick at least one public peer from [the repository](https://github.com/yggdrasil-network/public-peers) 
and add it to the `Peers` section of your Yggdrasil config file (usually at `/etc/yggdrasil.conf`).

_Yggdrasil needs to have at least one public peer configured before your node 
can interact with the wider network. With a single public peer, your node will
send and receive data but will not route data for other Yggdrasil peers. Read 
the [practical peering blog post](https://yggdrasil-network.github.io/2019/03/25/peering.html) 
if you're interested in more details._

Restart Yggdrasil once you have updated the configuration. You can use the 
`yggdrasilctl getSelf` command to check your connection status; if the 
coordinates are empty, you are not yet connected. Once connected, you can
try to connect to some of the 
[Yggdrasil services](https://yggdrasil-network.github.io/services.html) to 
test the connection.

Note down the IPv6 address from the output of `yggdrasilctl getSelf`; 
this will form part of the URL at which other Solar peers can connect to your 
local Solar node.

## Solar Installation

Follow the [installation instruction for Solar](https://github.com/mycognosist/solar). 

_Note that release binaries are currently only available for Linux AMD64 and
ARM64._

## Solar Configuration

Since Solar does not yet feature a connection manager, it is recommended
to make a connection manually (unless you are running multiple nodes on the
same local network).

Run the first node, setting the TCP server IP to `::` (the IPv6 wildcard address) 
and turning selective replication off:

`RUST_LOG=info solar --ip :: --selective false`

Then run a second node from another computer with the same settings as the first 
node - with the addition of the connection option and the URL of the first node. 
For example:

`RUST_LOG=info solar --ip :: --selective false --connect "tcp://[200:9730:17c:7f5b:c7c6:c999:7b2a:c958]:8008?shs=o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc="`

In the example above, the IPv6 address (in square brackets) is the Yggdrasil
address of the first node. `8008` is the port that the first Solar node is
listening on and `o8lW...=` is the Scuttlebutt public key of the first node.

Watch the log output of either peer to see if the connection is made 
successfully. For example:

`[2022-12-30T07:53:57Z INFO  solar::actors::peer] 💃 connected to peer o8lWpyLeSqV/BJV9pbxFhKpwm6Lw5k+sqexYK+zT9Tc=.ed25519`

## Conclusion

With a bit of luck, you have now made your first Solar-Solar peering connection
over Yggdrasil. If the connection didn't work or you have suggestions for 
improving this short tutorial, please file an issue or a pull request on the
[Solar repo](https://github.com/mycognosist/solar).
