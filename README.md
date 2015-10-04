# Abstract

For spies, a "drop site" is a place where a package can be left for another spy to pick up. In a similar sense, a digital drop site is a location on the web where a piece of information can be left, only to be retrieved later on.

The **dropsite** package provides a mechanism through which network connections can be proxied through online drop sites, locations where data can be placed and later retrieved.

# Virtual connections

A **virtual connection** is an indirect network connection made between a **client** and a **server** through a **proxy client** and a **proxy server**. A *virtual connection* emulates a TCP socket, leading the *client* and the *server* to believe they are directly connected.

Data is transferred from the *client* to the *server* in three steps, and data is transferred back in the inverse of these three steps. First, the *client* sends data to the *proxy client* using a TCP socket. Next, the *proxy client* sends the data to the *proxy server* using online drop sites. Finally, the *proxy server* sends the data to the *server* through another TCP socket.

The important thing to notice is that the *client* and the *server* both believe they are communicating directly over TCP. This makes it possible to tunnel all sorts of applications over a *virtual connection*.

There is one fine detail which is worth noting. While all data sent over the *virtual connection* is transmitted through drop sites, the *proxy client* and the *proxy server* still communicate some information over a TCP socket called the **coordination socket**. The *coordination socket* is used to determine which drop sites to use and when to use them. The *proxy client* and the *proxy server* also use the *coordination socket* to send acknowledgements when data is received and to convey errors when they occur.

# File transfers

*Virtual connections* are great for most things, but they are not optimal for transferring files. When transferring a file, latency is not very important and bandwidth is key. To optimize for this case, **dropsite** provides an API specifically for transferring files via drop sites.

A **file transfer** is the process by which a file is transferred across hosts. First, the **client** establishes an **FTP socket** (a TCP socket) to the **server**. Next, the *client* tells the *server* whether it wishes to upload or download a file. This step establishes who will be the **sender** and who will be the **receiver**. For the remainder of the transfer, the *sender* uploads data chunks to drop sites and tells the *receiver* about these drop sites.

# TODO

* Implement file transfers
