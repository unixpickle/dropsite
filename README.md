# Abstract

For spies, a "drop site" is a place where a package can be left for another spy to pick up. In a similar sense, a digital drop site is a location on the web where a piece of information can be left, only to be retrieved by another client later on. The **dropsite** package provides a mechanism through which network connections can be proxied through online drop sites.

# Overview & Terminology

A **virtual connection** is an indirect network connection made between a **client** and a **server** through a **proxy client** and a **proxy server**.

Data is transferred from the *client* to the *server* in three steps, and data is transferred back in the inverse of these three steps. First, the *client* sends data to the *proxy client* using a TCP socket. Next, the *proxy client* sends the data to the *proxy server* using online drop sites. Finally, the *proxy server* sends the data to the *server* through another TCP socket.

The important thing to notice is that the *client* and the *server* both believe they are communicating directly over TCP. This makes it possible to tunnel all sorts of applications over a *virtual connection*.

There is one fine detail which is worth noting. While all data sent over the *virtual connection* is transmitted through drop sites, the *proxy client* and the *proxy server* still communicate some information over a TCP called the **coordination socket**. The *coordination socket* is used to determine which drop sites to use and when to use them. The *proxy client* and the *proxy server* also use the *coordination socket* to send acknowledgements when data is received and to convey errors when they occur.

# TODO

Implement everythinhg.
