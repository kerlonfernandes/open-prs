#!/bin/sh
if ! [ $(tty) = "/dev/tty1" ] ; then
    exit
fi