# Arpio Terraform Provider Build Utilities

This repo isn't a Terraform provider (but its name makes it look kind of like one). Instead, it contains utilities that
the Arpio Terraform providers use during build, test, and release.

## mirror-provider.py

Generates contents required to serve
a [Provider Network Mirror Protocol](https://www.terraform.io/docs/internals/provider-network-mirror-protocol.html) from
binary provider release archives. The input archive files are read from an S3 bucket at a specified prefix and the
output files are written to an S3 bucket at a specified prefix. 
