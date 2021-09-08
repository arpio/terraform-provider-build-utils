#!/usr/bin/env python3
#
# Reads local archives of a binary Terraform provider, constructs the metadata
# files that implement the Terraform Provider Network Mirror[1] protocol,
# and copies the archives and those metadata files to the correct keys
# in a specified S3 bucket.
#
# This program requires the dirhasher[2] program to be installed.
#
# The input files may span one or more released versions, but since the output
# prefix is specific to a repository, namespace, and type, you'll need
# to invoke this program multiple times if you need to mirror providers
# belonging to multiple repositories, namespaces, or types.
#
# The release archives are copied to the mirror along side the metadata files,
# even though the metadata could point to existing URLs.  This simplifies URL
# generation in the output because we can use relative paths there and not
# have to know about the other URLs they're available at.
#
# Input archive files must be named in the standard way like:
#
#   {provider}_{version}_{os}_{arch}.zip
#
# This program constructs keys in the output (provider mirror) bucket that look like:
#
#   {mirror_prefix}/{namespace}/{type}/{version}.json
#   {mirror_prefix}/{namespace}/{type}/index.json
#
# The mirror prefix must contain the name of the registry ("registry.terraform.io"),
# the namespace of the provider ("hashicorp"), and the type of the provider ("aws"),
# in accordance with the network mirror protocol[1].
#
# An example output mirror bucket with prefix argument:
#
#   my-mirror-bucket/registry.terraform.io/hashicorp/aws/
#
# Some example input files that should be mirrored to that bucket at the prefix:
#
#   terraform-provider-aws_3.55.0_linux_x86_64.zip
#   terraform-provider-aws_3.55.0_windows_x86_64.zip
#   terraform-provider-aws_3.56.0_linux_x86_64.zip
#   terraform-provider-aws_3.56.0_windows_x86_64.zip
#
# And the example output keys that will be created in that bucket:
#
#   registry.terraform.io/hashicorp/aws/index.json
#   registry.terraform.io/hashicorp/aws/3.55.0.json
#   registry.terraform.io/hashicorp/aws/terraform-provider-aws_3.55.0_linux_x86_64.zip
#   registry.terraform.io/hashicorp/aws/terraform-provider-aws_3.55.0_windows_x86_64.zip
#   registry.terraform.io/hashicorp/aws/3.56.0.json
#   registry.terraform.io/hashicorp/aws/terraform-provider-aws_3.56.0_linux_x86_64.zip
#   registry.terraform.io/hashicorp/aws/terraform-provider-aws_3.56.0_windows_x86_64.zip
#
# [1] https://www.terraform.io/docs/internals/provider-network-mirror-protocol.html
# [2] https://github.com/arpio/dirhasher

import json
import os.path
import subprocess
import sys
from collections import defaultdict
from typing import Tuple, NamedTuple, Optional

import boto3
from botocore.exceptions import ClientError

DIRHASH_METADATA = 'dirhash'
"""Custom S3 metadata header we set so we can avoid uploading the archive if it's already current."""


class MirrorError(Exception):
    pass


class Archive(NamedTuple):
    path: str
    file_name: str
    provider: str
    version: str
    os: str
    arch: str

    @classmethod
    def parse(cls, archive_path: str) -> Optional['Archive']:
        file_name = os.path.basename(archive_path)
        if not file_name.endswith('.zip'):
            return None

        without_ext, _ = os.path.splitext(file_name)
        parts = without_ext.split('_', 3)
        if len(parts) != 4 or any(p == '' for p in parts):
            return None

        return Archive(
            path=archive_path,
            file_name=file_name,
            provider=parts[0],
            version=parts[1],
            os=parts[2],
            arch=parts[3],
        )


def parse_bucket_and_prefix(v: str) -> Tuple[str, str]:
    parts = v.split('/', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ''


def check_bucket_access(bucket_name: str) -> None:
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        raise MirrorError(f'Cannot access bucket {bucket_name}: {e}')


def dirhash(path: str) -> str:
    p = subprocess.run(['dirhasher', path], capture_output=True)
    if p.returncode != 0:
        raise MirrorError(f'dirhasher failed with code: {p.returncode}')
    return str(p.stdout, 'utf-8').strip()


def object_exists(obj: 's3.Object') -> bool:
    try:
        obj.load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise


def copy_archive(archive_path: str, mirror_obj: 's3.Object') -> Tuple[str, bool]:
    # Compute the dirhash of the archive
    h1 = dirhash(archive_path)

    # We can skip the copy if the mirror object exists with the correct dirhash
    copy_required = True
    if object_exists(mirror_obj) and mirror_obj.metadata.get(DIRHASH_METADATA) == h1:
        copy_required = False

    # Copy the archive to the mirror if it's not already there with the correct hash
    if not copy_required:
        return h1, False

    mirror_obj.upload_file(
        Filename=archive_path,
        ExtraArgs=dict(
            ContentType='application/zip',
            Metadata={
                DIRHASH_METADATA: h1,
            },
        )
    )
    return h1, True


def main() -> None:
    if len(sys.argv) < 3:
        raise MirrorError(f'usage: {sys.argv[0]} output-mirror-bucket/prefix provider-archive.zip...')

    mirror_bucket_name, out_prefix = parse_bucket_and_prefix(sys.argv[1])
    archive_paths = sys.argv[2:]

    s3 = boto3.resource('s3')
    check_bucket_access(mirror_bucket_name)
    mirror_bucket = s3.Bucket(mirror_bucket_name)

    # Gather info about the archives
    provider_versions = defaultdict(lambda: defaultdict(set))
    for archive_path in archive_paths:
        archive = Archive.parse(archive_path)
        if archive:
            provider_versions[archive.provider][archive.version].add(archive)

    for provider in sorted(provider_versions):
        print(f'{provider}')
        versions = provider_versions[provider]
        total_archives = 0

        index_data = {'versions': {}}
        for version in sorted(versions):
            print(f' {version}')
            archives = versions[version]

            version_data = {'archives': {}}
            for archive in sorted(archives):
                mirror_obj = mirror_bucket.Object(f'{out_prefix}{archive.file_name}')

                h1, copied = copy_archive(archive.path, mirror_obj)
                copy_status = '+' if copied else '='
                print(f'  {copy_status} {mirror_obj.bucket_name}/{mirror_obj.key} {h1}')

                # Construct the entry for the version file
                os_arch = f'{archive.os}_{archive.arch}'
                version_data['archives'][os_arch] = {
                    'hashes': [h1],
                    'url': archive.file_name,
                }
                total_archives += 1

            # Put the version JSON
            version_obj = mirror_bucket.Object(f'{out_prefix}{version}.json')
            version_obj.put(
                Body=bytes(json.dumps(version_data, sort_keys=True, indent=2), 'utf-8'),
                ContentType='application/json',
            )
            print(f'  + {version_obj.bucket_name}/{version_obj.key}')

            # Add an entry for this version to the index
            index_data['versions'][version] = {}

        # Put the index JSON
        index_obj = mirror_bucket.Object(f'{out_prefix}index.json')
        index_obj.put(
            Body=bytes(json.dumps(index_data, sort_keys=True, indent=2), 'utf-8'),
            ContentType='application/json',
        )

        print(f' <{total_archives} archives>')
        print(f'  + {index_obj.bucket_name}/{index_obj.key}')


if __name__ == '__main__':
    try:
        main()
        sys.exit(0)
    except MirrorError as e:
        print(e)
        sys.exit(1)
