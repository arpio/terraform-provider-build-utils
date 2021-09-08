#!/usr/bin/env python3
#
# Reads an S3 bucket containing binary releases of a Terraform provider and
# writes an S3 bucket containing the files that constitute a provider
# network mirror [1].
#
# The release archives are copied to the mirror along side the metadata files,
# even though the metadata could point to existing URLs.  This simplifies URL
# generation in the output because we can use relative paths there and not
# have to know about the other URLs they're available at.
#
# This program requires the dirhasher[2] program to be installed.
#
# During copying, if a mirror object exists for an archive that would be copied,
# and that mirror object has a dirhash metadata header, that dirhash value
# is used and the file is not copied.  This optimization lets us rebuild
# mirrors efficiently from large ranges of existing provider versions.
# However, this means that if the release archive is replaced after the
# mirror has been generated, with a content change that would change its dirhash,
# the mirror archives for that verison must be manually deleted and this program
# run again so it will copy the changed archives to the mirror (and compute their
# new dirhashes).  This is not expected to be a common situation.
#
# We expect keys in the input (releases) bucket to be formed like:
#
#   {rel_prefix}/{provider}_{version}_{os}_{arch}.zip
#
# We'll construct keys in the output (provider mirror) bucket that look like:
#
#   {mirror_prefix}/{namespace}/{type}/{version}.json
#   {mirror_prefix}/{namespace}/{type}/index.json
#
# The mirror prefix must usually be longer than the releases prefix, because it
# must contain the name of the registry ("registry.terraform.io"), the namespace
# of the provider ("hashicorp"), and the type of the provider ("aws"),
# in accordance with the network mirror protocol [1].
#
# Some example input keys:
#
#   terraform-provider-aws/3.55.0/terraform-provider-aws_3.55.0_linux_x86_64.zip
#   terraform-provider-aws/3.55.0/terraform-provider-aws_3.55.0_windows_x86_64.zip
#   terraform-provider-aws/3.56.0/terraform-provider-aws_3.56.0_linux_x86_64.zip
#   terraform-provider-aws/3.56.0/terraform-provider-aws_3.56.0_windows_x86_64.zip
#
# And the corresponding output keys:
#
#   registry.terraform.io/hashicorp/aws/index.json
#   registry.terraform.io/hashicorp/aws/3.55.0.json
#   registry.terraform.io/hashicorp/aws/3.55.0/terraform-provider-aws_3.55.0_linux_x86_64.zip
#   registry.terraform.io/hashicorp/aws/3.55.0/terraform-provider-aws_3.55.0_windows_x86_64.zip
#   registry.terraform.io/hashicorp/aws/3.56.0.json
#   registry.terraform.io/hashicorp/aws/3.56.0/terraform-provider-aws_3.56.0_linux_x86_64.zip
#   registry.terraform.io/hashicorp/aws/3.56.0/terraform-provider-aws_3.56.0_windows_x86_64.zip
#
# [1] https://www.terraform.io/docs/internals/provider-network-mirror-protocol.html
# [2] https://github.com/arpio/dirhasher

import json
import subprocess
import sys
import tempfile
from collections import defaultdict
from typing import Tuple, NamedTuple, Optional

import boto3
from botocore.exceptions import ClientError

DIRHASH_METADATA = 'dirhash'
"""Custom S3 metadata header we set so we can avoid uploading the archive if it's already current."""


class MirrorError(Exception):
    pass


class Archive(NamedTuple):
    key: str
    file_name: str
    provider: str
    version: str
    os: str
    arch: str

    @classmethod
    def parse(cls, key: str) -> Optional['Archive']:
        file_name = key.split('/')[-1]
        if not file_name.endswith('.zip'):
            return None

        without_ext = file_name[:-4]
        parts = without_ext.split('_', 3)
        if len(parts) != 4 or any(p == '' for p in parts):
            return None

        return Archive(
            key=key,
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


def dirhash(obj: 's3.Object') -> str:
    with tempfile.NamedTemporaryFile() as tf:
        obj.download_file(tf.name)
        p = subprocess.run(['dirhasher', tf.name], capture_output=True)
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


def copy_archive(rel_obj: 's3.Object', mirror_obj: 's3.Object') -> Tuple[str, bool]:
    # We can skip the copy if the mirror object exists and has a dirhash
    if object_exists(mirror_obj) and mirror_obj.metadata.get(DIRHASH_METADATA):
        return mirror_obj.metadata[DIRHASH_METADATA], False

    # Compute the dirhash of the release archive
    h1 = dirhash(rel_obj)

    # Copy the archive to the mirror if it's not already there with the correct hash
    metadata = dict(rel_obj.metadata)
    metadata[DIRHASH_METADATA] = h1
    mirror_obj.copy_from(
        CopySource={
            'Bucket': rel_obj.bucket_name,
            'Key': rel_obj.key,
        },
        Metadata=metadata,
        MetadataDirective='REPLACE'
    )
    return h1, True


def main() -> None:
    if len(sys.argv) != 3:
        raise MirrorError(f'usage: {sys.argv[0]} input-releases-bucket/prefix output-mirror-bucket/prefix')

    rel_bucket_name, rel_prefix = parse_bucket_and_prefix(sys.argv[1])
    mirror_bucket_name, out_prefix = parse_bucket_and_prefix(sys.argv[2])

    s3 = boto3.resource('s3')
    check_bucket_access(rel_bucket_name)
    check_bucket_access(mirror_bucket_name)

    rel_bucket = s3.Bucket(rel_bucket_name)
    mirror_bucket = s3.Bucket(mirror_bucket_name)

    # Gather all the released archives
    provider_versions = defaultdict(lambda: defaultdict(set))
    for rel_obj in rel_bucket.objects.filter(Prefix=rel_prefix):
        archive = Archive.parse(rel_obj.key)
        if archive:
            provider_versions[archive.provider][archive.version].add(archive)

    for provider in sorted(provider_versions):
        print(f'{provider}')
        versions = provider_versions[provider]

        index_data = {'versions': {}}
        for version in sorted(versions):
            print(f' {version}')
            archives = versions[version]

            version_data = {'archives': {}}
            for archive in sorted(archives):
                rel_obj = rel_bucket.Object(archive.key)
                mirror_obj = mirror_bucket.Object(f'{out_prefix}{archive.version}/{archive.file_name}')

                h1, copied = copy_archive(rel_obj, mirror_obj)
                copy_status = '+' if copied else '='
                print(f'  {copy_status} {mirror_obj.bucket_name}/{mirror_obj.key} {h1}')

                # Construct the entry for the version file with a relative url
                os_arch = f'{archive.os}_{archive.arch}'
                version_data['archives'][os_arch] = {
                    'hashes': [h1],
                    'url': f'{archive.version}/{archive.file_name}',
                }

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
        print(f' *')
        print(f'  + {index_obj.bucket_name}/{index_obj.key}')


if __name__ == '__main__':
    try:
        main()
        sys.exit(0)
    except MirrorError as e:
        print(e)
        sys.exit(1)
