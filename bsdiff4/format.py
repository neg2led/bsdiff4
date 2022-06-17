from __future__ import absolute_import

import bz2
import sys
from enum import Enum

import brotli

if sys.version_info[0] == 2:
    from cStringIO import StringIO as BytesIO
else:
    from io import BytesIO

# Magic header for BSDIFF4
BSDIFF_MAGIC = b"BSDIFF40"

# Magic header for BSDF2:
# 0 5 BSDF2
# 5 1 compression type for control stream
# 6 1 compression type for diff stream
# 7 1 compression type for extra stream
BSDF2_MAGIC = b"BSDF2"


# import bsdiff4.core as core
from bsdiff4 import core


class CompressionType(Enum):
    """CompressionType is an enumeration of the available compression types."""

    NoCompression = 0
    BZ2 = 1
    Brotli = 2


def create_bsdf2_decompressor(type: CompressionType):
    if type == CompressionType.NoCompression:
        return lambda x: x
    elif type == CompressionType.BZ2:
        return bz2.BZ2Decompressor
    elif type == CompressionType.Brotli:
        return brotli
    else:
        raise ValueError("Unknown compression type")


def write_patch(fo, len_dst, tcontrol, bdiff, bextra):
    """write a BSDIFF4-format patch to stream 'fo'"""
    fo.write(BSDIFF_MAGIC)
    faux = BytesIO()
    # write control tuples as series of offts
    for c in tcontrol:
        for x in c:
            faux.write(core.encode_int64(x))
    # compress each block
    bcontrol = bz2.compress(faux.getvalue())
    bdiff = bz2.compress(bdiff)
    bextra = bz2.compress(bextra)
    for n in len(bcontrol), len(bdiff), len_dst:
        fo.write(core.encode_int64(n))
    fo.write(bcontrol)
    fo.write(bdiff)
    fo.write(bextra)


def read_patch(fi, header_only=False):
    """read a BSDIFF4-format patch from stream 'fi'"""
    magic = fi.read(8)

    if magic[:4] == BSDF2_MAGIC[:4]:
        control_decompressor = create_bsdf2_decompressor(CompressionType(magic[5]))
        diff_decompressor = create_bsdf2_decompressor(CompressionType(magic[6]))
        extra_decompressor = create_bsdf2_decompressor(CompressionType(magic[7]))
        pass
    elif magic[:7] == BSDIFF_MAGIC[:7]:
        control_decompressor, diff_decompressor, extra_decompressor = (
            bz2.BZ2Decompressor,
            bz2.BZ2Decompressor,
            bz2.BZ2Decompressor,
        )
        pass
    else:
        raise ValueError("Not a BSDIFF4 patch")

    # length headers
    len_control = core.decode_int64(fi.read(8))
    len_diff = core.decode_int64(fi.read(8))
    len_dst = core.decode_int64(fi.read(8))
    # read the control header
    bcontrol = control_decompressor.decompress(fi.read(len_control))
    tcontrol = [
        (
            core.decode_int64(bcontrol[i : i + 8]),
            core.decode_int64(bcontrol[i + 8 : i + 16]),
            core.decode_int64(bcontrol[i + 16 : i + 24]),
        )
        for i in range(0, len(bcontrol), 24)
    ]
    if header_only:
        return len_control, len_diff, len_dst, tcontrol
    # read the diff and extra blocks
    bdiff = diff_decompressor.decompress(fi.read(len_diff))
    bextra = extra_decompressor.decompress(fi.read())
    return len_dst, tcontrol, bdiff, bextra


def read_data(path):
    with open(path, "rb") as fi:
        data = fi.read()
    return data


def diff(src_bytes, dst_bytes):
    """diff(src_bytes, dst_bytes) -> bytes

    Return a BSDIFF4-format patch (from src_bytes to dst_bytes) as bytes.
    """
    faux = BytesIO()
    write_patch(faux, len(dst_bytes), *core.diff(src_bytes, dst_bytes))
    return faux.getvalue()


def file_diff(src_path, dst_path, patch_path):
    """file_diff(src_path, dst_path, patch_path)

    Write a BSDIFF4-format patch (from the file src_path to the file dst_path)
    to the file patch_path.
    """
    src = read_data(src_path)
    dst = read_data(dst_path)
    with open(patch_path, "wb") as fo:
        write_patch(fo, len(dst), *core.diff(src, dst))


def patch(src_bytes, patch_bytes):
    """patch(src_bytes, patch_bytes) -> bytes

    Apply the BSDIFF4-format patch_bytes to src_bytes and return the bytes.
    """
    return core.patch(src_bytes, *read_patch(BytesIO(patch_bytes)))


def file_patch_inplace(path, patch_path):
    """file_patch_inplace(path, patch_path)

    Apply the BSDIFF4-format file patch_path to the file 'path' in place.
    """
    with open(patch_path, "rb") as fi:
        with open(path, "r+b") as fo:
            data = fo.read()
            fo.seek(0)
            fo.write(core.patch(data, *read_patch(fi)))
            fo.truncate()


def file_patch(src_path, dst_path, patch_path):
    """file_patch(src_path, dst_path, patch_path)

    Apply the BSDIFF4-format file patch_path to the file src_path and
    write the result to the file dst_path.
    """
    from os.path import abspath

    if abspath(dst_path) == abspath(src_path):
        file_patch_inplace(src_path, patch_path)
        return

    with open(patch_path, "rb") as fi:
        with open(dst_path, "wb") as fo:
            fo.write(core.patch(read_data(src_path), *read_patch(fi)))
