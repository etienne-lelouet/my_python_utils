import asyncio
from async_process_utils import run_single_command
import os
import time
from pathlib import Path
from stat import S_ISDIR, S_ISREG
import sys


def remote_stat(path, connectionWrapper):
    sftp_client = connectionWrapper.connection.sftp()
    try:
        return sftp_client.stat(path)
    except FileNotFoundError as e:
        return None
    except Exception as e:
        print(f"Error stating path {path}: {e} {type(e)}")
        sys.exit(1)


def remote_mkdir(remote_path, connectionWrapper):
    stat = remote_stat(remote_path, connectionWrapper)

    if stat is not None:
        if S_ISDIR(stat.st_mode):
            return
        else:
            print(f"Path {remote_path} exists but is not a directory")
            sys.exit(1)

    sftp_client = connectionWrapper.connection.sftp()

    try:
        sftp_client.mkdir(remote_path)
    except Exception as e:
        print(remote_path)
        print(f"Failed to create remote directory {remote_path}: {e}")
        sys.exit(1)


def put_file(local_path, remote_path, connectionWrapper):
    """Copies a local file to a remote path. If the remote path is a directory, the file is copied into that directory."""
    sftp_client = connectionWrapper.connection.sftp()
    stat = remote_stat(remote_path, connectionWrapper)
    if stat is not None:
        if S_ISDIR(stat.st_mode):
            if remote_path.endswith('/'):
                remote_path = os.path.join(
                    remote_path, os.path.basename(local_path))
            else:
                print(
                    f"Remote path {remote_path} is a directory. Append with / to copy file into it.")
                sys.exit(1)
        elif not S_ISREG(stat.st_mode):
            print(
                f"Remote path {remote_path} exists but is not a regular file or directory.")
            sys.exit(1)
    try:
        sftp_client.put(local_path, remote_path)
    except Exception as e:
        print(
            f"Failed to copy {local_path} to {remote_path}: {e}, type(e): {type(e)}")
        sys.exit(1)


def get_file(remote_path, local_path, connectionWrapper):
    sftp_client = connectionWrapper.connection.sftp()
    stat = os.stat(local_path) if os.path.exists(local_path) else None
    if stat is not None:
        if S_ISDIR(stat.st_mode):
            if local_path.endswith('/'):
                local_path = os.path.join(
                    local_path, os.path.basename(remote_path))
            else:
                print(
                    f"Local path {local_path} is a directory. Append with / to copy file into it.")
                sys.exit(1)
        else:
            print(
                f"Local path {local_path} exists but is not a regular file or directory.")
            sys.exit(1)

    try:
        sftp_client.get(remote_path, local_path)
    except Exception as e:
        print(f"Failed to copy {remote_path} to {local_path}: {e}")
        sys.exit(1)


async def compress_folder(folder, archive_name, connectionWrapper=None):
    """
    Compress files into an archive using zstd.
    """

    folder = Path(folder)

    parent_directory_name = str(folder.parent)

    directory_name = str(folder.name)

    command = ["tar", "-c", "-I", "'zstd --ultra --long -T0'", "-f",
               archive_name, "-C", parent_directory_name, directory_name]
    await run_single_command(command, connectionWrapper, no_pipe=True, no_output=True)


async def compress_file(file, archive_name, connectionWrapper=None):
    """
    Compress a single file using zstd.
    """

    command = ["zstd", "-T0", file, "-o", archive_name]
    await run_single_command(command, connectionWrapper, no_pipe=True, no_output=True)


async def decompress_folder(archive, destination=None, connectionWrapper=None):
    """
    Decompress a tar.zst archive.
    """
    if destination is None:
        destination = os.path.dirname(archive)
    else:
        destination = Path(destination).parent

    command = ["tar", "-x", "-I", "'zstd --ultra --long -T0'",
               "-f", archive, "-C", str(destination)]
    await run_single_command(command, connectionWrapper, no_pipe=True, no_output=True)


async def decompress_file(archive, destination=None, connectionWrapper=None):
    """
    Decompress a zst file.
    """
    if destination is None:
        destination_dir = os.path.dirname(archive)
        destination = os.path.join(
            destination_dir, os.path.basename(archive).replace('.zst', ''))

    command = ["zstd", "-d", "-T0", archive, "-o", destination]
    await run_single_command(command, connectionWrapper, no_pipe=True, no_output=True)


async def decompress(archive, destination=None, connectionWrapper=None):
    if archive.endswith(".tar.zst"):
        await decompress_folder(archive, destination, connectionWrapper)
    elif archive.endswith(".zst"):
        await decompress_file(archive, destination, connectionWrapper)


async def copy_to_remote(source, destination, connectionWrapper, silent=False):
    if not silent:
        print(f"Copying {source} to {connectionWrapper.name}:{destination}")

    loop = asyncio.get_running_loop()

    source_path = Path(source)
    destination_path = Path(destination)

    if source_path.is_dir():
        source_basename = source_path.name
        source_archive = f"/tmp/{source_basename}_{connectionWrapper.name}.tar.zst"

        await compress_folder(source, source_archive, None)

        destination_basename = destination_path.name
        destination_archive = f"/tmp/{destination_basename}_{connectionWrapper.name}.tar.zst"

        await loop.run_in_executor(None, put_file, source_archive, destination_archive, connectionWrapper)

        await decompress(destination_archive, destination, connectionWrapper)

        os.remove(source_archive)
        await run_single_command(["rm", "-f", destination_archive], connectionWrapper, no_pipe=True, no_output=True)

    elif source_path.is_file():
        # source_archive = f"{source}.zst"

        # await compress_file(source, source_archive, None)

        # destination_archive = f"{destination}.zst"

        await loop.run_in_executor(None, put_file, source, destination, connectionWrapper)

        # await decompress(destination_archive, destination, connectionWrapper)

    else:
        print(f"Source {source} is neither a file nor a directory.")
        sys.exit(1)
    
    if not silent:
        print(f"Finished copying {source} to {connectionWrapper.name}:{destination}")


async def copy_from_remote(source, destination, connectionWrapper, silent=False):
    if not silent:
        print(f"Copying {connectionWrapper.name}:{source} to {destination}")

    loop = asyncio.get_running_loop()

    source_path = Path(source)
    destination_path = Path(destination)

    source_stat = remote_stat(source, connectionWrapper)

    if source_stat is None:
        print(f"Source {source} does not exist on the remote server.")
        sys.exit(1)

    if S_ISDIR(source_stat.st_mode):
        source_basename = source_path.name
        source_archive = f"/tmp/{source_basename}_{connectionWrapper.name}.tar.zst"

        await compress_folder(source, source_archive, connectionWrapper)

        destination_basename = destination_path.name
        destination_archive = f"/tmp/{destination_basename}_{connectionWrapper.name}.tar.zst"

        await loop.run_in_executor(None, get_file, source_archive, destination_archive, connectionWrapper)

        await decompress(destination_archive, destination, None)

        os.remove(destination_archive)
        await run_single_command(["rm", "-f", source_archive], connectionWrapper, no_pipe=True, no_output=True)

    elif S_ISREG(source_stat.st_mode):
        # source_archive = f"{source_path}.zst"

        # await compress_file(source, source_archive, connectionWrapper)

        # destination_archive = f"{destination}.zst"

        await loop.run_in_executor(None, get_file, source, destination, connectionWrapper)

        # await decompress(destination_archive, destination, None)

    else:
        print(
            f"Source {connectionWrapper.name}:{source} is neither a file nor a directory.")
        sys.exit(1)

    if not silent:
        print(f"Finished copying {connectionWrapper.name}:{source} to {destination}")
