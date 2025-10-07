import functools
from typing import Optional
from fabric import Connection, Config
from paramiko import SSHConfig, Agent
import asyncio
import sys
import os

# TODO: Improve command logging
# - 3 things can be printed when running commands :
#   - The prologue / epilogue "running this command on host X" / "ran x command on host X return code Y"
#   - The output of the command itself
#   - The echo of the command output
# Considerations : most of the time, we use pty=True

def get_key_from_agent_by_pubkey(pubkey_path):
    """Get the specific key from agent matching the public key file"""
    agent = Agent()
    agent_keys = agent.get_keys()
    
    # Read the public key file
    with open(os.path.expanduser(pubkey_path), 'r') as f:
        pubkey_data = f.read().strip()
    
    # Extract the key data (second field in "ssh-rsa AAAAB3... comment" format)
    pubkey_b64 = pubkey_data.split()[1]
    
    # Find matching key in agent
    for key in agent_keys:
        if key.get_base64() == pubkey_b64:
            return key
    
    return None

class ConnectionWrapper:
    """
    A wrapper class for Fabric Connection to handle local and remote connections.
    """

    def __init__(self, connection: Connection,  name: str | None = None):
        self.connection = connection
        if name is None:
            self.name = connection.host
        else:
            self.name = name


def create_connection_from_config(ssh_config: dict | None, fabric_config=None) -> ConnectionWrapper:
    if ssh_config is None:
        print("Error: ssh_config is None. Cannot create connection.")
        sys.exit(1)

    user_ssh_config = SSHConfig.from_path(os.path.expanduser('~/.ssh/config'))
    host = ssh_config["host"]
    host_config = user_ssh_config.lookup(host)
    specific_key = None

    connect_kwargs = ssh_config.get("connect_kwargs", {})
    
    if host_config is not None:
        host_pubkey = host_config.get('identityfile', [None])[0]
        if host_pubkey is not None:
            connect_kwargs['allow_agent'] = False
            connect_kwargs['look_for_keys'] = False
            specific_key = get_key_from_agent_by_pubkey(host_pubkey)
            if specific_key is not None:
                connect_kwargs['pkey'] = specific_key
        else:
            specific_key = None

    if fabric_config is None:
        fabric_config = Config()

    if ssh_config.get("gateway") is not None and not ssh_config.get("persistent_ssh_tunnel", False):
        gateway = ssh_config["gateway"]
        gateway = create_connection_from_config(gateway)
        gateway = gateway.connection  # Get the actual Connection object from the wrapper
    else:
        gateway = None

    if ssh_config.get("user") is None:
        c = Connection(
            host=host,
            connect_kwargs=connect_kwargs,
            port=ssh_config.get("local_port", 22),
            gateway=gateway,
            config=fabric_config
        )
    else:
        c = Connection(
            host=host,
            user=ssh_config["user"],
            connect_kwargs=connect_kwargs,
            port=ssh_config.get("local_port", 22),
            gateway=gateway,
            config=fabric_config
        )
    return ConnectionWrapper(c, name=ssh_config.get("name", None))


async def check_iface(iface: str, connection: Connection | None) -> bool:
    """
    Check if the interface exists.
    """
    cmd = f"ip link show {iface}"
    result = await run_single_command(cmd, connection, no_pipe=True, fail_on_returncode=False, no_output=True)
    return result.return_code == 0


async def delete_iface(iface: str, connection: Connection | None):
    """
    Delete the interface.
    """
    cmd = f"ip link delete {iface}"
    print(
        f"Deleting interface: {iface} on host {connection.name if connection else 'localhost'}")
    await run_single_command(cmd, connection, no_pipe=True, sudo=True, no_output=True)


async def get_devicefile_name(name: str, connection: Connection | None) -> str:
    """
    Get the device file name for the given interface.
    """
    ifindex_filename = f"/sys/class/net/{name}/ifindex"
    cmd = f"cat {ifindex_filename}"
    result = await asyncio.create_task(run_single_command(cmd, connection, no_pipe=True))
    ifindex = result.stdout.strip()
    ifacepath = f"/dev/tap{ifindex}"
    result = await asyncio.create_task(run_single_command(f"ls -l {ifacepath}", connection, no_pipe=True, fail_on_returncode=False))
    if (result.return_code != 0):
        print(
            f"Error: Interface '{name}' does not exist or is not a tap interface.")
        sys.exit(1)
    return ifacepath


async def set_iface_up(name: str, connection: Connection | None):
    """
    Set the interface up.
    """
    cmd = f"ip link set {name} up"
    print(
        f"Setting interface '{name}' up on host {connection.name if connection else 'localhost'}.")
    await asyncio.create_task(run_single_command(cmd, connection, sudo=True, no_pipe=True, no_output=True))


async def set_iface_down(name: str, connection: Connection | None):
    """
    Set the interface up.
    """
    cmd = f"ip link set {name} down"
    print(
        f"Setting interface '{name}' down on host {connection.name if connection else 'localhost'}.")
    await asyncio.create_task(run_single_command(cmd, connection, sudo=True, no_pipe=True, no_output=True))


async def set_iface_ip(name: str, ip_address: str, connection: Connection | None):
    """
    Set the IP address for the interface.
    """
    cmd = f"ip addr add {ip_address} dev {name}"
    print(
        f"Setting IP address '{ip_address}' on interface '{name}', on host {connection.name if connection else 'localhost'}.")
    await asyncio.create_task(run_single_command(cmd, connection, sudo=True, no_pipe=True, no_output=True))


async def set_iface_mac(name: str, mac_address: str, connection: Connection | None):
    """
    Set the MAC address for the interface.
    """
    cmd = f"ip link set {name} address {mac_address}"
    print(
        f"Setting MAC address '{mac_address}' on interface '{name}' on host {connection.name if connection else 'localhost'}.")
    await asyncio.create_task(run_single_command(cmd, connection, sudo=True, no_pipe=True, no_output=True))


async def get_username(connection: Connection | None) -> str:
    """
    Get the username of the current user.
    """
    result = await asyncio.create_task(run_single_command("whoami", connection, no_pipe=True, no_output=True))
    return result.stdout.strip()


class MacVlan:
    def __init__(self, interface: dict, Connection=None):
        # Mandatory fields
        try:
            self.name: str = interface["name"]
            self.master: str = interface["master"]
            self.ip_address: str = interface["ip_address"]
        except KeyError as e:
            print(f"Missing mandatory field in macvlan dict: {e}")
            sys.exit(1)
        self.connection = Connection

    async def create(self):
        if await check_iface(self.name, self.connection):
            print(f"Interface '{self.name}' already exists. Deleting it.")
            await delete_iface(self.name, self.connection)

        cmd = f"ip link add link {self.master} name {self.name} type macvlan mode bridge".split(
        )
        print(
            f"Creating macvlan interface: {self.name}, on host {self.connection.name if self.connection else 'localhost'}.")
        await run_single_command(cmd, self.connection, sudo=True, no_pipe=True, no_output=True)

        await set_iface_ip(self.name, self.ip_address, self.connection)

        await set_iface_up(self.name, self.connection)


class MacVtap:
    def __init__(self, interface: dict, Connection=None):
        # Mandatory fields
        try:
            self.name: str = interface["name"]
            self.master: str = interface["master"]
            self.mac_address: str = interface["mac_address"]
            self.queue_count: int = interface.get("queue_count", 1)
        except KeyError as e:
            print(f"Missing mandatory field in macvlan dict: {e}")
            sys.exit(1)
        self.vhost = interface.get("vhost", False)
        self.connection = Connection

    async def create(self):
        if await check_iface(self.name, self.connection):
            print(f"Interface '{self.name}' already exists. Deleting it.")
            await delete_iface(self.name, self.connection)

        cmd = f"ip link add link {self.master} name {self.name} type macvtap mode bridge"
        print(
            f"Creating macvtap interface: {self.name}, on host {self.connection.name if self.connection else 'localhost'}.")
        await run_single_command(cmd, self.connection, sudo=True, no_pipe=True)
        # If the host macvtap and the guest virtual interface don't have the same MAC address
        # then no packets will be forwarded to the guest (the "bridge" doesn't actually learn because
        # it knows at runtime all the mac addresses that are connected to it. Therefore, if it
        # receives a packet for a mac it doesn't recognize, it won't broadcast it, and it won't parse ARP
        # packets either).
        await set_iface_mac(self.name, self.mac_address, self.connection)

        username = await get_username(self.connection)
        device_file_name = await get_devicefile_name(self.name, self.connection)
        cmd = f"chown {username}:{username} {device_file_name}"
        print(f"Changing ownership of {device_file_name} to {username}")
        await run_single_command(cmd, self.connection, sudo=True, no_pipe=True)

        await set_iface_up(self.name, self.connection)

    async def get_args(self, fd: int):
        """
        Get the arguments for the macvtap interface.
        """
        device_file_name = await get_devicefile_name(self.name, self.connection)
        if self.vhost:
            vhost_option = ",vhost=on"
        else:
            vhost_option = ""
        mq_option = ""
        tap_queues = ""
        if self.queue_count > 1:
            vectors = 2 * self.queue_count + 1
            mq_option = f",mq=on,vectors={vectors}"
            tap_queues = f",queues={self.queue_count}"
        return [
            "-netdev",
            f"tap,id={self.name},fd={fd}{vhost_option}{tap_queues}",
            f"{fd}<>{device_file_name}",
            "-device",
            f"virtio-net-pci,netdev={self.name},mac={self.mac_address}{mq_option}"
        ]


class Tap:
    def __init__(self, interface: dict, Connection=None):
        # Mandatory fields
        try:
            self.name: str = interface["name"]
            self.mac_address: str = interface["mac_address"]
        except KeyError as e:
            print(f"Missing mandatory field in macvlan dict: {e}")
            sys.exit(1)

        if not (("ip_address" in interface) ^ ("master" in interface)):
            print("Either 'ip_address' or 'master' is required for tap interface.")
            sys.exit(1)

        self.vhost: bool = interface.get("vhost", False)
        self.queue_count: int = interface.get("queue_count", 1)
        self.vhost = interface.get("vhost", False)
        self.master: Optional[str] = interface.get("master")
        self.ip_address: Optional[str] = interface.get("ip_address")
        self.connection = Connection

    async def create(self):
        if await check_iface(self.name, self.connection):
            print(f"Interface '{self.name}' already exists. Deleting it.")
            await delete_iface(self.name, self.connection)

        username = await get_username(self.connection)

        multi_queue = ""
        if self.queue_count > 1:
            multi_queue = " multi_queue"

        print(
            f"Creating tap interface: {self.name}, with MAC {self.mac_address}")

        cmd = f"ip tuntap add dev {self.name} mode tap{multi_queue} user {username} group {username}"
        await run_single_command(cmd, self.connection, sudo=True, no_pipe=True, no_output=True)
        # Setting the same mac address on both the guest and the host ifaces can lead to issues.
        # await set_iface_mac(self.name, self.mac_address, self.connection)
        if self.ip_address:
            print(
                f"Setting IP for tap interface '{self.name}' to '{self.master}'.")
            await set_iface_ip(self.name, self.ip_address, self.connection)
        elif self.master:
            cmd = f"ip link set {self.name} master {self.master}"
            print(
                f"Setting master for tap interface '{self.name}' to '{self.master}'.")
            await run_single_command(cmd, self.connection, sudo=True, no_pipe=True, no_output=True)

        await set_iface_up(self.name, self.connection)

    async def get_args(self, fd: int):
        """
        Get the arguments for the tap interface.
        """
        if self.vhost:
            vhost_option = ",vhost=on"
        else:
            vhost_option = ""
        mq_option = ""
        tap_queues = ""
        if self.queue_count > 1:
            vectors = 2 * self.queue_count + 1
            mq_option = f",mq=on,vectors={vectors}"
            tap_queues = f",queues={self.queue_count}"
        return [
            "-netdev",
            f"tap,id={self.name},ifname={self.name},script=no,downscript=no{vhost_option}{tap_queues}",
            "-device",
            f"virtio-net-pci,netdev={self.name},mac={self.mac_address}{mq_option}"
        ]


class Bridge:
    def __init__(self, bridge: dict, Connection=None):
        # Mandatory fields
        try:
            self.name: str = bridge["name"]
        except KeyError as e:
            print(f"Missing mandatory field in bridge dict: {e}")
            sys.exit(1)

        # Optional fields
        self.ip_address: Optional[str] = bridge.get("ip_address")
        self.childs: list = bridge.get("childs", [])
        self.connection = Connection

    async def create(self):
        if await check_iface(self.name, self.connection):
            print(f"Interface '{self.name}' already exists. Deleting it.")
            await delete_iface(self.name, self.connection)

        cmd = f"ip link add name {self.name} type bridge"
        print(f"Creating bridge interface: {self.name}")
        await run_single_command(cmd, self.connection, sudo=True, no_pipe=True)

        if self.ip_address:
            await set_iface_ip(self.name, self.ip_address, self.connection)

        for child in self.childs:
            await set_iface_down(child, self.connection)
            cmd = f"ip link set {child} master {self.name}"
            print(f"Adding child interface '{child}' to bridge '{self.name}'.")
            await run_single_command(cmd, self.connection, sudo=True, no_pipe=True)
            await set_iface_up(child, self.connection)
        await set_iface_up(self.name, self.connection)


class User:
    def __init__(self, interface: dict, Connection=None):
        # Mandatory fields
        try:
            self.mac_address: str = interface["mac_address"]
            self.name: str = interface["name"]
        except KeyError as e:
            print(f"Missing mandatory field in user dict: {e}")
            sys.exit(1)

    def create(self):
        """
        Create a user interface. No operation needed for user type interfaces.
        """
        print(f"Creating user interface with MAC address {self.mac_address}")
        # No operation needed for user type interfaces, as they are created by QEMU automatically.

    def get_args(self):
        """
        Get the arguments for the user interface.
        """
        return [
            "-netdev",
            f"user,id={self.name}",
            "-device",
            f"virtio-net-pci,netdev={self.name},mac={self.mac_address}"
        ]


async def check_file_exists(file_path: str, connection: Connection) -> bool:
    """
    Check if a file exists on the local or remote system.

    :param file_path: Path to the file to check.
    :param connection: Fabric Connection object (local or remote).
    :return: True if the file exists, False otherwise.
    """
    command = f"test -e {file_path}"
    result = await run_single_command(command, connection, no_pipe=True, fail_on_returncode=False, no_output=True)

    if result.return_code == 0:
        return True
    return False


async def validate_image_use(disk_image_path: str, connection: Connection, kill_running_vms: bool = True) -> bool:
    """
    Validate the image path.
    """

    print(f"Checking for other vms using image: {disk_image_path}")

    result = await run_single_command(f"lsof -t {disk_image_path}", connection, no_pipe=True, fail_on_returncode=False, no_output=True)

    pids = [pid for pid in result.stdout.strip().split('\n') if pid]
    if pids:
        # Check if any of the processes is a qemu process
        for pid in pids:
            comm = await run_single_command(f"ps -p {pid} -o comm=", connection, no_pipe=True, fail_on_returncode=False, no_output=True)
            if comm.stdout.strip().startswith("qemu"):
                print(
                    f"Error: Image file '{disk_image_path}' is currently used by qemu process (PID {pid}).")
                if kill_running_vms:
                    print(f"Killing qemu process with PID {pid}.")
                    await run_single_command(f"kill -9 {pid}", connection, sudo=True, no_pipe=True, no_output=True)
                else:
                    print(
                        "Set 'kill_running_vms' to True to automatically kill running VMs using this disk image.")
                    return False
    return True


class CommandResult:
    """
    A class to represent the result of a command execution.
    Attributes:
        command: The command that was executed.
        return_code: The return code of the command execution.
        stdout: The standard output of the command execution.
        stderr: The standard error of the command execution.
        host: The host on which the command was executed (if applicable).
    """

    def __init__(self, host: str, command: str, return_code: int, stdout: str, stderr: str):
        self.command = command
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
        self.host = host

    def __str__(self):
        return f"CommandResult(command={self.command}, return_code={self.return_code}, stdout={self.stdout}, stderr={self.stderr})"

    def __repr__(self):
        return f"CommandResult(command={self.command}, return_code={self.return_code}, stdout={self.stdout}, stderr={self.stderr})"


# def wait_for_command_result(promise: Promise, host) -> CommandResult:
#     try:
#         # print(("Waiting for command result on host: " + host))
#         result = promise.join()
#     except ConnectionError as e:
#         print(f"[ERROR] Failed to connect to host '{host}': {e}")
#         sys.exit(1)

#     except UnexpectedExit as e:
#         print(
#             f"[ERROR] Command {e.result.command} failed on host '{host}' with exit code {e.result.exited}")
#         print(f"STDOUT:\n{e.result.stdout}")
#         print(f"STDERR:\n{e.result.stderr}")
#         sys.exit(1)

#     return CommandResult(
#         host=host,
#         command=result.command,
#         return_code=result.return_code,
#         stdout=result.stdout.strip().replace('[sudo] password:', ''),
#         stderr=result.stderr.strip()
#     )

class CommandException(Exception):
    pass


async def run_single_command(command: str | list[str], connectionWrapper: ConnectionWrapper | None, sudo: bool = False, no_pipe: bool = False, fail_on_returncode: bool = True, disown: bool = False, asynchronous: bool = True, no_output: bool = False, pty=False) -> CommandResult:
    if disown and asynchronous:
        print("[Error] Cannot use 'disown' with 'asynchronous' mode.")
        sys.exit(1)

    if asynchronous:
        loop = asyncio.get_event_loop()
        ret = await loop.run_in_executor(None, functools.partial(__run_single_command, command, connectionWrapper, sudo, no_pipe, fail_on_returncode, disown, no_output, pty))
        return ret

    return __run_single_command(command, connectionWrapper, sudo, no_pipe, fail_on_returncode, disown, no_output, pty)


def __run_single_command(command: str | list[str], connectionWrapper: ConnectionWrapper | None, sudo: bool = False, no_pipe: bool = False, fail_on_returncode: bool = True, disown: bool = False, no_output: bool = False, pty=False) -> CommandResult:
    """
    Executes a shell command on a given Fabric Connection object.

    :param command: List of strings representing the shell command and its arguments.
    :param connection: ConnectionWrapper object. If none, a default refering to localhost is created.
    :param sudo: Whether to run the command with sudo privileges ().
    :param no_pipe: If True, do not use a pseudo-terminal and hide output.
    :param fail_on_returncode: If True, raise an exception if the command returns a non-zero exit code.
    :param disown: If True, run the command in the background (nohup).
    :param asynchronous: If True, run the command in a separate thread.
    :param no_output: If True, do not log anything related to the execution.
    :return: None
    """

    if isinstance(command, list):
        command = " ".join(command)

    if disown:
        command = f"nohup {command} &"

    if pty:
        hide = False
    else:
        hide = True

    if connectionWrapper is None:
        connectionWrapper = ConnectionWrapper(
            Connection(host="localhost"), "localhost")
        if not no_output:
            print(f"Command: {command}, host: {connectionWrapper.name}")
        fb_result = connectionWrapper.connection.local(
            command, hide=hide, pty=pty, warn=True, disown=disown)
    else:
        if not no_output:
            print(f"Command: {command}, host: {connectionWrapper.name}")
        if sudo:
            fb_result = connectionWrapper.connection.sudo(
                command, hide=hide, pty=pty, warn=True, disown=disown)
        else:
            fb_result = connectionWrapper.connection.run(
                command, hide=hide, pty=pty, warn=True, disown=disown)

    if disown:
        return CommandResult(
            host=connectionWrapper.name,
            command=command,
            return_code=0,
            stdout="",
            stderr=""
        )

    result = CommandResult(
        host=connectionWrapper.name,
        command=command,
        return_code=fb_result.exited,
        stdout=fb_result.stdout.strip().replace('[sudo] password:', ''),
        stderr=fb_result.stderr.strip()

    )

    if fail_on_returncode and result.return_code != 0:
        print("\n")
        print(
            f"Error: Command '{command}' failed on host '{connectionWrapper.name}' with exit code {result.return_code}.")
        print("")
        if len(result.stdout) > 0:
            print(f"STDOUT:\n{result.stdout}")
            print("")
        if len(result.stderr) > 0:
            print(f"STDERR:\n{result.stderr}")
            print("")
        raise CommandException(f"Command failed")

    if not no_output:
        print(
            f"Command: {command}, host: {connectionWrapper.name}, return code: {result.return_code}")
    if not no_pipe:
        if len(result.stdout) > 0:
            print(f"STDOUT:\n{result.stdout}")
            print("")
        if len(result.stderr) > 0:
            print(f"STDERR:\n{result.stderr}")
            print("")

    return result
