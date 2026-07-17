from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import yaml

from .manylinux_version_utils import SUPPORTED_MANYLINUX_VERSIONS
from .proxy_environment_utils import detect_proxy_settings, normalize_proxy_env
from .sudo_prefix_utils import sudo_prefix

__all__ = [
    "SUPPORTED_MANYLINUX_VERSIONS",
    "_img_slugify",
    "_docker_build_require_mapping",
    "_docker_build_require_non_empty_string",
    "_docker_build_optional_string_list",
    "_docker_build_optional_mapping_list",
    "_docker_build_render_run",
    "_docker_build_resolve_host_path",
    "_docker_build_materialize_path",
    "_docker_build_script_executor",
    "_docker_build_append_command_sequence",
    "_load_docker_image_build_config",
    "image_ref_from_build_config",
    "build_docker_image_from_config",
    "docker_check_call",
    "docker_check_output",
    "build_docker_run_cmd",
]



def _img_slugify(s: str) -> str:
    """Slugify for Docker name components: keep alnum, dash, underscore."""
    return ''.join(c if c.isalnum() or c in ('-', '_') else '_' for c in str(s))


def _docker_build_require_mapping(raw_value: Any, *, field_name: str) -> Dict[str, Any]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return raw_value


def _docker_build_require_non_empty_string(raw_value: Any, *, field_name: str) -> str:
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be a string")
    value = raw_value.strip()
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def _docker_build_optional_string_list(raw_value: Any, *, field_name: str) -> List[str]:
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError(f"{field_name} must be a list")
    result: List[str] = []
    for idx, item in enumerate(raw_value):
        if not isinstance(item, str):
            raise ValueError(f"{field_name}[{idx}] must be a string")
        value = item.strip()
        if not value:
            raise ValueError(f"{field_name}[{idx}] must not be empty")
        result.append(value)
    return result


def _docker_build_optional_mapping_list(raw_value: Any, *, field_name: str) -> List[Dict[str, Any]]:
    if raw_value is None:
        return []
    if not isinstance(raw_value, list):
        raise ValueError(f"{field_name} must be a list")
    result: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw_value):
        if not isinstance(item, dict):
            raise ValueError(f"{field_name}[{idx}] must be a mapping")
        result.append(item)
    return result


def _docker_build_render_run(commands: Sequence[str]) -> str:
    if not commands:
        return "RUN :"
    body = " && \\\n    ".join(commands)
    return "RUN set -euo pipefail; \\\n    " + body


def _docker_build_resolve_host_path(config_dir: Path, raw_path: str, *, field_name: str) -> Path:
    candidate = Path(raw_path)
    resolved = candidate.resolve() if candidate.is_absolute() else (config_dir / candidate).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"{field_name} path does not exist: {resolved}")
    return resolved


def _docker_build_materialize_path(
    *,
    context_root: Path,
    source_path: Path,
    label: str,
    input_index: int,
) -> str:
    safe_label = _img_slugify(label) or "input"
    rel_path = Path("build_inputs") / f"{input_index:04d}_{safe_label}"
    target_path = context_root / rel_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.is_dir():
        shutil.copytree(source_path, target_path, symlinks=True)
    elif source_path.is_file():
        shutil.copy2(source_path, target_path)
    else:
        raise ValueError(f"unsupported build input path kind: {source_path}")
    return rel_path.as_posix()


def _docker_build_script_executor(script_path: Path) -> str:
    return "python3" if script_path.suffix == ".py" else "/bin/bash"


def _docker_build_append_command_sequence(
    *,
    dockerfile_lines: List[str],
    commands: Sequence[str],
    config_dir: Path,
    context_root: Path,
    next_input_index: List[int],
    script_label_prefix: str,
) -> None:
    pending_commands: List[str] = []
    for raw_command in commands:
        command = _docker_build_require_non_empty_string(raw_command, field_name=f"{script_label_prefix}.commands")
        if not command.startswith("file:"):
            pending_commands.append(command)
            continue

        if pending_commands:
            dockerfile_lines.append(_docker_build_render_run(pending_commands))
            pending_commands = []

        script_ref = command[len("file:") :].strip()
        script_path = _docker_build_resolve_host_path(
            config_dir,
            script_ref,
            field_name=f"{script_label_prefix}.file",
        )
        context_rel_path = _docker_build_materialize_path(
            context_root=context_root,
            source_path=script_path,
            label=script_path.name,
            input_index=next_input_index[0],
        )
        next_input_index[0] += 1
        container_script_path = f"/tmp/fluxon_image_build/{next_input_index[0]:04d}_{script_path.name}"
        dockerfile_lines.append(f"COPY {context_rel_path} {container_script_path}")
        dockerfile_lines.append(
            _docker_build_render_run(
                [
                    f"chmod +x {shlex.quote(container_script_path)}",
                    f"{_docker_build_script_executor(script_path)} {shlex.quote(container_script_path)}",
                    f"rm -f {shlex.quote(container_script_path)}",
                ]
            )
        )

    if pending_commands:
        dockerfile_lines.append(_docker_build_render_run(pending_commands))


def _load_docker_image_build_config(config_yaml: Union[str, Path]) -> tuple[Path, Dict[str, Any]]:
    cfg_path = Path(config_yaml).resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Docker image build config not found: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"Docker image build config must be a mapping: {cfg_path}")
    _docker_build_require_non_empty_string(cfg.get("base_image"), field_name="base_image")
    _docker_build_require_non_empty_string(cfg.get("image_name"), field_name="image_name")
    _docker_build_require_non_empty_string(cfg.get("image_tag"), field_name="image_tag")
    return cfg_path, cfg


def image_ref_from_build_config(config_yaml: Union[str, Path]) -> str:
    _, cfg = _load_docker_image_build_config(config_yaml)
    image_name = _docker_build_require_non_empty_string(cfg.get("image_name"), field_name="image_name")
    image_tag = _docker_build_require_non_empty_string(cfg.get("image_tag"), field_name="image_tag")
    return f"{image_name}:{image_tag}"


def build_docker_image_from_config(project_root: Union[str, Path], config_yaml: Union[str, Path]) -> str:
    """Build a Docker image directly from the repo YAML config.

    The config intentionally stays explicit:
    - image identity comes only from base_image/image_name/image_tag;
    - host-side build inputs come only from declared script `copies` and `file:` references;
    - unsupported config features fail fast instead of falling back to hidden behavior.
    """
    project_root_path = Path(project_root).resolve()
    config_input_path = Path(config_yaml)
    if not config_input_path.is_absolute():
        config_input_path = (project_root_path / config_input_path).resolve()
    cfg_path, cfg = _load_docker_image_build_config(config_input_path)
    config_dir = cfg_path.parent

    base_image = _docker_build_require_non_empty_string(cfg.get("base_image"), field_name="base_image")
    image_name = _docker_build_require_non_empty_string(cfg.get("image_name"), field_name="image_name")
    image_tag = _docker_build_require_non_empty_string(cfg.get("image_tag"), field_name="image_tag")
    image_ref = f"{image_name}:{image_tag}"

    heavy_setup = _docker_build_require_mapping(cfg.get("heavy_setup"), field_name="heavy_setup")
    light_setup = _docker_build_require_mapping(cfg.get("light_setup"), field_name="light_setup")
    ssh_cfg = _docker_build_require_mapping(cfg.get("ssh"), field_name="ssh")

    def _require_port(raw_value: Any, *, field_name: str) -> int:
        if isinstance(raw_value, int):
            port = raw_value
        elif isinstance(raw_value, str) and raw_value.strip().isdigit():
            port = int(raw_value.strip())
        else:
            raise ValueError(f"{field_name} must be an integer port")
        if not (1 <= port <= 65535):
            raise ValueError(f"{field_name} must be between 1 and 65535")
        return port

    build_args: Dict[str, str] = {}
    if bool(cfg.get("inherit_proxy")):
        normalize_proxy_env()
        build_args.update(detect_proxy_settings())
    if bool(cfg.get("inherit_timezone")):
        tz = os.environ.get("TZ")
        if isinstance(tz, str) and tz.strip():
            build_args["TZ"] = tz.strip()

    apt_sources = _docker_build_optional_string_list(cfg.get("apt_sources"), field_name="apt_sources")
    apt_packages = _docker_build_optional_string_list(
        heavy_setup.get("apt_packages"),
        field_name="heavy_setup.apt_packages",
    )
    yum_packages = _docker_build_optional_string_list(
        heavy_setup.get("yum_packages"),
        field_name="heavy_setup.yum_packages",
    )
    pip_packages = _docker_build_optional_string_list(
        heavy_setup.get("pip_packages"),
        field_name="heavy_setup.pip_packages",
    )
    script_installs = _docker_build_optional_mapping_list(
        heavy_setup.get("script_installs"),
        field_name="heavy_setup.script_installs",
    )
    config_files = _docker_build_optional_mapping_list(
        light_setup.get("config_files"),
        field_name="light_setup.config_files",
    )

    ssh_enabled = bool(ssh_cfg.get("enabled"))
    ssh_port = _require_port(ssh_cfg.get("port"), field_name="ssh.port") if ssh_enabled else None
    ssh_password_auth = bool(ssh_cfg.get("password_auth")) if ssh_enabled else False

    user_value = cfg.get("user")
    user_name: str | None = None
    if user_value is not None:
        user_name = _docker_build_require_non_empty_string(user_value, field_name="user")
    password_value = cfg.get("password")
    user_password: str | None = None
    if password_value is not None:
        user_password = _docker_build_require_non_empty_string(password_value, field_name="password")
    sudo_enabled = bool(cfg.get("sudo"))
    if user_password is not None and user_name is None:
        raise ValueError("password requires explicit user")
    if sudo_enabled and user_name is None:
        raise ValueError("sudo=true requires explicit user")

    expose_ports = _docker_build_optional_string_list(cfg.get("expose_ports"), field_name="expose_ports")
    volume_paths = _docker_build_optional_string_list(cfg.get("volumes"), field_name="volumes")

    dockerfile_lines: List[str] = [
        f"FROM {base_image}",
        'SHELL ["/bin/bash", "-lc"]',
    ]
    for key in sorted(build_args):
        dockerfile_lines.append(f"ARG {key}")
    for key in sorted(build_args):
        dockerfile_lines.append(f'ENV {key}="${key}"')

    if apt_sources:
        dockerfile_lines.append(
            "RUN cat > /etc/apt/sources.list <<'EOF'\n"
            + "\n".join(apt_sources)
            + "\nEOF"
        )

    if apt_packages:
        dockerfile_lines.append(
            _docker_build_render_run(
                [
                    "apt-get update",
                    "DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "
                    + shlex.join(apt_packages),
                    "rm -rf /var/lib/apt/lists/*",
                ]
            )
        )

    if yum_packages:
        dockerfile_lines.append(
            _docker_build_render_run(
                [
                    "yum install -y " + shlex.join(yum_packages),
                    "yum clean all",
                    "rm -rf /var/cache/yum",
                ]
            )
        )

    next_input_index = [0]
    with tempfile.TemporaryDirectory(prefix="fluxon_docker_build_") as temp_dir_str:
        context_root = Path(temp_dir_str)
        for step_idx, step_cfg in enumerate(script_installs):
            step_name = _docker_build_require_non_empty_string(
                step_cfg.get("name"),
                field_name=f"heavy_setup.script_installs[{step_idx}].name",
            )
            file_ref = step_cfg.get("file")
            commands_raw = step_cfg.get("commands")
            if file_ref is not None and commands_raw is not None:
                raise ValueError(
                    f"heavy_setup.script_installs[{step_idx}] cannot specify both file and commands"
                )

            copies = _docker_build_optional_mapping_list(
                step_cfg.get("copies"),
                field_name=f"heavy_setup.script_installs[{step_idx}].copies",
            )
            for copy_idx, copy_cfg in enumerate(copies):
                copy_kind = _docker_build_require_non_empty_string(
                    copy_cfg.get("kind"),
                    field_name=f"heavy_setup.script_installs[{step_idx}].copies[{copy_idx}].kind",
                )
                if copy_kind != "path":
                    raise ValueError(
                        "only copy kind 'path' is supported by the direct Docker builder: "
                        f"heavy_setup.script_installs[{step_idx}].copies[{copy_idx}].kind={copy_kind}"
                    )
                src_raw = _docker_build_require_non_empty_string(
                    copy_cfg.get("src"),
                    field_name=f"heavy_setup.script_installs[{step_idx}].copies[{copy_idx}].src",
                )
                dst_path = _docker_build_require_non_empty_string(
                    copy_cfg.get("dst"),
                    field_name=f"heavy_setup.script_installs[{step_idx}].copies[{copy_idx}].dst",
                )
                if not dst_path.startswith("/"):
                    raise ValueError(
                        f"heavy_setup.script_installs[{step_idx}].copies[{copy_idx}].dst must be absolute"
                    )
                source_path = _docker_build_resolve_host_path(
                    config_dir,
                    src_raw,
                    field_name=f"heavy_setup.script_installs[{step_idx}].copies[{copy_idx}].src",
                )
                context_rel_path = _docker_build_materialize_path(
                    context_root=context_root,
                    source_path=source_path,
                    label=source_path.name,
                    input_index=next_input_index[0],
                )
                next_input_index[0] += 1
                dockerfile_lines.append(f"COPY {context_rel_path} {dst_path}")

            if file_ref is not None:
                command_list = [f"file:{_docker_build_require_non_empty_string(file_ref, field_name=f'heavy_setup.script_installs[{step_idx}].file')}"]
            else:
                command_list = _docker_build_optional_string_list(
                    commands_raw,
                    field_name=f"heavy_setup.script_installs[{step_idx}].commands",
                )
            if not command_list:
                raise ValueError(
                    f"heavy_setup.script_installs[{step_idx}] must declare file or commands"
                )
            _docker_build_append_command_sequence(
                dockerfile_lines=dockerfile_lines,
                commands=command_list,
                config_dir=config_dir,
                context_root=context_root,
                next_input_index=next_input_index,
                script_label_prefix=f"heavy_setup.script_installs[{step_idx}]({step_name})",
            )

        if pip_packages:
            dockerfile_lines.append(
                _docker_build_render_run(
                    [
                        "python3 -m pip install --no-cache-dir " + shlex.join(pip_packages),
                    ]
                )
            )

        for cfg_idx, cfg_step in enumerate(config_files):
            cfg_name = _docker_build_require_non_empty_string(
                cfg_step.get("name"),
                field_name=f"light_setup.config_files[{cfg_idx}].name",
            )
            command_list = _docker_build_optional_string_list(
                cfg_step.get("commands"),
                field_name=f"light_setup.config_files[{cfg_idx}].commands",
            )
            if not command_list:
                raise ValueError(f"light_setup.config_files[{cfg_idx}] must declare commands")
            _docker_build_append_command_sequence(
                dockerfile_lines=dockerfile_lines,
                commands=command_list,
                config_dir=config_dir,
                context_root=context_root,
                next_input_index=next_input_index,
                script_label_prefix=f"light_setup.config_files[{cfg_idx}]({cfg_name})",
            )

        if ssh_enabled:
            ssh_commands = [
                "mkdir -p /var/run/sshd",
                (
                    "if grep -Eq '^[#[:space:]]*Port ' /etc/ssh/sshd_config; then "
                    f"sed -i 's/^[#[:space:]]*Port .*/Port {ssh_port}/' /etc/ssh/sshd_config; "
                    f"else echo 'Port {ssh_port}' >> /etc/ssh/sshd_config; fi"
                ),
            ]
            if ssh_password_auth:
                ssh_commands.append(
                    "if grep -Eq '^[#[:space:]]*PasswordAuthentication ' /etc/ssh/sshd_config; then "
                    "sed -i 's/^[#[:space:]]*PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config; "
                    "else echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config; fi"
                )
                if user_name == "root":
                    ssh_commands.append(
                        "if grep -Eq '^[#[:space:]]*PermitRootLogin ' /etc/ssh/sshd_config; then "
                        "sed -i 's/^[#[:space:]]*PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config; "
                        "else echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config; fi"
                    )
            dockerfile_lines.append(_docker_build_render_run(ssh_commands))

        if user_name is not None:
            create_user_commands = [f"useradd -m -s /bin/bash {shlex.quote(user_name)} || true"]
            if user_password is not None:
                create_user_commands.append(
                    f"echo {shlex.quote(f'{user_name}:{user_password}')} | chpasswd"
                )
            if sudo_enabled and user_name != "root":
                create_user_commands.extend(
                    [
                        f"usermod -aG sudo {shlex.quote(user_name)} || true",
                        f"echo {shlex.quote(f'{user_name} ALL=(ALL) NOPASSWD:ALL')} >> /etc/sudoers",
                    ]
                )
            dockerfile_lines.append(_docker_build_render_run(create_user_commands))
            workdir = "/root" if user_name == "root" else f"/home/{user_name}"
            dockerfile_lines.append(f"WORKDIR {workdir}")
            dockerfile_lines.append(f"USER {user_name}")

        exposed_ports: List[str] = []
        if ssh_enabled and ssh_port is not None:
            exposed_ports.append(str(ssh_port))
        exposed_ports.extend(expose_ports)
        if exposed_ports:
            seen_ports: set[str] = set()
            ordered_ports: List[str] = []
            for port in exposed_ports:
                if port in seen_ports:
                    continue
                seen_ports.add(port)
                ordered_ports.append(port)
            dockerfile_lines.append("EXPOSE " + " ".join(ordered_ports))

        if volume_paths:
            volume_expr = ", ".join(f'"{path}"' for path in volume_paths)
            dockerfile_lines.append(f"VOLUME [{volume_expr}]")

        dockerfile_path = context_root / "Dockerfile"
        dockerfile_path.write_text("\n\n".join(dockerfile_lines) + "\n", encoding="utf-8")

        cmd: List[str] = sudo_prefix() + ["docker", "build", "-t", image_ref]
        for key in sorted(build_args):
            cmd.extend(["--build-arg", f"{key}={build_args[key]}"])
        cmd.append(str(context_root))
        print(f"🔨 Building Docker image from config: {cfg_path}")
        print(f"   image_ref={image_ref}")
        subprocess.check_call(cmd)

    return image_ref


def docker_check_call(args: Sequence[str]) -> None:
    """Run a docker subcommand (with optional sudo prefix); print and raise on errors.

    Args:
        args: Arguments passed to `docker`, e.g. ["image", "inspect", "name:tag"].
    """
    cmd = sudo_prefix() + ["docker", *args]
    try:
        subprocess.check_call(cmd)
    except FileNotFoundError:
        print("❌ docker command not found; ensure Docker is installed and in PATH")
        raise
    except subprocess.CalledProcessError as e:
        joined = " ".join(cmd)
        print(f"❌ docker command failed: {joined} (code={e.returncode})")
        raise


def docker_check_output(args: Sequence[str]) -> str:
    """Run a docker subcommand and return its text output.

    Args:
        args: Arguments passed to `docker`, e.g. ["ps", "-a", "--format", "{{.Names}}"].
    """
    cmd = sudo_prefix() + ["docker", *args]
    try:
        return subprocess.check_output(cmd, text=True)
    except FileNotFoundError:
        print("❌ docker command not found; ensure Docker is installed and in PATH")
        raise
    except subprocess.CalledProcessError as e:
        joined = " ".join(cmd)
        print(f"❌ docker command failed: {joined} (code={e.returncode})")
        raise


def build_docker_run_cmd(
    image: str,
    *,
    name: Optional[str] = None,
    remove: bool = False,
    detach: bool = False,
    network: Optional[str] = None,
    volumes: Optional[Iterable[str]] = None,
    env: Optional[Dict[str, str]] = None,
    ports: Optional[Iterable[str]] = None,
    args: Optional[Iterable[str]] = None,
) -> List[str]:
    """Build a `docker run` command (without executing it).

    Args:
        image: Image name (with tag), e.g. "fluxon_quick_start:0.2.1".
        name: Container name (`--name`).
        remove: Auto-remove on exit (`--rm`).
        detach: Run detached (`-d`).
        network: Docker network name (`--network`).
        volumes: Each element is the right-hand side of `-v`, e.g. "/host:/container".
        env: Environment variables, expanded into multiple `-e KEY=VALUE`.
        ports: Each element is the right-hand side of `-p`, e.g. "127.0.0.1:3000:3000".
        args: Command args after image, e.g. ["python3", "entrypoint.py", "/app"].
    """
    cmd: List[str] = sudo_prefix() + ["docker", "run"]
    if remove:
        cmd.append("--rm")
    if detach:
        cmd.append("-d")
    if name is not None:
        cmd.extend(["--name", name])
    if network is not None:
        cmd.extend(["--network", network])
    if volumes is not None:
        for v in volumes:
            cmd.extend(["-v", v])
    if env is not None:
        for key, value in env.items():
            cmd.extend(["-e", f"{key}={value}"])
    if ports is not None:
        for p in ports:
            cmd.extend(["-p", p])
    cmd.append(image)
    if args is not None:
        cmd.extend(list(args))
    return cmd
