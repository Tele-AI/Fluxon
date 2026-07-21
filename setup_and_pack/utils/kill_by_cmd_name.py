import psutil

def kill_processes_by_exact_cmdline(target_cmdline):
    """
    target_cmdline: 完整命令行字符串，比如 "python myscript.py --arg test"
    """
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if not cmdline:
                continue
            full_cmd = ' '.join(cmdline)
            # if full_cmd == target_cmdline:
            if target_cmdline in full_cmd:
                print(f"Killing PID {proc.pid}: {full_cmd}")
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

# 示例：匹配精确的命令行
kill_processes_by_exact_cmdline(
    "rust-analyzer"
    # "/home/example-user-a/.vscode-server/cli/servers/Stable-c306e94f98122556ca081f527b466015e1bc37b0/server/node"
    # "/home/example-user-b/.cursor-server/extensions/rust-lang.rust-analyzer-0.3.2474-linux-x64/server/rust-analyzer"
    )
