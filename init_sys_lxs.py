#!/usr/bin/env python3
"""Linux system initialization script - interactive installer."""

import os
import subprocess
import sys
import shutil


# ── 颜色定义 ──────────────────────────────────────────────────
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def red(text):
    return f"{RED}{text}{RESET}"


def green(text):
    return f"{GREEN}{text}{RESET}"


def yellow(text):
    return f"{YELLOW}{text}{RESET}"


def run_cmd(cmd, error_msg=None):
    """Run a shell command, return True on success."""
    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError:
        if error_msg:
            print(red(f"  {error_msg}"))
        return False


def ask_install(tool_name):
    """Ask user whether to install a tool. Returns True if yes."""
    while True:
        ans = input(yellow(f"是否安装 {tool_name}？[y/n]: ")).strip().lower()
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print(red("  请输入 y 或 n"))


# ── 安装动作定义 ──────────────────────────────────────────────
# 每个动作是一个 dict，包含:
#   name:        显示名称
#   check:       检查是否已安装的函数，返回 True 表示已安装
#   install:     执行安装的函数，返回 True 表示成功
#   description: (可选) 简短说明

ACTIONS = []


def register_action(name, check, install, description="", installed_path=None):
    """Register an installation action."""
    ACTIONS.append({
        "name": name,
        "check": check,
        "install": install,
        "description": description,
        "installed_path": installed_path,
    })


# ── chsrc ─────────────────────────────────────────────────────

CHSRC_URL = "https://share.leonlandry.top/chsrc-x64-linux-v025"
CHSRC_BIN = "/usr/local/bin/chsrc"


def _check_chsrc():
    return shutil.which("chsrc") is not None or os.path.isfile(CHSRC_BIN)


def _install_chsrc():
    if os.path.isfile(CHSRC_BIN):
        print(green(f"  chsrc 已存在于 {CHSRC_BIN}，无需重复安装。"))
        return True

    print(yellow("  正在下载 chsrc ..."))
    if not run_cmd(
        f'wget --no-check-certificate -O "{CHSRC_BIN}" "{CHSRC_URL}" -q --show-progress',
        "下载失败！",
    ):
        return False

    run_cmd(f'chmod +x "{CHSRC_BIN}"', "赋予执行权限失败")

    result = subprocess.run(
        ["chsrc", "--version"], capture_output=True, text=True
    )
    if result.returncode == 0:
        print(green(f"  chsrc 版本: {result.stdout.strip()}"))

    print(green(f"  chsrc 已安装到 {CHSRC_BIN}，全局可用。"))
    return True


register_action("chsrc", _check_chsrc, _install_chsrc, "换源工具", CHSRC_BIN)


def ask_path(prompt, default):
    """Ask user to confirm or input a path. Returns the chosen path."""
    ans = input(yellow(f"{prompt} [{default}]: ")).strip()
    return ans if ans else default


MIHOMO_GZ_URL = "https://share.leonlandry.top/mihomo-linux-amd64-v1.18.0.gz"
MIHOMO_MMDB_URL = "https://share.leonlandry.top/country-lite.mmdb"
MIHOMO_GEOSITE_URL = "https://share.leonlandry.top/GeoSite.dat"
MIHOMO_DEFAULT_DIR = os.path.expanduser("~/.config/mihomo")


def _check_mihomo():
    return os.path.isfile(os.path.join(MIHOMO_DEFAULT_DIR, "mihomo"))


def _install_mihomo():
    mihomo_dir = ask_path("安装路径", MIHOMO_DEFAULT_DIR)
    os.makedirs(mihomo_dir, exist_ok=True)

    gz_path = os.path.join(mihomo_dir, "mihomo.gz")
    print(yellow("  下载 mihomo ..."))
    if not run_cmd(
        f'wget --no-check-certificate -O "{gz_path}" "{MIHOMO_GZ_URL}" -q --show-progress',
        "mihomo 下载失败！",
    ):
        return False

    print(yellow("  解压 mihomo ..."))
    if not run_cmd(f'gunzip -f "{gz_path}"', "解压失败！"):
        return False

    mihomo_bin = os.path.join(mihomo_dir, "mihomo")
    if not os.path.isfile(mihomo_bin):
        print(red("  解压后未找到 mihomo 可执行文件"))
        return False
    run_cmd(f'chmod +x "{mihomo_bin}"', "赋予执行权限失败")

    print(yellow("  下载 Country.mmdb ..."))
    mmdb_dst = os.path.join(mihomo_dir, "Country.mmdb")
    if not run_cmd(
        f'wget --no-check-certificate -O "{mmdb_dst}" "{MIHOMO_MMDB_URL}" -q --show-progress',
        "Country.mmdb 下载失败！",
    ):
        return False

    print(yellow("  下载 GeoSite.dat ..."))
    geosite_dst = os.path.join(mihomo_dir, "GeoSite.dat")
    if not run_cmd(
        f'wget --no-check-certificate -O "{geosite_dst}" "{MIHOMO_GEOSITE_URL}" -q --show-progress',
        "GeoSite.dat 下载失败！",
    ):
        return False

    config_path = os.path.join(mihomo_dir, "config.yaml")
    if not os.path.isfile(config_path):
        open(config_path, "w").close()

    print(green(f"  mihomo 已安装到 {mihomo_dir}"))
    print(yellow(f"  请编辑配置文件: {config_path}"))
    print(yellow(f"  配置完成后执行: cd {mihomo_dir} && ./mihomo -d ./"))
    return True


register_action("mihomo", _check_mihomo, _install_mihomo, "代理工具", MIHOMO_DEFAULT_DIR)


# ── 后续添加更多安装动作，在此区域注册即可 ────────────────────
# register_action("工具名", check_func, install_func, "说明")


# ── 主流程 ─────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  Linux 系统初始化脚本")
    print("=" * 50)

    if os.geteuid() != 0:
        print(yellow("警告: 当前非 root 用户，部分操作可能需要 sudo 权限。\n"))

    to_install = []

    for action in ACTIONS:
        if action["check"]():
            path = action.get("installed_path", "")
            path_info = f" ({path})" if path else ""
            print(green(f"[已安装] {action['name']}{path_info} — {action['description']}"))
            continue

        desc = f" — {action['description']}" if action["description"] else ""
        if ask_install(f"{action['name']}{desc}"):
            to_install.append(action)

    if not to_install:
        print(green("\n无需安装任何组件。"))
        return

    print(yellow(f"\n将安装以下组件: {', '.join(a['name'] for a in to_install)}"))
    if not ask_install("确认开始？"):
        print(yellow("已取消。"))
        return

    print()
    results = []
    for action in to_install:
        print(yellow(f"[安装] {action['name']} ..."))
        ok = action["install"]()
        results.append((action["name"], ok))
        print()

    print("=" * 50)
    print("  安装结果汇总")
    print("=" * 50)
    for name, ok in results:
        status = green("成功") if ok else red("失败")
        print(f"  {name}: {status}")

    failed = [n for n, ok in results if not ok]
    if failed:
        print(red(f"\n以下组件安装失败: {', '.join(failed)}"))
        sys.exit(1)
    else:
        print(green("\n全部安装成功！"))


if __name__ == "__main__":
    main()
