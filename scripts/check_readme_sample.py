#!/usr/bin/env python3
"""门禁：README 里的 Quick Start 样例，外部照着做能不能编过。

为什么是 README 而不是别处：flare-core 的官网文档站在一个**私有仓**里、没有
部署路径，外部使用者实际拿得到的只有 crates.io 与 docs.rs。也就是说
**README.md 就是这个项目对外的门面文档**——crates.io 页面渲染的就是它。

2026-08-20 发现它两处不对，而所有测试和 CI 全绿：

  1. Quick Start 样例调 `server.run()`——这个方法在任何已发布版本上都不存在。
     真名是 `start()`，而且它启动后即返回、不阻塞，所以样例连「进程怎么活着」
     都是错的。照着做的人第一次 `cargo build` 就失败。
  2. 六处依赖示例写 `flare-core = "1.0.1"`，而 registry 上已经是 1.1.1。
     **这一条要说准**：cargo 的 `"1.0.1"` 是 `^1.0.1`，实际解析到的就是 1.1.1
     （已实测 Cargo.lock 确认），所以它不会让人装到旧版本。坏的是**文档本身**：
     README 是 crates.io 页面渲染的内容，上面白纸黑字写着一个早就不是当前版本
     的数字，读的人会据此判断项目活跃度、也会照抄进自己的清单。

这两条要**两种不同的判据**，别指望一个抓住另一个：

  - 第 1 条只有真编一遍才抓得到。`cargo test` 不碰 README，`cargo clippy`
    不碰 README，doctest 也不碰（README 的代码块没有被
    `#![doc = include_str!]` 引进来）。
  - 第 2 条编译**抓不到**——1.0.1 和 1.1 都解析到 1.1.1，两边都编得过。
    第一版门禁就是只做了编译判据，把版本号改回 1.0.1 照样绿。
    所以另加一条显式的「pin 是否还是当前发布版」检查。

编译判据必须**依赖走 registry**，不能用工作区里的同级 path 依赖：
那验的是我们本地，不是外部读者拿到的东西。

用法：
    python3 scripts/check_readme_sample.py
"""

import json
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 点名清单，不做「扫到什么算什么」。扫描式覆盖会悄悄缩水：改个围栏标记、
# 把样例拆成片段，门禁就从「验了 2 份」变成「验了 0 份」并且照样输出绿。
# 所以这里点名，且下面对「点名了却抽不出块」判红。
READMES = ["README.md", "README.zh-CN.md"]

# 抽法：紧挨在第一个 ```rust 之前的那个 ```toml 块，就是这份样例的依赖。
# 位置规则简单可陈述；有人重排了顺序就抽不出来 → 判红，而不是悄悄少验一份。
#
# 不用一条正则跨着两个块去配：README 里 ```rust 之前有好几个 ```toml
# （安装那节按 feature 集列了五六个），非贪婪也会从第一个 toml 一路吃到
# rust 那里，把中间的围栏一并卷进依赖块——第一版就是这么坏的。
# 所以先把所有围栏块按位置列出来，再取「rust 前面最近的那个 toml」。
FENCE = re.compile(r"```([a-zA-Z]*)\n(.*?)```", re.S)


def extract_pair(text):
    """返回 (deps, code)；抽不出返回 None。"""
    blocks = [(m.start(), m.group(1).lower(), m.group(2)) for m in FENCE.finditer(text)]
    first_rust = next((i for i, b in enumerate(blocks) if b[1] == "rust"), None)
    if first_rust is None:
        return None
    prior_toml = next(
        (blocks[i] for i in range(first_rust - 1, -1, -1) if blocks[i][1] == "toml"),
        None,
    )
    if prior_toml is None:
        return None
    return prior_toml[2], blocks[first_rust][2]


CRATE = "flare-core"
UA = "flare-core-readme-gate (https://github.com/flare-im/flare-transport)"


def latest_published(name: str):
    """registry 上的最新版本；网络不通返回 None（跳过而不是判红）。"""
    req = urllib.request.Request(
        f"https://crates.io/api/v1/crates/{name}", headers={"User-Agent": UA}
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)["crate"]["max_version"]


def pin_is_current(pin: str, latest: str) -> bool:
    """README 里写的 pin 还是不是当前发布版。

    判「前缀是否对得上」而不是判「能否解析到」：`^1.0.1` 能解析到 1.1.1，
    所以按可解析性判，一个两年前的数字也算合格——那就白检了。
    这里要的是**文档上那个数字本身**还准不准：`1.1` 对 1.1.1 算准，
    `1.0.1` 对 1.1.1 不算。
    """
    pin_parts = pin.lstrip("^~=").split(".")
    latest_parts = latest.split(".")
    return latest_parts[: len(pin_parts)] == pin_parts


def build(deps: str, code: str, target_dir: Path) -> subprocess.CompletedProcess:
    crate = Path(tempfile.mkdtemp(prefix="flare-readme-sample-"))
    try:
        (crate / "src").mkdir()
        (crate / "Cargo.toml").write_text(
            '[package]\nname = "readme-sample"\nversion = "0.0.0"\nedition = "2021"\n\n'
            f"{deps.strip()}\n",
            encoding="utf-8",
        )
        (crate / "src/main.rs").write_text(code, encoding="utf-8")
        return subprocess.run(
            ["cargo", "build", "--quiet"],
            cwd=crate,
            capture_output=True,
            text=True,
            # 每份样例都用新临时目录，target 若跟着走就每次全量重编。
            # 固定到一个共享目录，两份 README 之间以及 CI 缓存都能命中。
            env={**__import__("os").environ, "CARGO_TARGET_DIR": str(target_dir)},
        )
    finally:
        shutil.rmtree(crate, ignore_errors=True)


def main() -> int:
    target_dir = ROOT / "target" / "readme-sample"
    failed = 0
    checked = 0

    try:
        latest = latest_published(CRATE)
        print(f"  · registry 上 {CRATE} 最新版：{latest}")
    except (urllib.error.URLError, OSError, KeyError, ValueError) as e:
        latest = None
        print(f"  · 跳过版本新鲜度判据（查不到 registry：{e}）")

    for name in READMES:
        path = ROOT / name
        text = path.read_text(encoding="utf-8")
        pair = extract_pair(text)

        if not pair:
            print(f"✗ {name}：抽不出「紧挨 ```rust 之前的 ```toml」这一对块", file=sys.stderr)
            print(
                "  这份 README 在清单里，抽不出块就等于没验——按红处理，别静默放过。",
                file=sys.stderr,
            )
            failed += 1
            continue

        deps, code = pair

        if "path" in deps and re.search(r"\bpath\s*=", deps):
            print(f"✗ {name}：依赖块里有 path 依赖", file=sys.stderr)
            print("  外部读者没有我们的同级目录，README 里不该出现 path。", file=sys.stderr)
            failed += 1
            continue

        proc = build(deps, code, target_dir)
        out = f"{proc.stdout}{proc.stderr}".strip()

        if proc.returncode == 0:
            checked += 1
            # 两种写法都要认，只认字符串式会在依赖块改成表式那天悄悄失效：
            #   flare-x = "1.2"
            #   flare-x = { version = "1.2", features = [...] }
            # 这不是假设——本仓的安装块正是为了标注 feature 才改成表式的，
            # 第一版正则当场就把 pin 读成了空，版本判据静默变成空断言。
            dep_line = next(
                (l for l in deps.splitlines() if re.match(rf'\s*{re.escape(CRATE)}\s*=', l)),
                None,
            )
            pin = None
            if dep_line:
                m = re.search(r'"(\d[^"]*)"', dep_line)
                pin = m.group(1) if m else None

            if not pin:
                failed += 1
                print(f"✗ {name}：抽不出 {CRATE} 的版本号", file=sys.stderr)
                print(
                    "  依赖块的写法变了？抽不出就等于版本判据没跑——按红处理，别静默放过。",
                    file=sys.stderr,
                )
                continue

            print(f"  ✓ {name}：样例按 registry 依赖编得过（钉 {CRATE} {pin}）")

            # 编译判据到此为止——它对版本号是瞎的。下面这条才管数字准不准。
            if latest and not pin_is_current(pin, latest):
                failed += 1
                print(
                    f"✗ {name}：依赖示例写的是 {CRATE} {pin}，当前发布版是 {latest}",
                    file=sys.stderr,
                )
                print(
                    "  编译判据抓不到这个：cargo 的 ^ 语义会把旧 pin 解析到新版本，两边都编得过。\n"
                    "  但 README 是 crates.io 页面渲染的内容，上面那个数字是给人读、给人照抄的。",
                    file=sys.stderr,
                )
            continue

        # 网络不可用与「样例真的坏了」是两回事，别混为一谈。
        if re.search(r"failed to (get|download|fetch)|could not connect|network|dns error", out, re.I):
            print(f"  · 跳过 {name}（拉不到 registry：网络不可用）")
            continue

        # 缺构建前置（protoc 之类）同样不是「样例坏了」。这条照样判红——环境没配好
        # 就等于没验，不能放过——但**必须报对原因**：第一次接这道门禁时 CI 少装了
        # protoc，输出却是「样例编不过、去改 README」，把人指向完全错误的方向。
        if re.search(r"Could not find `protoc`|failed to run custom build command", out):
            missing = "protoc" if "protoc" in out else "构建前置"
            print(f"✗ {name}：环境缺 {missing}，本次判据没跑成", file=sys.stderr)
            print(
                f"  这不是样例的问题——是跑门禁的机器没装齐前置。装上 {missing} 再跑。\n"
                f"  （顺带：外部读者也需要它，README 的安装说明里该写清楚。）",
                file=sys.stderr,
            )
            failed += 1
            continue

        print(f"✗ {name}：样例编不过", file=sys.stderr)
        print("\n".join(f"  {line}" for line in out.splitlines()[-25:]), file=sys.stderr)
        failed += 1

    if failed:
        print("", file=sys.stderr)
        print(
            "README 是 crates.io 页面渲染的内容，也是外部使用者唯一拿得到的文档。\n"
            "它与已发布的包对不上，照着做的人第一步就卡住。\n"
            "改 README，或者先把修好的版本发出去——别只改代码不改文档。",
            file=sys.stderr,
        )
        return 1

    if checked == 0:
        print("SKIP: 一份样例都没编成（多半是网络不可用），本次不判定")
        return 0

    print(f"  ✓ {checked} 份 README 样例按 registry 依赖编得过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
