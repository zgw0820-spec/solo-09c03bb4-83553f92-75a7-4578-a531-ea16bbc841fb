"""集群配置。

机架拓扑可通过环境变量覆盖：

- ``CLUSTER_RACKS``: 逗号分隔的各机架节点数，例如 ``"4,4"`` 表示两个机架、
  每个机架 4 个节点。也支持 ``"3x4"`` 形式表示 4 个机架、每个 3 节点。
- 未设置时使用默认拓扑 2 个机架 x 4 节点。
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _parse_racks(spec: str) -> list[int]:
    racks: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "x" in part.lower():
            size, _, count = part.lower().partition("x")
            n = int(size)
            k = int(count)
            if n <= 0 or k <= 0:
                raise ValueError(f"非法机架定义: {part!r}")
            racks.extend([n] * k)
        else:
            n = int(part)
            if n <= 0:
                raise ValueError(f"非法机架节点数: {part!r}")
            racks.append(n)
    if not racks:
        raise ValueError("CLUSTER_RACKS 至少需要一个机架")
    return racks


@dataclass(frozen=True)
class Settings:
    rack_capacities: tuple[int, ...]

    @property
    def rack_names(self) -> list[str]:
        return [f"r{i + 1}" for i in range(len(self.rack_capacities))]

    @property
    def total_nodes(self) -> int:
        return sum(self.rack_capacities)


def load_settings() -> Settings:
    raw = os.environ.get("CLUSTER_RACKS", "4x2").strip()
    try:
        capacities = tuple(_parse_racks(raw))
    except ValueError as exc:
        raise SystemExit(f"环境变量 CLUSTER_RACKS 配置错误: {exc}") from exc
    return Settings(rack_capacities=capacities)


settings = load_settings()
